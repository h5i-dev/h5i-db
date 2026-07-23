#!/usr/bin/env python3
"""Build the h5i-db documentation site into docs/.

Sources:
  docs-src/manual/*.md      hand-written manual pages (front matter: title,
                            description, order)
  docs-src/api/*.md         Python API reference pages
  ../h5i-db-cookbook/notebooks/*/*.ipynb
                            executed notebooks, rendered as tutorials
  docs-src/templates/       page.html shell, docs.css, docs.js

Output (committed, served by GitHub Pages):
  docs/manual/*.html  docs/api/*.html  docs/cookbook/<section>/*.html
  docs/_static/docs.css  docs/_static/docs.js  docs/_static/search-index.json

Usage:
  python tools/build_docs.py [--cookbook PATH] [--skip-cookbook]

No external services; the only dependencies are `markdown` and `pygments`.
"""

from __future__ import annotations

import argparse
import base64
import html
import json
import re
import shutil
import sys
from dataclasses import dataclass, field
from pathlib import Path

import markdown as md_lib
from pygments import highlight
from pygments.formatters import HtmlFormatter
from pygments.lexers import get_lexer_by_name

REPO = Path(__file__).resolve().parent.parent
SRC = REPO / "docs-src"
OUT = REPO / "docs"
DEFAULT_COOKBOOK = REPO.parent / "h5i-db-cookbook"

MD_EXTENSIONS = ["extra", "admonition", "toc", "sane_lists"]
MD_CONFIG = {
    "toc": {"permalink": "#", "permalink_class": "headerlink", "toc_depth": "2-3"},
    "codehilite": {"guess_lang": False, "css_class": "highlight"},
}

COOKBOOK_SECTIONS = [
    ("00_fundamentals", "Fundamentals"),
    ("01_market_data_engineering", "Market data engineering"),
    ("02_alpha_research", "Alpha research"),
    ("03_risk_and_production", "Risk & production"),
]

ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
STYLE_RE = re.compile(r"<style[^>]*>.*?</style>", re.S)
SCRIPT_RE = re.compile(r"<script[^>]*>.*?</script>", re.S)
TAG_RE = re.compile(r"<[^>]+>")


# ── Page model ───────────────────────────────────────────────────


@dataclass
class Page:
    url: str            # site-relative, e.g. "manual/quickstart.html"
    section: str        # top nav section key: manual | api | cookbook
    title: str
    description: str
    body_html: str = ""
    toc_tokens: list = field(default_factory=list)
    search_text: str = ""
    group: str = ""     # sidebar group label (cookbook sections)
    order: float = 0.0


# ── Markdown rendering ───────────────────────────────────────────


def make_md() -> md_lib.Markdown:
    return md_lib.Markdown(extensions=MD_EXTENSIONS + ["codehilite"], extension_configs=MD_CONFIG)


def wrap_tables(html_text: str) -> str:
    """Wrap bare markdown tables in a horizontally scrollable card."""
    return re.sub(
        r"<table>(.*?)</table>",
        r'<div class="table-wrap"><table>\1</table></div>',
        html_text,
        flags=re.S,
    )


def render_markdown(text: str) -> tuple[str, list]:
    md = make_md()
    body = md.convert(text)
    return wrap_tables(body), getattr(md, "toc_tokens", [])


# ── API/reference enhancement ────────────────────────────────────
# Turns flat rendered markdown for reference pages into member cards:
# each `### name` heading + its signature code block + typed parameter
# list becomes one bordered `.api-member`.

API_LABELS = (
    "Parameters", "Keyword Arguments", "Returns", "Return type", "Raises",
    "Yields", "Example", "Examples", "Note", "Notes", "Warning", "See also",
)
_LABEL_RE = re.compile(
    r"<p><strong>(" + "|".join(API_LABELS) + r")</strong></p>"
)
_HEADING_SPLIT_RE = re.compile(r"(?=<h[23] id=)")
_H3_RE = re.compile(r'\A(<h3 id="[^"]*">)(.*?)(</h3>)', re.S)
_FIRST_HIGHLIGHT_RE = re.compile(
    r'\A(\s*)(<div class="highlight[^"]*">.*?</div>)', re.S
)
_FIRST_CODE_RE = re.compile(r"<code>(.*?)</code>", re.S)


def _style_heading(inner: str, style: str) -> str:
    """Dim the qualifier of a member name (``Database.`` / ``h5i-db ``)."""
    if style == "plain":
        return inner
    m = _FIRST_CODE_RE.search(inner)
    if not m:
        return inner
    text = m.group(1)
    if style == "dotted" and "." in text:
        qual, name = text.rsplit(".", 1)
        # keep the dot with the method so ".apply" reads as one unit
        new = f'<span class="api-qual">{qual}</span><span class="api-name">.{name}</span>'
    elif style == "cli" and text.startswith("h5i-db "):
        binary, rest = text.split(" ", 1)
        new = f'<span class="api-qual">{binary} </span><span class="api-name">{rest}</span>'
    else:
        new = f'<span class="api-name">{text}</span>'
    return inner[: m.start()] + f"<code>{new}</code>" + inner[m.end():]


def enhance_api_html(body: str, style: str) -> str:
    body = _LABEL_RE.sub(r'<p class="api-label">\1</p>', body)
    out = []
    for chunk in _HEADING_SPLIT_RE.split(body):
        if not chunk.startswith("<h3 id="):
            out.append(chunk)
            continue
        m = _H3_RE.match(chunk)
        if not m:
            out.append(chunk)
            continue
        heading = m.group(1) + _style_heading(m.group(2), style) + m.group(3)
        rest = chunk[m.end():]
        sig = ""
        sm = _FIRST_HIGHLIGHT_RE.match(rest)
        if sm:
            sig = sm.group(2).replace(
                '<div class="highlight"', '<div class="highlight api-sig"', 1
            )
            rest = rest[sm.end():]
        out.append(
            f'<section class="api-member">{heading}{sig}'
            f'<div class="api-body">{rest}</div></section>'
        )
    return "".join(out)


def enhance_style(section: str, stem: str) -> str | None:
    if section == "api":
        return "dotted"
    if section == "manual" and stem == "cli":
        return "cli"
    if section == "manual" and stem == "sql":
        return "plain"
    return None


FRONT_RE = re.compile(r"\A---\s*\n(.*?)\n---\s*\n", re.S)


def parse_front_matter(text: str) -> tuple[dict, str]:
    m = FRONT_RE.match(text)
    if not m:
        return {}, text
    meta = {}
    for line in m.group(1).splitlines():
        if ":" in line:
            k, v = line.split(":", 1)
            meta[k.strip()] = v.strip().strip('"')
    return meta, text[m.end():]


def plain_text(html_text: str, limit: int = 4000) -> str:
    text = STYLE_RE.sub(" ", html_text)
    text = SCRIPT_RE.sub(" ", text)
    text = TAG_RE.sub(" ", text)
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip().lower()[:limit]


# ── Notebook rendering ───────────────────────────────────────────


def highlight_code(code: str, lang: str = "python") -> str:
    try:
        lexer = get_lexer_by_name(lang)
    except Exception:
        lexer = get_lexer_by_name("text")
    return highlight(code, lexer, HtmlFormatter(cssclass="highlight"))


def render_output(out: dict) -> str:
    """Render one notebook output object to HTML (or '' to skip)."""
    typ = out.get("output_type")
    if typ == "stream":
        text = ANSI_RE.sub("", "".join(out.get("text", [])))
        if not text.strip():
            return ""
        cls = "stderr" if out.get("name") == "stderr" else ""
        return f'<pre class="{cls}">{html.escape(text.rstrip())}</pre>'
    if typ == "error":
        tb = ANSI_RE.sub("", "\n".join(out.get("traceback", [])))
        return f'<pre class="nb-error">{html.escape(tb.rstrip())}</pre>'
    if typ in ("execute_result", "display_data"):
        data = out.get("data", {})
        if "text/html" in data:
            body = "".join(data["text/html"])
            body = STYLE_RE.sub("", body)
            body = SCRIPT_RE.sub("", body)
            if not body.strip():
                return ""
            return f'<div class="nb-html">{body}</div>'
        if "image/png" in data:
            b64 = data["image/png"]
            if isinstance(b64, list):
                b64 = "".join(b64)
            b64 = b64.replace("\n", "")
            return f'<img src="data:image/png;base64,{b64}" alt="output figure" loading="lazy">'
        if "image/svg+xml" in data:
            svg = "".join(data["image/svg+xml"])
            return f'<div class="nb-html">{svg}</div>'
        if "text/plain" in data:
            text = ANSI_RE.sub("", "".join(data["text/plain"]))
            if not text.strip():
                return ""
            return f"<pre>{html.escape(text.rstrip())}</pre>"
    return ""


def render_notebook(path: Path) -> tuple[str, str, str, str, list]:
    """Return (title, description, body_html, search_text, toc_tokens)."""
    nb = json.loads(path.read_text())
    parts: list[str] = []
    toc_tokens: list = []
    search_parts: list[str] = []
    title = path.stem.replace("_", " ")
    description = ""
    slugger = make_md()  # reuse toc slug logic across markdown cells via ids

    seen_ids: set[str] = set()

    def unique_id(text: str) -> str:
        base = re.sub(r"[^\w\- ]", "", text.lower()).strip().replace(" ", "-")
        base = re.sub(r"-+", "-", base) or "section"
        candidate, n = base, 1
        while candidate in seen_ids:
            n += 1
            candidate = f"{base}-{n}"
        seen_ids.add(candidate)
        return candidate

    first_md = True
    for cell in nb.get("cells", []):
        ctype = cell.get("cell_type")
        source = "".join(cell.get("source", []))
        if ctype == "markdown":
            if first_md:
                # First markdown cell: mine the H1 for the page title and the
                # first paragraph for the description; drop the H1 from the
                # body (the shell renders its own <h1>).
                m = re.match(r"\s*#\s+(.+?)\s*\n", source)
                if m:
                    title = m.group(1).strip().replace("`", "")
                    source = source[m.end():]
                para = next(
                    (p.strip() for p in source.split("\n\n") if p.strip() and not p.strip().startswith("#")),
                    "",
                )
                description = re.sub(r"\s+", " ", TAG_RE.sub("", para))
                description = re.sub(r"[*_`]|\[|\]\([^)]*\)", "", description)
                if len(description) > 220:  # cut at a word boundary
                    description = description[:220].rsplit(" ", 1)[0].rstrip(",;:") + " …"
                first_md = False
            body, cell_toc = render_markdown(source)
            toc_tokens.extend(cell_toc)
            parts.append(body)
            search_parts.append(plain_text(body, 2000))
        elif ctype == "code":
            if not source.strip():
                continue
            code_html = highlight_code(source, "python")
            outputs = [render_output(o) for o in cell.get("outputs", [])]
            outputs = [o for o in outputs if o]
            if outputs:
                out_html = (
                    '<div class="nb-output"><div class="nb-output-label">output</div>'
                    + "".join(outputs)
                    + "</div>"
                )
                parts.append(f'<div class="nb-cell has-output">{code_html}{out_html}</div>')
            else:
                parts.append(f'<div class="nb-cell">{code_html}</div>')
    _ = unique_id  # slug helper reserved for future heading rewriting
    _ = slugger
    body_html = "\n".join(parts)
    search_text = " ".join(search_parts)[:4000]
    return title, description, body_html, search_text, toc_tokens


# ── Shell rendering ──────────────────────────────────────────────


def toc_html(tokens: list) -> str:
    if not tokens:
        return ""

    def items(toks):
        out = []
        for t in toks:
            kids = items(t.get("children", []))
            # toc_tokens names arrive already HTML-escaped by python-markdown
            out.append(
                f'<li><a href="#{t["id"]}">{t["name"]}</a>'
                + (f"<ul>{kids}</ul>" if kids else "")
                + "</li>"
            )
        return "".join(out)

    inner = items(tokens)
    if not inner:
        return ""
    return f'<div class="toc-label">On this page</div><ul>{inner}</ul>'


def sidebar_html(groups: list[tuple[str, list[Page]]], current: Page, root: str) -> str:
    out = []
    for label, pages in groups:
        lis = []
        for p in pages:
            cls = ' class="active"' if p.url == current.url else ""
            lis.append(f'<li><a href="{root}{p.url}"{cls}>{html.escape(p.title)}</a></li>')
        out.append(
            f'<div class="sidebar-group"><div class="group-label">{html.escape(label)}</div>'
            f'<ul>{"".join(lis)}</ul></div>'
        )
    return "\n".join(out)


def render_page(page: Page, template: str, sidebar: str, breadcrumb: str,
                prev_page: Page | None, next_page: Page | None) -> str:
    depth = page.url.count("/")
    root = "../" * depth if depth else "./"
    prevnext = ""
    if prev_page:
        prevnext += (
            f'<a class="prev" href="{root}{prev_page.url}"><span class="dir">← Previous</span>'
            f'<span class="pn-title">{html.escape(prev_page.title)}</span></a>'
        )
    if next_page:
        prevnext += (
            f'<a class="next" href="{root}{next_page.url}"><span class="dir">Next →</span>'
            f'<span class="pn-title">{html.escape(next_page.title)}</span></a>'
        )
    out = template
    for key, val in {
        "{{title}}": html.escape(page.title),
        "{{description}}": html.escape(page.description),
        "{{root}}": root,
        "{{active_manual}}": 'class="active"' if page.section == "manual" else "",
        "{{active_api}}": 'class="active"' if page.section == "api" else "",
        "{{active_cookbook}}": 'class="active"' if page.section == "cookbook" else "",
        "{{sidebar}}": sidebar,
        "{{breadcrumb}}": breadcrumb,
        "{{content}}": page.body_html,
        "{{prevnext}}": prevnext,
        "{{toc}}": toc_html(page.toc_tokens),
    }.items():
        out = out.replace(key, val)
    return out


# ── Site assembly ────────────────────────────────────────────────


def load_md_pages(directory: Path, section: str) -> list[Page]:
    pages = []
    for path in sorted(directory.glob("*.md")):
        meta, body_src = parse_front_matter(path.read_text())
        body, toc_tokens = render_markdown(body_src)
        style = enhance_style(section, path.stem)
        if style:
            body = enhance_api_html(body, style)
        slug = "index.html" if path.stem == "index" else f"{path.stem}.html"
        title = meta.get("title", path.stem.replace("-", " ").title())
        page = Page(
            url=f"{section}/{slug}",
            section=section,
            title=title,
            description=meta.get("description", ""),
            body_html=body,
            toc_tokens=toc_tokens,
            search_text=plain_text(body),
            order=float(meta.get("order", 99)),
        )
        pages.append(page)
    pages.sort(key=lambda p: p.order)
    return pages


def heading_index(page: Page) -> list[dict]:
    out = []

    def walk(tokens):
        for t in tokens:
            out.append({"id": t["id"], "text": html.unescape(t["name"])})
            walk(t.get("children", []))

    walk(page.toc_tokens)
    return out


#: extra markdown sources pulled into the manual from elsewhere in the repo,
#: so they stay single-source: (path, slug, meta)
EXTRA_MANUAL = [
    (
        OUT / "OPERATIONS.md",
        "operations",
        {
            "title": "Operations guide",
            "description": "Running h5i-db in production: backup and restore, vacuum and "
                           "compaction cadence, plan hygiene, disk-usage math, filesystem "
                           "caveats, and the torn-HEAD recovery runbook.",
            "order": "7",
        },
    ),
]


def build(cookbook_dir: Path, skip_cookbook: bool) -> None:
    template = (SRC / "templates" / "page.html").read_text()

    manual_pages = load_md_pages(SRC / "manual", "manual")
    for path, slug, meta in EXTRA_MANUAL:
        _, body_src = parse_front_matter(path.read_text())
        body, toc_tokens = render_markdown(body_src)
        manual_pages.append(Page(
            url=f"manual/{slug}.html",
            section="manual",
            title=meta["title"],
            description=meta["description"],
            body_html=body,
            toc_tokens=toc_tokens,
            search_text=plain_text(body),
            order=float(meta["order"]),
        ))
    manual_pages.sort(key=lambda p: p.order)
    api_pages = load_md_pages(SRC / "api", "api")

    # ── Cookbook ────────────────────────────────────────────────
    cookbook_pages: list[Page] = []
    cookbook_groups: dict[str, list[Page]] = {}
    if not skip_cookbook:
        nb_root = cookbook_dir / "notebooks"
        if not nb_root.is_dir():
            sys.exit(f"error: cookbook notebooks not found at {nb_root} "
                     f"(pass --cookbook PATH or --skip-cookbook)")
        for sec_dir, sec_label in COOKBOOK_SECTIONS:
            group: list[Page] = []
            for nb_path in sorted((nb_root / sec_dir).glob("*.ipynb")):
                title, desc, body, search_text, toc_tokens = render_notebook(nb_path)
                page = Page(
                    url=f"cookbook/{sec_dir}/{nb_path.stem}.html",
                    section="cookbook",
                    title=title,
                    description=desc,
                    body_html=f"<h1>{html.escape(title)}</h1>\n" + body,
                    toc_tokens=toc_tokens,
                    search_text=search_text,
                    group=sec_label,
                )
                group.append(page)
                cookbook_pages.append(page)
            cookbook_groups[sec_label] = group

        # cookbook index page: card grid per section
        idx_parts = [
            "<h1>Cookbook</h1>",
            '<p class="doc-lede">Executed, end-to-end notebooks: from your first database to '
            "point-in-time factor research and production risk workflows. Every recipe runs "
            "top to bottom against real or deterministic synthetic market data.</p>",
            '<div class="doc-divider"></div>',
        ]
        for sec_dir, sec_label in COOKBOOK_SECTIONS:
            idx_parts.append(f'<h2 id="{sec_dir}">{html.escape(sec_label)}'
                             f'<a class="headerlink" href="#{sec_dir}">#</a></h2>')
            cards = []
            for i, p in enumerate(cookbook_groups[sec_label], 1):
                cards.append(
                    f'<a class="card" href="{p.url.split("cookbook/", 1)[1]}">'
                    f'<span class="card-no">{i:02d}</span>'
                    f'<span class="card-title">{html.escape(p.title)}</span>'
                    f'<span class="card-desc">{html.escape(p.description)}</span></a>'
                )
            idx_parts.append(f'<div class="card-grid">{"".join(cards)}</div>')
        cookbook_index = Page(
            url="cookbook/index.html",
            section="cookbook",
            title="Cookbook",
            description="Executed notebook tutorials for h5i-db: fundamentals, market data "
                        "engineering, alpha research, and risk & production workflows.",
            body_html="\n".join(idx_parts),
            toc_tokens=[{"id": s, "name": lbl, "children": []} for s, lbl in COOKBOOK_SECTIONS],
            search_text=" ".join(
                (p.title + " " + p.description).lower() for p in cookbook_pages
            )[:4000],
        )

    # ── Sidebar groups ──────────────────────────────────────────
    def groups_for(page: Page) -> list[tuple[str, list[Page]]]:
        groups: list[tuple[str, list[Page]]] = [
            ("Manual", manual_pages),
            ("Python API", api_pages),
        ]
        if skip_cookbook:
            return groups
        if page.section == "cookbook":
            # inside the cookbook: index link + the recipes of each section,
            # expanded only for the section the page belongs to
            groups.append(("Cookbook", [cookbook_index]))
            for _sec_dir, sec_label in COOKBOOK_SECTIONS:
                sec_pages = cookbook_groups[sec_label]
                if page.group == sec_label or page.url == "cookbook/index.html":
                    if page.group == sec_label:
                        groups.append((sec_label, sec_pages))
        else:
            groups.append(("Cookbook", [cookbook_index]))
        return groups

    # ── Orderings for prev/next ─────────────────────────────────
    ordered: list[Page] = manual_pages + api_pages
    if not skip_cookbook:
        ordered += [cookbook_index] + cookbook_pages

    # ── Write pages ─────────────────────────────────────────────
    section_labels = {"manual": "Manual", "api": "Python API", "cookbook": "Cookbook"}
    for i, page in enumerate(ordered):
        depth = page.url.count("/")
        root = "../" * depth if depth else "./"
        crumbs = [f'<a href="{root}">h5i-db</a>', '<span class="sep">/</span>',
                  f'<a href="{root}{page.section}/">{section_labels[page.section]}</a>']
        if page.group:
            crumbs += ['<span class="sep">/</span>', f"<span>{html.escape(page.group)}</span>"]
        crumbs += ['<span class="sep">/</span>', f"<span>{html.escape(page.title)}</span>"]
        prev_page = ordered[i - 1] if i > 0 else None
        next_page = ordered[i + 1] if i + 1 < len(ordered) else None
        html_out = render_page(
            page, template,
            sidebar_html(groups_for(page), page, root),
            "\n".join(crumbs),
            prev_page, next_page,
        )
        dest = OUT / page.url
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(html_out)

    # ── Static assets ───────────────────────────────────────────
    static = OUT / "_static"
    static.mkdir(exist_ok=True)
    shutil.copy(SRC / "templates" / "docs.css", static / "docs.css")
    shutil.copy(SRC / "templates" / "docs.js", static / "docs.js")

    # ── Search index ────────────────────────────────────────────
    search_index = [
        {
            "url": p.url,
            "section": (p.group or section_labels[p.section]),
            "title": p.title,
            "headings": heading_index(p),
            "body": p.search_text,
        }
        for p in ordered
    ]
    (static / "search-index.json").write_text(json.dumps(search_index, separators=(",", ":")))

    n_nb = len(cookbook_pages)
    print(f"built {len(ordered)} pages "
          f"({len(manual_pages)} manual, {len(api_pages)} api, {n_nb} cookbook) -> {OUT}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--cookbook", type=Path, default=DEFAULT_COOKBOOK,
                    help=f"path to the h5i-db-cookbook checkout (default: {DEFAULT_COOKBOOK})")
    ap.add_argument("--skip-cookbook", action="store_true",
                    help="build only manual and API pages")
    args = ap.parse_args()
    build(args.cookbook, args.skip_cookbook)


if __name__ == "__main__":
    main()
