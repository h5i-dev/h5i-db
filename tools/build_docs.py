#!/usr/bin/env python3
"""Build the h5i-db documentation site into docs/.

Sources:
  docs-src/manual/*.md      hand-written manual pages (front matter: title,
                            description, order)
  docs-src/api/*.md         Python API reference pages
  ../h5i-db-cookbook/notebooks/*/*.ipynb
                            executed notebooks, rendered as tutorials
  docs-src/templates/       page.html shell, docs.css, docs.js

Output (committed, served by GitHub Pages). Pages live at directory URLs
(/manual/sql/), with a redirect stub left at each page's former *.html path:
  docs/manual/<page>/index.html  docs/api/<page>/index.html
  docs/cookbook/<section>/<page>/index.html
  docs/_static/docs.css  docs/_static/docs.js  docs/_static/search-index.json

Usage:
  python tools/build_docs.py [--cookbook PATH] [--skip-cookbook]

No external services; the only dependencies are `markdown` and `pygments`.
"""

from __future__ import annotations

import argparse
import base64
import datetime
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
BASE_URL = "https://db.h5i.dev/"

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
    ("04_event_driven_backtesting", "Event-driven backtesting"),
    ("05_prediction_markets", "Prediction markets"),
    ("06_performance_analytics", "Performance analytics"),
]

ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
STYLE_RE = re.compile(r"<style[^>]*>.*?</style>", re.S)
SCRIPT_RE = re.compile(r"<script[^>]*>.*?</script>", re.S)
TAG_RE = re.compile(r"<[^>]+>")

# Search engines cut the <title> around 60 characters and the description
# around 160. Both budgets are for the *rendered* string, so the branding
# suffix has to be counted against the title, not added after it.
TITLE_BUDGET = 60
DESC_BUDGET = 155
TITLE_SUFFIXES = (" · h5i-db docs", " · h5i-db", "")


# In-content cross-links are authored relative to the source tree
# ("sql.html", "../manual/concepts.html#forks"). Pages are served from
# directory URLs, so rewrite them to root-absolute paths, which cannot go
# stale when a page's depth changes.
_DOC_LINK_RE = re.compile(r'href="(?!https?:|/|#)([^"]+?)\.html(#[^"]*)?"')


def absolutize_links(body: str, section: str) -> str:
    def sub(m: "re.Match") -> str:
        target, frag = m.group(1), m.group(2) or ""
        parts = [seg for seg in target.split("/") if seg not in ("", ".")]
        while parts and parts[0] == "..":
            parts.pop(0)
            # a leading ".." meant "leave my section"; the rest names its own
        if parts and parts[0] in ("manual", "api", "cookbook"):
            path = "/".join(parts)
        else:
            path = f"{section}/" + "/".join(parts)
        if path.endswith("/index"):
            path = path[: -len("index")]
        else:
            path += "/"
        return f'href="/{path}{frag}"'
    return _DOC_LINK_RE.sub(sub, body)


def write_redirect(old_url: str, new_url: str) -> None:
    """Leave a redirect where a page used to live.

    The site was published with `.html` URLs; moving to directory URLs would
    otherwise 404 every link already in the wild. A zero-delay meta refresh
    is the only redirect a static host can serve, and the canonical plus
    noindex keep the stub itself out of the index.
    """
    dest = OUT / old_url
    dest.parent.mkdir(parents=True, exist_ok=True)
    target = f"/{new_url}"
    dest.write_text(
        "<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n<meta charset=\"utf-8\">\n"
        f'<meta http-equiv="refresh" content="0; url={target}">\n'
        '<meta name="robots" content="noindex, follow">\n'
        f'<link rel="canonical" href="{BASE_URL}{new_url}">\n'
        "<title>Moved</title>\n</head>\n<body>\n"
        f'<p>This page moved to <a href="{target}">{target}</a>.</p>\n'
        f'<script>location.replace("{target}" + location.hash);</script>\n'
        "</body>\n</html>\n"
    )


# ── Page model ───────────────────────────────────────────────────


@dataclass
class Page:
    url: str            # site-relative directory URL, e.g. "manual/quickstart/"
    section: str        # top nav section key: manual | api | cookbook
    title: str
    description: str
    body_html: str = ""
    toc_tokens: list = field(default_factory=list)
    search_text: str = ""
    group: str = ""     # sidebar group label (cookbook sections)
    order: float = 0.0
    source: "Path | None" = None   # source markdown file, for llms-full.txt
    # Title for <title>/og/twitter when the nav title is too terse to stand
    # alone in a search result ("Overview" says nothing, and two sections
    # both have one). Frontmatter `seo_title:`; defaults to `title`.
    seo_title: str = ""


def head_title(page: "Page") -> str:
    """Search-result title: the page's own words first, branding only if it
    still fits the ~60-character budget."""
    base = page.seo_title or page.title
    for suffix in TITLE_SUFFIXES:
        if len(base) + len(suffix) <= TITLE_BUDGET:
            return base + suffix
    return base


def trim_description(text: str) -> str:
    """Fit a description to the snippet budget, preferring a clean sentence
    end over a mid-thought ellipsis."""
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) <= DESC_BUDGET:
        return text
    # Longest run of whole sentences that fits.
    out = ""
    for part in re.findall(r"[^.!?]*[.!?]+(?:\s|$)", text):
        if len(out) + len(part) > DESC_BUDGET:
            break
        out += part
    out = out.strip()
    if len(out) >= 80:          # enough to be a real snippet
        return out
    return text[: DESC_BUDGET - 1].rsplit(" ", 1)[0].rstrip(",;:·-") + "…"


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
        # class and method share one colour; the dot is the only accent
        new = (f'<span class="api-name">{qual}</span>'
               f'<span class="api-dot">.</span>'
               f'<span class="api-name">{name}</span>')
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


# ── llms.txt / llms-full.txt (answer-engine optimization) ────────
# https://llmstxt.org — a root-level markdown index that lets LLMs and
# answer engines discover and ingest the docs efficiently.

LLMS_SUMMARY = (
    "h5i-db is a high-performance, embedded, versioned analytical database for "
    "quantitative finance and time-series workloads. It runs full DataFusion SQL "
    "with native ASOF joins, OHLCV/VWAP rollups, time travel, and previewable "
    "mutations over immutable, time-sorted Parquet segments — driven from a CLI, "
    "Rust, or Python, and designed to be safe for AI agents. Written in Rust; "
    "Apache-2.0."
)
LLMS_INTRO = (
    "A database is a single directory on disk; there is no server. Every write is "
    "an atomic commit that produces a new immutable version, and any past version "
    "is readable in O(1). Storage is time-sorted and pruned by manifest statistics "
    "before I/O. Destructive changes (delete/replace ranges) can be staged as "
    "previewable plans and gated by a mutation policy. The CLI emits machine-"
    "readable output and structured errors with stable exit codes."
)


def _llms_link(page: Page) -> str:
    line = f"- [{page.title}]({BASE_URL}{page.url})"
    if page.description:
        line += f": {page.description}"
    return line


def write_llms_index(manual, api, cookbook_groups, cookbook_index, skip_cookbook):
    """Write docs/llms.txt — the structured, link-first index."""
    out = ["# h5i-db", "", f"> {LLMS_SUMMARY}", "", LLMS_INTRO, ""]

    out.append("## Manual")
    out += [_llms_link(p) for p in manual]
    out.append("")

    out.append("## Python API")
    out += [_llms_link(p) for p in api]
    out.append("")

    if not skip_cookbook:
        out.append("## Cookbook")
        out.append(_llms_link(cookbook_index))
        out.append("")
        for _sec_dir, label in COOKBOOK_SECTIONS:
            out.append(f"## Cookbook: {label}")
            out += [_llms_link(p) for p in cookbook_groups[label]]
            out.append("")

    out.append("## Optional")
    out += [
        f"- [Full documentation in one file]({BASE_URL}llms-full.txt): the entire "
        "manual and Python API reference as plain markdown.",
        "- [GitHub repository](https://github.com/h5i-dev/h5i-db): source, issues, and README.",
        "- [Design document](https://github.com/h5i-dev/h5i-db/blob/main/DESIGN.md): "
        "storage engine and query-layer internals.",
        "- [Benchmark methodology](https://github.com/h5i-dev/h5i-db/blob/main/benchmarks/RESULTS.md): "
        "full benchmark setup and results.",
    ]
    out.append("")

    out.append("## Related projects")
    out += [
        "- [h5i](https://h5i.dev/): the sibling project from the same authors. An auditable "
        "workspace layer for AI coding agents: isolated Git worktrees, sandbox policy, prompt "
        "and model provenance, and a neutral verifier, all stored under `refs/h5i/*`.",
        "- [h5i docs index](https://h5i.dev/llms.txt): the structured, link-first index for the "
        "h5i manual, guides, and engineering blog.",
    ]
    out.append("")
    (OUT / "llms.txt").write_text("\n".join(out))


# ── sitemap.xml / robots.txt (search-engine discovery) ───────────
# db.h5i.dev is a subdomain with no inbound links from h5i.dev's visible
# navigation, so crawlers reach it through these two files (plus the
# `Sitemap:` line cross-submitted from https://h5i.dev/robots.txt).

# Landing page first, then section indexes, then leaf pages.
SITEMAP_PRIORITY = {"": "1.0", "manual/": "0.9",
                    "api/": "0.9", "cookbook/": "0.8",
                    "demo/": "0.4", "demo/ui/": "0.4"}

# Hand-maintained pages that build() does not generate but index.html links to.
SITEMAP_EXTRA = ["", "demo/", "demo/ui/"]


def write_sitemap(pages: "list[Page]") -> None:
    """Write docs/sitemap.xml listing the landing page and every built page."""
    today = datetime.date.today().isoformat()
    out = ['<?xml version="1.0" encoding="UTF-8"?>',
           '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">']
    for url in SITEMAP_EXTRA + [p.url for p in pages]:
        priority = SITEMAP_PRIORITY.get(url, "0.7")
        out += ["  <url>",
                f"    <loc>{BASE_URL}{url}</loc>",
                f"    <lastmod>{today}</lastmod>",
                "    <changefreq>weekly</changefreq>",
                f"    <priority>{priority}</priority>",
                "  </url>"]
    out += ["</urlset>", ""]
    (OUT / "sitemap.xml").write_text("\n".join(out))


# Answer-engine crawlers are opted in explicitly, matching h5i.dev/robots.txt.
AI_CRAWLERS = ["GPTBot", "OAI-SearchBot", "ChatGPT-User", "ClaudeBot", "Claude-User",
               "anthropic-ai", "PerplexityBot", "Perplexity-User", "Google-Extended",
               "Applebot-Extended", "CCBot"]


def write_robots() -> None:
    """Write docs/robots.txt pointing crawlers at the sitemap and llms.txt."""
    out = ["# robots.txt for db.h5i.dev",
           "# Everyone is welcome, including AI answer engines and search crawlers.",
           "User-agent: *", "Allow: /", ""]
    out.append("# Explicit opt-in for AI / answer-engine crawlers (AEO).")
    for bot in AI_CRAWLERS:
        out += [f"User-agent: {bot}", "Allow: /"]
    out += ["",
            f"Sitemap: {BASE_URL}sitemap.xml",
            f"# LLM-friendly index: {BASE_URL}llms.txt",
            f"# Full docs as one markdown file: {BASE_URL}llms-full.txt",
            ""]
    (OUT / "robots.txt").write_text("\n".join(out))


def write_llms_full(manual, api):
    """Write docs/llms-full.txt — manual + API reference concatenated as markdown."""
    out = [
        "# h5i-db — full documentation",
        "",
        f"> {LLMS_SUMMARY}",
        "",
        f"Canonical site: {BASE_URL}",
        "This file concatenates the manual and Python API reference as plain "
        "markdown for LLM ingestion. Cookbook tutorials "
        f"are at {BASE_URL}cookbook/.",
        "",
    ]
    for page in list(manual) + list(api):
        if not page.source:
            continue
        _, body = parse_front_matter(page.source.read_text())
        # drop navigational card-grid HTML blocks (redundant with llms.txt links)
        body = re.sub(r'<div class="card-grid">.*?</div>\s*', "", body, flags=re.S)
        # unwrap the lede paragraph wrapper, keeping its text
        body = re.sub(r'<p class="doc-lede">(.*?)</p>', r"\1", body, flags=re.S)
        out.append("\n\n---\n")
        out.append(f"# {page.title}   ({BASE_URL}{page.url})\n")
        # keep the page's own body; drop a leading duplicate H1 if present
        body = re.sub(r"\A\s*#\s+.*?\n", "", body, count=1)
        out.append(body.strip())
        out.append("")
    (OUT / "llms-full.txt").write_text("\n".join(out))


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
                # A notebook has no frontmatter, so its opening paragraph is
                # the only description available; fit it to the snippet
                # budget so search results end on a sentence, not mid-clause.
                description = trim_description(description)
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


def jsonld(page: Page, trail: "list[tuple[str, str]]", modified: str) -> str:
    """Structured data for one page: what it is, and where it sits.

    The breadcrumb trail is the same one rendered above the article, so the
    markup can never disagree with what a reader sees — which is the
    condition search engines actually enforce.
    """
    kind = {"api": "APIReference"}.get(page.section, "TechArticle")
    url = f"{BASE_URL}{page.url}"
    doc = {
        "@context": "https://schema.org",
        "@graph": [
            {
                "@type": kind,
                "@id": f"{url}#article",
                "headline": page.seo_title or page.title,
                "name": page.seo_title or page.title,
                "description": page.description,
                "url": url,
                "inLanguage": "en",
                "dateModified": modified,
                "isPartOf": {"@type": "WebSite", "@id": f"{BASE_URL}#website"},
                "publisher": {"@type": "Organization", "@id": "https://h5i.dev/#org"},
                "about": {"@type": "SoftwareApplication", "name": "h5i-db",
                          "applicationCategory": "DeveloperApplication"},
            },
            {
                "@type": "BreadcrumbList",
                "@id": f"{url}#breadcrumb",
                "itemListElement": [
                    {"@type": "ListItem", "position": i + 1, "name": name,
                     **({"item": item} if item else {})}
                    for i, (name, item) in enumerate(trail)
                ],
            },
        ],
    }
    return ('<script type="application/ld+json">'
            + json.dumps(doc, separators=(",", ":"))
            + "</script>")


def render_page(page: Page, template: str, sidebar: str, breadcrumb: str,
                prev_page: Page | None, next_page: Page | None,
                structured_data: str = "") -> str:
    root = "/"
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
        "{{head_title}}": html.escape(head_title(page)),
        "{{jsonld}}": structured_data,
        "{{description}}": html.escape(page.description),
        "{{root}}": root,
        "{{canonical}}": f"{BASE_URL}{page.url}",
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
        slug = "" if path.stem == "index" else f"{path.stem}/"
        title = meta.get("title", path.stem.replace("-", " ").title())
        page = Page(
            url=f"{section}/{slug}",
            section=section,
            title=title,
            seo_title=meta.get("seo_title", ""),
            description=meta.get("description", ""),
            body_html=body,
            toc_tokens=toc_tokens,
            search_text=plain_text(body),
            order=float(meta.get("order", 99)),
            source=path,
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
                           "compaction cadence, disk-usage math, and the torn-HEAD "
                           "recovery runbook.",
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
            url=f"manual/{slug}/",
            section="manual",
            title=meta["title"],
            description=meta["description"],
            body_html=body,
            toc_tokens=toc_tokens,
            search_text=plain_text(body),
            order=float(meta["order"]),
            source=path,
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
                    url=f"cookbook/{sec_dir}/{nb_path.stem}/",
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
            "event-driven backtesting, prediction markets and performance analytics. "
            "Every recipe runs top to bottom against real or deterministic synthetic "
            "market data.</p>",
            '<div class="doc-divider"></div>',
        ]
        for sec_dir, sec_label in COOKBOOK_SECTIONS:
            idx_parts.append(f'<h2 id="{sec_dir}">{html.escape(sec_label)}'
                             f'<a class="headerlink" href="#{sec_dir}">#</a></h2>')
            cards = []
            for i, p in enumerate(cookbook_groups[sec_label], 1):
                cards.append(
                    f'<a class="card" href="/{p.url}">'
                    f'<span class="card-no">{i:02d}</span>'
                    f'<span class="card-title">{html.escape(p.title)}</span>'
                    f'<span class="card-desc">{html.escape(p.description)}</span></a>'
                )
            idx_parts.append(f'<div class="card-grid">{"".join(cards)}</div>')
        cookbook_index = Page(
            url="cookbook/",
            section="cookbook",
            title="Cookbook",
            description="Executed notebook tutorials for h5i-db: "
            + ", ".join(label for _, label in COOKBOOK_SECTIONS).lower()
            + ".",
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
                if page.group == sec_label or page.url == "cookbook/":
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
    today = datetime.date.today().isoformat()
    for i, page in enumerate(ordered):
        root = "/"
        # One trail, rendered twice: as the visible breadcrumb and as the
        # BreadcrumbList. A group (cookbook section) has no page of its own,
        # so it is a name without an `item`.
        trail = [("h5i-db", BASE_URL),
                 (section_labels[page.section], f"{BASE_URL}{page.section}/")]
        if page.group:
            trail.append((page.group, ""))
        trail.append((page.title, f"{BASE_URL}{page.url}"))
        crumbs = [f'<a href="{root}">h5i-db</a>', '<span class="sep">/</span>',
                  f'<a href="{root}{page.section}/">{section_labels[page.section]}</a>']
        if page.group:
            crumbs += ['<span class="sep">/</span>', f"<span>{html.escape(page.group)}</span>"]
        crumbs += ['<span class="sep">/</span>', f"<span>{html.escape(page.title)}</span>"]
        prev_page = ordered[i - 1] if i > 0 else None
        next_page = ordered[i + 1] if i + 1 < len(ordered) else None
        page.body_html = absolutize_links(page.body_html, page.section)
        html_out = render_page(
            page, template,
            sidebar_html(groups_for(page), page, root),
            "\n".join(crumbs),
            prev_page, next_page,
            jsonld(page, trail, today),
        )
        dest = OUT / page.url / "index.html"
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(html_out)
        # Where this page used to live, before directory URLs.
        if page.url.rstrip("/") and not page.url.endswith(("manual/", "api/", "cookbook/")):
            write_redirect(page.url.rstrip("/") + ".html", page.url)

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

    # ── llms.txt / llms-full.txt (answer-engine optimization) ────
    write_llms_index(manual_pages, api_pages, cookbook_groups,
                     None if skip_cookbook else cookbook_index, skip_cookbook)
    write_llms_full(manual_pages, api_pages)

    # ── sitemap.xml / robots.txt (search-engine discovery) ───────
    write_sitemap(ordered)
    write_robots()

    n_nb = len(cookbook_pages)
    print(f"built {len(ordered)} pages "
          f"({len(manual_pages)} manual, {len(api_pages)} api, {n_nb} cookbook) "
          f"+ llms.txt, llms-full.txt, sitemap.xml, robots.txt -> {OUT}")


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
