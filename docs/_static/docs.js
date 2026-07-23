/* h5i-db docs — nav, search, toc scrollspy, code copy. No dependencies. */
(function () {
  'use strict';
  var ROOT = window.__DOCS_ROOT__ || './';

  /* ── Mobile nav / sidebar toggles ─────────────────────────── */
  var hamburger = document.getElementById('hamburger');
  var navLinks = document.getElementById('nav-links');
  if (hamburger && navLinks) {
    hamburger.addEventListener('click', function () { navLinks.classList.toggle('open'); });
    document.addEventListener('click', function (e) {
      if (!navLinks.contains(e.target) && !hamburger.contains(e.target)) navLinks.classList.remove('open');
    });
  }
  var sbToggle = document.getElementById('sidebar-toggle');
  var sidebar = document.getElementById('sidebar');
  if (sbToggle && sidebar) {
    sbToggle.addEventListener('click', function (e) { e.stopPropagation(); sidebar.classList.toggle('open'); });
    document.addEventListener('click', function (e) {
      if (window.innerWidth <= 860 && !sidebar.contains(e.target) && !sbToggle.contains(e.target)) {
        sidebar.classList.remove('open');
      }
    });
  }

  /* ── Copy buttons on code blocks ──────────────────────────── */
  document.querySelectorAll('article.doc div.highlight').forEach(function (block) {
    var btn = document.createElement('button');
    btn.className = 'copy-code';
    btn.textContent = 'copy';
    btn.setAttribute('aria-label', 'Copy code');
    btn.addEventListener('click', function () {
      var pre = block.querySelector('pre');
      var text = pre ? pre.innerText : '';
      /* strip console prompts when copying shell blocks */
      text = text.replace(/^\$ /gm, '');
      navigator.clipboard.writeText(text).then(function () {
        btn.textContent = 'copied!';
        setTimeout(function () { btn.textContent = 'copy'; }, 1800);
      });
    });
    block.appendChild(btn);
  });

  /* ── TOC scrollspy ────────────────────────────────────────── */
  var tocLinks = Array.prototype.slice.call(document.querySelectorAll('.toc a'));
  if (tocLinks.length) {
    var byId = {};
    tocLinks.forEach(function (a) {
      var id = (a.getAttribute('href') || '').replace(/^#/, '');
      if (id) byId[id] = a;
    });
    var current = null;
    var spy = new IntersectionObserver(function (entries) {
      entries.forEach(function (en) {
        if (en.isIntersecting) {
          if (current) current.classList.remove('active');
          current = byId[en.target.id];
          if (current) current.classList.add('active');
        }
      });
    }, { rootMargin: '-80px 0px -70% 0px', threshold: 0 });
    Object.keys(byId).forEach(function (id) {
      var el = document.getElementById(id);
      if (el) spy.observe(el);
    });
  }

  /* ── Search ───────────────────────────────────────────────── */
  var input = document.getElementById('doc-search');
  var resultsBox = document.getElementById('search-results');
  var index = null;
  var sel = -1;

  function loadIndex() {
    if (index !== null) return Promise.resolve(index);
    return fetch(ROOT + '_static/search-index.json')
      .then(function (r) { return r.json(); })
      .then(function (data) { index = data; return index; })
      .catch(function () { index = []; return index; });
  }

  function escapeHtml(s) {
    return s.replace(/[&<>"]/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c];
    });
  }

  function search(q) {
    q = q.trim().toLowerCase();
    if (!q) return [];
    var terms = q.split(/\s+/);
    var scored = [];
    index.forEach(function (page) {
      var title = page.title.toLowerCase();
      var score = 0;
      var frag = '';
      var ok = terms.every(function (t) {
        if (title.indexOf(t) !== -1) { score += title === t ? 100 : (title.indexOf(t) === 0 ? 40 : 20); return true; }
        var h = page.headings.find(function (hd) { return hd.text.toLowerCase().indexOf(t) !== -1; });
        if (h) { score += 10; if (!frag) frag = h.text; return true; }
        var i = page.body.indexOf(t);
        if (i !== -1) {
          score += 2;
          if (!frag) {
            var start = Math.max(0, i - 30);
            frag = (start > 0 ? '…' : '') + page.body.slice(start, i + 60) + '…';
          }
          return true;
        }
        return false;
      });
      if (ok) scored.push({ page: page, score: score, frag: frag });
    });
    scored.sort(function (a, b) { return b.score - a.score; });
    return scored.slice(0, 12);
  }

  function render(results, q) {
    sel = -1;
    if (!q.trim()) { resultsBox.classList.remove('open'); resultsBox.innerHTML = ''; return; }
    if (!results.length) {
      resultsBox.innerHTML = '<div class="empty">no matches</div>';
      resultsBox.classList.add('open');
      return;
    }
    resultsBox.innerHTML = results.map(function (r) {
      return '<a href="' + ROOT + r.page.url + '">' +
        '<span class="r-sec">' + escapeHtml(r.page.section) + '</span> ' +
        '<div class="r-title">' + escapeHtml(r.page.title) + '</div>' +
        (r.frag ? '<div class="r-frag">' + escapeHtml(r.frag) + '</div>' : '') +
        '</a>';
    }).join('');
    resultsBox.classList.add('open');
  }

  if (input && resultsBox) {
    input.addEventListener('focus', loadIndex);
    input.addEventListener('input', function () {
      loadIndex().then(function () { render(search(input.value), input.value); });
    });
    input.addEventListener('keydown', function (e) {
      var items = resultsBox.querySelectorAll('a');
      if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
        e.preventDefault();
        if (!items.length) return;
        if (sel >= 0) items[sel].classList.remove('sel');
        sel = e.key === 'ArrowDown' ? (sel + 1) % items.length : (sel - 1 + items.length) % items.length;
        items[sel].classList.add('sel');
        items[sel].scrollIntoView({ block: 'nearest' });
      } else if (e.key === 'Enter' && sel >= 0 && items[sel]) {
        window.location.href = items[sel].href;
      } else if (e.key === 'Escape') {
        input.blur(); render([], '');
      }
    });
    document.addEventListener('click', function (e) {
      if (!input.contains(e.target) && !resultsBox.contains(e.target)) resultsBox.classList.remove('open');
    });
    document.addEventListener('keydown', function (e) {
      if (e.key === '/' && document.activeElement !== input &&
          !/^(input|textarea|select)$/i.test((document.activeElement || {}).tagName || '')) {
        e.preventDefault(); input.focus();
      }
    });
  }
})();
