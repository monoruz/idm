// Small glue on top of htmx: live progress over SSE, dialogs, clipboard and
// drag-and-drop queue ordering.
(() => {
  const $ = (s, el = document) => el.querySelector(s);
  const dlg = () => $('#dlg');
  if (!dlg()) return; // login page

  // ---- formatting (mirrors funcs.go) ----
  const fmtBytes = n => {
    if (n < 0) return '?';
    if (n < 1024) return n + ' B';
    let u = -1;
    do { n /= 1024; u++; } while (n >= 1024 && u < 5);
    return n.toFixed(1) + ' ' + 'KMGTPE'[u] + 'B';
  };
  const fmtEta = s => s < 0 ? '' : s < 60 ? s + 's' : s < 3600 ? `${s / 60 | 0}m ${s % 60}s` : `${s / 3600 | 0}h ${(s % 3600) / 60 | 0}m`;

  // ---- view refresh ----
  let dragging = null, pending = false, timer = 0;
  const refresh = () => {
    if (dragging) { pending = true; return; }
    clearTimeout(timer);
    timer = setTimeout(() => {
      htmx.ajax('GET', '/view' + location.search, { target: '#main', swap: 'innerHTML' });
    }, 120);
  };
  document.body.addEventListener('refresh', refresh);

  // ---- live progress ----
  const connect = () => {
    const es = new EventSource('/events');
    let wasDown = false;
    es.addEventListener('refresh', refresh);
    es.addEventListener('progress', e => {
      const p = JSON.parse(e.data);
      const total = $('#total');
      if (total) total.textContent = p.total > 0 ? fmtBytes(p.total) + '/s' : '';
      for (const it of p.items) {
        const row = document.getElementById('d-' + it.id);
        if (!row) continue;
        const pct = it.size > 0 ? it.done * 100 / it.size : 0;
        const bar = $('.bar i', row);
        if (bar) bar.style.width = pct.toFixed(1) + '%';
        const set = (sel, v) => { const el = $(sel, row); if (el) el.textContent = v; };
        set('.done', fmtBytes(it.done));
        set('.pct', it.size > 0 ? pct.toFixed(1) + '%' : '');
        set('.speed', fmtBytes(it.speed) + '/s');
        set('.eta', fmtEta(it.eta));
      }
      if (!p.items.length && total) total.textContent = '';
    });
    es.onopen = () => { if (wasDown) refresh(); wasDown = false; };
    es.onerror = () => { wasDown = true; };
  };
  connect();
  // When the last download stops the server stops sending progress.
  document.body.addEventListener('htmx:afterSwap', e => {
    if (e.detail.target.id === 'main' && !$('.row.active')) { const t = $('#total'); if (t) t.textContent = ''; }
  });

  // ---- dialogs and toasts ----
  document.body.addEventListener('htmx:afterSwap', e => {
    const t = e.detail.target;
    if (t.id === 'dlg-body' && !dlg().open) {
      dlg().showModal();
      const af = $('[autofocus]', t);
      if (af) af.focus();
      syncStartRow();
    }
    if (t.id === 'toast') {
      clearTimeout(t._h);
      t._h = setTimeout(() => { t.innerHTML = ''; }, 4500);
    }
  });
  document.body.addEventListener('closeDialog', () => dlg().close());
  document.addEventListener('click', e => {
    if (e.target.closest('[data-close]')) dlg().close();
    if (e.target === dlg()) dlg().close(); // backdrop click
  });
  document.addEventListener('DOMContentLoaded', () => {
    dlg().addEventListener('close', () => { $('#dlg-body').innerHTML = ''; });
  });
  const toast = msg => {
    const t = $('#toast');
    t.innerHTML = '';
    const d = document.createElement('div');
    d.className = 'toast';
    d.textContent = msg;
    t.appendChild(d);
    clearTimeout(t._h);
    t._h = setTimeout(() => { t.innerHTML = ''; }, 4500);
  };

  // ---- add dialog ----
  // "Start the queue now" only matters when the chosen queue is idle.
  const syncStartRow = () => {
    const sel = $('#add-queue');
    if (!sel) return;
    const running = sel.selectedOptions[0]?.dataset.running === 'true';
    $('#start-row').hidden = running;
    $('#running-hint').hidden = !running;
  };
  document.addEventListener('change', e => {
    if (e.target.id === 'add-queue') syncStartRow();
    if (e.target.matches('[data-all]')) {
      for (const cb of e.target.closest('table').querySelectorAll('input[name=inc]')) cb.checked = e.target.checked;
    }
  });

  const fillLinks = text => {
    const ta = $('#add-text');
    if (!ta) return;
    ta.value = ta.value ? ta.value.trimEnd() + '\n' + text : text;
    htmx.ajax('POST', '/add/preview', { target: '#preview', swap: 'innerHTML', values: { text: ta.value } });
  };

  const openAdd = text => {
    const q = new URLSearchParams(location.search).get('q') || '';
    htmx.ajax('GET', '/add?q=' + q, { target: '#dlg-body' }).then(() => fillLinks(text));
  };

  document.addEventListener('click', async e => {
    if (!e.target.closest('[data-clip]')) return;
    if (!navigator.clipboard?.readText) {
      toast('Clipboard access needs HTTPS or localhost here — click the box and press Ctrl+V instead.');
      $('#add-text')?.focus();
      return;
    }
    try { fillLinks(await navigator.clipboard.readText()); }
    catch { toast('Clipboard permission denied — press Ctrl+V in the box instead.'); }
  });

  // Ctrl+V anywhere outside a form field opens the add dialog with the links.
  document.addEventListener('paste', e => {
    if (e.target.closest('input, textarea, select, [contenteditable]')) return;
    const text = e.clipboardData?.getData('text') || '';
    if (!/https?:\/\//i.test(text)) return;
    e.preventDefault();
    if (dlg().open && $('#add-text')) fillLinks(text);
    else if (!dlg().open) openAdd(text);
  });

  // ---- drag & drop ordering ----
  const sortable = () => $('#list[data-sortable]');
  document.addEventListener('mousedown', e => {
    const row = e.target.closest('.row.unf');
    if (row && sortable()) row.draggable = !!e.target.closest('.handle');
  });
  document.addEventListener('dragstart', e => {
    const row = e.target.closest?.('.row.unf');
    if (!row || !sortable()) return;
    dragging = row;
    row.classList.add('dragging');
    e.dataTransfer.effectAllowed = 'move';
  });
  document.addEventListener('dragover', e => {
    if (!dragging) return;
    const over = e.target.closest?.('.row.unf');
    e.preventDefault();
    if (!over || over === dragging) return;
    const r = over.getBoundingClientRect();
    over.parentNode.insertBefore(dragging, e.clientY < r.top + r.height / 2 ? over : over.nextSibling);
  });
  document.addEventListener('dragend', async () => {
    if (!dragging) return;
    const list = sortable();
    dragging.classList.remove('dragging');
    dragging.draggable = false;
    dragging = null;
    if (!list) return;
    const body = new URLSearchParams();
    for (const r of list.querySelectorAll('.row.unf')) body.append('ids', r.dataset.id);
    await fetch(`/queues/${list.dataset.sortable}/order`, { method: 'POST', body, headers: { 'HX-Request': 'true' } });
    pending = false;
    refresh();
  });
})();
