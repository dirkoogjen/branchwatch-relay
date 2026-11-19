/* Saved design questions. Only the existing jobs form can create a programming job. */
(function () {
  'use strict';
  const KEY = 'obf-monitor2-discussion-draft';
  const PENDING = ['queued', 'starting', 'running'];
  const STATUS = { queued: 'Wacht op Claude', starting: 'Wacht op Claude', running: 'Claude antwoordt', completed: 'Antwoord klaar', stopped: 'Gestopt', failed: 'Probleem' };

  function createDiscussionUI({ document: doc, window: win, fetch: request }) {
    const byId = id => doc.getElementById(id);
    let saved = {};
    try { saved = JSON.parse(win.sessionStorage.getItem(KEY) || '{}') || {}; } catch (_) { /* storage is optional */ }
    const state = { selected: saved.selected || null, detail: null, data: null, busy: false, pending: saved.pending || null,
      drafts: saved.drafts || {}, form: saved.form || {}, listSignature: null, generation: 0, mode: saved.mode || 'execute', refreshing: false };
    function persist() {
      try { win.sessionStorage.setItem(KEY, JSON.stringify({ selected: state.selected, pending: state.pending, drafts: state.drafts, form: state.form, mode: state.mode })); } catch (_) { /* still usable without storage */ }
    }
    function h(tag, className, text) {
      const n = doc.createElement(tag);if (className) n.className = className;if (text !== undefined) n.textContent = text;return n;
    }
    function replace(node, children) { node.textContent = '';children.forEach(child => node.appendChild(child)); }
    function note(text, bad) { const n = byId('discussion-note');n.textContent = text || '';n.hidden = !text;n.dataset.tone = bad ? 'bad' : 'ok'; }
    function nonce() {
      try { if (win.crypto && typeof win.crypto.randomUUID === 'function') return win.crypto.randomUUID().replace(/-/g, ''); } catch (_) { /* HTTP LAN fallback */ }
      return (Date.now().toString(16) + Math.random().toString(16).slice(2) + '00000000').slice(0, 32);
    }
    function latest() { return state.detail && state.detail.turns[state.detail.turns.length - 1]; }
    function rememberInput() { state.drafts[state.selected || 'new'] = byId('discussion-question').value;state.form = { by: byId('discussion-by').value, node: byId('discussion-node').value, repository: byId('discussion-repository').value };persist(); }
    function mode(value) {
      state.mode = value;
      byId('discussions-panel').hidden = value !== 'discuss';byId('jobs-form').hidden = value === 'discuss';
      byId('mode-discuss').setAttribute('aria-pressed', String(value === 'discuss'));
      byId('mode-execute').setAttribute('aria-pressed', String(value !== 'discuss'));persist();
    }
    function options(select, values, label) {
      const previous = select.value;replace(select, values.map(value => { const n = h('option', null, label ? label(value) : value);n.value = value;return n; }));
      select.value = values.includes(previous) ? previous : (values[0] || '');
    }
    function projectOptions() {
      const node = byId('discussion-node').value || 'GERARD';
      const projects = state.data && state.data.projects || {};
      options(byId('discussion-repository'), [...(node === 'GERARD' ? [''] : []), ...(projects[node] || [])], path => path ? path.split('/').filter(Boolean).pop() + ' · ' + path : 'Algemeen overleg · project nog onbekend');
    }
    function controls() {
      const loading = state.selected && (!state.detail || state.detail.conversation_id !== state.selected);
      const t = latest();const pending = t && PENDING.includes(t.status);const readonly = state.data && state.data.actions_enabled === false;
      byId('discussion-send').disabled = Boolean(loading) || state.busy || readonly || (pending && !state.pending);
      byId('discussion-send').textContent = state.pending ? 'Ontvangst controleren' : state.detail ? 'Vervolgvraag sturen' : 'Vraag stellen';
      byId('discussion-question').disabled = Boolean(loading) || state.busy || readonly || Boolean(state.pending) || Boolean(pending);
      byId('discussion-by').disabled = Boolean(loading) || state.busy || readonly || Boolean(state.pending);
      ['discussion-node', 'discussion-repository'].forEach(id => { byId(id).disabled = Boolean(loading) || state.busy || readonly || Boolean(state.detail) || Boolean(state.pending); });
      byId('discussion-stop').hidden = !pending;byId('discussion-stop').disabled = Boolean(loading) || state.busy || readonly || Boolean(t && t.stop_requested);
      byId('discussion-draft').disabled = Boolean(loading) || state.busy || readonly || !t || t.status !== 'completed' || !state.detail.repository;
      byId('discussion-copy').disabled = !state.detail;
      byId('discussion-model').textContent = 'Claude · ' + (state.detail ? state.detail.model : state.data && state.data.model || 'ingesteld standaardmodel') + ' · alleen lezen';
    }
    function detail() {
      const c = state.detail;const nodes = [];
      byId('discussion-heading').textContent = c ? c.title : 'Bespreek eerst je idee';
      const caption = c && STATUS[latest().status];const reason = c && latest().reason || '';
      byId('discussion-status').textContent = c ? caption + (reason && reason.replace(/[.!]$/, '') !== caption ? ' — ' + reason : '') : 'Je kunt ook zonder project overleggen. Programmering begint pas na een aparte opdracht.';
      if (c) {
        c.turns.forEach(t => {
          const question = h('article', 'discussion-message discussion-message--user');question.appendChild(h('strong', null, 'Jij'));question.appendChild(h('div', 'discussion-text', t.question));nodes.push(question);
          if (t.answer) {
            const answer = h('article', 'discussion-message discussion-message--claude');answer.appendChild(h('strong', null, t.status === 'completed' ? 'Claude' : 'Claude · onvolledig antwoord'));
            answer.appendChild(h('div', 'discussion-text', t.answer));nodes.push(answer);
          }
          if (t.status === 'failed') nodes.push(h('p', 'discussion-failure', 'Probleem: ' + (t.reason || 'Geen volledig antwoord ontvangen.')));
        });
        byId('discussion-node').value = c.node;projectOptions();byId('discussion-repository').value = c.repository;
      }
      const thread = byId('discussion-thread');const previousScroll = thread.scrollTop || 0;const followEnd = (thread.scrollHeight || 0) - (thread.clientHeight || 0) - previousScroll < 40;
      replace(thread, nodes);thread.scrollTop = followEnd ? thread.scrollHeight : previousScroll;controls();renderCards();
    }
    function list() {
      const signature = JSON.stringify([state.selected, (state.data && state.data.conversations || []).map(c => [c.conversation_id, c.title, c.node, c.status])]);
      if (signature === state.listSignature) return;state.listSignature = signature;
      const items = (state.data && state.data.conversations || []).map(c => {
        const button = h('button', 'discussion-choice' + (c.conversation_id === state.selected ? ' is-selected' : ''), c.title + ' · ' + (c.node === 'LINUX' ? 'STUDIO' : c.node) + ' · ' + (STATUS[c.status] || c.status));
        button.type = 'button';button.addEventListener('click', () => select(c.conversation_id));return button;
      });
      replace(byId('discussion-list'), items.length ? items : [h('p', 'discussion-empty', 'Je gesprekken verschijnen hier.')]);
    }
    async function call(body) {
      const response = await request('/api/discussions', { method: 'POST', credentials: 'same-origin', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
      const data = await response.json();
      return { ok: response.ok && data.ok !== false, data, status: response.status || (data.ok === false ? 503 : 200) };
    }
    async function select(id) {
      rememberInput();state.selected = id || null;state.detail = null;state.generation += 1;const ticket = state.generation;persist();mode('discuss');
      byId('discussion-question').value = state.drafts[state.selected || 'new'] || '';
      detail();list();if (!id) return;
      try {
        const response = await request('/api/discussions?conversation_id=' + encodeURIComponent(id), { cache: 'no-store', credentials: 'same-origin' });const data = await response.json();
        if (ticket !== state.generation || state.selected !== id) return;
        if (!response.ok || !data.conversation || data.conversation.conversation_id !== id) throw new Error();
        state.detail = data.conversation;detail();
      } catch (_) { if (ticket === state.generation) note('Dit gesprek is nu niet bereikbaar. De opgeslagen geschiedenis blijft behouden.', true); }
    }
    async function refresh() {
      if (state.refreshing) return;state.refreshing = true;
      try {
        const response = await request('/api/discussions', { cache: 'no-store', credentials: 'same-origin' });const data = await response.json();
        if (!response.ok || data.ok === false) { note(data.message || 'Overleg is tijdelijk niet bereikbaar.', true);return; }
        const first = !state.data;state.data = data;
        if (first) {
          const users = data.meta && data.meta.users || [];options(byId('discussion-by'), users);
          if (users.includes(state.form.by)) byId('discussion-by').value = state.form.by;
          if (['GERARD', 'LINUX'].includes(state.form.node)) byId('discussion-node').value = state.form.node;
          projectOptions();
          if (((data.projects || {})[byId('discussion-node').value] || []).includes(state.form.repository)) byId('discussion-repository').value = state.form.repository;
        }
        list();controls();renderCards();
        const id = state.selected;const ticket = state.generation;
        if (id && !state.busy) {
          const r = await request('/api/discussions?conversation_id=' + encodeURIComponent(id), { cache: 'no-store', credentials: 'same-origin' });const c = await r.json();
          if (r.ok && c.conversation && c.conversation.conversation_id === id && ticket === state.generation && id === state.selected && (!state.detail || c.conversation.version > state.detail.version)) { state.detail = c.conversation;detail(); }
        }
      } catch (_) { note('Geen verbinding met de overleggeschiedenis. De vraag en antwoorden blijven bewaard.', true); }
      finally { state.refreshing = false; }
    }
    async function ask() {
      if (state.busy || state.data && state.data.actions_enabled === false) return;
      if (!state.pending && state.selected && (!state.detail || state.detail.conversation_id !== state.selected)) { note('Wacht tot het gekozen gesprek is geladen. Probeer het gesprek opnieuw te openen als het niet bereikbaar is.', true);return; }
      if (!state.pending) {
        const question = byId('discussion-question').value;const by = byId('discussion-by').value;
        if (!question.trim() || question.length > 20000 || !by) { note('Kies je naam en schrijf een vraag van maximaal 20.000 tekens.', true);return; }
        if (latest() && PENDING.includes(latest().status)) return;
        const body = { action: 'ask', by, question, nonce: nonce() };
        if (state.detail) Object.assign(body, { conversation_id: state.detail.conversation_id, version: state.detail.version });
        else {
          body.node = byId('discussion-node').value;body.repository = byId('discussion-repository').value;
          if (!['GERARD', 'LINUX'].includes(body.node) ) { note('Kies eerst een machine.', true);return; }
        }
        state.pending = body;persist();
      }
      const ticket = state.generation;const submitted = state.pending;
      state.busy = true;controls();mode('discuss');
      try {
        const result = await call(state.pending);
        if (!result.ok || !result.data.conversation) {
          if (result.status < 500) { state.pending = null;persist(); }
          note(result.data.message || 'Ontvangst is onzeker. Kies Ontvangst controleren; dezelfde vraag wordt niet dubbel aangemaakt.', true);return;
        }
        const c = result.data.conversation;
        const origin = submitted.conversation_id || 'new';
        if (state.drafts[origin] === submitted.question) state.drafts[origin] = '';
        state.pending = null;
        if (ticket !== state.generation) { persist();note('De eerdere vraag is ontvangen en staat in de bewaarde gesprekken. Je huidige concept blijft staan.');return; }
        state.selected = c.conversation_id;state.detail = c;state.generation += 1;state.pending = null;
        byId('discussion-question').value = '';persist();detail();note('Vraag ontvangen. Je kunt dit scherm verlaten; het antwoord wordt bewaard.');
      } catch (_) { note('Ontvangst is onzeker. Kies Ontvangst controleren om dezelfde vraag veilig terug te vinden.', true); }
      finally { state.busy = false;controls(); }
    }
    async function action(kind) {
      if (state.busy || !state.detail || state.data && state.data.actions_enabled === false) return;
      const c = state.detail;const t = latest();const by = byId('discussion-by').value;
      if (!by) { note('Kies eerst je naam.', true);return; }
      if (kind === 'cancel' && !win.confirm('Alleen deze overlegvraag stoppen? Ontvangen tekst blijft bewaard.')) return;
      if (kind === 'draft' && t.status !== 'completed') return;
      const ticket = state.generation;state.busy = true;controls();
      try {
        const result = await call({ action: kind, by, nonce: nonce(), conversation_id: c.conversation_id, turn_id: t.turn_id, version: c.version });
        if (ticket !== state.generation || state.selected !== c.conversation_id) return;
        if (!result.ok) { note(result.data.message || 'De handeling is niet bevestigd; ververs de status.', true);return; }
        if (kind === 'draft') {
          const draft = result.data.draft;
          if (!draft || !draft.source || draft.source.conversation_id !== c.conversation_id || draft.source.turn_id !== t.turn_id || draft.source.version !== c.version) throw new Error();
          if (win.obfJobs && win.obfJobs.prepareJobDraft(draft)) { mode('execute');note('Concept klaargezet. Lees en bewerk het en zet het zelf in de wachtrij.'); }
        } else if (result.data.conversation) { state.detail = result.data.conversation;detail(); }
      } catch (_) { note('De handeling is niet bevestigd. Controleer de actuele status voordat je verdergaat.', true); }
      finally { state.busy = false;controls(); }
    }
    function renderCards() {
      if (typeof doc.querySelectorAll !== 'function') return;
      doc.querySelectorAll('[data-discussion-node]').forEach(badge => {
        const rows = state.data && state.data.conversations || [];
        const active = rows.find(c => c.node.toLowerCase() === badge.dataset.discussionNode && ['starting', 'running'].includes(c.status));
        badge.hidden = !active;
        const scene = badge.parentNode;const chain = scene && scene.querySelector('.studio-chain');
        if (chain) chain.hidden = Boolean(active);
        if (active) {
          badge.textContent = 'Overleg · ' + (STATUS[active.status] || 'Wacht op Claude');
          const pill = scene.querySelector('.card-status .pill');if (pill) { pill.textContent = 'OVERLEG · ' + STATUS[active.status].toUpperCase();pill.dataset.tone = 'busy'; }
        }
      });
    }
    function boot() {
      mode(state.mode);byId('discussion-question').value = state.drafts[state.selected || 'new'] || '';
      byId('mode-discuss').addEventListener('click', () => mode('discuss'));byId('mode-execute').addEventListener('click', () => mode('execute'));
      byId('discussion-new').addEventListener('click', () => select(null));byId('discussion-node').addEventListener('change', () => { projectOptions();rememberInput(); });
      ['discussion-by', 'discussion-repository'].forEach(id => byId(id).addEventListener('change', rememberInput));
      byId('discussion-question').addEventListener('input', rememberInput);
      byId('discussion-form').addEventListener('submit', e => { e.preventDefault();ask(); });
      byId('discussion-draft').addEventListener('click', () => action('draft'));byId('discussion-stop').addEventListener('click', () => action('cancel'));
      byId('discussion-copy').addEventListener('click', async () => {
        if (!state.detail) return;
        const text = state.detail.turns.map(t => 'Jij:\n' + t.question + '\n\nClaude:\n' + (t.answer || '')).join('\n\n');
        try { await win.navigator.clipboard.writeText(text);note('Gesprek gekopieerd.'); } catch (_) { note('Kopiëren lukte niet. Selecteer en kopieer de tekst handmatig.', true); }
      });
      detail();refresh();win.setInterval(refresh, 4000);controls();
    }
    return { boot, refresh, select, ask, draft: () => action('draft'), stop: () => action('cancel'), setMode: mode, renderCards };
  }
  if (typeof module !== 'undefined' && module.exports) module.exports = { createDiscussionUI, STATUS };
  else {
    const start = () => { const ui = createDiscussionUI({ document, window, fetch: window.fetch.bind(window) });window.obfDiscussions = ui;ui.boot(); };
    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', start);else start();
  }
})();
