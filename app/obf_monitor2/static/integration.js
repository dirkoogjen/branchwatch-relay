/* Monitor 2 -- Integratie naar main (paneel op het scherm Opdrachten).
 *
 * Route: aanvraag van Claude -> bekijken -> goedkeuren (naam + goedkeuringscode) -> uitvoeren door Monitor 2 zelf
 * (``/api/integration``, zie app/obf_monitor2/integration.py). Voor repositories met een vastgezet beleid (eenmalige
 * instelling, app/obf_monitor2/auto_integration.py) integreert Monitor 2 afgeronde taken zelf; dit paneel toont dan het beleid,
 * wat wacht, wat aandacht vraagt, en laat stilzetten (zonder code) en hervatten (met code). Het paneel verschijnt alleen als de machine het aan heeft
 * (OBF_INTEGRATION=1). Alle tekst gaat via textContent; de goedkeuringscode wordt nergens bewaard en na gebruik gewist.
 * Er wordt nooit gepusht of uitgerold.
 *
 * De gedeelde Ubuntu-keten op de machinekaarten toont de actuele integratorstatus.
 * Op Opdrachten staat geen afzonderlijk extern integratorvak meer. De status-API blijft
 * beschikbaar voor diagnose en toezicht. Het handmatige paneel hieronder verandert niet.
 */
(function () {
  'use strict';

  const USER_KEY = 'obf-monitor2-jobs-user';
  const POLL_MS = 8000;
  const state = { meta: null, facts: null, request: null, busy: false };
  let root = null;

  function h(tag, props, ...kids) {
    const node = document.createElement(tag);
    Object.entries(props || {}).forEach(([k, v]) => { if (k === 'class') node.className = v; else if (v !== false && v != null) node.setAttribute(k, v === true ? '' : v); });
    kids.flat().forEach((kid) => node.append(kid instanceof Node ? kid : document.createTextNode(String(kid))));
    return node;
  }

  function nonce() {
    try {
      if (window.crypto && window.crypto.randomUUID) return window.crypto.randomUUID().replace(/-/g, '');
    } catch (err) { /* val terug */ }
    return (Date.now().toString(16) + Math.random().toString(16).slice(2) + '00000000').slice(0, 32);
  }

  function who() {
    const select = document.getElementById('jobs-by');
    if (select && select.value) return select.value;
    try {
      const stored = JSON.parse(window.localStorage.getItem(USER_KEY));
      return typeof stored === 'string' ? stored : '';
    } catch (err) { return ''; }
  }

  async function call(method, body) {
    const response = await fetch('/api/integration', {
      method, credentials: 'same-origin', headers: { 'Content-Type': 'application/json' },
      body: method === 'POST' ? JSON.stringify(body) : undefined,
    });
    let payload = {};
    try { payload = await response.json(); } catch (err) { payload = {}; }
    return { ok: response.ok && payload.ok !== false, payload };
  }

  function say(text, bad) {
    const box = root.querySelector('[data-role="say"]');
    box.textContent = text || '';
    box.hidden = !text;
    box.dataset.tone = bad ? 'bad' : 'ok';
  }

  function short(sha) { return (sha || '').slice(0, 10); }

  function field(label, control) {
    return h('div', { class: 'jobs-field' }, h('label', {}, label), control);
  }

  function build() {
    const repo = h('select', { id: 'integ-repo' });
    const branch = h('input', { id: 'integ-branch', type: 'text', maxlength: '100', placeholder: 'bijv. r2/R2-1234abcd5678' });
    const mode = h('select', { id: 'integ-mode' }, h('option', { value: 'ff-only' }, 'Alleen fast-forward (standaard)'),
      h('option', { value: 'no-ff' }, 'Samenvoegcommit (no-ff)'));
    const tests = h('textarea', { id: 'integ-tests', rows: '3', placeholder: 'Eén testbestand per regel, bijv. tests/test_obf_monitor2_integration.py' });
    const job = h('input', { id: 'integ-job', type: 'text', maxlength: '20', placeholder: 'Optioneel: opdracht-id (J-…)' });
    const note = h('input', { id: 'integ-note', type: 'text', maxlength: '500', placeholder: 'Optionele opmerking' });
    const secret = h('input', { id: 'integ-secret', type: 'password', maxlength: '200', autocomplete: 'off',
      placeholder: 'Goedkeuringscode (alleen jij kent die)' });
    const protectedBox = h('input', { id: 'integ-protected', type: 'checkbox' });
    const preview = h('button', { type: 'button', class: 'btn btn--small', 'data-role': 'preview' }, 'Bekijken');
    const grant = h('button', { type: 'button', class: 'btn btn--small btn--primary', 'data-role': 'grant', disabled: true }, 'Goedkeuren');
    return h('section', { id: 'integration', class: 'panel jobs-form', 'aria-labelledby': 'integ-heading', hidden: true },
      h('div', { class: 'jobs-field jobs-field--wide' }, h('h3', { id: 'integ-heading' }, 'Integratie naar main (eenmalige toestemming)'),
        h('p', { class: 'jobs-empty' }, 'Claude kan alleen een aanvraag doen. Jij bekijkt de feiten, geeft met je goedkeuringscode één ' +
          'toestemming voor precies één commit op precies één main-stand, en daarna voert Monitor 2 zelf de samenvoeging uit ' +
          '(met herstelbranch). Pushen en uitrollen zijn hier nooit onderdeel van.')),
      h('div', { class: 'jobs-field jobs-field--wide', 'data-role': 'auto' }),
      h('div', { class: 'jobs-field jobs-field--wide', 'data-role': 'requests' }),
      field('Repository', repo), field('Bronbranch', branch), field('Modus', mode), field('Opdracht (optioneel)', job),
      h('div', { class: 'jobs-field jobs-field--wide' }, h('label', {}, 'Gerichte tests na afloop'), tests),
      h('div', { class: 'jobs-field jobs-field--wide' }, h('label', {}, 'Opmerking'), note),
      field('Goedkeuringscode', secret),
      h('div', { class: 'jobs-field jobs-field--wide', 'data-role': 'protected', hidden: true },
        h('label', {}, protectedBox, ' Ik heb gelezen dat deze wijziging de integratie-controller of het Runner-beleid zelf raakt.')),
      h('div', { class: 'jobs-field jobs-field--wide jobs-submit-row' }, preview, grant),
      h('p', { class: 'dialog-note', role: 'status', 'data-role': 'say', hidden: true }),
      h('div', { class: 'jobs-field jobs-field--wide', 'data-role': 'facts' }),
      h('div', { class: 'jobs-field jobs-field--wide', 'data-role': 'grants' }));
  }

  function renderFacts() {
    const box = root.querySelector('[data-role="facts"]');
    const facts = state.facts;
    box.textContent = '';
    const mode = root.querySelector('#integ-mode').value;
    root.querySelector('[data-role="grant"]').disabled = !facts || facts.already_merged || facts.commits_ahead === 0 ||
      (mode === 'ff-only' && !facts.fast_forward) || (mode === 'no-ff' && facts.conflicts === true) ||
      !!(state.request && state.request.current === false);
    root.querySelector('[data-role="protected"]').hidden = !(facts && facts.protected_touched && facts.protected_touched.length);
    if (!facts) return;
    const list = h('ul', {});
    facts.commits.slice(0, 10).forEach((c) => list.append(h('li', {}, short(c.sha) + ' ' + c.subject)));
    box.append(
      h('p', {}, `main nu: ${short(facts.main_head)} · ${facts.source_branch}: ${short(facts.source_tip)} · ` +
        (facts.already_merged ? 'al opgenomen in main' : facts.fast_forward ? 'fast-forward mogelijk' : facts.conflicts ? 'conflicten' : 'samenvoegen mogelijk')),
      h('p', {}, `${facts.commits_ahead} commit(s), ${facts.files_total} bestand(en) gewijzigd${facts.shortstat ? ' (' + facts.shortstat + ')' : ''}.`),
      state.request && state.request.current === false ? h('p', { class: 'dialog-note', 'data-tone': 'bad' },
        'Let op: de branch wijst niet meer naar de commit die Claude aanvroeg; goedkeuren kan niet tot er een nieuwe aanvraag is.') : '',
      facts.protected_touched && facts.protected_touched.length ? h('p', { class: 'dialog-note', 'data-tone': 'bad' },
        'Raakt de controller zelf: ' + facts.protected_touched.join(', ')) : '',
      list,
      h('p', {}, facts.tracked_changes.length ? `Let op: ${facts.tracked_changes.length} niet-gecommitte wijziging(en) in de hoofdcheckout; de uitvoering weigert zolang dat zo is.` :
        'De hoofdcheckout heeft geen niet-gecommitte wijzigingen aan getrackte bestanden.'),
      h('p', {}, facts.untracked.length ? `${facts.untracked.length} untracked bestand(en) blijven onaangeroerd.` : 'Geen untracked bestanden.'),
      facts.checked_out !== facts.main_branch ? h('p', {}, `Let op: de hoofdcheckout staat op ${facts.checked_out || 'een losse HEAD'}, niet op main; de uitvoering weigert dan.`) : '');
  }

  const OUTCOME = { passed: 'geslaagd', failed: 'mislukt', timeout: 'TIME-OUT: afgebroken, telt als mislukt',
    stalled: 'STILGEVALLEN: afgebroken, telt als mislukt', error: 'kon niet starten, telt als mislukt' };
  const MODE = { fixed: 'vast budget', fallback: 'terugval op het vaste budget', explicit: 'uitdrukkelijk opgegeven' };

  function secs(value) {
    if (typeof value !== 'number' || !isFinite(value)) return '?';
    const s = Math.round(value);
    return s >= 120 ? `${s} s (${Math.round(s / 6) / 10} min)` : `${s} s`;
  }

  function renderTestRun(box, run) {
    if (!run) return;
    const cur = run.current;
    if (cur && cur.state === 'running') {
      const pct = typeof cur.percent === 'number' ? ` · pytest ${cur.percent}%` : '';
      const quiet = typeof cur.quiet_s === 'number' && cur.quiet_s >= 30 ? ` · ${secs(cur.quiet_s)} geen uitvoer (stilval bij ${secs(run.stall_s)})` : '';
      const age = typeof cur.age_s === 'number' && cur.age_s >= 15 ? ` · stand ${secs(cur.age_s)} oud` : '';
      box.append(h('p', { 'data-role': 'test-run' }, `Gerichte tests bezig (${cur.runner || '?'}${cur.grant ? ', ' + cur.grant : ''}): ` +
        `${secs(cur.elapsed_s)} van ${secs(cur.budget_s)} budget · ` +
        (cur.estimate_s != null ? `geschat ${secs(cur.estimate_s)}` : 'vast budget, geen schatting') + pct + quiet + age + '.'));
      if (typeof cur.budget_s === 'number' && cur.budget_s > 0 && typeof cur.elapsed_s === 'number') {
        box.append(h('progress', { max: String(cur.budget_s), value: String(Math.min(cur.elapsed_s, cur.budget_s)) }));
      }
    } else if (cur && cur.state === 'interrupted') {
      box.append(h('p', { class: 'dialog-note', 'data-tone': 'bad', 'data-role': 'test-run' },
        `De testrun van ${cur.grant || '?'} is niet afgemaakt (na ${secs(cur.elapsed_s)}; de integrator is gestopt of herstart). ` +
        'Die run telt niet als geslaagd.'));
    }
    const last = run.last || [];
    if (last.length) {
      box.append(h('p', {}, h('em', {}, `Laatste testrun${last[0].grant ? ' (' + last[0].grant + ')' : ''}: geschat versus werkelijk`)));
      const ul = h('ul', {});
      last.forEach((g) => ul.append(h('li', g.outcome === 'passed' ? {} : { 'data-tone': 'bad' },
        `${g.runner || '?'}${g.tests_count != null ? ' (' + g.tests_count + ' bestand(en))' : ''}: ${OUTCOME[g.outcome] || g.outcome || '?'} ` +
        `na ${secs(g.elapsed_s)} · budget ${secs(g.budget_s)} · ` +
        (g.estimate_s != null ? `geschat ${secs(g.estimate_s)}` : `geen schatting (${MODE[g.mode] || g.mode || 'onbekend'})`))));
      box.append(ul);
    }
    if (run.history_problem) box.append(h('p', { class: 'jobs-empty' }, `Testduurhistorie: ${run.history_problem}; zulke metingen tellen niet mee, het budget wordt dan ruim (conservatief) geschat.`));
  }

  function renderAuto() {
    const box = root.querySelector('[data-role="auto"]');
    box.textContent = '';
    const auto = (state.meta || {}).auto || {};
    if (!auto.configured) return;
    box.append(h('strong', {}, 'Automatische integratie naar lokale main'));
    if (!auto.enabled) {
      box.append(h('p', { class: 'dialog-note', 'data-tone': 'bad' },
        'Staat UIT: ' + (auto.problem || 'het beleid is niet geldig') + '. Aanvragen blijven op handmatige goedkeuring wachten.'));
      return;
    }
    (auto.repos || []).forEach((r) => box.append(h('p', {},
      `Aan voor ${r.repo} · bereik ${r.scope === 'chain' ? 'afgeronde keten' : 'afgeronde taak'} · alleen fast-forward · tests vooraf: ` +
      `${(r.tests || []).join(', ') || 'uit de aanvraag'} · beleid ${short(auto.policy_sha256)}. Pushen en uitrollen blijven verboden.`)));
    const by = who();
    if (auto.paused) {
      box.append(h('p', { class: 'dialog-note', 'data-tone': 'bad' }, `STILGEZET door ${auto.paused.by} (${auto.paused.at}); er wordt niets automatisch geïntegreerd.`));
      const resume = h('button', { type: 'button', class: 'btn btn--small btn--primary', 'data-role': 'auto-resume' }, 'Hervatten');
      resume.addEventListener('click', () => autoSwitch('auto_resume'));
      box.append(resume);
    } else {
      const pause = h('button', { type: 'button', class: 'btn btn--small btn--ghost', 'data-role': 'auto-pause' }, 'Stilzetten');
      pause.addEventListener('click', () => autoSwitch('auto_pause'));
      box.append(auto.running ? h('p', {}, `Bezig: ${auto.running}`) : '', pause);
    }
    renderTestRun(box, auto.test_run);
    const items = (label, list, tone) => {
      if (!(list || []).length) return;
      box.append(h('p', {}, h('em', {}, label)));
      const ul = h('ul', {});
      list.slice(0, 10).forEach((i) => ul.append(h('li', tone ? { 'data-tone': tone } : {},
        `${i.id || i.grant || ''} · ${i.code || i.state}: ${i.text || ''}${i.recovery_branch ? ' · herstelbranch ' + i.recovery_branch : ''}`)));
      box.append(ul);
    };
    items('Vraagt aandacht van een mens (er is niets geforceerd)', auto.attention, 'bad');
    items('Wacht op een voorwaarde (Monitor 2 probeert het vanzelf opnieuw)', auto.waiting);
    items('Recent', auto.recent);
    const r2 = auto.r2_actions || {};
    if (r2.problem) {
      box.append(h('p', { class: 'dialog-note', 'data-tone': 'bad' },
        `Koppeling met de open acties van Runner 2: ${r2.problem}. Monitor 2 probeert het vanzelf opnieuw; de integratie zelf wordt er niet door gehinderd.`));
    }
    const how = { recorded: 'vastgelegd', existing: 'stond al open', suppressed: 'al gedekt door een ander punt', unlinked: 'geen taak om aan te hangen (alleen hier)' };
    items('Vastgelegd als open actie in Runner 2 (blijft staan tot een mens het sluit)',
      (r2.linked || []).map((l) => ({ id: l.request, code: l.code, text: `${how[l.state] || l.state}${l.action_id ? ' · ' + l.action_id : ''}${l.task ? ' · taak ' + l.task : ''}` })));
    if (!by) box.append(h('p', { class: 'jobs-empty' }, 'Kies bij Opdrachten wie je bent om stil te zetten of te hervatten.'));
  }

  async function autoSwitch(action) {
    const by = who();
    if (state.busy) return;
    if (!by) { say('Kies eerst bij Opdrachten wie je bent.', true); return; }
    if (action === 'auto_resume' && !secretValue()) { say('Vul je goedkeuringscode in om te hervatten.', true); return; }
    state.busy = true;
    const body = { action, by, nonce: nonce() };
    if (action === 'auto_resume') body.secret = secretValue();
    const result = await call('POST', body);
    state.busy = false;
    clearSecret();
    say(result.payload.message || (result.ok ? 'Gedaan.' : 'Mislukt.'), !result.ok);
    refresh();
  }

  function renderRequests() {
    const box = root.querySelector('[data-role="requests"]');
    box.textContent = '';
    const open = ((state.meta || {}).requests || []).filter((r) => r.state === 'open');
    box.append(h('strong', {}, 'Aanvragen van Claude'));
    if (!open.length) { box.append(h('p', { class: 'jobs-empty' }, 'Geen openstaande aanvragen.')); return; }
    open.forEach((r) => {
      const row = h('div', { class: 'jobs-field jobs-field--wide' },
        h('strong', {}, r.id), ` ${r.source_branch} · ${short(r.target_commit)} · ${r.mode} · taak ${r.task_id || '?'} · ` +
        `${(r.tests || []).length} test(s) · ${r.created_at}${r.note ? ' · "' + r.note + '"' : ''} `);
      const view = h('button', { type: 'button', class: 'btn btn--small' }, 'Bekijken');
      view.addEventListener('click', () => previewRequest(r));
      const reject = h('button', { type: 'button', class: 'btn btn--small btn--ghost' }, 'Afwijzen');
      reject.addEventListener('click', () => rejectRequest(r.id));
      row.append(view, ' ', reject);
      box.append(row);
    });
  }

  function renderGrants() {
    const box = root.querySelector('[data-role="grants"]');
    box.textContent = '';
    const meta = state.meta || {};
    const grants = meta.grants || [];
    if (!meta.secret_configured) box.append(h('p', { class: 'dialog-note', 'data-tone': 'bad' },
      'Er is geen goedkeuringscode ingesteld (OBF_INTEGRATION_SECRET_HASH): goedkeuren en uitvoeren zijn uit.'));
    if (meta.audit_ok === false) box.append(h('p', { class: 'dialog-note', 'data-tone': 'bad' }, 'Waarschuwing: de audittrail is niet intact (' + meta.audit_problem + ').'));
    if (!grants.length) { box.append(h('p', { class: 'jobs-empty' }, 'Nog geen toestemmingen.')); return; }
    grants.forEach((g) => {
      const busy = meta.running === g.id;
      const unfinished = g.state === 'used' && !g.result;
      const label = busy ? 'BEZIG' : unfinished ? 'ONDERBROKEN?' : ({ open: 'OPEN', used: 'GEBRUIKT', revoked: 'INGETROKKEN', expired: 'VERLOPEN',
        invalid: 'ONGELDIG' }[g.state] || g.state);
      const result = g.result ? ` · uitkomst: ${g.result.result}${g.result.recovery_branch ? ' · herstelbranch ' + g.result.recovery_branch : ''}` :
        (g.used && g.used.recovery_branch ? ` · herstelbranch ${g.used.recovery_branch}` : '');
      const why = g.state === 'invalid' ? ` (${g.problem === 'key_changed' ? 'Monitor 2 is herstart; opnieuw goedkeuren' : g.problem})` : '';
      const row = h('div', { class: 'jobs-field jobs-field--wide' },
        h('strong', {}, `${g.id} ${label}`),
        ` ${g.source_branch || ''}${why} → main (${g.mode || ''}) · ${short(g.expected_main_head)} → ${short(g.target_commit)} · door ${g.approved_by || '?'}, ` +
        `geldig tot ${g.expires_at || '?'}${g.request_id ? ' · aanvraag ' + g.request_id : ''}${result} `);
      if (g.state === 'open' && !busy) {
        const check = h('button', { type: 'button', class: 'btn btn--small' }, 'Controleren');
        check.addEventListener('click', () => checkGrant(g.id));
        const run = h('button', { type: 'button', class: 'btn btn--small btn--primary' }, 'Uitvoeren');
        run.addEventListener('click', () => execute(g));
        const revokeButton = h('button', { type: 'button', class: 'btn btn--small btn--ghost' }, 'Intrekken');
        revokeButton.addEventListener('click', () => revoke(g.id));
        row.append(check, ' ', run, ' ', revokeButton);
      }
      box.append(row);
    });
  }

  async function refresh() {
    const result = await call('GET');
    const meta = result.payload || {};
    state.meta = meta;
    if (!meta.enabled) { root.hidden = true; return; }
    root.hidden = false;
    const select = root.querySelector('#integ-repo');
    if (select.options.length !== (meta.repos || []).length) {
      select.textContent = '';
      (meta.repos || []).forEach((r) => select.append(h('option', { value: r }, r)));
    }
    renderAuto();
    renderRequests();
    renderGrants();
  }

  function values() {
    return {
      repository: root.querySelector('#integ-repo').value,
      source_branch: root.querySelector('#integ-branch').value.trim(),
    };
  }

  function secretValue() { return root.querySelector('#integ-secret').value; }
  function clearSecret() { root.querySelector('#integ-secret').value = ''; }

  async function preview() {
    if (state.busy) return;
    state.busy = true;
    state.request = null;
    say('Bezig met bekijken…');
    const result = await call('POST', { action: 'preview', ...values(), nonce: nonce() });
    state.busy = false;
    state.facts = result.ok ? result.payload.facts : null;
    say(result.ok ? '' : (result.payload.message || 'Bekijken mislukt.'), !result.ok);
    renderFacts();
  }

  async function previewRequest(request) {
    if (state.busy) return;
    state.busy = true;
    say('Bezig met bekijken…');
    const result = await call('POST', { action: 'preview', request_id: request.id, nonce: nonce() });
    state.busy = false;
    if (!result.ok) { state.facts = null; state.request = null; say(result.payload.message || 'Bekijken mislukt.', true); renderFacts(); return; }
    const req = result.payload.request;
    state.facts = result.payload.facts;
    state.request = { id: req.id, sha256: req.sha256, current: result.payload.request_current, tests: req.tests, mode: req.mode };
    const select = root.querySelector('#integ-repo');
    if (![...select.options].some((o) => o.value === req.repo)) select.append(h('option', { value: req.repo }, req.repo));
    select.value = req.repo;
    root.querySelector('#integ-branch').value = req.source_branch;
    root.querySelector('#integ-tests').value = (req.tests || []).join('\n');
    root.querySelector('#integ-mode').value = req.mode;
    root.querySelector('#integ-note').value = req.note || '';
    say('');
    renderFacts();
  }

  async function grant() {
    const facts = state.facts;
    const by = who();
    if (state.busy || !facts) return;
    if (!by) { say('Kies eerst bij Opdrachten wie je bent.', true); return; }
    if (!secretValue()) { say('Vul je goedkeuringscode in.', true); return; }
    const tests = root.querySelector('#integ-tests').value.split('\n').map((t) => t.trim()).filter(Boolean);
    const ok = window.confirm(`Toestemming geven om ${facts.source_branch} (${short(facts.source_tip)}) één keer naar main te integreren, ` +
      `op main-stand ${short(facts.main_head)}?\n\nDit voert nog niets uit; uitvoeren is een aparte stap. Niets wordt gepusht.`);
    if (!ok) return;
    state.busy = true;
    const body = { action: 'grant', ...values(), expected_main_head: facts.main_head, target_commit: facts.source_tip,
      mode: root.querySelector('#integ-mode').value, tests, by, secret: secretValue(), note: root.querySelector('#integ-note').value,
      nonce: nonce() };
    if (root.querySelector('#integ-protected').checked) body.allow_protected = true;
    if (state.request) { body.request_id = state.request.id; body.request_sha256 = state.request.sha256; }
    const job = root.querySelector('#integ-job').value.trim();
    if (job) body.job_id = job;
    const result = await call('POST', body);
    state.busy = false;
    clearSecret();
    say(result.ok ? `Toestemming ${result.payload.grant.id} gegeven. Controleer en voer hem hieronder uit.` : (result.payload.message || 'Mislukt.'), !result.ok);
    if (result.ok) { state.facts = null; state.request = null; renderFacts(); }
    refresh();
  }

  async function checkGrant(id) {
    const by = who();
    if (!by) { say('Kies eerst bij Opdrachten wie je bent.', true); return; }
    const result = await call('POST', { action: 'check', grant_id: id, by, nonce: nonce() });
    const info = result.payload.check || {};
    say(result.ok ? `${id}: kan nu worden uitgevoerd (${info.commits} commit(s), ${(info.files || []).length} bestand(en)).` :
      (result.payload.message || 'Controle mislukt.'), !result.ok);
  }

  async function execute(g) {
    const by = who();
    if (state.busy) return;
    if (!by) { say('Kies eerst bij Opdrachten wie je bent.', true); return; }
    if (!secretValue()) { say('Vul je goedkeuringscode in.', true); return; }
    const typed = window.prompt(`Typ de eerste 12 tekens van de commit om ${g.id} nu uit te voeren (${g.source_branch} → main):\n${g.target_commit}`);
    if (typed === null) return;
    state.busy = true;
    const result = await call('POST', { action: 'execute', grant_id: g.id, by, secret: secretValue(), confirm: typed.trim(), nonce: nonce() });
    state.busy = false;
    clearSecret();
    say(result.ok ? (result.payload.message || `${g.id}: uitgevoerd.`) : (result.payload.message || 'Mislukt.'), !result.ok);
    refresh();
  }

  async function rejectRequest(id) {
    const by = who();
    if (!by) { say('Kies eerst bij Opdrachten wie je bent.', true); return; }
    const reason = window.prompt('Reden (optioneel):') || '';
    const result = await call('POST', { action: 'reject', request_id: id, by, reason, nonce: nonce() });
    say(result.ok ? `Aanvraag ${id} afgewezen.` : (result.payload.message || 'Mislukt.'), !result.ok);
    refresh();
  }

  async function revoke(id) {
    const by = who();
    if (!by) { say('Kies eerst bij Opdrachten wie je bent.', true); return; }
    const result = await call('POST', { action: 'revoke', grant_id: id, by, nonce: nonce() });
    say(result.ok ? `Toestemming ${id} ingetrokken.` : (result.payload.message || 'Mislukt.'), !result.ok);
    refresh();
  }

  function start() {
    const anchor = document.getElementById('jobs-columns');
    if (!anchor || !anchor.parentNode) return;
    root = build();
    anchor.parentNode.insertBefore(root, anchor);
    root.querySelector('[data-role="preview"]').addEventListener('click', preview);
    root.querySelector('[data-role="grant"]').addEventListener('click', grant);
    root.querySelector('#integ-mode').addEventListener('change', renderFacts);
    refresh();
    window.setInterval(() => { if (!document.hidden) refresh(); }, POLL_MS);
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', start); else start();
})();
