/* Monitor 2 — de Studio-OBF-monitor: een dunne weergave van Claude Runner 2 op vier machines.
 *
 * Deze pagina beslist niets. De server (obf_monitor2) leest Runner 2 zelf en stelt per
 * machine (MAC, LINUX, GERARD, WINDOWS) een kaart samen; hier wordt die kaart alleen
 * getekend. Welke toestand WACHT OP ACTIE is, welke knoppen zinvol zijn, de vraag zelf en
 * of er hersteld wordt komt kant-en-klaar uit Runner 2. Pauze, Hervatten, Stop en
 * Beantwoorden gaan via de server naar dezelfde controllerfuncties als de rest van Runner 2.
 *
 * Wat de pagina wél zelf bijhoudt:
 *  - per machine een optionele taakkeuze (`sel`); de kaart heeft geen keuzelijst meer en toont alleen de actuele taak;
 *  - de lokale geluid-/spraakinstellingen (localStorage, alleen deze machine).
 */

'use strict';

const POLL_OK_MS = 3000;
const POLL_FAIL_MS = 6000;
// Een poll die niet antwoordt (netwerkwissel, slapende laptop, haperende tailnetroute) mag het pollen niet
// voor altijd vasthouden: na zoveel ms wordt hij afgebroken en volgt een nieuwe.
const POLL_TIMEOUT_MS = 15000;
const SOUND_KEY = 'obf-monitor2-sound';
// Meldingsgeheugen van deze browser: welk volgnummer van het meldingsjournaal van Monitor 2 al verwerkt is.
const NOTIFY_KEY = 'obf-monitor2-notify';
// Een melding die langer geleden begon klinkt niet meer (maar blijft zichtbaar als "gemist"); één die niet kon
// klinken omdat de browser geluid blokkeerde blijft zo lang klaarstaan voor het moment dat de gebruiker ontgrendelt.
const CATCHUP_MAX_MS = 15 * 60 * 1000;
const PENDING_MAX_MS = 30 * 60 * 1000;
const KEEP_ANNOUNCED = 200;
// Standaard (2026-09-26): geluid en spraak aan, stil van 20:00 tot 07:00. Geldt
// alleen zolang deze browser op dit adres nog geen eigen keuze bewaarde.
const SOUND_DEFAULTS = { sound: true, speech: true, quietFrom: '20:00', quietTo: '07:00' };

// Naast het lampje en het woord een vorm, zodat de toestand ook zonder kleur te lezen is.
const STATE_ICONS = {
  wacht_op_actie: '!', bezig: '', gepauzeerd: '❚❚', herstellen: '↻', klaar: '✓', mislukt: '✕', gestopt: '■',
  rust: '○', vertraagd: '…', onbereikbaar: '⚠', niet_geinstalleerd: '—',
};
const STATE_TEXT = {
  wacht_op_actie: 'WACHT OP ACTIE', bezig: 'BEZIG', gepauzeerd: 'GEPAUZEERD', herstellen: 'HERSTELLEN', klaar: 'KLAAR',
  mislukt: 'MISLUKT', gestopt: 'GESTOPT', rust: 'GEEN TAAK', vertraagd: 'GEGEVENS VERTRAAGD', onbereikbaar: 'NIET BEREIKBAAR',
  niet_geinstalleerd: 'NIET GEÏNSTALLEERD',
};
const STATE_TONE = {
  wacht_op_actie: 'human', bezig: 'run', gepauzeerd: 'off', herstellen: 'warn', klaar: 'ok', mislukt: 'alert',
  gestopt: 'off', rust: 'ok', vertraagd: 'warn', onbereikbaar: 'alert', niet_geinstalleerd: 'off',
};

const page = {
  serverGeneration: null, // generatie van Monitor 2 zelf; een nieuwe = nieuw actiecookie
  actionsEnabled: false,
  sel: {},                // node -> getoonde taak-id
  last: null,             // laatste dashboardantwoord
  timer: null,
  failures: 0,
  refetch: false,
  dialog: null,           // { kind, node }; bij een rapport ook { task, status, report, error }
  notes: {},              // node -> { text, tone } laatste knopmelding
  sound: { ...SOUND_DEFAULTS },
  notify: null,           // meldingsgeheugen { journal, seq, announced } (zie planNotifications)
  busy: false,
  view: null,             // getoond scherm: 'monitor' of 'opdrachten' (zie showView)
};

/* ================================================================ zuiver */

/** De vraag aan de server: per machine de getoonde taak. */
function dashboardQuery(state, allIds) {
  const params = new URLSearchParams();
  (allIds || Object.keys(state.sel)).forEach((id) => {
    if (state.sel[id]) params.set('sel.' + id, state.sel[id]);
  });
  return params.toString();
}

/** Een afgeronde of gestopte taak is geen actueel werk meer: de kaart toont dan alleen de status. */
function isFinished(state) {
  return state === 'klaar' || state === 'gestopt';
}

/** De toestand van een machinekaart: die van zijn taak, anders die van Runner 2 zelf. */
function nodeState(node) {
  if (!node.installed) return 'niet_geinstalleerd';
  if (!node.available) return node.data_delay ? 'vertraagd' : 'onbereikbaar';
  if (node.task && STATE_TEXT[node.task.state]) return node.task.state;
  return 'rust';
}

function stateTone(state) {
  return STATE_TONE[state] || 'off';
}

/** "laatste stand 12 s geleden" voor een node waarvan de gegevens even achterlopen; anders null. */
function delayText(node) {
  const delay = node && node.data_delay;
  if (!delay) return null;
  return delay.seconds ? 'Gegevens vertraagd — laatste stand ' + delay.seconds + ' s geleden.'
    : 'Gegevens vertraagd — de node antwoordt nog niet.';
}

/** Lokale klok HH:MM (24 uur) van een ISO-tijdstip; null als het ontbreekt of onleesbaar is. */
function clockLabel(iso) {
  if (!iso) return null;
  const at = new Date(iso);
  if (Number.isNaN(at.getTime())) return null;
  return String(at.getHours()).padStart(2, '0') + ':' + String(at.getMinutes()).padStart(2, '0');
}

/**
 * Het moment waarop de runner echt klaar werd, als HH:MM; anders null. Een gewone taak: zijn eigen
 * einde. Een autonome keten: pas het einde van de keten; de voltooiing van een tussenschakel telt
 * niet zolang de keten zelf verdergaat.
 */
function doneClock(task) {
  if (!task || task.state !== 'klaar') return null;
  const sup = task.supervisor;
  if (sup) return sup.state === 'project_complete' ? clockLabel(sup.ended_at) : null;  // pas het einde van de hele run
  const chain = task.chain;
  if (!chain) return clockLabel(task.finished_at);
  if (chain.state === 'active') return null;
  return clockLabel(chain.ended_at);
}

/** De statustekst, bij KLAAR aangevuld met het lokale klaartijdstip ("KLAAR · 19:42"). */
function stateLabel(state, task) {
  const clock = state === 'klaar' ? doneClock(task) : null;
  return clock ? STATE_TEXT[state] + ' · ' + clock : STATE_TEXT[state];
}

/**
 * Aantal openstaande menselijke actiepunten dat het Machines-blok toont (ACTIE OPEN · N), of 0. Dit is alleen een teller: de
 * inhoud vraagt de gebruiker aan ChatGPT. Bij WACHT OP ACTIE (een blokkerende vraag) hoort de teller er niet bij,
 * en een node die niet te lezen is toont niets.
 */
function actionOpenCount(node) {
  if (!node || !node.installed || !node.available) return 0;
  if (nodeState(node) === 'wacht_op_actie') return 0;
  const n = node.action_open;
  return Number.isInteger(n) && n > 0 ? n : 0;
}

function actionOpenText(count) {
  return 'ACTIE OPEN \u00b7 ' + count;
}

/**
 * Supervisie (ChatGPT boven de Claude-runners): het label van de ergste aandacht op deze machine, of null.
 * GEBLOKKEERD (dossier stuk of lease-limiet) gaat voor NODIG (er wacht een beslissing); een node die niet te lezen is toont niets.
 */
function supervisionAttention(node) {
  if (!node || !node.installed || !node.available) return null;
  const sv = node.supervision;
  if (!sv || !Array.isArray(sv.projects)) return null;
  const open = sv.projects.filter((p) => p && p.attention);
  if (!open.length) return null;
  const blocked = open.some((p) => p.blocked);
  return { label: blocked ? 'SUPERVISIE GEBLOKKEERD' : 'SUPERVISIE NODIG', tone: blocked ? 'alert' : 'human', count: open.length };
}

/** De pil op de kaart: de taakstatus in het Nederlands, anders de status van Runner 2 op die machine. */
function pillFor(node) {
  const flow = queueFlowStatus(node);
  if (flow) {
    const short = { 'GEBLOKKEERD · INTEGRATIECONTROLE': 'GEBLOKKEERD', 'KETENSTATUS ONBEKEND': 'KETEN ONBEKEND',
      'WACHT OP INTEGRATIECONTROLE': 'CONTROLE', 'INTEGRATIE BEZIG': 'INTEGRATIE',
      'WACHTRIJ GEPAUZEERD': 'GEPAUZEERD', 'VOLGENDE TAAK START OF LOOPT': 'START',
      'WACHT OP VOLGENDE TAAK': 'WACHT', 'WACHT OP GOEDKEURING': 'GOEDKEURING',
      'WACHT OP VRIJGAVE': 'VRIJGAVE', 'WACHT OP VOORGANGER': 'VOORGANGER' };
    return { ...flow, text: short[flow.text] || flow.text };
  }
  const state = nodeState(node);
  if (node.task && node.available) return { tone: stateTone(state), text: stateLabel(state, node.task), pulse: state === 'wacht_op_actie' || state === 'herstellen' };
  if (state === 'vertraagd') return { tone: 'warn', text: STATE_TEXT.vertraagd, pulse: false };
  return { tone: node.tone || stateTone(state), text: node.headline || STATE_TEXT[state], pulse: false };
}

/** Welke knoppen de kaart toont; de controller weigert een niet-toegestane actie zelf ook. */
function cardButtons(node, actionsEnabled) {
  const task = node.installed && node.available ? node.task : null;
  const allowed = task ? task.actions : {};
  const off = (name, why) => ({
    disabled: !actionsEnabled || !allowed[name],
    why: !actionsEnabled ? 'Monitor 2 draait zonder bediening (alleen lezen).' : (allowed[name] ? '' : why),
  });
  return {
    pause: { ...off('pause', 'Pauzeren kan alleen terwijl de taak bezig is.'),
      label: task && task.pausing ? 'Pauze gevraagd' : 'Pauze' },
    resume: off('resume', 'Hervatten kan bij een gepauzeerde of mislukte taak.'),
    stop: off('stop', 'Er is hier nu niets dat gestopt kan worden.'),
    answer: { hidden: !task || !task.question, disabled: !actionsEnabled || !allowed.answer },
  };
}

/** De POST-body voor één knop; `text`/`reason` alleen als de knop dat nodig heeft. */
function actionBody(node, action, nonce, extra) {
  const task = node.task;
  if (!task || !task.task_id) return null;
  const body = { node: node.id, task: task.task_id, action, nonce };
  Object.keys(extra || {}).forEach((key) => { body[key] = extra[key]; });
  return body;
}

/** De rapportknop van een kaart: alleen als Runner 2 op die machine een leesbaar eindrapport kent (ook van een eerdere taak). */
function reportButton(node) {
  const report = node && node.installed && node.available ? node.report : null;
  if (!report || !report.task_id) return { hidden: true, title: '' };
  const clock = clockLabel(report.finished_at);
  const parts = [report.title || report.task_id, report.title ? report.task_id : null, clock ? 'klaar om ' + clock : null];
  return { hidden: false, title: 'Eindrapport van ' + parts.filter(Boolean).join(' · ') };
}

/** Het adres waarop de server het eindrapport van één taak levert (alleen lezen). */
function reportUrl(nodeId, taskId) {
  return '/api/report?' + new URLSearchParams({ node: nodeId, task: taskId }).toString();
}

/** "1 u 05 m" -- alleen de schrijfwijze; de seconden komen van Runner 2. */
function durationLabel(seconds) {
  if (typeof seconds !== 'number' || !Number.isFinite(seconds) || seconds < 0) return null;
  const total = Math.round(seconds);
  if (total < 60) return total + ' s';
  const minutes = Math.floor(total / 60);
  if (minutes < 60) return minutes + ' min';
  const hours = Math.floor(minutes / 60);
  return hours + ' u ' + String(minutes % 60).padStart(2, '0') + ' m';
}

const SUPERVISOR_PHASES = {
  chain_running: 'keten loopt', starting_chain: 'nieuwe keten wordt gestart', between_chains: 'beoordeling verwerken',
  judging: 'ketenresultaat wordt beoordeeld', waiting_for_human: 'wacht op actie', paused: 'gepauzeerd', failed: 'mislukt',
  stopped: 'gestopt', ended: 'afgelopen',
};
const SUPERVISOR_END = {
  project_complete: 'project voltooid', blocked: 'geblokkeerd', limit_reached: 'limiet bereikt', stopped: 'gestopt', failed: 'mislukt',
};

/** Eén regel over de supervisorrun ("keten 2 van 4 — ketenresultaat wordt beoordeeld"), of null zonder supervisor. */
function supervisorText(task) {
  const sup = task && task.supervisor;
  if (!sup) return null;
  const chain = sup.chain ? 'keten ' + sup.chain + (sup.max_chains ? ' van ' + sup.max_chains : '') : 'autonome run';
  const milestone = sup.chain_kind === 'quality' ? ' (kwaliteitsmijlpaal)' : '';
  const what = sup.state === 'active' ? (SUPERVISOR_PHASES[sup.phase] || sup.phase) : (SUPERVISOR_END[sup.state] || sup.state);
  let text = chain + milestone + ' — ' + what;
  if (sup.state !== 'active' && sup.stopped_because && sup.stopped_because.message) text += ': ' + sup.stopped_because.message;
  return text;
}

/** De hersteltekst: alleen tonen als er iets te vertellen valt, zonder technische ruis. */
function recoveryText(task) {
  const rec = task && task.recovery;
  if (!rec) return null;
  const max = rec.max_attempts ? ' van ' + rec.max_attempts : '';
  if (rec.active) {
    const watch = rec.watch_healthy === false ? ' — de watch meldt zich niet' : '';
    return 'De taak wordt hersteld (poging ' + Math.max(1, rec.attempts) + max + '), '
      + (rec.same_session ? 'dezelfde sessie wordt hervat' : 'een nieuwe sessie wordt gestart') + watch + '.';
  }
  if (rec.attempts) {
    return 'Hersteld na ' + rec.attempts + (rec.attempts === 1 ? ' onderbreking' : ' onderbrekingen') + max
      + (rec.same_session ? ' — dezelfde sessie hervat.' : '.');
  }
  return null;
}

/**
 * Hoe een machine op de pagina heet: de weergavenaam van de server (LINUX = "STUDIO"), anders het label.
 * ``node.label`` en ``node.id`` blijven de Runner-2-node-ID voor route, API, wachtrij en historie.
 */
function nodeName(node) {
  return (node && (node.name || node.label)) || '';
}

/** De kopregel: draait Runner 2 waar het draaien moet? Uit de machines zelf. */
function summaryText(nodes) {
  const list = nodes || [];
  // Een node waar deze machine geen route naartoe heeft (route_missing, GERARD) is geen storing van Runner 2 zelf:
  // hij wordt eerlijk als 'niet bereikbaar vanaf hier' genoemd en telt niet mee als actief of als aandachtspunt.
  const unreachable = list.filter((node) => node.installed && node.route_missing);
  const installed = list.filter((node) => node.installed && !node.route_missing);
  const down = installed.filter((node) => !node.available || (node.watch && node.watch.healthy === false));
  const human = installed.filter((node) => node.task && node.task.state === 'wacht_op_actie');
  const tail = unreachable.length ? ' · niet bereikbaar vanaf hier: ' + unreachable.map(nodeName).join(', ') : '';
  if (human.length) return { tone: 'human', text: 'WACHT OP ACTIE — ' + human.map(nodeName).join(', ') + tail };
  if (!installed.length) return { tone: unreachable.length ? 'warn' : 'off', text: unreachable.length ? 'Runner 2 niet te lezen' + tail : 'Runner 2 is nergens geïnstalleerd' };
  if (down.length) return { tone: 'alert', text: 'Runner 2 vraagt aandacht — ' + down.map(nodeName).join(', ') + tail };
  return { tone: 'ok', text: 'Runner 2 actief op ' + installed.map(nodeName).join(', ') + tail };
}

/** Korte statuslabels voor de machineregel (zelfde toestanden als STATE_TEXT, alleen compacter). */
const MACHINE_STATUS_TEXT = {
  wacht_op_actie: 'WACHT', bezig: 'BEZIG', gepauzeerd: 'GEPAUZEERD', herstellen: 'HERSTELLEN', klaar: 'KLAAR',
  mislukt: 'FOUT', gestopt: 'GESTOPT', rust: 'GEEN TAAK', vertraagd: 'VERTRAAGD', onbereikbaar: 'ONBEREIKBAAR',
  niet_geinstalleerd: 'UIT',
};

/** De actuele Runner-2-status van een machine voor de vaste statuskolom van het Machines-blok. */
function machineStatus(node) {
  const state = nodeState(node);
  return { state, tone: stateTone(state), text: MACHINE_STATUS_TEXT[state] || STATE_TEXT[state],
    pulse: state === 'wacht_op_actie' || state === 'herstellen' };
}

/**
 * Wat een machine in het bovenste Machines-blok na zijn status toont: de badges, of niets.
 * Supervisie (SUPERVISIE NODIG/GEBLOKKEERD) en ACTIE OPEN zijn onafhankelijk: staan ze er allebei, dan komen ze
 * allebei, in die volgorde, ieder met zijn eigen venster. Alleen zonder die twee volgt één aandachtspunt (watch of vertraging).
 * De status zelf staat in machineStatus; bij WACHT OP ACTIE of een onleesbare node komt er geen badge.
 */
function machineMarkers(node) {
  if (!node.installed || !node.available) return [];
  if (nodeState(node) === 'wacht_op_actie') return [];
  const markers = [];
  const sup = supervisionAttention(node);
  if (sup) markers.push({ kind: 'supervision', tone: sup.tone, text: sup.label + (sup.count > 1 ? ' \u00b7 ' + sup.count : ''), pulse: sup.tone === 'alert' });
  const open = actionOpenCount(node);
  if (open) markers.push({ kind: 'action', tone: 'human', text: actionOpenText(open), pulse: false });
  if (markers.length) return markers;
  if (node.watch && node.watch.healthy === false) return [{ kind: 'attention', tone: 'warn', text: 'WATCH STAAT NIET', pulse: false }];
  if (node.data_delay) return [{ kind: 'attention', tone: 'warn', text: STATE_TEXT.vertraagd, pulse: false }];
  return [];
}

/** De eerste badge van machineMarkers, of een lege markering (text ''). */
function machineMarker(node) {
  const first = machineMarkers(node)[0];
  if (first) return first;
  if (!node.installed || !node.available) return { kind: '', tone: 'off', text: '', pulse: false };
  if (nodeState(node) === 'wacht_op_actie') return { kind: '', tone: 'human', text: '', pulse: false };
  return { kind: '', tone: node.tone || 'ok', text: '', pulse: false };
}

function parseClock(value) {
  const match = /^([01][0-9]|2[0-3]):([0-5][0-9])$/.exec(String(value || '').trim());
  return match ? Number(match[1]) * 60 + Number(match[2]) : null;
}

function inQuietHours(minutes, from, to) {
  if (from === null || to === null || from === to) return false;
  return from < to ? (minutes >= from && minutes < to) : (minutes >= from || minutes < to);
}

/** Het meldingsgeheugen uit localStorage, met veilige standaardwaarden. */
function normalizeNotifyMemory(raw) {
  const source = raw && typeof raw === 'object' ? raw : {};
  return {
    journal: typeof source.journal === 'string' ? source.journal : null,
    seq: Number.isInteger(source.seq) && source.seq >= 0 ? source.seq : null,
    announced: Array.isArray(source.announced) ? source.announced.filter((key) => typeof key === 'string').slice(-KEEP_ANNOUNCED) : [],
  };
}

/**
 * Wat is er nieuw in het meldingsjournaal van Monitor 2 sinds deze pagina het laatst keek? Zuiver.
 *
 * ``feed`` = { id, head, events: [{ seq, key, kind, at, seen_at, baseline, ... }] } uit het dashboardantwoord. De
 * server geeft elke gebeurtenis (taak klaar, keten echt klaar, mislukt, wacht op actie) één volgnummer; de pagina
 * onthoudt het hoogste verwerkte nummer. Daardoor gaat niets verloren als een poll gemist werd (tabblad op de
 * achtergrond, node of netwerk even weg, pagina herladen, Monitor 2 herstart) en klinkt niets tweemaal (ook niet via
 * een tweede sleutelcontrole). Eerste keer, nieuw of vervangen journaal: alleen bijwerken, niets melden.
 *
 * ``isQuiet(ms)`` zegt of een tijdstip in de stille uren valt; geluid blijft dan uit, de melding blijft zichtbaar.
 * Geeft ``{ memory, fresh: [{ event, moment, audible }] }`` terug.
 */
function planNotifications(feed, memory, nowMs, isQuiet) {
  const mem = normalizeNotifyMemory(memory);
  if (!feed || typeof feed.id !== 'string' || !Number.isInteger(feed.head) || !Array.isArray(feed.events)) return { memory: mem, fresh: [] };
  const known = new Set(mem.announced);
  if (mem.journal !== feed.id || mem.seq === null || mem.seq > feed.head) {
    feed.events.forEach((event) => known.add(event.key));
    return { memory: { journal: feed.id, seq: feed.head, announced: [...known].slice(-KEEP_ANNOUNCED) }, fresh: [] };
  }
  const fresh = [];
  feed.events.filter((event) => event.seq > mem.seq).sort((a, b) => a.seq - b.seq).forEach((event) => {
    if (known.has(event.key)) return;
    known.add(event.key);
    if (event.baseline) return;
    const at = Date.parse(event.at);
    const seen = typeof event.seen_at === 'number' ? event.seen_at * 1000 : NaN;
    // Het moment waarop de pagina het had kunnen weten: de latere van het einde zelf en de eerste keer dat Monitor 2 het zag.
    const moment = Math.max(Number.isNaN(at) ? 0 : at, Number.isNaN(seen) ? 0 : seen) || nowMs;
    fresh.push({ event, moment, audible: nowMs - moment <= CATCHUP_MAX_MS && !isQuiet(moment) && !isQuiet(nowMs) });
  });
  return { memory: { journal: feed.id, seq: feed.head, announced: [...known].slice(-KEEP_ANNOUNCED) }, fresh };
}

function normalizeSound(raw) {
  const source = raw && typeof raw === 'object' ? raw : {};
  const clock = (value, fallback) => (value === '' ? '' : (parseClock(value) === null ? fallback : value));
  return {
    sound: typeof source.sound === 'boolean' ? source.sound : SOUND_DEFAULTS.sound,
    speech: typeof source.speech === 'boolean' ? source.speech : SOUND_DEFAULTS.speech,
    quietFrom: clock(source.quietFrom, SOUND_DEFAULTS.quietFrom),
    quietTo: clock(source.quietTo, SOUND_DEFAULTS.quietTo),
  };
}

/** "3 min geleden" -- alleen de schrijfwijze; het moment komt van de runner. */
function agoLabel(iso, nowMs) {
  if (!iso) return null;
  const at = Date.parse(iso);
  if (Number.isNaN(at)) return null;
  const seconds = Math.max(0, Math.round(((nowMs || Date.now()) - at) / 1000));
  if (seconds < 45) return 'zojuist';
  const minutes = Math.round(seconds / 60);
  if (minutes < 60) return minutes + ' min geleden';
  const hours = Math.floor(minutes / 60);
  if (hours < 48) return hours + ' u ' + String(minutes % 60).padStart(2, '0') + ' m geleden';
  return Math.floor(hours / 24) + ' dagen geleden';
}

function resetText(iso, nowMs) {
  if (!iso) return 'reset onbekend';
  const at = Date.parse(iso);
  if (Number.isNaN(at)) return 'reset onbekend';
  const left = Math.max(0, Math.round((at - (nowMs || Date.now())) / 60000));
  if (left <= 0) return 'reset nu';
  const days = Math.floor(left / 1440);
  const hours = Math.floor((left % 1440) / 60);
  const minutes = left % 60;
  if (days) return 'reset over ' + days + 'd ' + hours + 'u';
  if (hours) return 'reset over ' + hours + 'u ' + minutes + 'm';
  return 'reset over ' + minutes + 'm';
}

function newNonce() {
  try {
    if (window.crypto && typeof window.crypto.randomUUID === 'function') {
      return window.crypto.randomUUID().replace(/-/g, '');
    }
  } catch (err) { /* val terug */ }
  return (Date.now().toString(16) + Math.random().toString(16).slice(2) + '00000000').slice(0, 32);
}

/* ================================================================ DOM */

const el = (id) => document.getElementById(id);

function make(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined && text !== null) node.textContent = String(text);
  return node;
}

function clear(node) {
  while (node.firstChild) node.removeChild(node.firstChild);
}

function led(tone, pulse, big) {
  const node = make('span', 'led' + (big ? ' led--big' : ''));
  node.dataset.tone = tone;
  node.dataset.pulse = pulse ? 'true' : 'false';
  node.setAttribute('aria-hidden', 'true');
  return node;
}

function readStored(key, fallback) {
  try {
    const raw = window.localStorage.getItem(key);
    return raw === null ? fallback : JSON.parse(raw);
  } catch (err) { return fallback; }
}

function writeStored(key, value) {
  try { window.localStorage.setItem(key, JSON.stringify(value)); } catch (err) { /* geen opslag */ }
}

/* ---------------------------------------------------------------- poll */

async function poll() {
  if (page.timer) { clearTimeout(page.timer); page.timer = null; }
  let data = null;
  try {
    const ids = page.last ? page.last.nodes.map((node) => node.id) : null;
    const ctl = typeof AbortController === 'undefined' ? null : new AbortController();
    const guard = ctl ? setTimeout(() => ctl.abort(), POLL_TIMEOUT_MS) : null;
    try {
      const response = await fetch('/api/dashboard?' + dashboardQuery(page, ids), { cache: 'no-store', signal: ctl ? ctl.signal : undefined });
      if (!response.ok) throw new Error('HTTP ' + response.status);
      data = await response.json();
    } finally {
      if (guard) clearTimeout(guard);
    }
    page.failures = 0;
    showConnection(null);
  } catch (err) {
    page.failures += 1;
    invalidateIntegrator();
    renderStoringen(null);
    showConnection('Monitor 2 krijgt geen antwoord van zijn eigen server (' + page.failures +
      'x). Het werk van Runner 2 loopt gewoon door.');
  }
  if (data) {
    try { render(data); } catch (err) { showConnection('Weergavefout: ' + err.message); }
  }
  page.timer = setTimeout(poll, data ? POLL_OK_MS : POLL_FAIL_MS);
}

function showConnection(message) {
  const bar = el('connection');
  bar.hidden = !message;
  bar.textContent = message || '';
}

/* -------------------------------------------------------------- render */

function render(data) {
  // Eerst de meldingen, los van het tekenen: een weergavefout verderop mag nooit een melding laten verdwijnen.
  try { processNotifications(data.notify); } catch (err) { /* de pagina tekent door; de volgende poll probeert opnieuw */ }
  // Het actiecookie hoort bij één Monitor 2-proces (SEC-06: het token staat niet meer
  // in dit antwoord). Na een herstart haalt alleen de pagina zelf een nieuw cookie;
  // ververs dus, maar nooit midden in een open dialoog.
  if (page.serverGeneration && data.generation && data.generation !== page.serverGeneration && !page.dialog) {
    window.location.reload();
    return;
  }
  page.serverGeneration = data.generation || page.serverGeneration;
  page.last = data;
  page.actionsEnabled = data.actions_enabled !== false;
  renderUsage(data.usage);
  ['linux','gerard'].forEach(id => receiveIntegrator(data.integrators ? data.integrators[id] : (id === 'linux' ? data.integrator : null), id));
  renderMachines(data.nodes);
  renderCards(data.nodes);
  renderStoringen(data.storingen);
  if (typeof window !== 'undefined' && window.obfDiscussions) window.obfDiscussions.renderCards();
  renderWebserver(data.nuc);
  renderTasks(data.nodes);
  renderNotifications(data.notifications);
  renderSummary(data.nodes);
  if (jobsPage.data) renderJobs();
  if (page.dialog) refreshDialog();
}

/* ---- Machines-blok bovenaan: vier regels, één per machine ---- */

function renderMachines(nodes) {
  const list = el('machine-list');
  clear(list);
  nodes.forEach((node) => {
    const markers = machineMarkers(node);
    const item = make('li', 'machine-row');
    item.dataset.node = node.id;
    item.dataset.state = nodeState(node);
    item.dataset.marker = markers.map((m) => m.kind).join(' ');
    item.appendChild(make('strong', 'machine-name', nodeName(node)));
    const status = machineStatus(node);
    item.dataset.status = status.state;
    const statusCell = make('span', 'machine-status');
    statusCell.dataset.tone = status.tone;
    statusCell.appendChild(led(status.tone, status.pulse));
    statusCell.appendChild(make('span', null, status.text));
    item.appendChild(statusCell);
    // Altijd aanwezig, ook leeg: de rij houdt zo vaste kolommen en niets verspringt als de tekst wisselt.
    // Elke ACTIE OPEN- en SUPERVISIE-badge is zelf de knop naar het venster met de punten van deze machine.
    const flags = make('span', 'machine-flags');
    (markers.length ? markers : [null]).forEach((marker) => flags.appendChild(machineFlag(node, marker)));
    item.appendChild(flags);
    list.appendChild(item);
  });
  const installed = nodes.filter((node) => node.installed && !node.route_missing).length;
  const unreachable = nodes.filter((node) => node.route_missing).length;
  el('machines-hint').textContent = unreachable
    ? installed + ' van ' + nodes.length + ' machines gelezen · ' + unreachable + ' niet bereikbaar vanaf hier.'
    : installed === 1
      ? '1 van ' + nodes.length + ' machines draait Runner 2.'
      : installed + ' van ' + nodes.length + ' machines draaien Runner 2.';
}

/** Eén badge in de machineregel; zonder markering een lege plaatshouder. */
function machineFlag(node, marker) {
  const clickable = Boolean(marker) && (marker.kind === 'action' || marker.kind === 'supervision');
  const flag = make(clickable ? 'button' : 'span', 'machine-flag');
  if (!marker) return flag;
  flag.dataset.kind = marker.kind;
  if (clickable) {
    flag.type = 'button';
    const supervision = marker.kind === 'supervision';
    flag.title = (supervision ? 'Toon de supervisiepunten van ' : 'Toon de open acties van ') + nodeName(node);
    flag.addEventListener('click', () => (supervision ? openSupervision(node.id) : openActions(node.id)));
  }
  flag.dataset.tone = marker.tone;
  flag.appendChild(led(marker.tone, marker.pulse));
  flag.appendChild(make('span', null, marker.text));
  return flag;
}

/* ---- kaarten: één keer opbouwen, daarna alleen bijwerken ---- */

function backupBadge(node) {
  let b = node.backup || {};
  if (Number.isFinite(b.collected) && Date.now() / 1000 - b.collected > 180) b = {state: 'unknown', detail: 'Statusoverdracht verouderd'};
  const tone = {ok:'ok', running:'busy', failed:'alert', warning:'busy', prepared:'off', unknown:'off'}[b.state] || 'off';
  let text;
  if (node.id === 'windows') {
    if (Number.isFinite(b.free_bytes) && Number.isFinite(b.total_bytes) && b.total_bytes > 0 && b.state !== 'unknown') {
      const free = b.free_bytes >= 1e12 ? (b.free_bytes / 1e12).toFixed(2) + ' TB' : Math.round(b.free_bytes / 1e9) + ' GB';
      text = 'Backupruimte: ' + free + ' vrij';
    } else text = 'Backupruimte: onbekend';
  } else {
    text = 'Backup: ' + ({ok:'klaar',running:'bezig',failed:'fout',prepared:'in voorbereiding',unknown:'onbekend',warning:'aandacht'}[b.state] || 'onbekend');
    if (b.state === 'running' && Number.isFinite(b.percent) && b.percent >= 0 && b.percent <= 100) text += ' · ' + Math.round(b.percent) + '%';
  }
  const success = b.success && new Date(b.success);
  const last = success && Number.isFinite(success.getTime()) ? 'Laatste succes: ' + success.toLocaleString('nl-NL',{day:'2-digit',month:'2-digit',hour:'2-digit',minute:'2-digit'}) : '';
  return {text, tone, detail: [b.detail,last].filter(Boolean).join(' · '), last};
}

function renderBackup(target, node) {
  const badge = backupBadge(node);
  target.dataset.tone = badge.tone;
  target.textContent = badge.text;
  target.title = badge.detail;
  target.setAttribute('aria-label', badge.text + (badge.detail ? '. ' + badge.detail : ''));
}

const cardNodes = {};

function buildCard(node) {
  const root = make('article', 'card');
  root.dataset.platform = node.platform;
  // De kaart toont de weergavenaam (LINUX heet hier "STUDIO"); node.label blijft de node-ID voor al het andere.
  root.setAttribute('aria-label', nodeName(node));

  const scene = make('div', 'card-scene');
  const id = make('div', 'card-id');
  const name = make('div', 'card-name');
  name.appendChild(make('strong', null, nodeName(node)));
  name.appendChild(make('span', null, node.subtitle));
  id.appendChild(name);
  scene.appendChild(id);
  const status = make('div', 'card-status');
  const pill = make('div', 'pill');
  status.appendChild(pill);
  // De NUC (node-ID linux) toont direct onder de machinestatus ook de status van de webserver.
  const web = node.id === 'linux' ? make('div', 'card-web') : null;
  if (web) status.appendChild(web);
  scene.appendChild(status);
  const chain = ['linux','gerard'].includes(node.id) ? make('section', 'studio-chain') : null;
  if (chain) { chain.setAttribute('aria-label', 'Keten van de ' + nodeName(node) + '-taak'); scene.appendChild(chain); }
  if (['linux', 'gerard'].includes(node.id)) { const badge = make('div', 'card-discussion'); badge.dataset.discussionNode = node.id; badge.hidden = true; scene.appendChild(badge); }
  const backup = make('div', 'card-backup');
  backup.setAttribute('role', 'status');
  scene.appendChild(backup);
  root.appendChild(scene);

  const controls = make('div', 'card-controls');
  const button = (className) => { const b = make('button', 'btn ' + className); b.type = 'button'; return b; };
  const pause = button('btn--pause');
  const resume = button('btn--resume');
  const stop = button('btn--stop');
  const answer = button('btn--respond');
  controls.append(pause, resume, stop, answer);
  root.appendChild(controls);

  const info = make('section', 'card-info');
  const rows = make('dl', 'rows');
  info.appendChild(rows);
  const note = make('p', 'card-note');
  info.appendChild(note);
  const report = make('button', 'btn btn--ghost btn--small btn--report', 'Lees rapport');
  report.type = 'button';
  report.hidden = true;
  const links = make('div', 'card-links');
  links.appendChild(report);
  const incidents = null;
  info.appendChild(links);
  root.appendChild(info);

  pause.addEventListener('click', () => sendAction(node.id, 'pause'));
  resume.addEventListener('click', () => sendAction(node.id, 'resume'));
  stop.addEventListener('click', () => sendStop(node.id));
  answer.addEventListener('click', () => openDialog('respond', node.id));
  report.addEventListener('click', () => openReport(node.id));
  const nodes = { root, pill, web, chain, backup, pause, resume, stop, answer, rows, note, report, incidents };
  cardNodes[node.id] = nodes;
  return nodes;
}

function renderStoringen(data) {
  const button = cardNodes.linux && cardNodes.linux.incidents;
  if (!button) return;
  const known = data && data.beschikbaar === true && Number.isInteger(data.aantal_open)
    && data.aantal_open >= 0 && data.aantal_open <= 100;
  const count = known ? data.aantal_open : null;
  button.dataset.attention = count > 0 ? 'true' : 'false';
  button.textContent = count === null ? 'Storingen (?)' : (count > 0 ? 'Storingen (' + count + ')' : 'Storingen');
  button.title = count === null ? 'Kan de actuele storingen niet lezen.'
    : (count > 0 ? count + ' open storingen vragen aandacht.' : 'Geen open storingen.');
  button.setAttribute('aria-label', button.textContent + ' — ' + button.title);
}

function renderCards(list) {
  const host = el('cards');
  host.style.setProperty('--cols', String(Math.max(1, list.length)));
  list.forEach((node) => {
    const nodes = cardNodes[node.id] || buildCard(node);
    if (nodes.root.parentNode !== host) host.appendChild(nodes.root);
    updateCard(nodes, node);
  });
}

function setButton(button, icon, label, state) {
  clear(button);
  button.appendChild(make('span', null, icon));
  button.appendChild(make('span', 'btn-label', label));
  button.disabled = state.disabled;
  button.title = state.why || '';
}

function updateCard(nodes, node) {
  const root = nodes.root;
  const state = nodeState(node);
  const buttons = cardButtons(node, page.actionsEnabled);
  root.dataset.state = state;
  root.dataset.human = buttons.answer.hidden ? 'false' : 'true';
  root.dataset.installed = node.installed ? 'true' : 'false';

  const pill = pillFor(node);
  clear(nodes.pill);
  nodes.pill.dataset.tone = pill.tone;
  nodes.pill.appendChild(led(pill.tone, pill.pulse, true));
  nodes.pill.appendChild(make('span', null, pill.text));

  setButton(nodes.pause, '❚❚', buttons.pause.label, buttons.pause);
  setButton(nodes.resume, '▶', 'Hervatten', buttons.resume);
  setButton(nodes.stop, '■', 'Stop', buttons.stop);
  nodes.answer.hidden = buttons.answer.hidden;
  clear(nodes.answer);
  nodes.answer.appendChild(make('span', null, '💬'));
  nodes.answer.appendChild(make('span', 'btn-label', 'Beantwoorden'));
  nodes.answer.disabled = buttons.answer.disabled;

  const report = reportButton(node);
  nodes.report.hidden = report.hidden;
  nodes.report.title = report.title;
  if (nodes.backup) renderBackup(nodes.backup, node);
  renderStudioChain(nodes.chain, node);
  renderRows(nodes.rows, node);
  const note = page.notes[node.id];
  nodes.note.hidden = !note || !note.text;
  nodes.note.textContent = note ? note.text || '' : '';
  if (note && note.tone) nodes.note.dataset.tone = note.tone; else delete nodes.note.dataset.tone;
}

function addRow(list, label, build) {
  list.appendChild(make('dt', null, label));
  const dd = make('dd');
  build(dd);
  list.appendChild(dd);
}

function statusLine(state, task) {
  const line = make('span', 'state-line');
  line.dataset.state = state;
  line.appendChild(led(stateTone(state), state === 'bezig' || state === 'wacht_op_actie' || state === 'herstellen'));
  if (STATE_ICONS[state]) line.appendChild(make('span', 'state-icon', STATE_ICONS[state]));
  line.appendChild(make('span', null, stateLabel(state, task)));
  return line;
}

/* De Runner kan klaar zijn terwijl de wachtrij nog op integratiecontrole wacht.
 * Alleen presentatie: de primaire wachtrij blijft beslissen over vrijgave. */
function integrationEvidencePending(error) {
  return Boolean(error && error.code === 'integration_check_blocked' &&
    /\bintegration_evidence_incomplete\b/.test(String(error.message || '')));
}

function queueFlowStatus(node) {
  if (!['linux','gerard'].includes(node.id) || !node.available || !['klaar', 'rust'].includes(nodeState(node))) return null;
  const unknown = { text: 'KETENSTATUS ONBEKEND', tone: 'warn', pulse: false,
    detail: 'De actuele wachtrij en vrijgave konden niet worden vastgesteld.' };
  if (!jobsPage.data || jobsPage.error || !jobsPage.receivedAt || Date.now() - jobsPage.receivedAt > 15000) return unknown;
  const data = jobsPage.data.nodes && jobsPage.data.nodes[node.id.toUpperCase()];
  if (!data || !Array.isArray(data.queue) || !Array.isArray(data.finished) || !Array.isArray(data.active)) return unknown;
  if (data.active.length) return { text: 'VOLGENDE TAAK START OF LOOPT', tone: 'run', pulse: true,
    detail: 'De wachtrij meldt een actieve opdracht; de Runnerstatus wordt ververst.' };
  if (!data.queue.length) return null;
  const next = data.queue[0];
  if (jobsPage.data.paused_all || data.paused) return { text: 'WACHTRIJ GEPAUZEERD', tone: 'warn', pulse: false,
    detail: 'De opvolger blijft wachten totdat de wachtrij wordt hervat.' };
  const waiting = (next.depends_on || []).map((id) => data.finished.find((j) => j.id === id))
    .find((j) => j && j.status === 'done' && j.require_check && !(j.check && j.check.state === 'afgekeurd') && (j.state === 'claude_klaar' ||
      !(j.check && j.check.state === 'ok')));
  if (waiting) {
    const detail = String(waiting.title || waiting.id) + ' → ' + String(next.title || next.id) + '. ';
    if (integratorFor(node) && ['running', 'integreren'].includes(integratorFor(node).state)) return {
      text: 'INTEGRATIE BEZIG', tone: 'run', pulse: true, detail: detail + 'Programmeren is klaar; integratie en controles lopen.' };
    const err = waiting.last_error;
    const blocked = err && err.code === 'integration_check_blocked' && !integrationEvidencePending(err);
    const reasons = { tests_not_proven: 'Het testbewijs ontbreekt of de tests zijn niet geslaagd.',
      request_missing_or_ambiguous: 'De integratieaanvraag ontbreekt of is niet eenduidig.',
      integration_evidence_unavailable: 'Het integratiebewijs is niet bereikbaar.',
      integration_evidence_incomplete: 'Wacht op het volledige integratiebewijs.' };
    const reason = err && Object.keys(reasons).find((code) => String(err.message).includes(code));
    return { text: blocked ? 'GEBLOKKEERD · INTEGRATIECONTROLE' : 'WACHT OP INTEGRATIECONTROLE',
      tone: blocked ? 'alert' : 'warn', pulse: false,
      detail: detail + (reason ? reasons[reason] : 'GERARD moet integratie, tests en audit bewijzen voordat de opvolger start.') };
  }
  const blocked = ['geblokkeerd', 'afgekeurd', 'mislukt'].includes(next.state);
  const labels = { vastgehouden: 'WACHT OP VRIJGAVE', wacht_op_goedkeuring: 'WACHT OP GOEDKEURING',
    wacht_op_voorganger: 'WACHT OP VOORGANGER' };
  return { text: blocked ? 'GEBLOKKEERD' : labels[next.state] || 'WACHT OP VOLGENDE TAAK',
    tone: blocked ? 'alert' : 'warn', pulse: false, detail: String(next.why || next.title || '').slice(0, 300) };
}

function queueStatusLine(node, state, task) {
  const flow = queueFlowStatus(node);
  if (!flow) return statusLine(state, task);
  const box = make('span');
  const line = make('span', 'state-line');
  line.dataset.state = flow.tone === 'alert' ? 'mislukt' : 'herstellen';
  line.appendChild(led(flow.tone, flow.pulse));
  line.appendChild(make('span', null, flow.text));
  box.appendChild(line);
  box.appendChild(make('span', 'sub wrap', flow.detail));
  return box;
}

function refreshQueueCards() {
  if (page.last && page.last.nodes) renderCards(page.last.nodes);
}

/* Per Ubuntu-node een onafhankelijke integratorstatus.
 * De server begrenst de geldigheid; ook een hangende volgende poll mag geen oude stand vasthouden. */
function integratorFor(node) {
  return (page.integrators || {})[node.id] || null;
}

function receiveIntegrator(summary, id = 'linux') {
  page.integratorTimers = page.integratorTimers || {};
  page.integrators = page.integrators || {};
  if (page.integratorTimers[id]) clearTimeout(page.integratorTimers[id]);
  const valid = summary && Number.isFinite(summary.valid_for_s) && summary.valid_for_s > 0;
  page.integrators[id] = valid ? summary : null;
  if (id === 'linux') page.integrator = page.integrators[id];
  page.integratorTimers[id] = valid ? setTimeout(() => invalidateIntegrator(id), Math.min(10800, summary.valid_for_s) * 1000) : null;
}

function invalidateIntegrator(id) {
  const ids = id ? [id] : ['linux','gerard'];
  ids.forEach(key => {
    receiveIntegrator(null, key);
    const node = page.last && page.last.nodes.find(n => n.id === key);
    if (node && cardNodes[key]) updateCard(cardNodes[key], node);
  });
}

function integratorTime(at) {
  const date = new Date(at);
  if (!at || !Number.isFinite(date.getTime())) return '';
  return date.toLocaleString('nl-NL', { day: '2-digit', month: '2-digit', year: 'numeric', hour: '2-digit', minute: '2-digit' });
}

/* Eén keten voor de huidige STUDIO-taak. Historisch succes van een andere taak
 * mag de volgende cyclus nooit groen maken. Dit is uitsluitend presentatie. */
function studioChainState(node) {
  const pending = ['pending', 'pending', 'pending'];
  const current = nodeState(node);
  const data = jobsPage.data && jobsPage.data.nodes && jobsPage.data.nodes[node.id.toUpperCase()];
  const fresh = data && !jobsPage.error && jobsPage.receivedAt && Date.now() - jobsPage.receivedAt <= 15000;
  const active = fresh && (data.active || [])[0];
  const task = node.task;
  const title = (active && active.title) || (task && task.title) || node.name || node.label;
  if (!node.available || !node.installed) return { title, steps: ['unknown','unknown','unknown'], detail: 'Runner niet bereikbaar; de ketenstatus is onbekend.' };
  // De primaire wachtrij kan de nieuwe taak al tonen vóór de Runnerkaart ververst is.
  if (active && (!task || active.task_id !== task.task_id)) return { title, steps: ['active','pending','pending'], detail: 'De volgende programmeertaak wordt gestart.' };
  if (['bezig','herstellen','wacht_op_actie','gepauzeerd'].includes(current)) {
    pending[0] = current === 'bezig' || current === 'herstellen' ? 'active' : 'waiting';
    return { title, steps: pending, detail: current === 'wacht_op_actie' ? 'Programmeren wacht op een antwoord.' :
      current === 'gepauzeerd' ? 'Programmeren is gepauzeerd.' : current === 'herstellen' ? 'De Runner herstelt de programmeertaak.' : 'De Runner werkt aan deze taak.' };
  }
  if (['mislukt','gestopt'].includes(current)) return { title, steps: ['blocked','pending','pending'], detail: 'De programmeertaak is mislukt of gestopt; de opvolger wacht.' };
  const programmed = current === 'klaar';
  if (!fresh) return { title, steps: [programmed ? 'done' : 'unknown','unknown','unknown'], detail: 'De actuele wachtrij en vrijgave zijn onbekend.' };
  const job = task && (data.finished || []).find(j => j.task_id === task.task_id);
  const check = job && job.check;
  const proven = check && check.state === 'ok' && check.source === 'integrator' && check.evidence &&
    check.evidence.ok === true && check.evidence.task_id === job.task_id && /^[0-9a-f]{40}$/.test(check.evidence.commit || '');
  if (proven) return { title, steps: ['done','done','done'], detail: 'Integratie en controle geslaagd. ' + ((jobsPage.data.paused_all || data.paused) ? 'Wachtrij gepauzeerd; opvolger wacht.' : 'De opvolger mag starten.') };
  if (programmed && (!job || !job.require_check)) return { title, steps: ['done','unknown','unknown'], detail: 'Eerdere taak zonder automatisch integratiebewijs. ' + ((queueFlowStatus(node) || {}).detail || 'Er wacht nu geen ketenopdracht.') };
  if (!programmed) return { title, steps: ['pending','pending','pending'], detail: 'Er loopt nu geen programmeertaak.' };
  const summary = integratorFor(node);
  if (summary && ['running','integreren'].includes(summary.state)) return { title, steps: ['done','active','pending'], detail: 'De integrator verwerkt aanvragen en voert integratiecontroles uit.' };
  const last = summary && (summary.state === 'geslaagd' ? summary : summary.last_result);
  const integrated = job && last && last.state === 'geslaagd' && last.task_id === job.task_id;
  const flow = queueFlowStatus(node);
  const rejected = check && check.state === 'afgekeurd';
  const pendingEvidence = job && integrationEvidencePending(job.last_error);
  const blocked = rejected || (job && job.last_error && job.last_error.code === 'integration_check_blocked' && !pendingEvidence);
  const disabled = jobsPage.data.paused_all || data.paused;
  return { title, steps: ['done', integrated ? 'done' : 'unknown', blocked ? 'blocked' : disabled || pendingEvidence ? 'waiting' : integrated ? 'active' : 'waiting'],
    detail: rejected ? 'De controle is afgekeurd; de opvolger blijft geblokkeerd.' :
      disabled ? 'De wachtrij is gepauzeerd.' : (flow && flow.detail) || 'Wacht op taakgebonden integratiebewijs en vrijgave van de opvolger.' };
}

function renderStudioChain(host, node) {
  if (!host) return;
  clear(host);
  const view = studioChainState(node);
  const names = ['Programmeren','Integreren','Controleren'];
  const labels = { done:'Gereed', active:'Bezig', waiting:'Wacht', pending:'Nog niet', blocked:'Geblokkeerd', unknown:'Onbekend' };
  const symbols = { done:'✓', active:'◉', waiting:'Ⅱ', pending:'○', blocked:'!', unknown:'?' };
  const track = make('ol','studio-chain-track');
  view.steps.forEach((state,i) => {
    const step = make('li','chain-step'); step.dataset.state = state;
    step.setAttribute('aria-label',names[i] + ': ' + labels[state]);
    if (['active','blocked','waiting'].includes(state)) step.setAttribute('aria-current','step');
    step.appendChild(make('span','chain-step-name',names[i]));
    const dot = make('span','chain-step-dot',symbols[state]);dot.setAttribute('aria-hidden','true');step.appendChild(dot);
    step.appendChild(make('span','chain-step-status',labels[state]));
    if (i === 2) step.title = 'Controle van integratiebewijs en vrijgave van de opvolger.';
    track.appendChild(step);
  });
  host.appendChild(track);
  const detail=make('p','studio-chain-detail',view.detail);detail.title=view.detail;host.appendChild(detail);
}


function renderRows(list, node) {
  clear(list);
  const state = nodeState(node);
  if (!node.installed || !node.available) {
    addRow(list, 'Status', (dd) => {
      dd.appendChild(statusLine(state));
      dd.appendChild(make('span', 'sub wrap', delayText(node) || node.message || ''));
    });
    return;
  }
  const task = node.task;
  const delayed = delayText(node);
  if (!task) {
    addRow(list, 'Taak', (dd) => dd.appendChild(make('span', 'value', 'Er loopt nu niets op deze machine.')));
    addRow(list, 'Status', (dd) => {
      dd.appendChild(queueStatusLine(node, 'rust'));
      if (delayed) dd.appendChild(make('span', 'sub wrap', delayed));
      if (node.message) dd.appendChild(make('span', 'sub wrap', node.message));
    });
    return;
  }
  const supervisor = supervisorText(task);
  if (isFinished(state)) {
    addRow(list, 'Status', (dd) => {
      dd.appendChild(queueStatusLine(node, state, task));
      if (supervisor) dd.appendChild(make('span', 'sub wrap', supervisor));
      if (delayed) dd.appendChild(make('span', 'sub wrap', delayed));
    });
    return;
  }
  addRow(list, 'Taak', (dd) => dd.appendChild(make('span', 'value', task.title || task.task_id)));
  addRow(list, 'Status', (dd) => {
    dd.appendChild(queueStatusLine(node, state, task));
    const last = task.activity && task.activity.length ? task.activity[task.activity.length - 1] : null;
    if (state === 'bezig') dd.appendChild(make('span', 'sub', last || '\u00a0'));
    if (task.pausing) dd.appendChild(make('span', 'sub', 'Pauze gevraagd — na de huidige stap.'));
    if (supervisor) dd.appendChild(make('span', 'sub wrap', supervisor));
    if (delayed) dd.appendChild(make('span', 'sub wrap', delayed));
  });
  if (task.question) {
    addRow(list, 'Vraag', (dd) => {
      const box = make('span', 'question-line', task.question.text);
      box.title = task.question.text;
      dd.appendChild(box);
    });
  }
  const recovery = recoveryText(task);
  if (recovery) addRow(list, 'Herstel', (dd) => dd.appendChild(make('span', 'value value--wrap', recovery)));
  if (task.failure && task.failure.message) {
    addRow(list, 'Fout', (dd) => dd.appendChild(make('span', 'value value--wrap', task.failure.message)));
  }
  if (task.model_badge) {
    addRow(list, 'Model', (dd) => {
      const line = make('span', 'value value--badged');
      line.appendChild(make('span', 'model-badge', task.model_badge));
      if (task.role) line.appendChild(make('span', 'value-text', task.role));
      dd.appendChild(line);
    });
  }
  const elapsed = durationLabel(task.elapsed_s);
  if (elapsed) {
    addRow(list, 'Verstreken', (dd) => {
      dd.appendChild(make('span', 'value', elapsed));
      if (task.turn) dd.appendChild(make('span', 'sub', 'beurt ' + task.turn));
    });
  }
}

/* ---- taakblokken onder de kaarten ---- */

const taskNodes = {};

function buildTaskPanel(node) {
  const root = make('section', 'panel mini task-panel');
  root.dataset.node = node.id;
  root.setAttribute('aria-label', 'Runner 2-taak op ' + nodeName(node));
  const head = make('div', 'mini-head');
  head.appendChild(make('h2', null, 'Taak · ' + nodeName(node)));
  const hint = make('span', 'mini-hint');
  head.appendChild(hint);
  root.appendChild(head);
  const body = make('div', 'task-body');
  root.appendChild(body);
  taskNodes[node.id] = { root, body, hint };
  return taskNodes[node.id];
}

function renderTasks(list) {
  const host = el('tasks');
  host.style.setProperty('--cols', String(Math.max(1, list.length)));
  list.forEach((node) => {
    const nodes = taskNodes[node.id] || buildTaskPanel(node);
    if (nodes.root.parentNode !== host) host.appendChild(nodes.root);
    fillTask(nodes, node);
  });
}

function taskFacts(task) {
  const rows = [];
  if (task.prompt) rows.push({ label: 'Opdracht', value: task.prompt, wrap: true });
  if (task.session_id) rows.push({ label: 'Sessie', value: task.session_id, mono: true });
  if (task.worktree) rows.push({ label: 'Worktree', value: task.worktree, mono: true });
  if (task.branch) rows.push({ label: 'Branch', value: task.branch, mono: true });
  const activity = (task.activity || []).slice(-2);
  if (activity.length) rows.push({ label: 'Nu bezig', value: activity.join(' · ') });
  if (task.tests && (task.tests.result || (task.tests.run && task.tests.run.length))) {
    const count = task.tests.run ? task.tests.run.length : 0;
    rows.push({ label: 'Tests', value: (task.tests.result || '') + (count ? ' (' + count + ' uitgevoerd)' : ''), wrap: true });
  }
  if (task.stop && task.stop.reason) rows.push({ label: 'Gestopt', value: task.stop.reason });
  return rows;
}

function fillTask(nodes, node) {
  const body = nodes.body;
  clear(body);
  const state = nodeState(node);
  const task = node.installed && node.available && !isFinished(state) ? node.task : null;
  nodes.root.dataset.state = state;
  nodes.hint.textContent = task ? task.task_id : '';
  if (!task) {
    body.appendChild(make('p', 'task-empty', !node.installed
      ? 'Runner 2 is op ' + nodeName(node) + ' nog niet geïnstalleerd.'
      : (!node.available ? (node.message || 'Runner 2 is niet bereikbaar.') : 'Geen Runner 2-taak op ' + nodeName(node) + '.')));
    return;
  }
  const line = make('div', 'task-line');
  line.appendChild(led(stateTone(state), state === 'wacht_op_actie' || state === 'herstellen' || state === 'bezig'));
  line.appendChild(make('span', 'task-title', task.title || task.task_id));
  body.appendChild(line);
  if (task.question) {
    const box = make('div', 'task-question');
    box.appendChild(make('strong', null, 'WACHT OP ACTIE'));
    box.appendChild(make('span', null, task.question.text));
    body.appendChild(box);
  }
  const list = make('dl', 'task-facts');
  taskFacts(task).forEach((row) => {
    list.appendChild(make('dt', null, row.label));
    const dd = make('dd', (row.mono ? 'mono ' : '') + (row.wrap ? 'wrap' : ''), row.value);
    dd.title = row.value;
    list.appendChild(dd);
  });
  body.appendChild(list);
}

function renderSummary(nodes) {
  const summary = summaryText(nodes);
  el('system-summary').querySelector('.led').dataset.tone = summary.tone;
  el('system-summary-text').textContent = summary.text;
}

/* ------------------------------------------------------------ dialogen */

function nodeById(id) {
  return page.last ? page.last.nodes.find((node) => node.id === id) || null : null;
}

function openDialog(kind, nodeId) {
  page.dialog = { kind, node: nodeId, editing: false, note: null, tone: null, busy: false, draft: undefined };
  const dialog = el('dialog');
  dialog.dataset.kind = kind;
  refreshDialog(true);
  if (!dialog.open) dialog.showModal();
}

function closeDialog() {
  const dialog = el('dialog');
  page.dialog = null;
  if (dialog.open) dialog.close();
}

function refreshDialog(force) {
  const state = page.dialog;
  if (!state) return;
  // Niet hertekenen terwijl de gebruiker typt of er een verzoek loopt.
  if (!force && (state.editing || state.busy || state.kind === 'report' || state.kind === 'job' || state.kind === 'actionreview' || state.kind === 'actionhistory')) return;
  const body = el('dialog-body');
  if (state.kind === 'actions' || state.kind === 'supervision') {
    // Alleen opnieuw tekenen als de punten echt veranderden: een poll mag de leesplek niet verspringen.
    const actions = state.kind === 'actions';
    const signature = (actions ? actionsSignature : supervisionSignature)(nodeById(state.node));
    if (!force && signature === state.signature) return;
    state.signature = signature;
    clear(body);
    (actions ? dialogActions : dialogSupervision)(body, state);
    return;
  }
  clear(body);
  if (state.kind === 'settings') { dialogSettings(body); return; }
  if (state.kind === 'report') { dialogReport(body, state); return; }
  if (state.kind === 'actionreview') { dialogActionReview(body, state); return; }
  if (state.kind === 'actionhistory') { dialogActionHistory(body, state); return; }
  if (state.kind === 'job') { dialogJob(body, state); return; }
  const node = nodeById(state.node);
  if (!node) { el('dialog-title').textContent = 'Onbekende machine'; return; }
  if (state.kind === 'respond') dialogRespond(body, node);
  else dialogDetails(body, node);
}

function dialogNote(body) {
  const state = page.dialog;
  if (!state || !state.note) return;
  const note = make('p', 'dialog-note', state.note);
  if (state.tone) note.dataset.tone = state.tone;
  body.appendChild(note);
}

function facts(rows) {
  const list = make('dl', 'dialog-facts');
  rows.forEach((row) => {
    if (row.value === null || row.value === undefined || row.value === '') return;
    list.appendChild(make('dt', null, row.label));
    list.appendChild(make('dd', row.mono ? 'mono' : null, row.value));
  });
  return list;
}

/** Beantwoorden: de vraag van Runner 2 zelf, met zijn aanbeveling en alternatieven, en een tekstvak. */
function dialogRespond(body, node) {
  const task = node.task;
  const question = task && task.question;
  const state = page.dialog;
  el('dialog-title').textContent = 'Beantwoorden — ' + nodeName(node);
  if (!question) {
    body.appendChild(make('p', 'dialog-why', 'Er wacht op dit moment niets op jou op ' + nodeName(node) + '.'));
    dialogNote(body);
    return;
  }
  if (task.title) body.appendChild(make('p', 'dialog-why', task.title));
  body.appendChild(make('p', 'dialog-question', question.text));
  if (question.reason) body.appendChild(make('p', 'dialog-why', question.reason));
  if (question.recommendation) {
    body.appendChild(make('p', 'dialog-section-title', 'Aanbeveling van Claude'));
    body.appendChild(make('p', null, question.recommendation));
  }
  const area = make('textarea');
  area.id = 'dialog-answer';
  area.setAttribute('aria-label', 'Jouw antwoord');
  area.value = state.draft !== undefined ? state.draft : '';
  area.disabled = state.busy || !page.actionsEnabled;
  area.addEventListener('input', () => { state.draft = area.value; state.editing = true; });
  if (question.alternatives && question.alternatives.length) {
    body.appendChild(make('p', 'dialog-section-title', 'Keuzes die Claude noemt (klik om over te nemen)'));
    const list = make('div', 'dialog-controls');
    question.alternatives.forEach((option) => {
      const chip = make('button', 'btn btn--ghost btn--small', option);
      chip.type = 'button';
      chip.addEventListener('click', () => { state.draft = option; state.editing = true; area.value = option; });
      list.appendChild(chip);
    });
    body.appendChild(list);
  }
  body.appendChild(make('p', 'dialog-section-title', 'Jouw antwoord'));
  body.appendChild(area);
  const row = make('div', 'dialog-controls');
  const send = make('button', 'btn btn--warm', 'Antwoord versturen');
  send.type = 'button';
  send.id = 'dialog-send';
  send.disabled = state.busy || !page.actionsEnabled || !task.actions.answer;
  send.addEventListener('click', () => {
    const value = (state.draft !== undefined ? state.draft : area.value).trim();
    if (!value) { state.note = 'Schrijf eerst een antwoord.'; state.tone = 'bad'; refreshDialog(true); return; }
    sendAction(node.id, 'answer', { text: value });
  });
  row.appendChild(send);
  body.appendChild(row);
  dialogNote(body);
}

function dialogDetails(body, node) {
  el('dialog-title').textContent = 'Details — ' + nodeName(node);
  const task = node.task;
  if (!task) {
    body.appendChild(make('p', 'dialog-why', 'Geen taakgegevens beschikbaar.'));
    return;
  }
  body.appendChild(facts([
    { label: 'Taak-id', value: task.task_id, mono: true },
    { label: 'Status (Runner 2)', value: task.status },
    { label: 'Model', value: task.model, mono: true },
    { label: 'Sessie', value: task.session_id, mono: true },
    { label: 'Worktree', value: task.worktree, mono: true },
    { label: 'Branch', value: task.branch, mono: true },
    { label: 'Beurt', value: task.turn },
    { label: 'Verstreken', value: durationLabel(task.elapsed_s) },
    { label: 'Herstel', value: recoveryText(task) },
  ]));
  if (task.warnings && task.warnings.length) {
    body.appendChild(make('p', 'dialog-section-title', 'Waarschuwingen'));
    body.appendChild(make('p', 'mono', task.warnings.join('\n')));
  }
}


/* ---- eindrapport van een afgeronde taak ---- */

/** Open het rapport van het laatst afgeronde rapport van deze machine; de inhoud komt van de server (``runner2_result``). */
async function openReport(nodeId) {
  const node = nodeById(nodeId);
  const target = node && node.report;
  if (!target || !target.task_id) return;
  return openReportTask(nodeId, nodeName(node), target.task_id, target.title, target.finished_at);
}

/** Het rapport van één taak op één machine; ook voor een opdracht uit de wachtrij (Lees rapport). */
async function openReportTask(nodeId, label, taskId, title, finishedAt) {
  const state = { kind: 'report', node: nodeId, label, task: taskId, title,
    finishedAt, status: 'loading', report: null, error: null, note: null, tone: null,
    editing: false, busy: false, draft: undefined };
  page.dialog = state;
  const dialog = el('dialog');
  dialog.dataset.kind = 'report';
  refreshDialog(true);
  if (!dialog.open) dialog.showModal();
  try {
    const response = await fetch(reportUrl(nodeId, taskId), { cache: 'no-store' });
    let payload = {};
    try { payload = await response.json(); } catch (err) { payload = {}; }
    if (page.dialog !== state) return;                       // inmiddels gesloten of vervangen
    if (response.ok && payload.ok !== false && payload.report) { state.status = 'ok'; state.report = payload.report; }
    else { state.status = 'error'; state.error = payload.message || 'Het rapport kon niet worden opgehaald (HTTP ' + response.status + ').'; }
  } catch (err) {
    if (page.dialog !== state) return;
    state.status = 'error';
    state.error = 'Geen antwoord van Monitor 2: ' + err.message;
  }
  refreshDialog(true);
}

/* ---- open acties van één machine (de ACTIE OPEN-badge) ---- */

/** De punten die het venster toont: precies wat de badge telt, dus niets bij WACHT OP ACTIE of een onleesbare node. */
function openActionItems(node) {
  if (!actionOpenCount(node)) return [];
  return Array.isArray(node.action_items) ? node.action_items.filter((item) => item && item.text) : [];
}

function actionsSignature(node) {
  return node ? JSON.stringify([actionOpenCount(node), openActionItems(node), actionBoardGroups(node)]) : '';
}

/**
 * De open punten van een machine in drie categorieën (menselijke beslissing / in uitvoering bij Claude / wacht op technische
 * controle), zoals de server ze indeelde op de feitelijke status; null zonder bord (oudere server) of zonder open punten.
 * Alleen weergave: niets wordt hier bepaald of gesloten. Onzekere punten staan al bij 'mens' en dragen zeker=false.
 */
function actionBoardGroups(node) {
  const board = node && actionOpenCount(node) ? node.action_board : null;
  if (!board || !Array.isArray(board.categories)) return null;
  const groups = board.categories
    .filter((c) => c && typeof c.key === 'string')
    .map((c) => ({ key: c.key, title: c.title || c.key, hint: c.hint || '',
      items: Array.isArray(c.items) ? c.items.filter((item) => item && item.text) : [] }));
  return groups.length ? { groups, not_shown: Number.isInteger(board.not_shown) ? board.not_shown : 0,
    uncertain: Number.isInteger(board.uncertain) ? board.uncertain : 0 } : null;
}

function openActions(nodeId) {
  const node = nodeById(nodeId);
  if (!node || !actionOpenCount(node)) return;
  page.dialog = { kind: 'actions', node: nodeId, editing: false, note: null, tone: null, busy: false, draft: undefined, signature: null };
  const dialog = el('dialog');
  dialog.dataset.kind = 'actions';
  refreshDialog(true);
  if (!dialog.open) dialog.showModal();
}

function actionSourceLabel(source) {
  const src = source || {};
  return [src.title, src.task_id].filter(Boolean).join(' · ');
}

function actionCreatedLabel(iso) {
  const at = iso ? new Date(iso) : null;
  if (!at || Number.isNaN(at.getTime())) return null;
  return at.toLocaleString('nl-NL', { day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit' });
}

function dialogActions(body, state) {
  const node = nodeById(state.node);
  const count = node ? actionOpenCount(node) : 0;
  const label = nodeName(node);
  el('dialog-title').textContent = 'Open acties \u00b7 ' + label + ' (' + count + ')';
  const bar = make('div', 'report-bar');
  const close = make('button', 'btn btn--ghost btn--small', 'Sluiten');
  close.type = 'button';
  close.id = 'actions-close';
  close.addEventListener('click', closeDialog);
  bar.appendChild(close);
  if (count) {
    // Rustig naast Sluiten: een alleen-lezen overzicht, geen bediening. Het voorstel wordt pas bij een klik opgehaald.
    const proposal = make('button', 'btn btn--ghost btn--small action-review-open', 'Opschoonvoorstel');
    proposal.type = 'button';
    proposal.id = 'actions-review';
    proposal.title = 'Alleen lezen: wat Runner 2 zou opschonen. Er wordt niets gesloten of gewijzigd.';
    proposal.addEventListener('click', () => openActionReview(state.node));
    bar.appendChild(proposal);
  }
  const history = make('button', 'btn btn--ghost btn--small action-history-open', 'Historie');
  history.type = 'button';
  history.id = 'actions-history';
  history.title = 'Alleen lezen: alle acties van deze machine, ook de afgehandelde.';
  history.addEventListener('click', () => openActionHistory(state.node));
  bar.appendChild(history);
  bar.appendChild(make('span', 'report-heading', 'Alleen lezen \u2014 de punten staan in Runner 2 van deze machine.'));
  body.appendChild(bar);
  const items = node ? openActionItems(node) : [];
  if (!items.length) {
    body.appendChild(make('p', 'dialog-why actions-empty', count
      ? 'De inhoud van de ' + count + ' open acties is niet beschikbaar in deze weergave.'
      : 'Er staan geen open acties meer.'));
    return;
  }
  const board = actionBoardGroups(node);
  if (board) {
    if (board.not_shown) {
      body.appendChild(make('p', 'dialog-note', 'Runner 2 telt ' + count + ' open acties; ' + items.length + ' zijn hier ingedeeld, ' + board.not_shown + ' worden niet getoond.'));
    }
    board.groups.forEach((group) => {
      const section = make('section', 'review-section action-group');
      section.dataset.category = group.key;
      section.appendChild(make('h3', 'dialog-section-title', group.title + ' (' + group.items.length + ')'));
      if (group.hint) section.appendChild(make('p', 'dialog-why', group.hint));
      if (!group.items.length) {
        section.appendChild(make('p', 'dialog-why', 'Geen.'));
      } else {
        const list = make('ol', 'action-list');
        group.items.forEach((item) => list.appendChild(actionEntry(item)));
        section.appendChild(list);
      }
      body.appendChild(section);
    });
    return;
  }
  if (items.length < count) {
    body.appendChild(make('p', 'dialog-note', items.length + ' van de ' + count + ' open acties worden getoond.'));
  }
  const list = make('ol', 'action-list');
  items.forEach((item) => list.appendChild(actionEntry(item)));
  body.appendChild(list);
}

/** Eén open punt: de vraag prominent; reden, technische toelichting, bron en (bij de indeling) de grond ervan ingeklapt onder Details. */
function actionEntry(item) {
  const entry = make('li', 'action-item');
  if (item.category) entry.dataset.category = item.category;
  entry.appendChild(make('div', 'action-text', item.text));
  if (item.certain === false) {
    const tags = make('div', 'review-tags');
    tags.appendChild(make('span', 'review-tag review-tag--doubt', 'status onzeker'));
    entry.appendChild(tags);
  }
  const more = make('details', 'action-details');
  more.appendChild(make('summary', 'action-details-toggle', 'Details'));
  if (item.basis) {
    const basis = make('div', 'action-reason');
    basis.appendChild(make('strong', null, 'Indeling: '));
    basis.appendChild(make('span', null, item.basis));
    more.appendChild(basis);
  }
  if (item.reason) {
    const reason = make('div', 'action-reason');
    reason.appendChild(make('strong', null, 'Reden: '));
    reason.appendChild(make('span', null, item.reason));
    more.appendChild(reason);
  }
  if (item.details) {
    const tech = make('div', 'action-reason');
    tech.appendChild(make('strong', null, 'Technisch: '));
    tech.appendChild(make('span', null, item.details));
    more.appendChild(tech);
  }
  const created = actionCreatedLabel(item.created_at);
  const source = make('div', 'action-source');
  source.appendChild(make('strong', null, 'Bron: '));
  source.appendChild(make('span', null, [actionSourceLabel(item.source), created ? 'sinds ' + created : null].filter(Boolean).join(' \u00b7 ')));
  more.appendChild(source);
  entry.appendChild(more);
  return entry;
}

/* ---- actiehistorie van één machine (open én afgehandeld; alleen lezen, nooit sluiten of wissen) ---- */

/** Het adres waarop de server de volledige actiehistorie van één machine levert (alleen lezen). */
function actionHistoryUrl(nodeId) {
  return '/api/action-history?' + new URLSearchParams({ node: nodeId }).toString();
}

/** De machines waarvan de historie opvraagbaar is: alleen die met een lezende Runner 2. */
function historyNodes() {
  return (page.last ? page.last.nodes : []).filter((node) => node.installed && node.available);
}

async function openActionHistory(nodeId) {
  const nodes = historyNodes();
  const node = nodeById(nodeId) || nodes[0];
  if (!node) return;
  const state = { kind: 'actionhistory', node: node.id, label: nodeName(node), status: 'loading', history: null, error: null,
    note: null, tone: null, editing: false, busy: false, draft: undefined };
  page.dialog = state;
  const dialog = el('dialog');
  dialog.dataset.kind = 'actionhistory';
  refreshDialog(true);
  if (!dialog.open) dialog.showModal();
  try {
    // Alleen GET: dit venster kan niets sluiten, heropenen, wijzigen of wissen.
    const response = await fetch(actionHistoryUrl(node.id), { cache: 'no-store' });
    let payload = {};
    try { payload = await response.json(); } catch (err) { payload = {}; }
    if (page.dialog !== state) return;
    if (payload.ok && payload.history) { state.status = 'ok'; state.history = payload.history; }
    else { state.status = 'error'; state.error = payload.message || 'De actiehistorie kon niet worden opgehaald (HTTP ' + response.status + ').'; }
  } catch (err) {
    if (page.dialog !== state) return;
    state.status = 'error';
    state.error = 'Geen antwoord van Monitor 2: ' + err.message;
  }
  refreshDialog(true);
}

function dialogActionHistory(body, state) {
  el('dialog-title').textContent = 'Actiehistorie \u00b7 ' + state.label;
  const bar = make('div', 'report-bar');
  const nodes = historyNodes();
  nodes.forEach((node) => {
    const pick = make('button', 'btn btn--ghost btn--small history-node', nodeName(node));
    pick.type = 'button';
    pick.dataset.node = node.id;
    pick.setAttribute('aria-pressed', node.id === state.node ? 'true' : 'false');
    pick.addEventListener('click', () => openActionHistory(node.id));
    bar.appendChild(pick);
  });
  const close = make('button', 'btn btn--ghost btn--small', 'Sluiten');
  close.type = 'button';
  close.id = 'history-close';
  close.addEventListener('click', closeDialog);
  bar.appendChild(close);
  bar.appendChild(make('span', 'report-heading', 'Alleen lezen \u2014 afgehandelde acties blijven hier bewaard; er wordt niets gesloten of gewist.'));
  body.appendChild(bar);
  if (state.status === 'loading') {
    body.appendChild(make('p', 'dialog-why report-status', 'Actiehistorie wordt opgehaald\u2026'));
    return;
  }
  if (state.status === 'error') {
    const error = make('p', 'dialog-note report-status', state.error);
    error.dataset.tone = 'bad';
    body.appendChild(error);
    return;
  }
  const history = state.history;
  body.appendChild(make('p', 'review-headline', state.label + ': ' + history.total + ' acties \u00b7 ' + history.open + ' open \u00b7 ' + history.resolved + ' afgehandeld'));
  if (history.not_shown) {
    body.appendChild(make('p', 'dialog-note', 'De nieuwste ' + history.items.length + ' worden getoond; ' + history.not_shown + ' oudere staan nog in Runner 2.'));
  }
  if (!history.items.length) {
    body.appendChild(make('p', 'dialog-why', 'Er zijn geen acties vastgelegd.'));
    return;
  }
  const list = make('ol', 'action-list');
  history.items.forEach((item) => {
    const entry = make('li', 'action-item history-item');
    entry.dataset.status = item.status;
    entry.appendChild(make('div', 'action-text', item.text));
    const tags = make('div', 'review-tags');
    tags.appendChild(make('span', item.status === 'resolved' ? 'review-tag review-tag--safe' : 'review-tag review-tag--doubt', item.status === 'resolved' ? 'afgehandeld' : item.status === 'open' ? 'open' : 'status onbekend'));
    entry.appendChild(tags);
    const more = make('details', 'action-details');
    more.appendChild(make('summary', 'action-details-toggle', 'Details'));
    const row = (title, value) => {
      if (!value) return;
      const line = make('div', 'action-reason');
      line.appendChild(make('strong', null, title + ': '));
      line.appendChild(make('span', null, value));
      more.appendChild(line);
    };
    row('Gesteld', actionCreatedLabel(item.created_at));
    row('Afgehandeld', actionCreatedLabel(item.resolved_at));
    row('Afgehandeld door', item.resolved_by);
    row('Notitie', item.resolved_note);
    row('Taak', item.source && item.source.task_id);
    row('Keten', item.source && item.source.chain_id);
    row('Punt-id', item.id);
    entry.appendChild(more);
    list.appendChild(entry);
  });
  body.appendChild(list);
}

/* ---- opschoonvoorstel van één machine (DRY RUN van het actiebeheer; alleen lezen, nooit sluiten) ---- */

/** Het adres waarop de server het opschoonvoorstel van één machine levert (alleen lezen, 10 minuten gecached). */
function actionReviewUrl(nodeId, refresh) {
  const query = { node: nodeId };
  if (refresh) query.refresh = '1';
  return '/api/action-review?' + new URLSearchParams(query).toString();
}

/** De drie groepen van het venster uit de bakken van het plan: wat veilig aantoonbaar dubbel is, wat misschien dubbel of
 *  twijfel is, en wat echt een mens moet beslissen. Alleen indelen; de bak bepaalt Runner 2 en bij twijfel wint twijfel. */
function actionReviewGroups(review) {
  const list = (key) => (review && Array.isArray(review[key]) ? review[key].filter((e) => e && e.id && e.text) : []);
  const review_ = list('review');
  const decide = list('needs_decision');
  const maybe = decide.filter((e) => e.possible_duplicate_of);
  return {
    safe: list('would_close'),
    doubt: [...review_.map((entry) => ({ entry, kind: 'doubt' })), ...maybe.map((entry) => ({ entry, kind: 'maybe' }))],
    human: decide.filter((e) => !e.possible_duplicate_of),
  };
}

async function openActionReview(nodeId, refresh) {
  const node = nodeById(nodeId);
  if (!node) return;
  const state = { kind: 'actionreview', node: nodeId, label: nodeName(node), status: 'loading', review: null, error: null,
    cached: false, note: null, tone: null, editing: false, busy: false, draft: undefined };
  page.dialog = state;
  const dialog = el('dialog');
  dialog.dataset.kind = 'actionreview';
  refreshDialog(true);
  if (!dialog.open) dialog.showModal();
  try {
    // Alleen GET: dit venster kan niets sluiten, wijzigen of bevestigen.
    const response = await fetch(actionReviewUrl(nodeId, refresh), { cache: 'no-store' });
    let payload = {};
    try { payload = await response.json(); } catch (err) { payload = {}; }
    if (page.dialog !== state) return;                       // inmiddels gesloten of vervangen
    if (payload.ok && payload.review) { state.status = 'ok'; state.review = payload.review; state.cached = !!payload.cached; }
    else if (payload.reason === 'review_unsupported') { state.status = 'unsupported'; state.error = payload.message || REVIEW_UNSUPPORTED_TEXT; }
    else { state.status = 'error'; state.error = payload.message || 'Het opschoonvoorstel kon niet worden opgehaald (HTTP ' + response.status + ').'; }
  } catch (err) {
    if (page.dialog !== state) return;
    state.status = 'error';
    state.error = 'Geen antwoord van Monitor 2: ' + err.message;
  }
  refreshDialog(true);
}

const REVIEW_UNSUPPORTED_TEXT = 'Deze Runner kent het opschoonvoorstel nog niet.';

function reviewItem(entry, kind) {
  const card = make('li', 'action-item review-item');
  card.dataset.kind = kind;
  const head = make('div', 'action-text', entry.text);
  card.appendChild(head);
  const tags = make('div', 'review-tags');
  if (kind === 'doubt') tags.appendChild(make('span', 'review-tag review-tag--doubt', 'twijfel'));
  if (kind === 'maybe') tags.appendChild(make('span', 'review-tag review-tag--doubt', 'mogelijk dubbel'));
  if (kind === 'safe') tags.appendChild(make('span', 'review-tag review-tag--safe', 'dubbel'));
  if (Array.isArray(entry.protected) && entry.protected.length) {
    tags.appendChild(make('span', 'review-tag review-tag--protected', 'beschermd: ' + entry.protected.join(', ')));
  }
  if (tags.children.length) card.appendChild(tags);
  if (entry.label) card.appendChild(make('div', 'action-reason', entry.label));
  if (entry.hint) card.appendChild(make('div', 'action-reason', entry.hint));
  if (entry.evidence_id) card.appendChild(make('div', 'action-reason', 'bewijs: ' + entry.evidence_id));
  if (entry.possible_duplicate_of) card.appendChild(make('div', 'action-reason', 'mogelijk dubbel van ' + entry.possible_duplicate_of));
  const more = make('details', 'action-details');
  more.appendChild(make('summary', 'action-details-toggle', 'Details'));
  const row = (title, value) => {
    if (!value) return;
    const line = make('div', 'action-reason');
    line.appendChild(make('strong', null, title + ': '));
    line.appendChild(make('span', null, value));
    more.appendChild(line);
  };
  (Array.isArray(entry.facts) ? entry.facts : []).forEach((fact) => row('Feit', fact));
  row('Blijvend punt / besluit', entry.evidence_text);
  row('Besluitnotitie', entry.evidence_note);
  row('Regel', entry.rule);
  row('Punt-id', entry.id);
  row('Bewijs-id', entry.evidence_id);
  row('Taak', entry.task_id);
  row('Bewijstaak', entry.evidence_task_id);
  row('Repository', entry.repo);
  const created = actionCreatedLabel(entry.created_at);
  row('Sinds', created);
  card.appendChild(more);
  return card;
}

function reviewSection(body, title, count, items, kindOf) {
  const section = make('section', 'review-section');
  section.appendChild(make('h3', 'dialog-section-title', title + ' (' + count + ')'));
  if (!items.length) {
    section.appendChild(make('p', 'dialog-why', 'Geen.'));
  } else {
    const list = make('ol', 'action-list');
    items.forEach((it) => list.appendChild(kindOf ? reviewItem(it.entry, it.kind) : reviewItem(it, 'human')));
    section.appendChild(list);
  }
  body.appendChild(section);
  return section;
}

function dialogActionReview(body, state) {
  el('dialog-title').textContent = 'Opschoonvoorstel · ' + state.label;
  const bar = make('div', 'report-bar');
  const back = make('button', 'btn btn--ghost btn--small', 'Terug naar open acties');
  back.type = 'button';
  back.id = 'review-back';
  back.addEventListener('click', () => openActions(state.node));
  const refresh = make('button', 'btn btn--ghost btn--small', 'Vernieuwen');
  refresh.type = 'button';
  refresh.id = 'review-refresh';
  refresh.disabled = state.status === 'loading';
  refresh.addEventListener('click', () => openActionReview(state.node, true));
  const close = make('button', 'btn btn--ghost btn--small', 'Sluiten');
  close.type = 'button';
  close.id = 'review-close';
  close.addEventListener('click', closeDialog);
  bar.append(back, refresh, close);
  const review = state.review;
  const stamp = review && clockLabel(review.generated_at);
  bar.appendChild(make('span', 'report-heading', 'Alleen lezen — een voorstel van Runner 2, er wordt niets gesloten' + (stamp ? ' · bijgewerkt om ' + stamp : '')));
  body.appendChild(bar);
  if (state.status === 'loading') {
    body.appendChild(make('p', 'dialog-why report-status', 'Opschoonvoorstel wordt opgehaald…'));
    return;
  }
  if (state.status === 'unsupported') {
    body.appendChild(make('p', 'dialog-note review-unsupported', state.error || REVIEW_UNSUPPORTED_TEXT));
    return;
  }
  if (state.status === 'error') {
    const error = make('p', 'dialog-note report-status', state.error);
    error.dataset.tone = 'bad';
    body.appendChild(error);
    return;
  }
  const banner = make('p', 'dialog-note review-banner', review.closing_enabled
    ? 'Voorstel – Monitor 2 sluit nooit iets; sluiten gebeurt niet vanuit dit venster.'
    : 'Voorstel – er wordt niets gesloten');
  body.appendChild(banner);
  body.appendChild(make('p', 'review-headline', review.headline));
  if (review.breaker) {
    const warn = make('p', 'dialog-note review-breaker', 'Waarschuwing: de stroomonderbreker van Runner 2 is afgegaan. ' + (review.breaker.message || ''));
    warn.dataset.tone = 'bad';
    body.appendChild(warn);
  }
  const groups = actionReviewGroups(review);
  const counts = review.counts || {};
  reviewSection(body, 'Zou sluiten — veilig aantoonbare dubbelen', counts.would_close || 0, groups.safe.map((e) => ({ entry: e, kind: 'safe' })), true);
  const doubtCount = (counts.review || 0) + groups.doubt.filter((d) => d.kind === 'maybe').length;
  reviewSection(body, 'Mogelijke dubbelen en twijfel', doubtCount, groups.doubt, true);
  reviewSection(body, 'Echte menselijke beslissingen', Math.max(0, (counts.needs_decision || 0) - groups.doubt.filter((d) => d.kind === 'maybe').length), groups.human, false);
  if (review.not_shown) {
    body.appendChild(make('p', 'dialog-note', review.not_shown + ' punten worden hier niet getoond (alleen de eerste ' + 100 + ' per groep).'));
  }
  if (state.cached) body.appendChild(make('p', 'dialog-why', 'Uit het geheugen van Monitor 2 (maximaal 10 minuten oud). Gebruik Vernieuwen voor een nieuwe lezing.'));
}

/* ---- supervisiepunten van één machine (de SUPERVISIE NODIG/GEBLOKKEERD-badge) ---- */

const SUP_EVENT_TEXT = { run_ended: 'run afgelopen', project_started: 'project gestart', goal_amended: 'projectdoel gewijzigd',
  request_review: 'herbeoordeling gevraagd', blocker_resolved: 'blokkade opgelost', manual_assign: 'handmatige opdracht' };
const SUP_ASSIGNMENT_TEXT = { planned: 'gepland', running: 'loopt', ended: 'afgerond' };
const SUP_TASK_TEXT = { completed: 'klaar', failed: 'mislukt', waiting_for_human: 'wacht op antwoord', running: 'bezig',
  stopped: 'gestopt', paused: 'gepauzeerd' };
const SUP_CONTROL_TEXT = { active: 'project actief', paused: 'project gepauzeerd', stopped: 'project gestopt', complete: 'project klaar' };

/** De punten die het venster toont: precies wat de badge telt (projecten met aandacht), anders niets. */
function supervisionItems(node) {
  if (!supervisionAttention(node)) return [];
  return node.supervision.projects.filter((p) => p && p.attention);
}

function supervisionSignature(node) {
  return node ? JSON.stringify(supervisionItems(node)) : '';
}

function openSupervision(nodeId) {
  const node = nodeById(nodeId);
  if (!node || !supervisionAttention(node)) return;
  page.dialog = { kind: 'supervision', node: nodeId, editing: false, note: null, tone: null, busy: false, draft: undefined, signature: null };
  const dialog = el('dialog');
  dialog.dataset.kind = 'supervision';
  refreshDialog(true);
  if (!dialog.open) dialog.showModal();
}

/** De betreffende opdracht in één regel: "A2 · taak afgerond (goal_reached) · klaar sinds 9 okt. 08:26". */
function supervisionTaskStatus(task) {
  if (!task) return null;
  const status = SUP_ASSIGNMENT_TEXT[task.status] || task.status;
  const outcome = task.outcome ? ' (' + task.outcome + ')' : '';
  const taskState = task.task_status ? 'taak ' + (SUP_TASK_TEXT[task.task_status] || task.task_status) : null;
  const ended = actionCreatedLabel(task.ended_at);
  return [status ? 'opdracht ' + status + outcome : null, taskState, ended ? 'geëindigd ' + ended : null, task.error ? 'fout: ' + task.error : null]
    .filter(Boolean).join(' · ');
}

function supervisionStatusText(p) {
  const backlog = p.backlog || {};
  const usage = p.usage || {};
  return [p.label, SUP_CONTROL_TEXT[p.control] || p.control, supervisionTaskStatus(p.assignment),
    Number.isInteger(backlog.open) ? 'backlog ' + backlog.open + ' open, ' + (backlog.done || 0) + ' klaar' : null,
    usage.assignments ? 'opdrachten ' + usage.assignments : null].filter(Boolean).join(' · ');
}

function supervisionBlockText(p) {
  const parts = [];
  if (p.integrity) parts.push('dossier: ' + p.integrity);
  if (p.event && p.event.state === 'escalated') parts.push('event ' + p.event.id + ' is geëscaleerd');
  (p.blockers || []).forEach((b) => parts.push(b.id + ': ' + (b.question || 'open vraag')));
  return parts.length ? parts.join(' · ') : 'Geen blokkade; alleen de beslissing ontbreekt.';
}

function supervisionEventText(p) {
  const ev = p.event;
  if (!ev) return null;
  const since = actionCreatedLabel(ev.created_at);
  const more = Number.isInteger(p.events_open) && p.events_open > 1 ? ' · ' + p.events_open + ' open events' : '';
  return (SUP_EVENT_TEXT[ev.kind] || ev.kind) + ' (' + ev.id + ')' + (since ? ' sinds ' + since : '') + more
    + (ev.summary ? ': ' + ev.summary : '');
}

function supervisionDecisionText(last) {
  if (!last) return null;
  const at = actionCreatedLabel(last.at);
  return [last.kind, last.by ? 'door ' + last.by : null, at].filter(Boolean).join(' · ') + (last.reason ? ': ' + last.reason : '');
}

async function copySupervisionRequest(state, project) {
  const ok = await copyText(project.request || '');
  if (page.dialog !== state) return;
  state.note = ok ? 'Verzoek voor ' + project.project_id + ' gekopieerd; plak het in ChatGPT.'
    : 'Kopiëren lukte niet; selecteer de tekst en kopieer hem handmatig.';
  state.tone = ok ? 'ok' : 'bad';
  refreshDialog(true);
}

function supervisionEntry(project, state) {
  const entry = make('li', 'sup-item');
  entry.dataset.project = project.project_id || '';
  const head = make('div', 'sup-head');
  head.appendChild(make('strong', 'sup-title', project.title || project.project_id));
  const pill = make('span', 'sup-label', project.label || '');
  pill.dataset.tone = project.blocked ? 'alert' : 'human';
  head.appendChild(pill);
  entry.appendChild(head);
  const task = project.assignment || {};
  // De opdracht noemt de Runner bij zijn node-ID (LINUX); op de pagina staat de weergavenaam (STUDIO).
  const runner = nodeName(nodeById(task.node)) || task.node_label || task.node;
  entry.appendChild(facts([
    { label: 'Runner', value: runner || 'nog geen opdracht' },
    { label: 'Opdracht', value: task.assignment_id ? task.assignment_id + (task.goal ? ' · ' + task.goal : '') : null },
    { label: 'Taak-ID', value: task.task_id || null, mono: true },
    { label: 'Project', value: project.project_id, mono: true },
    { label: 'Reden', value: project.reason || null },
    { label: 'Open event', value: supervisionEventText(project) },
    { label: 'Volgende stap', value: project.next_step || null },
    { label: 'Status', value: supervisionStatusText(project) },
    { label: 'Blokkade', value: supervisionBlockText(project) },
    { label: 'Laatste besluit', value: supervisionDecisionText(project.last_decision) },
  ]));
  const actions = Array.isArray(project.actions) ? project.actions.filter((a) => a && a.title) : [];
  if (actions.length) {
    entry.appendChild(make('p', 'dialog-section-title', 'Wat kan het oplossen'));
    const list = make('ol', 'sup-actions');
    actions.forEach((a) => {
      const row = make('li', null);
      row.appendChild(make('strong', null, a.title + ': '));
      row.appendChild(make('span', null, a.how || ''));
      list.appendChild(row);
    });
    entry.appendChild(list);
  }
  const bar = make('div', 'sup-buttons');
  if (task.task_id && task.node) {
    const report = make('button', 'btn btn--warm btn--small', 'Lees taakrapport');
    report.type = 'button';
    report.dataset.task = task.task_id;
    report.title = 'Volledig eindrapport van ' + task.task_id + ' op ' + runner;
    report.addEventListener('click', () => openReportTask(task.node, runner, task.task_id,
      [project.title, task.assignment_id].filter(Boolean).join(' · '), task.ended_at));
    bar.appendChild(report);
  }
  if (project.request) {
    const copy = make('button', 'btn btn--ghost btn--small', 'Kopieer verzoek voor ChatGPT');
    copy.type = 'button';
    copy.addEventListener('click', () => copySupervisionRequest(state, project));
    bar.appendChild(copy);
  }
  if (bar.children.length) entry.appendChild(bar);
  return entry;
}

function dialogSupervision(body, state) {
  const node = nodeById(state.node);
  const items = node ? supervisionItems(node) : [];
  const label = nodeName(node);
  el('dialog-title').textContent = 'Supervisie · ' + label + ' (' + items.length + ')';
  const bar = make('div', 'report-bar');
  const close = make('button', 'btn btn--ghost btn--small', 'Sluiten');
  close.type = 'button';
  close.id = 'supervision-close';
  close.addEventListener('click', closeDialog);
  bar.appendChild(close);
  bar.appendChild(make('span', 'report-heading', 'Uit de supervisiedossiers van Runner 2 op deze machine; ververst vanzelf.'));
  body.appendChild(bar);
  dialogNote(body);
  if (!items.length) {
    body.appendChild(make('p', 'dialog-why sup-empty', 'Er staan geen supervisiepunten meer open.'));
    return;
  }
  const list = make('ol', 'sup-list');
  items.forEach((project) => list.appendChild(supervisionEntry(project, state)));
  body.appendChild(list);
}

/** Zet tekst op het klembord: de moderne route, anders een tijdelijk tekstvak (ook zonder beveiligde context). */
async function copyText(text) {
  const nav = window.navigator;
  if (nav && nav.clipboard && typeof nav.clipboard.writeText === 'function') {
    try { await nav.clipboard.writeText(text); return true; } catch (err) { /* val terug */ }
  }
  try {
    const area = document.createElement('textarea');
    area.value = text;
    area.setAttribute('readonly', '');
    area.className = 'copy-buffer';
    document.body.appendChild(area);
    area.select();
    const done = document.execCommand('copy');
    document.body.removeChild(area);
    return Boolean(done);
  } catch (err) { return false; }
}

async function copyReport() {
  const state = page.dialog;
  if (!state || state.kind !== 'report' || state.status !== 'ok' || !state.report) return;
  const ok = await copyText(state.report.text || '');
  if (page.dialog !== state) return;
  state.note = ok ? 'Rapport gekopieerd naar het klembord.' : 'Kopiëren lukte niet; selecteer de tekst en kopieer hem handmatig.';
  state.tone = ok ? 'ok' : 'bad';
  refreshDialog(true);
}

function dialogReport(body, state) {
  el('dialog-title').textContent = 'Eindrapport — ' + state.label;
  const bar = make('div', 'report-bar');
  const copy = make('button', 'btn btn--warm btn--small', 'Kopiëren');
  copy.type = 'button';
  copy.id = 'report-copy';
  copy.disabled = state.status !== 'ok';
  copy.addEventListener('click', copyReport);
  const close = make('button', 'btn btn--ghost btn--small', 'Sluiten');
  close.type = 'button';
  close.id = 'report-close';
  close.addEventListener('click', closeDialog);
  bar.append(copy, close);
  const clock = clockLabel(state.finishedAt);
  const heading = [state.title, state.task, clock ? 'klaar om ' + clock : null].filter(Boolean).join(' · ');
  bar.appendChild(make('span', 'report-heading', heading));
  body.appendChild(bar);
  if (state.note) {
    const note = make('p', 'dialog-note', state.note);
    if (state.tone) note.dataset.tone = state.tone;
    body.appendChild(note);
  }
  if (state.status === 'loading') {
    body.appendChild(make('p', 'dialog-why report-status', 'Rapport wordt opgehaald…'));
    return;
  }
  if (state.status === 'error') {
    const error = make('p', 'dialog-note report-status', state.error);
    error.dataset.tone = 'bad';
    body.appendChild(error);
    const retry = make('button', 'btn btn--ghost btn--small', 'Opnieuw proberen');
    retry.type = 'button';
    retry.id = 'report-retry';
    retry.addEventListener('click', () => openReportTask(state.node, state.label, state.task, state.title, state.finishedAt));
    body.appendChild(retry);
    return;
  }
  const article = make('article', 'report');
  (state.report.sections || []).forEach((section) => {
    const block = make('section', 'report-section');
    block.appendChild(make('h3', 'report-title', section.title));
    if (section.text) block.appendChild(make('div', 'report-text', section.text));
    if (section.items && section.items.length) {
      const list = make('ul', 'report-items');
      section.items.forEach((item) => list.appendChild(make('li', null, item)));
      block.appendChild(list);
    }
    article.appendChild(block);
  });
  body.appendChild(article);
}

/* -------------------------------------------------- usage en meldingen */

/** Claude Usage in één kolom: de limieten die Claude meldt (5-uurslimiet, week voor alle
 *  modellen, en een modelspecifieke week zoals Fable alleen als Claude die rapporteert)
 *  onder elkaar, in één gedeeld raster zodat alle balken even breed zijn. */
function usageSeverity(percent) {
  if (percent >= 100) return 'exhausted';
  if (percent >= 90) return 'critical';
  if (percent >= 75) return 'warning';
  return 'normal';
}

function renderUsage(usage) {
  const box = el('usage');
  box.dataset.status = usage.status || 'unavailable';
  const nowMs = Date.now();
  const list = el('usage-limits');
  clear(list);
  const ordered = usage.limits.filter((limit) => limit.group !== 'model')
    .concat(usage.limits.filter((limit) => limit.group === 'model'));
  ordered.forEach((limit) => {
    const row = make('li', 'usage-row');
    row.dataset.severity = usageSeverity(limit.percent);
    row.dataset.group = limit.group;
    row.appendChild(make('span', 'usage-label', limit.label));
    const bar = make('span', 'usage-bar');
    const inner = make('span', 'usage-fill');
    inner.style.width = Math.max(0, Math.min(100, limit.percent)) + '%';
    bar.appendChild(inner);
    row.appendChild(bar);
    row.appendChild(make('span', 'usage-percent', limit.percent + '%'));
    row.appendChild(make('span', 'usage-reset', resetText(limit.resets_at, nowMs)));
    list.appendChild(row);
  });
  if (!usage.limits.length) {
    list.appendChild(make('li', 'usage-empty',
      usage.message || 'Claude meldt nu geen limieten; er wordt niets geschat.'));
  }
  let note = null;
  if (usage.status === 'stale') note = (usage.message || 'Usage verouderd') + ' — niet de stand van nu.';
  else if (usage.status === 'pending') note = usage.message || 'Gebruik wordt opgehaald…';
  else if (usage.status !== 'ok' && usage.limits.length) note = usage.message || 'Usage niet beschikbaar.';
  // INF-02: de Claude-login loopt af; dat staat vóór alles, ook als de usage klopt.
  const expiry = usage.login_expiry && usage.login_expiry.warning ? usage.login_expiry.message : '';
  box.dataset.loginExpiry = expiry ? 'warning' : '';
  note = [expiry, note].filter(Boolean).join(' · ');
  el('usage-note').hidden = !note;
  el('usage-note').textContent = note || '';
}

/* ---- STUDIO (de NUC): de webserverstatus als compacte regel in de machinekaart (alleen op GERARD; de server leest, de pagina toont) ---- */

/** Zuiver: de NUC-stand van de server → de webserverstatus. Alleen een verse, expliciet gezonde stand is "gezond";
 *  geen gegevens, onbereikbaar, verouderd of onleesbaar is altijd "onbekend", nooit een stille "gezond". */
function webserverView(nuc) {
  const status = nuc && nuc.status ? nuc.status : null;
  if (nuc && nuc.state === 'gezond' && !nuc.stale && status && status.gezond === true) {
    return { state: 'gezond', tone: 'ok', icon: '\u{1F7E2}', text: 'Gezond' };
  }
  if (nuc && nuc.state === 'ongezond' && !nuc.stale && status && status.gezond === false) {
    return { state: 'storing', tone: 'alert', icon: '\u{1F534}', text: 'Storing' };
  }
  return { state: 'onbekend', tone: 'off', icon: '\u26AA', text: 'Onbekend' };
}

/** Zet de regel "Status webserver: …" in de kaart van de NUC (node-ID linux); andere kaarten hebben die regel niet.
 *  De technische details (healthz, release, schijf, ouderdom) blijven in /api/dashboard en in data-state voor diagnose. */
function renderWebserver(nuc) {
  const nodes = cardNodes.linux;
  if (!nodes || !nodes.web) return;
  const view = webserverView(nuc);
  nodes.web.dataset.tone = view.tone;
  nodes.web.dataset.state = view.state;
  nodes.web.dataset.nucState = nuc && nuc.state ? nuc.state : '';
  nodes.web.textContent = 'Status webserver: ' + view.icon + ' ' + view.text;
}

function renderNotifications(info) {
  const button = el('remote-switch');
  const available = info && info.available && typeof info.enabled === 'boolean';
  button.disabled = !available || !page.actionsEnabled || page.busy;
  button.setAttribute('aria-pressed', available && info.enabled ? 'true' : 'false');
  el('remote-text').textContent = !available ? 'niet beschikbaar' : (info.enabled ? '🔔 AAN' : '🔕 UIT');
  const parts = [];
  if (available) {
    if (!info.configured) parts.push('e-mail niet geconfigureerd');
    if (!info.notifier_running) parts.push('notifier draait niet');
    if (info.last_error && info.last_error.message) parts.push('laatste fout: ' + info.last_error.message);
  }
  const line = el('notify-line');
  const local = soundStateText();
  line.textContent = [local].concat(parts).join(' · ');
  line.title = line.textContent;
  line.dataset.tone = parts.length && available && info.enabled ? 'bad' : '';
  setToggle(el('sound-switch'), page.sound.sound);
  setToggle(el('speech-switch'), page.sound.speech);
}

function setToggle(button, on) {
  button.setAttribute('aria-pressed', on ? 'true' : 'false');
  button.querySelector('.toggle-text').textContent = on ? 'AAN' : 'UIT';
}

function dialogSettings(body) {
  el('dialog-title').textContent = 'Instellingen — meldingen en geluid';
  const info = page.last && page.last.notifications;
  body.appendChild(make('p', 'dialog-section-title', 'Remote meldingen'));
  body.appendChild(make('p', 'dialog-why', info && info.configured
    ? 'Stuur één herkenbare testmail naar de ingestelde ontvanger. Dit wijzigt geen taak of instelling.'
    : 'E-mail is niet volledig geconfigureerd; stel de SMTP-route eerst buiten Monitor 2 in.'));
  const testRow = make('div', 'dialog-controls');
  const testMail = make('button', 'btn btn--ghost', 'Stuur testmail');
  testMail.type = 'button';
  testMail.id = 'remote-test-mail';
  testMail.disabled = !page.actionsEnabled || page.dialog.busy || !info || !info.available || !info.configured;
  testMail.addEventListener('click', sendNotificationTest);
  testRow.appendChild(testMail);
  body.appendChild(testRow);

  body.appendChild(make('p', 'dialog-section-title', 'Geluid en spraak op deze machine'));
  body.appendChild(make('p', 'dialog-why',
    'Deze instellingen gelden alleen voor deze machine en dit Monitor 2-venster.'));
  const grid = make('div', 'settings-grid');
  const field = (label, key) => {
    const id = 'set-' + key;
    const lab = make('label', null, label);
    lab.htmlFor = id;
    const input = make('input');
    input.type = 'time';
    input.id = id;
    input.value = page.sound[key];
    input.addEventListener('change', () => {
      page.sound = normalizeSound({ ...page.sound, [key]: input.value });
      writeStored(SOUND_KEY, page.sound);
      renderNotifications(page.last && page.last.notifications);
    });
    grid.append(lab, input);
  };
  field('Stil vanaf', 'quietFrom');
  field('Stil tot', 'quietTo');
  body.appendChild(grid);
  body.appendChild(make('p', 'dialog-why',
    'Geluid en spraak klinken bij WACHT OP ACTIE, bij klaar en bij een fout — één keer per overgang, '
    + 'niet tijdens de stille uren. Het Monitor 2-venster staat geluid automatisch toe.'));
  dialogNote(body);
}

/* ------------------------------------------------------------ acties */

/** Pauze, Hervatten en Beantwoorden: één POST naar Monitor 2, dat de controllerfunctie van Runner 2 aanroept. */
async function sendAction(nodeId, action, extra) {
  const node = nodeById(nodeId);
  if (!node || page.busy) return;
  const body = actionBody(node, action, newNonce(), extra);
  if (!body) return;
  const state = page.dialog && page.dialog.node === nodeId && action === 'answer' ? page.dialog : null;
  page.busy = true;
  if (state) { state.busy = true; state.note = 'Bezig…'; state.tone = null; refreshDialog(true); }
  page.notes[nodeId] = { text: 'Bezig…', tone: null };
  render(page.last);
  let result;
  try {
    result = await post('/api/action', body);
  } catch (err) {
    result = { ok: false, payload: { message: 'Geen antwoord: ' + err.message + ' — er is niets uitgevoerd.' } };
  }
  page.busy = false;
  const message = result.payload.message || (result.ok ? 'Doorgegeven.' : 'Er is niets uitgevoerd.');
  page.notes[nodeId] = { text: message, tone: result.ok ? 'ok' : 'bad' };
  if (state) {
    state.busy = false;
    state.note = message;
    state.tone = result.ok ? 'ok' : 'bad';
    if (result.ok) { state.editing = false; state.draft = undefined; }
    refreshDialog(true);
  }
  render(page.last);
  poll();
}

async function sendStop(nodeId) {
  const node = nodeById(nodeId);
  if (!node || !node.task || !node.task.actions.stop) return;
  if (!window.confirm('De taak op ' + nodeName(node) + ' stoppen? De sessie wordt beëindigd; er wordt niets toegepast.')) return;
  await sendAction(nodeId, 'stop', { reason: 'via Monitor 2' });
}

/* ------------------------------------------------------------ acties */

async function post(path, body) {
  const response = await fetch(path, {
    method: 'POST',
    credentials: 'same-origin',   // het HttpOnly-actiecookie; Origin zet de browser zelf
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  let payload = {};
  try { payload = await response.json(); } catch (err) { payload = {}; }
  return { ok: response.ok && payload.ok !== false, status: response.status, payload };
}

async function toggleRemote() {
  const info = page.last && page.last.notifications;
  if (!info || !info.available || page.busy) return;
  page.busy = true;
  let result;
  try {
    result = await post('/api/notifications', { on: !info.enabled, nonce: newNonce() });
  } catch (err) {
    result = { ok: false, payload: { message: 'Niet omgezet: ' + err.message } };
  }
  page.busy = false;
  if (!result.ok) {
    el('notify-line').textContent = result.payload.message || 'Niet omgezet.';
    el('notify-line').title = el('notify-line').textContent;
    el('notify-line').dataset.tone = 'bad';
  }
  poll(true);
}

async function sendNotificationTest() {
  const state = page.dialog;
  const info = page.last && page.last.notifications;
  if (!state || state.kind !== 'settings' || state.busy || !page.actionsEnabled
      || !info || !info.available || !info.configured) return;
  state.busy = true;
  state.note = 'Testmail wordt verstuurd…';
  state.tone = null;
  refreshDialog(true);
  let result;
  try {
    result = await post('/api/notifications/test', { nonce: newNonce() });
  } catch (err) {
    result = { ok: false, payload: { message: 'Geen antwoord: ' + err.message + ' — er is niets verstuurd.' } };
  }
  state.busy = false;
  state.note = result.payload.message || (result.ok ? 'Testmail verstuurd.' : 'Testmail niet verstuurd.');
  state.tone = result.ok ? 'ok' : 'bad';
  refreshDialog(true);
}

/* ------------------------------------------------------ geluid en spraak */

// ctx: het audiokanaal; speechBlocked: de browser weigerde spraak (nog geen gebruikersgebaar); pending: meldingen die
// klinken moeten maar nog niet konden ({ event, moment }).
const audio = { ctx: null, speechBlocked: false, pending: [] };

function quietNow() {
  return quietAt(new Date());
}

function quietAt(when) {
  const at = typeof when === 'number' ? new Date(when) : when;
  return inQuietHours(at.getHours() * 60 + at.getMinutes(),
    parseClock(page.sound.quietFrom), parseClock(page.sound.quietTo));
}

function audioRunning() {
  return !!audio.ctx && audio.ctx.state === 'running';
}

/** Moet er geluid of spraak klinken dat de browser nu niet toestaat? Dan vraagt de pagina één klik. */
function audioBlocked() {
  return (page.sound.sound && !audioRunning()) || (page.sound.speech && audio.speechBlocked);
}

function soundStateText() {
  if (quietNow()) return '🌙 Stille uren tot ' + page.sound.quietTo;
  if (!page.sound.sound && !page.sound.speech) return 'Geluid en spraak uit — alleen beeld';
  if (audioBlocked()) return audio.pending.length ? '🔔 Er wacht een melding die je niet kon horen' : 'Geluid is in dit browservenster geblokkeerd';
  const quiet = page.sound.quietFrom && page.sound.quietTo
    ? ' (stil ' + page.sound.quietFrom + '–' + page.sound.quietTo + ')' : '';
  return 'Lokale meldingen actief' + quiet;
}

/**
 * Probeert het audiokanaal te activeren. Een browser staat dat pas toe na een gebaar van de gebruiker
 * (klik, toets, aanraking); zonder gebaar blijft het kanaal 'suspended' en toont de pagina de ontgrendelbalk.
 * Na één geslaagde ontgrendeling blijven toon, spraak en wachtende meldingen vanzelf werken.
 */
function unlockAudio(gesture) {
  const Ctor = window.AudioContext || window.webkitAudioContext;
  if (gesture) audio.speechBlocked = false;
  if (Ctor) {
    if (!audio.ctx) {
      try {
        audio.ctx = new Ctor();
        audio.ctx.onstatechange = () => { flushAudible(); };
      } catch (err) { audio.ctx = null; }
    }
    if (audio.ctx && audio.ctx.state !== 'running') {
      try { audio.ctx.resume().then(() => { flushAudible(); }, () => {}); } catch (err) { /* nog geblokkeerd */ }
    }
  }
  if (gesture) flushAudible();
}

const TONES = {
  done: [[880, 0, 0.16], [1318.5, 0.14, 0.34]],
  human: [[659.25, 0, 0.16], [880, 0.2, 0.16], [880, 0.44, 0.24]],
  error: [[392, 0, 0.26], [293.7, 0.24, 0.42]],
};

function playTone(kind) {
  const ctx = audio.ctx;
  if (!ctx || ctx.state !== 'running') return false;
  const start = ctx.currentTime + 0.02;
  (TONES[kind] || TONES.done).forEach(([freq, offset, length]) => {
    const osc = ctx.createOscillator();
    const gain = ctx.createGain();
    osc.frequency.value = freq;
    const t0 = start + offset;
    gain.gain.setValueAtTime(0.0001, t0);
    gain.gain.exponentialRampToValueAtTime(0.18, t0 + 0.03);
    gain.gain.exponentialRampToValueAtTime(0.0001, t0 + length);
    osc.connect(gain).connect(ctx.destination);
    osc.start(t0);
    osc.stop(t0 + length + 0.05);
  });
  return true;
}

/** Spreekt een zin uit; ``onBlocked`` volgt als de browser spraak zonder gebruikersgebaar weigert. Geeft false zonder spraakdienst. */
function speak(sentence, onBlocked) {
  const synth = window.speechSynthesis;
  if (!synth || !sentence || typeof SpeechSynthesisUtterance === 'undefined') return false;
  try {
    if (synth.paused) synth.resume();
    const utterance = new SpeechSynthesisUtterance(sentence);
    utterance.lang = 'nl-NL';
    utterance.onerror = (event) => {
      if (event && event.error === 'not-allowed') {
        audio.speechBlocked = true;
        if (onBlocked) onBlocked();
        updateAudioNotice();
      }
    };
    synth.speak(utterance);
    return true;
  } catch (err) { return false; }
}

const KIND_RANK = { human: 3, error: 2, done: 1 };

/** Laat alle wachtende meldingen klinken als het mag: één toon (de zwaarste soort) en elke zin één keer. */
function flushAudible() {
  const now = Date.now();
  audio.pending = audio.pending.filter((item) => now - item.moment <= PENDING_MAX_MS);
  if (audio.pending.length && (quietNow() || (!page.sound.sound && !page.sound.speech))) {
    audio.pending = [];                        // stille uren of alles uit: bewust niet later alsnog afspelen
    if (window.speechSynthesis && quietNow()) { try { window.speechSynthesis.cancel(); } catch (err) { /* geen spraak */ } }
  }
  if (audio.pending.length && !audioBlocked()) {
    const batch = audio.pending;
    audio.pending = [];
    const toned = batch.filter((item) => !item.speechOnly).map((item) => item.event.kind)
      .sort((a, b) => (KIND_RANK[b] || 0) - (KIND_RANK[a] || 0));
    if (page.sound.sound && toned.length) playTone(toned[0]);
    if (page.sound.speech) {
      const spoken = new Set();
      batch.forEach((item) => {
        if (spoken.has(item.event.speech) || spoken.size >= 3) return;
        spoken.add(item.event.speech);
        // Weigert de browser de spraak alsnog, dan blijft de melding klaarstaan voor de volgende ontgrendeling.
        speak(item.event.speech, () => {
          if (!audio.pending.some((p) => p.event.key === item.event.key)) audio.pending.push({ ...item, speechOnly: true });
        });
      });
    }
  }
  updateAudioNotice();
}

/** De zichtbare ontgrendelbalk: alleen als geluid of spraak aan staat maar de browser het (nog) niet toestaat. */
function updateAudioNotice() {
  if (typeof document === 'undefined') return;
  const gate = document.getElementById('audio-gate');
  if (!gate) return;
  const blocked = audioBlocked() && !quietNow();
  gate.hidden = !blocked;
  if (blocked) {
    gate.textContent = audio.pending.length
      ? '🔔 Er wacht een melding die je niet kon horen — klik hier één keer om geluid te activeren.'
      : '🔇 Geluid staat in dit venster nog uit — klik hier één keer; daarna klinken meldingen vanzelf.';
  }
  if (page.last) renderNotifications(page.last.notifications);
}

/**
 * Verwerkt het meldingsjournaal van een dashboardantwoord: nieuwe gebeurtenissen worden zichtbaar en, als het mag, hoorbaar.
 * Draait vóór het tekenen van de kaarten, zodat een weergavefout nooit een melding laat verdwijnen.
 */
function processNotifications(feed) {
  if (!page.notify) page.notify = normalizeNotifyMemory(readStored(NOTIFY_KEY, null));
  const plan = planNotifications(feed, page.notify, Date.now(), quietAt);
  const changed = JSON.stringify(plan.memory) !== JSON.stringify(page.notify);
  page.notify = plan.memory;
  if (changed) writeStored(NOTIFY_KEY, plan.memory);
  plan.fresh.forEach(({ event, moment, audible }) => {
    if (audible) audio.pending.push({ event, moment });
  });
  flushAudible();
}

/* ---------------------------------------------------------------- klok */

function tickClock() {
  const now = new Date();
  el('clock-time').textContent = now.toLocaleTimeString('nl-NL', { hour: '2-digit', minute: '2-digit' });
  const date = now.toLocaleDateString('nl-NL', { weekday: 'short', day: 'numeric', month: 'short', year: 'numeric' });
  el('clock-date').textContent = date.charAt(0).toUpperCase() + date.slice(1);
  flushAudible();                          // stille uren beginnen/eindigen, of geluid is intussen ontgrendeld
}

/* ================================================================ Opdrachten */

/* De opdrachtwachtrij leeft in Runner 2 (runner2_jobs, op de control plane). Deze pagina tekent hem alleen en
 * stuurt knopdrukken door via POST /api/jobs; welke knop kan en wat de status betekent komt uit het overzicht
 * zelf (``state``, ``label``, ``why``, ``status``). Gebruikersinvoer komt alleen als tekst op de pagina. */

const JOBS_POLL_OK_MS = 4000;
const JOBS_POLL_FAIL_MS = 8000;
const JOBS_USER_KEY = 'obf-monitor2-jobs-user';
// De laatst gekozen repository per Runner ({ MAC: pad, … }), in localStorage: overleeft verversen, sluiten en herstarten.
const JOBS_REPO_KEY = 'obf-monitor2-jobs-repo';
const REPO_AUTO = 'auto';   // de waarde van "Automatisch bepalen" in het menu en in de aanvraag
const JOB_NODES = ['MAC', 'GERARD', 'LINUX', 'WINDOWS'];
// Elke Runner krijgt een eigen, gelijkwaardig wachtrijkader: Mac, STUDIO (de NUC), Gerard en Windows, in die volgorde
// (dezelfde volgorde als de machinekaarten in het monitoroverzicht; alleen weergave, niet de uitvoeringsvolgorde).
// De NUC heet in Runner 2 LINUX (node-id, API, historie): alleen de weergave zegt "STUDIO" (ondertitel Ubuntu (NUC)).
const JOB_QUEUE_NODES = ['MAC', 'LINUX', 'GERARD', 'WINDOWS'];
const JOB_NODE_TITLE = { MAC: 'Mac \u2014 wachtrij', GERARD: 'Gerard \u2014 wachtrij', LINUX: 'STUDIO \u00b7 Ubuntu (NUC) \u2014 wachtrij',
  WINDOWS: 'Windows \u2014 wachtrij' };
const JOB_NODE_NAME = { MAC: 'Mac', GERARD: 'Gerard', LINUX: 'STUDIO', WINDOWS: 'Windows' };
// Hoe een node-id in meldingen en knoppen heet; de rest blijft gewoon het id (MAC, GERARD, WINDOWS).
const JOB_NODE_LABEL = { LINUX: 'STUDIO' };
function jobNodeLabel(id) { return JOB_NODE_LABEL[id] || id; }
const JOB_END_STATUSES = ['done', 'failed', 'incomplete', 'stopped', 'cancelled'];
const JOB_ACTIVE_STATUSES = ['starting', 'running'];

/** Statuskleur per ``state``-sleutel van Runner 2; naast de kleur staat altijd ook het label zelf. */
const JOB_BADGE = {
  bezig: 'actief', start: 'actief', wacht_op_actie: 'actief', wordt_geannuleerd: 'actief',
  in_wachtrij: 'wacht', vastgehouden: 'wacht', wacht_op_voorganger: 'wacht', wacht_op_runner: 'wacht',
  wachtrij_gepauzeerd: 'wacht', gepauzeerd: 'wacht',
  wacht_op_goedkeuring: 'goedkeuring', geblokkeerd: 'geblokkeerd',
  claude_klaar: 'klaar', chatgpt_gecontroleerd: 'gecontroleerd', gecontroleerd: 'gecontroleerd',
  afgekeurd: 'fout', mislukt: 'fout', limiet: 'fout', gestopt: 'fout', geannuleerd: 'afgebroken',
};

const jobsPage = {
  receivedAt: null,
  expiryTimer: null,
  data: null,          // laatste overzicht van /api/jobs
  meta: null,          // formulier- en gebruikersgegevens
  timer: null,
  failures: 0,
  busy: false,         // een verzending loopt
  note: null,          // { text, tone } laatste melding in het blok
  error: null,         // leesfout van het overzicht
  signature: null,
  open: {},            // node -> "recent afgerond" uitgeklapt
  requestId: null,     // kenmerk van de verzending die nu klaarstaat
  deps: new Set(),     // job-id's die bij de taak zelf (NU BEZIG / WACHTRIJ) zijn aangevinkt; ook uit het concept
  user: null,
  repoExtra: null,     // { split, project_repos }: antwoord op een vraag van Runner 2 bij Automatisch bepalen
  repoNode: null,      // voor welke Runner de repositorylijst nu getekend is
  repoSignature: null,
  draftRepo: null,     // { node, repo } uit het concept, tot de lijst er is
};

/** Bediening aan? Uit het eigen jobs-antwoord, los van het dashboard. */
function jobsEnabled() {
  return Boolean(jobsPage.data) && jobsPage.data.actions_enabled !== false && !jobsPage.data.handed_over;
}

function jobBadgeClass(state) {
  return JOB_BADGE[state] || 'wacht';
}

function jobIsQueued(job) { return Boolean(job) && job.status === 'queued'; }
function jobIsActive(job) { return Boolean(job) && JOB_ACTIVE_STATUSES.includes(job.status); }
function jobIsOpen(job) { return Boolean(job) && (job.status === 'queued' || JOB_ACTIVE_STATUSES.includes(job.status)); }

/** Mag er een rapport worden gelezen? Alleen als de opdracht een taak-id heeft en echt gestart is. */
function jobCanReport(job) {
  return Boolean(job && job.task_id && job.started_at && /^R2-[0-9a-f]{12}$/.test(job.task_id));
}

/** Het adres van het rapport van een opdracht: de bestaande rapportroute, met de node in kleine letters. */
function jobReportUrl(job) {
  return reportUrl(String(job.node || '').toLowerCase(), job.task_id);
}

/**
 * Welke knoppen een opdracht krijgt. ``ctx`` = { enabled, index, count } (plek in de wachtrij). Elke knop:
 * { key, label, action, fields, confirm, danger, disabled, why }. Alleen wat kan wordt getoond; Runner 2 weigert de rest zelf ook.
 */
function jobButtons(job, ctx) {
  const enabled = !ctx || ctx.enabled !== false;
  const index = ctx && Number.isInteger(ctx.index) ? ctx.index : 0;
  const count = ctx && Number.isInteger(ctx.count) ? ctx.count : 1;
  const out = [];
  const add = (key, label, action, fields, extra) => {
    const spec = { key, label, action, fields: fields || {}, confirm: null, danger: false, disabled: false, why: '', ...extra };
    if (!enabled) { spec.disabled = true; spec.why = 'Monitor 2 draait zonder bediening (alleen lezen).'; }
    out.push(spec);
  };
  const approval = job.approval && job.approval.required && job.approval.state === 'open' ? job.approval : null;
  if (jobIsQueued(job)) {
    add('top', 'Bovenaan', 'move', { to: 'top' }, index <= 0 ? { disabled: true, why: 'Staat al bovenaan.' } : {});
    add('up', 'Omhoog', 'move', { to: 'up' }, index <= 0 ? { disabled: true, why: 'Staat al bovenaan.' } : {});
    add('down', 'Omlaag', 'move', { to: 'down' }, index >= count - 1 ? { disabled: true, why: 'Staat al onderaan.' } : {});
    add(job.held ? 'release' : 'hold', job.held ? 'Vrijgeven' : 'Vasthouden', job.held ? 'release' : 'hold');
  }
  if (jobIsQueued(job) && approval) {
    const heavy = approval.kind === 'integratie' || approval.kind === 'publicatie';
    add('approve', 'Goedkeuren', 'approve', {}, {
      confirm: heavy ? 'Goedkeuring voor ' + approval.kind + ' geven? Daarmee mag deze opdracht uitgevoerd worden.' : null });
    add('reject', 'Weigeren', 'reject', {}, { danger: true,
      confirm: heavy ? 'Goedkeuring voor ' + approval.kind + ' weigeren? De opdracht wordt dan geannuleerd.' : null });
  }
  if (job.state === 'claude_klaar') {
    add('check_ok', 'Controle OK', 'check', { verdict: 'ok', source: 'mens' });
    add('check_no', 'Afkeuren', 'check', { verdict: 'afgekeurd', source: 'mens' }, { danger: true });
  }
  // Hervatten = de bestaande eenmalige taak verder laten gaan (werk blijft); een keten of supervisorrun begint opnieuw.
  if (job.state === 'mislukt' && job.started_at && !job.supervisor && !job.continuation) add('resume', 'Hervatten', 'resume');
  if (['mislukt', 'limiet', 'gestopt', 'geannuleerd', 'afgekeurd'].includes(job.state)) add('retry', 'Opnieuw', 'retry');
  if ((jobIsQueued(job) || jobIsActive(job)) && !job.cancel_requested) {
    add('cancel', 'Annuleren', 'cancel', {}, { danger: true,
      confirm: jobIsActive(job) ? 'Deze opdracht loopt al. Annuleren stopt de taak op de Runner. Doorgaan?' : null });
  }
  if (JOB_END_STATUSES.includes(job.status) && !job.archived) add('archive', 'Archiveren', 'archive');
  return out;
}

/** De opdrachten waar een nieuwe opdracht op kan wachten: alles wat nog niet is afgerond. */
function pickableDeps(data) {
  const out = [];
  const nodes = (data && data.nodes) || {};
  // De taken in de wachtrijkaders (MAC, GERARD, NUC/LINUX, WINDOWS) zijn kiesbaar; Runner 2 staat voorgangers op een andere node toe.
  JOB_QUEUE_NODES.forEach((id) => {
    const node = nodes[id];
    ['active', 'queue'].forEach((part) => ((node && node[part]) || []).forEach((job) => {
      if (jobIsOpen(job)) out.push({ id: job.id, title: job.title || job.id, node: job.node });
    }));
  });
  return out;
}

/** Een kenmerk van 8-64 tekens [A-Za-z0-9]; één per verzending, zodat dubbel klikken één opdracht geeft. */
function newRequestId() {
  try {
    if (window.crypto && typeof window.crypto.randomUUID === 'function') {
      return window.crypto.randomUUID().replace(/-/g, '');
    }
  } catch (err) { /* val terug */ }
  return ('r' + Date.now().toString(36) + Math.random().toString(36).slice(2) + '0000000000').slice(0, 32);
}

/**
 * De POST-body van het formulier, of { error }. ``form`` = { prompt, title, repository, node, model, supervisor,
 * depends_on, require_check, approval, held, by }; ``nonce`` en ``requestId`` komen van de verzending zelf.
 */
function jobFormBody(form, nonce, requestId, choices, extra) {
  const prompt = String(form.prompt || '').trim();
  const repository = String(form.repository || '').trim();
  if (!form.by) return { error: 'Kies eerst wie je bent.' };
  if (!prompt) return { error: 'Schrijf eerst de opdracht.' };
  if (!JOB_NODES.includes(form.node)) return { error: 'Kies een Runner.' };
  if (!repository) return { error: 'Kies een repository voor ' + jobNodeLabel(form.node) + '.' };
  // ``choices``: de gecontroleerde repositories van deze Runner; de server controleert het nog eens. "auto" laat
  // Runner 2 de repository uit de tekst bepalen (alleen aangeboden als de server dat kan, zie ``info.auto``).
  if (repository !== REPO_AUTO && Array.isArray(choices) && !choices.includes(repository)) {
    return { error: 'Repository ' + repository + ' staat niet in de lijst van ' + jobNodeLabel(form.node) + '; kies er een uit de lijst.' };
  }
  const body = { action: 'add', by: form.by, prompt, repository, node: form.node, model: form.model || 'standaard',
    supervisor: Boolean(form.supervisor), require_check: Boolean(form.require_check), held: Boolean(form.held),
    request_id: requestId, nonce };
  const title = String(form.title || '').trim();
  if (title) body.title = title;
  if (repository === REPO_AUTO && extra) {
    // Het antwoord op een eerdere vraag of de bevestiging van een splitsing; hoort bij precies deze tekst.
    if (extra.split) body.split = true;
    if (extra.project_repos && Object.keys(extra.project_repos).length) body.project_repos = Object.assign({}, extra.project_repos);
  }
  if (form.approval && form.approval !== 'geen') body.approval = form.approval;
  if (Array.isArray(form.depends_on) && form.depends_on.length) body.depends_on = form.depends_on.slice();
  return { body };
}

/** "wacht op J-…" enzovoort: de regel onder de titel. */
/** Titel bij een opdracht-id uit het laatste overzicht (voor "wacht op …"); onbekend = het id zelf. */
function jobTitleOf(id) {
  const nodes = (jobsPage.data && jobsPage.data.nodes) || {};
  for (const key of Object.keys(nodes)) {
    const n = nodes[key] || {};
    for (const j of [].concat(n.active || [], n.queue || [], n.finished || [])) {
      if (j && j.id === id && j.title) return j.title + ' (' + id + ')';
    }
  }
  return id;
}

function jobMetaText(job) {
  const parts = [];
  if (job.model) parts.push(modelLabel(job.model));
  else if (job.model_choice && job.model_choice !== 'standaard') parts.push(job.model_choice);
  if (job.supervisor) parts.push('supervisor');
  if (job.require_check) parts.push('controle vereist');
  if (job.approval && job.approval.required) {
    parts.push('goedkeuring ' + job.approval.kind + (job.approval.state === 'approved' ? ' gegeven' : ''));
  }
  if (job.depends_on && job.depends_on.length) parts.push('wacht op ' + job.depends_on.map(jobTitleOf).join(', '));
  if (job.created_by) parts.push('door ' + job.created_by);
  return parts.join(' · ');
}

function modelLabel(model) {
  const match = /^claude-([a-z]+)-(\d+)-(\d+)/.exec(String(model));
  return match ? match[1].charAt(0).toUpperCase() + match[1].slice(1) + ' ' + match[2] + '.' + match[3] : String(model);
}

/** De voortgangsregel: tekst van Runner 2, en `stale` als waarschuwing (`warn: true`). */
function jobProgress(job) {
  const progress = job.progress || null;
  const lines = [];
  // Bij een lopende opdracht is `why` al de voortgangstekst; die niet nog eens tonen.
  if (progress && progress.text && jobIsActive(job) && progress.text !== job.why) lines.push({ text: progress.text, warn: false });
  if (progress && progress.stale) lines.push({ text: 'Let op: ' + progress.stale, warn: true });
  const err = job.last_error;
  if (err && (err.message || err.code) && (job.status === 'starting' || job.state === 'wacht_op_runner')) {
    lines.push({ text: 'Laatste fout: ' + (err.message || err.code), warn: true });
  }
  return lines;
}

function jobsHeadline(data) {
  if (!data || !data.nodes) return '';
  const part = (id) => {
    const c = queueDisplayNode(id, data.nodes[id]).counts || {};
    return (JOB_NODE_NAME[id] || id) + ': ' + (c.active || 0) + ' bezig \u00b7 ' + (c.queued || 0) + ' in de wachtrij';
  };
  // Mac, STUDIO (de NUC), Gerard en Windows altijd, elk apart.
  const moved = data.handed_over ? 'OVERGEDRAGEN aan ' + (data.handed_over.to || '?') + ' (bedienen kan daar) \u00b7 ' : '';
  return moved + JOB_QUEUE_NODES.map(part).join('  |  ') + (data.paused_all ? ' \u00b7 ALLES GEPAUZEERD' : '');
}

/** Waarom een wachtende opdracht niet kan starten, per ``state``-sleutel van Runner 2 (de rest staat in ``job.why``). */
const JOB_WAIT_KIND = {
  wacht_op_goedkeuring: 'goedkeuring', vastgehouden: 'vastgehouden', wacht_op_voorganger: 'voorganger',
  geblokkeerd: 'geblokkeerd', wachtrij_gepauzeerd: 'gepauzeerd',
};

/**
 * Het overzicht van één machine, alleen afgeleid uit het antwoord van Runner 2 (de volgorde van ``node.queue`` is de
 * uitvoervolgorde; de dispatcher start de eerste opdracht met state ``in_wachtrij``). ``pausedAll`` = alles gepauzeerd.
 * Geeft { counts, next, nextId, nextRank, head: { tone, text }, waits: [{ id, position, title, kind, text }] }.
 */
function jobQueueSummary(node, pausedAll) {
  const active = (node && node.active) || [];
  const queue = (node && node.queue) || [];
  const paused = (node && node.paused) || pausedAll || null;
  const counts = { active: active.length, queued: queue.length, goedkeuring: 0, vastgehouden: 0, voorganger: 0, geblokkeerd: 0 };
  const waits = [];
  queue.forEach((job, index) => {
    const kind = JOB_WAIT_KIND[job.state];
    if (!kind) return;
    if (counts[kind] !== undefined) counts[kind] += 1;
    if (kind !== 'gepauzeerd') waits.push({ id: job.id, position: index + 1, title: job.title || job.id, kind, text: job.why || '' });
  });
  const next = paused ? null : queue.find((job) => job.state === 'in_wachtrij') || null;
  const first = active[0] || null;
  let head;
  if (paused) {
    head = { tone: 'warn', text: 'Wachtrij gepauzeerd' + (node && node.paused ? '' : ' (alles gepauzeerd)')
      + (paused.by ? ' door ' + paused.by : '') + (paused.reason ? ': ' + paused.reason : '') };
  } else if (first && first.state === 'wacht_op_runner') {
    head = { tone: 'wacht', text: 'Wacht op de machine: ' + (first.title || first.id) + ' \u2014 ' + (first.why || 'machine bezet') };
  } else if (first && first.state === 'wacht_op_actie') {
    head = { tone: 'goedkeuring', text: 'Wacht op een antwoord van de beheerder: ' + (first.title || first.id) };
  } else if (first) {
    head = { tone: 'actief', text: 'Nu bezig: ' + (first.title || first.id) };
  } else if (next) {
    head = { tone: 'wacht', text: 'Niets actief \u2014 start zodra de machine vrij is: ' + (next.title || next.id) };
  } else if (queue.length) {
    head = { tone: 'geblokkeerd', text: 'Niets actief en geen opdracht kan starten: ' + queue.length + ' wachtend op goedkeuring, voorganger of vrijgave' };
  } else {
    head = { tone: 'idle', text: 'Niets actief en niets in de wachtrij' };
  }
  return { counts, next, nextId: next ? next.id : null, nextRank: first ? 'daarna' : 'volgende', head, waits, paused: Boolean(paused) };
}

function pauseText(paused) {
  if (!paused) return null;
  return 'Gepauzeerd door ' + (paused.by || '?') + (paused.reason ? ': ' + paused.reason : '');
}

/* ---------------------------------------------------------------- DOM */

function jobsNote(text, tone) {
  jobsPage.note = text ? { text, tone: tone || null } : null;
  const box = el('jobs-note');
  box.textContent = text || '';
  box.hidden = !text;
  if (tone) box.dataset.tone = tone; else delete box.dataset.tone;
}

function jobsReadUser() {
  const stored = readStored(JOBS_USER_KEY, null);
  return typeof stored === 'string' ? stored : null;
}

function jobsCurrentUser() {
  return el('jobs-by').value || '';
}

/** De repositorykeuze van één Runner uit ``meta.repo_choices``: alleen paden; ``null`` = (nog) geen lijst van de server. */
function repoPaths(meta, node) {
  const info = meta && meta.repo_choices && meta.repo_choices[node];
  if (!info || !Array.isArray(info.repos)) return null;
  return info.repos.map((item) => item && item.path).filter((path) => typeof path === 'string' && path);
}

function readRepoMemory() {
  const stored = readStored(JOBS_REPO_KEY, null);
  return stored && typeof stored === 'object' && !Array.isArray(stored) ? stored : {};
}

/** Onthoud de gekozen repository van een Runner (een lege keuze wist niets). */
function rememberRepo(node, repo) {
  if (!JOB_NODES.includes(node) || typeof repo !== 'string' || !repo) return;
  const memory = readRepoMemory();
  if (memory[node] === repo) return;
  memory[node] = repo;
  writeStored(JOBS_REPO_KEY, memory);
}

/**
 * Welke repository voorgeselecteerd wordt: de huidige keuze (zelfde Runner), dan het concept van dit tabblad, dan de
 * onthouden keuze van deze Runner, dan de laatst verzonden (server), dan de enige. Nooit iets wat niet in de lijst staat.
 */
function pickRepo(paths, wish) {
  const want = wish || {};
  for (const candidate of [want.current, want.draft, want.remembered, want.last]) {
    if (typeof candidate === 'string' && candidate && (paths.includes(candidate) || (want.auto && candidate === REPO_AUTO))) {
      return candidate;
    }
  }
  if (paths.length === 1) return paths[0];
  return want.auto ? REPO_AUTO : '';
}

/** De regel onder het menu: waarom de lijst leeg is, of welke ingestelde repository niet getoond wordt en waarom. */
function repoNote(info, node) {
  const parts = [];
  const repos = (info && info.repos) || [];
  if (!info) parts.push('De lijst met repositories wordt geladen.');
  else if (!repos.length) {
    parts.push(info.checked === 'none'
      ? 'Nog geen gecontroleerde repository voor ' + jobNodeLabel(node) + ': Runner 2 daar is niet bereikbaar of heeft nog geen taak gedraaid.'
      : 'Geen bruikbare repository voor ' + jobNodeLabel(node) + '.');
  }
  ((info && info.hidden) || []).forEach((item) => {
    if (item && item.path) parts.push('Niet getoond: ' + item.path + ' (' + (item.why || 'niet gecontroleerd') + ').');
  });
  return parts.join(' ');
}

/** De tekst van één keuze: herkenbare projectnamen eerst (zonder die het pad zelf). */
function repoOptionText(item) {
  const names = Array.isArray(item.projects) ? item.projects.map((p) => p && p.name).filter(Boolean) : [];
  if (!names.length) return item.path;
  const base = String(item.path).split(/[\\/]/).filter(Boolean).pop() || item.path;
  return names.join(' + ') + ' \u2014 ' + base;
}

/** Tekent de repositorylijst voor de gekozen Runner; alleen opnieuw als Runner of lijst veranderde. */
function renderRepoChoices() {
  const select = el('jobs-repository');
  const node = el('jobs-node').value;
  const meta = jobsPage.meta;
  const info = (meta && meta.repo_choices && meta.repo_choices[node]) || null;
  const paths = repoPaths(meta, node) || [];
  const signature = JSON.stringify([node, info]);
  if (signature === jobsPage.repoSignature) return;
  jobsPage.repoSignature = signature;
  const sameNode = jobsPage.repoNode === node;
  const draft = jobsPage.draftRepo && jobsPage.draftRepo.node === node ? jobsPage.draftRepo.repo : null;
  const auto = Boolean(info && info.auto);
  const value = pickRepo(paths, { current: sameNode ? select.value : null, draft,
    remembered: readRepoMemory()[node], last: info && info.last, auto });
  if (info) jobsPage.draftRepo = null;
  jobsPage.repoNode = node;
  clear(select);
  if (!paths.length && !auto) {
    const none = make('option', null, info ? 'Geen repository beschikbaar' : 'Lijst wordt geladen…');
    none.value = '';
    select.appendChild(none);
  } else if (!value) {
    const none = make('option', null, 'Kies een repository…');
    none.value = '';
    select.appendChild(none);
  }
  if (auto) {
    const automatic = make('option', null, 'Automatisch bepalen (uit je tekst)');
    automatic.value = REPO_AUTO;
    automatic.title = 'Runner 2 kiest de repository uit de projecten die in je tekst staan en vraagt het als het niet duidelijk is.';
    select.appendChild(automatic);
  }
  ((info && info.repos) || []).forEach((item) => {
    if (!item || !paths.includes(item.path)) return;
    const option = make('option', null, repoOptionText(item));
    option.value = item.path;
    const why = item.source === 'instelling' ? 'Ingesteld voor ' + jobNodeLabel(node)
      : item.source === 'gevonden' ? 'Gevonden door Runner 2 op ' + jobNodeLabel(node) : 'Eerder gebruikt op ' + jobNodeLabel(node);
    option.title = item.path + ' — ' + why;
    select.appendChild(option);
  });
  select.value = value;
  select.disabled = !paths.length && !auto;
  const note = el('jobs-repo-note');
  const text = repoNote(info, node);
  note.textContent = text;
  note.hidden = !text;
  if (value) rememberRepo(node, value);
  syncRepoPlanButton();
}

function renderJobsMeta(meta) {
  if (!meta) return;
  renderRepoChoices();
  const key = JSON.stringify([meta.users, meta.control_node]);
  if (jobsPage.metaKey === key) return;
  jobsPage.metaKey = key;
  jobsPage.meta = meta;
  const select = el('jobs-by');
  const keep = select.value || jobsPage.user || jobsReadUser() || '';
  clear(select);
  const none = make('option', null, 'Kies…');
  none.value = '';
  select.appendChild(none);
  (meta.users || []).forEach((name) => {
    const option = make('option', null, name);
    option.value = name;
    select.appendChild(option);
  });
  select.value = (meta.users || []).includes(keep) ? keep : '';
  el('jobs-control').textContent = meta.control_node ? 'Wachtrij op: ' + meta.control_node : '';
}

/** Vinkjes van opdrachten die niet meer open zijn (klaar, geannuleerd) horen niet meer bij de keuze. */
function pruneDeps(data) {
  const open = new Set(pickableDeps(data).map((pick) => pick.id));
  let changed = false;
  Array.from(jobsPage.deps).forEach((id) => { if (!open.has(id)) { jobsPage.deps.delete(id); changed = true; } });
  if (changed) saveJobsDraft();
}

/** Het vinkje "nieuwe opdracht laten wachten op deze", in de kaart van de opdracht zelf. */
function jobPick(job) {
  const label = make('label', 'job-pick');
  const input = make('input');
  input.type = 'checkbox';
  input.value = job.id;
  input.checked = jobsPage.deps.has(job.id);
  input.dataset.depId = job.id;
  input.dataset.jobKey = job.id + ':dep';
  input.setAttribute('aria-label', 'Nieuwe opdracht laten wachten op ' + (job.title || job.id));
  input.addEventListener('change', () => {
    if (input.checked) jobsPage.deps.add(job.id); else jobsPage.deps.delete(job.id);
    jobsFormChanged();
    saveJobsDraft();
  });
  label.appendChild(input);
  label.appendChild(make('span', null, 'Nieuwe opdracht laten wachten op deze'));
  return label;
}

function jobCard(job, ctx) {
  const item = make('li', 'job');
  item.dataset.state = job.state || '';
  item.dataset.jobKey = job.id;
  const head = make('div', 'job-head');
  if (ctx && Number.isInteger(ctx.position)) head.appendChild(make('span', 'job-pos', String(ctx.position)));
  const badge = make('span', 'job-badge job-badge--' + jobBadgeClass(job.state), job.label || job.state || '');
  head.appendChild(badge);
  if (ctx && ctx.nextRank) {
    const mark = make('span', 'job-next', ctx.nextRank === 'daarna' ? 'EERSTVOLGENDE' : 'VOLGENDE');
    mark.title = 'De dispatcher van Runner 2 start deze opdracht als eerste zodra de machine vrij is.';
    head.appendChild(mark);
    item.dataset.next = 'true';
  }
  head.appendChild(make('strong', 'job-title', job.title || job.id));
  head.appendChild(make('span', 'job-id mono', job.id));
  item.appendChild(head);
  const meta = jobMetaText(job);
  if (meta) item.appendChild(make('div', 'job-meta', meta));
  if (job.why) item.appendChild(make('div', 'job-why', job.why));
  jobProgress(job).forEach((line) => {
    const row = make('div', 'job-progress', line.text);
    if (line.warn) row.dataset.tone = 'warn';
    item.appendChild(row);
  });
  if (ctx && ctx.pick && jobIsOpen(job)) item.appendChild(jobPick(job));
  const row = make('div', 'job-actions');
  jobButtons(job, ctx).forEach((spec) => {
    const button = make('button', 'btn btn--small ' + (spec.danger ? 'btn--danger btn--ghost' : 'btn--ghost'), spec.label);
    button.type = 'button';
    button.disabled = Boolean(spec.disabled) || jobsPage.busy;
    button.title = spec.why || '';
    button.dataset.jobKey = job.id + ':' + spec.key;
    button.setAttribute('aria-label', spec.label + ' — ' + (job.title || job.id));
    button.addEventListener('click', () => sendJob(spec.action, { job_id: job.id, ...spec.fields }, spec));
    row.appendChild(button);
  });
  const details = make('button', 'btn btn--small btn--ghost', 'Details');
  details.type = 'button';
  details.dataset.jobKey = job.id + ':details';
  details.setAttribute('aria-label', 'Details — ' + (job.title || job.id));
  details.addEventListener('click', () => openJobDialog(job.id));
  row.appendChild(details);
  if (jobCanReport(job)) {
    const report = make('button', 'btn btn--small btn--report', 'Lees rapport');
    report.type = 'button';
    report.dataset.jobKey = job.id + ':report';
    report.setAttribute('aria-label', 'Lees rapport — ' + (job.title || job.id));
    report.addEventListener('click', () => openJobReport(job));
    row.appendChild(report);
  }
  item.appendChild(row);
  return item;
}

/** Combineer wachtrijwerk met de actuele losse Runner-taak, zonder de wachtrij te wijzigen. */
function queueDisplayNode(id, node) {
  const base = node || {};
  const runner = ((page.last && page.last.nodes) || []).find(n => n.id === id.toLowerCase());
  const fresh = runner && runner.available && Number.isFinite(runner.observation_at)
    && Math.abs(Date.now() / 1000 - runner.observation_at) <= 30 && !page.failures;
  const active = base.active || [];
  const taskId = fresh && runner.active_task;
  const known = [].concat(active, base.queue || [], base.finished || []);
  if (!taskId || known.some(j => j.task_id === taskId || (j.progress && j.progress.current_task === taskId))) return base;
  const task = (runner.task && runner.task.task_id === taskId ? runner.task : null)
    || (runner.tasks || []).find(t => t.task_id === taskId);
  if (!task || !['running', 'starting', 'recovering', 'waiting_for_human'].includes(task.status)) return base;
  return { ...base, active: active.concat([{ task_id: taskId, title: task.title || taskId,
    external: true, label: task.status === 'waiting_for_human' ? 'WACHT OP ANTWOORD' : 'BEZIG',
    state: task.status === 'waiting_for_human' ? 'wacht_op_actie' : 'bezig' }]),
    counts: { ...base.counts, active: active.length + 1, queued: (base.queue || []).length } };
}

function externalTaskCard(task) {
  const item = make('li', 'job');
  item.dataset.state = task.state;
  item.appendChild(make('strong', 'job-title', task.title));
  item.appendChild(make('p', null, task.label));
  item.appendChild(make('span', 'job-id mono', task.task_id));
  return item;
}

function jobColumn(id, node, data) {
  node = queueDisplayNode(id, node);
  const queueFrame = JOB_QUEUE_NODES.includes(id);
  const name = JOB_NODE_TITLE[id] || id;
  const section = make('section', 'panel job-col' + (queueFrame ? ' job-col--queue' : ''));
  section.dataset.node = id;
  section.setAttribute('aria-label', queueFrame ? name : 'Opdrachten voor ' + id);
  const head = make('div', 'job-col-head');
  const title = make('h3', 'job-col-title', name);
  head.appendChild(title);
  const counts = (node && node.counts) || {};
  head.appendChild(make('span', 'job-col-count', (counts.active || 0) + ' bezig \u00b7 ' + (counts.queued || 0) + ' wachtrij'));
  const paused = node && node.paused;
  const pause = make('button', 'btn btn--small btn--ghost', paused ? 'Hervat wachtrij' : 'Pauzeer wachtrij');
  pause.type = 'button';
  pause.dataset.jobKey = 'queue:' + id;
  pause.disabled = !jobsEnabled() || jobsPage.busy;
  pause.setAttribute('aria-label', (paused ? 'Hervat wachtrij ' : 'Pauzeer wachtrij ') + jobNodeLabel(id));
  pause.addEventListener('click', () => toggleQueue(id, Boolean(paused)));
  head.appendChild(pause);
  section.appendChild(head);
  const active = (node && node.active) || [];
  const queue = (node && node.queue) || [];
  const enabled = jobsEnabled();
  const pausedAll = (data && data.paused_all) || null;
  const summary = jobQueueSummary(node, pausedAll);
  if (paused) {
    const banner = make('p', 'job-paused', pauseText(paused));
    banner.dataset.tone = 'warn';
    section.appendChild(banner);
  }
  if (queueFrame) {
    // Status in één oogopslag: wat draait er, en waarop wacht deze machine.
    const status = make('div', 'job-qstatus');
    status.dataset.tone = summary.head.tone;
    status.setAttribute('role', 'status');
    status.appendChild(make('p', 'job-qhead', summary.head.text));
    const chips = make('ul', 'job-chips');
    [['active', 'bezig'], ['queued', 'in wachtrij'], ['goedkeuring', 'wacht op goedkeuring'], ['voorganger', 'wacht op voorganger'],
      ['vastgehouden', 'vastgehouden'], ['geblokkeerd', 'geblokkeerd']].forEach(([key, text]) => {
      const n = summary.counts[key];
      if (key !== 'active' && key !== 'queued' && !n) return;     // alleen de redenen die er nu echt zijn
      const chip = make('li', 'job-chip', n + ' ' + text);
      chip.dataset.kind = key;
      chips.appendChild(chip);
    });
    status.appendChild(chips);
    if (summary.waits.length) {
      const list = make('ul', 'job-waits');
      summary.waits.forEach((w) => {
        const row = make('li', 'job-wait');
        row.dataset.kind = w.kind;
        row.appendChild(make('strong', null, '#' + w.position + ' ' + w.title));
        row.appendChild(make('span', null, ' \u2014 ' + (w.text || w.kind)));
        list.appendChild(row);
      });
      status.appendChild(list);
    }
    section.appendChild(status);
  }
  section.appendChild(make('h4', 'job-sub', queueFrame ? 'Nu bezig' : 'Actief'));
  const activeList = make('ul', 'job-list');
  active.forEach((job) => activeList.appendChild(job.external ? externalTaskCard(job) : jobCard(job, { enabled, index: 0, count: 1, pick: queueFrame })));
  if (!active.length) activeList.appendChild(make('li', 'jobs-empty', 'Niets actief.'));
  section.appendChild(activeList);
  section.appendChild(make('h4', 'job-sub', queueFrame ? 'Wachtrij \u2014 in uitvoervolgorde' : 'Wachtrij'));
  const queueList = make('ol', 'job-list job-list--queue');
  queue.forEach((job, index) => queueList.appendChild(jobCard(job, { enabled, index, count: queue.length, pick: queueFrame,
    position: queueFrame ? index + 1 : undefined, nextRank: queueFrame && job.id === summary.nextId ? summary.nextRank : null })));
  if (!queue.length) queueList.appendChild(make('li', 'jobs-empty', 'De wachtrij is leeg.'));
  section.appendChild(queueList);
  const finished = (node && node.finished) || [];
  const fold = make('details', 'job-fold');
  fold.open = Boolean(jobsPage.open[id]);
  fold.addEventListener('toggle', () => { jobsPage.open[id] = Boolean(fold.open); });
  fold.appendChild(make('summary', null, 'Recent afgerond (' + finished.length + ')'));
  const doneList = make('ul', 'job-list');
  finished.forEach((job) => doneList.appendChild(jobCard(job, { enabled, index: 0, count: 1 })));
  if (!finished.length) doneList.appendChild(make('li', 'jobs-empty', 'Nog niets afgerond.'));
  fold.appendChild(doneList);
  section.appendChild(fold);
  return section;
}

function renderJobs() {
  const data = jobsPage.data;
  const columns = el('jobs-columns');
  const error = el('jobs-error');
  error.hidden = !jobsPage.error;
  error.textContent = jobsPage.error || '';
  el('jobs-summary').textContent = data ? jobsHeadline(data) : '';
  const all = el('jobs-pause-all');
  all.textContent = data && data.paused_all ? 'Alles hervatten' : 'Alles pauzeren';
  all.disabled = !data || !jobsEnabled() || jobsPage.busy;
  renderNavCount(data);
  const displayNodes = Object.fromEntries(JOB_QUEUE_NODES.map(id => [id, queueDisplayNode(id, (data && data.nodes || {})[id])]));
  const signature = JSON.stringify([displayNodes, data && data.paused_all, jobsEnabled(), jobsPage.busy]);
  renderJobsMeta(jobsPage.meta);
  if (!data) return;
  pruneDeps(data);
  if (signature === jobsPage.signature) return;
  jobsPage.signature = signature;
  const active = typeof document !== 'undefined' && document.activeElement && document.activeElement.dataset
    ? document.activeElement.dataset.jobKey : null;
  clear(columns);
  JOB_QUEUE_NODES.forEach((id) => columns.appendChild(jobColumn(id, (data.nodes || {})[id], data)));
  if (active && typeof document.querySelectorAll === 'function') {
    // Een nieuwe tekening mag het toetsenbordfocus niet kwijtraken.
    const found = Array.from(document.querySelectorAll('[data-job-key]')).find((node) => node.dataset.jobKey === active);
    if (found && typeof found.focus === 'function') found.focus();
  }
}

async function pollJobs() {
  if (jobsPage.timer) { clearTimeout(jobsPage.timer); jobsPage.timer = null; }
  let ok = false;
  try {
    const response = await fetch('/api/jobs', { cache: 'no-store' });
    let payload = {};
    try { payload = await response.json(); } catch (err) { payload = {}; }
    if (payload.meta) jobsPage.meta = payload.meta;
    if (response.ok && payload.nodes) {
      jobsPage.data = payload;
      jobsPage.receivedAt = Date.now();
      if (jobsPage.expiryTimer) clearTimeout(jobsPage.expiryTimer);
      jobsPage.expiryTimer = setTimeout(refreshQueueCards, 15001);
      jobsPage.error = null;
      jobsPage.failures = 0;
      ok = true;
    } else {
      jobsPage.failures += 1;
      jobsPage.error = payload.message || ('De opdrachtwachtrij kon niet worden gelezen (HTTP ' + response.status + ').');
    }
  } catch (err) {
    jobsPage.failures += 1;
    jobsPage.error = 'Geen antwoord van Monitor 2 voor de opdrachten: ' + err.message;
  }
  try { renderJobs(); } catch (err) { jobsPage.error = 'Weergavefout: ' + err.message; }
  refreshQueueCards();
  jobsPage.timer = setTimeout(pollJobs, ok ? JOBS_POLL_OK_MS : JOBS_POLL_FAIL_MS);
}

/** Eén handeling doorgeven. ``spec`` bevat optioneel een bevestigingsvraag. */
async function sendJob(action, fields, spec) {
  if (jobsPage.busy) return null;
  const by = jobsCurrentUser();
  if (!by) { jobsNote('Kies eerst wie je bent.', 'bad'); return null; }
  if (spec && spec.confirm && !window.confirm(spec.confirm)) return null;
  jobsPage.busy = true;
  jobsNote('Bezig…', null);
  renderJobs();
  let result;
  try {
    result = await post('/api/jobs', { ...fields, action, by, nonce: newNonce() });
  } catch (err) {
    result = { ok: false, payload: { message: 'Geen antwoord: ' + err.message + ' — er is niets uitgevoerd.' } };
  }
  jobsPage.busy = false;
  jobsNote(result.payload.message || (result.ok ? 'Doorgegeven.' : 'Er is niets uitgevoerd.'), result.ok ? 'ok' : 'bad');
  jobsPage.signature = null;
  renderJobs();
  pollJobs();
  return result;
}

async function toggleQueue(node, paused) {
  let reason = '';
  if (!paused && typeof window.prompt === 'function') {
    reason = window.prompt('Reden voor het pauzeren van de wachtrij van ' + jobNodeLabel(node) + ' (mag leeg blijven):', '');
    if (reason === null) return;
  }
  const fields = { node };
  if (reason && reason.trim()) fields.reason = reason.trim().slice(0, 300);
  await sendJob(paused ? 'resume_queue' : 'pause_queue', fields, null);
}

async function toggleAll() {
  const paused = Boolean(jobsPage.data && jobsPage.data.paused_all);
  let reason = '';
  if (!paused) {
    if (typeof window.prompt === 'function') {
      reason = window.prompt('Reden voor het pauzeren van alle wachtrijen (mag leeg blijven):', '');
      if (reason === null) return;
    }
  }
  const fields = { node: 'ALL' };
  if (reason && reason.trim()) fields.reason = reason.trim().slice(0, 300);
  await sendJob(paused ? 'resume_queue' : 'pause_queue', fields, null);
}

/* ---- Automatisch bepalen: de vraag of het voorstel van Runner 2 ---- */

const REPO_QUESTIONS = ['repo_choice_needed', 'repo_unknown', 'repo_split_confirm'];

function clearRepoDecision() {
  const box = document.getElementById('jobs-repo-decision');
  if (!box) return;
  clear(box);
  box.hidden = true;
}

/** Eén knop per optie: label (projecten + map) en het volledige pad als tooltip. */
function repoOptionButton(option, onPick) {
  const button = make('button', 'btn btn--ghost btn--small jobs-repo-option', option.label || option.repository);
  button.type = 'button';
  button.title = option.repository + (option.verified === false ? ' (niet live gecontroleerd)' : '');
  button.addEventListener('click', onPick);
  return button;
}

/**
 * Toont wat Runner 2 bij Automatisch bepalen terugmeldt, en nooit meer dan één vraag:
 * - ``repo_split_confirm``: de opdracht hoort bij meerdere repositories; per repository komt een aparte opdracht
 *   (alleen na bevestiging);
 * - ``repo_choice_needed``: meerdere kandidaten voor één project, kies er één;
 * - ``repo_unknown``: niets herkend (of niet op deze Runner); kies zelf een repository uit de lijst van deze Runner.
 * ``plan`` (de uitkomst van "Toon wat Runner 2 kiest") gebruikt dezelfde tekening met ``reason`` = null.
 */
function showRepoDecision(reason, resolution, body) {
  const box = document.getElementById('jobs-repo-decision');
  if (!box || !resolution) return;
  clear(box);
  box.hidden = false;
  const groups = Array.isArray(resolution.groups) ? resolution.groups : [];
  const question = resolution.question || null;
  if (resolution.decision === 'resolved' || reason === 'repo_split_confirm') {
    const split = groups.length > 1;
    box.appendChild(make('p', 'jobs-repo-title', split
      ? 'Dit gaat over ' + groups.length + ' repositories: er komen ' + groups.length + ' aparte opdrachten.'
      : 'Runner 2 kiest deze repository:'));
    groups.forEach((group) => {
      const line = make('p', 'jobs-repo-group');
      const parts = (group.projects || []).map((p) => p.name + (p.component ? ' (' + p.component + ')' : ''));
      line.appendChild(make('strong', null, (parts.join(' + ') || group.label || group.name) + ' '));
      line.appendChild(document.createTextNode('→ ' + group.repository + ' op ' + group.node
        + (group.verified === false ? ' (niet live gecontroleerd)' : '')));
      box.appendChild(line);
    });
    if (split && reason === 'repo_split_confirm') {
      box.appendChild(make('p', 'jobs-repo-hint', 'Elke opdracht mag alleen aan zijn eigen onderdeel werken en wacht niet op de andere.'));
      const go = make('button', 'btn btn--small', 'Verstuur ' + groups.length + ' opdrachten');
      go.type = 'button';
      go.addEventListener('click', () => {
        jobsPage.repoExtra = Object.assign({}, jobsPage.repoExtra, { split: true });
        clearRepoDecision();
        submitJob();
      });
      box.appendChild(go);
    }
  } else if (question) {
    box.appendChild(make('p', 'jobs-repo-title', question.text || resolution.message || 'Kies een repository.'));
    if (resolution.message && resolution.message !== question.text) box.appendChild(make('p', 'jobs-repo-hint', resolution.message));
    const row = make('div', 'jobs-repo-options');
    (question.options || []).forEach((option) => {
      row.appendChild(repoOptionButton(option, () => {
        if (reason === 'repo_choice_needed' && question.project) {
          const picked = Object.assign({}, jobsPage.repoExtra && jobsPage.repoExtra.project_repos);
          picked[question.project] = option.repository;
          jobsPage.repoExtra = Object.assign({}, jobsPage.repoExtra, { project_repos: picked });
          clearRepoDecision();
          submitJob();
          return;
        }
        // Niets herkend: de gebruiker kiest een expliciete repository uit het menu van deze Runner.
        const select = el('jobs-repository');
        const known = Array.from(select.children || []).some((o) => o.value === option.repository);
        if (!known) { jobsNote('Die repository staat niet in de lijst van deze Runner; kies er een uit het menu.', 'bad'); return; }
        select.value = option.repository;
        rememberRepo(body.node, option.repository);
        jobsFormChanged();
        saveJobsDraft();
        jobsNote('Repository gekozen; druk op Verstuur.', 'ok');
      }));
    });
    box.appendChild(row);
  } else {
    box.appendChild(make('p', 'jobs-repo-title', resolution.message || 'Runner 2 kon de repository niet bepalen; kies er zelf een.'));
  }
}

/** "Toon wat Runner 2 kiest": alleen lezen, er wordt niets aangemaakt. */
async function planRepo() {
  if (jobsPage.busy) return;
  const form = jobsFormValues();
  if (!form.by) { jobsNote('Kies eerst wie je bent.', 'bad'); return; }
  if (!String(form.prompt || '').trim()) { jobsNote('Schrijf eerst de opdracht.', 'bad'); return; }
  const body = { action: 'plan', by: form.by, prompt: form.prompt.trim(), repository: REPO_AUTO, node: form.node, nonce: newNonce() };
  if (jobsPage.repoExtra && jobsPage.repoExtra.project_repos) body.project_repos = jobsPage.repoExtra.project_repos;
  jobsPage.busy = true;
  jobsNote('Runner 2 bekijkt de tekst…', null);
  let result;
  try { result = await post('/api/jobs', body); }
  catch (err) { result = { ok: false, payload: { message: 'Geen antwoord: ' + err.message } }; }
  jobsPage.busy = false;
  const plan = result.ok && result.payload && result.payload.plan;
  if (!plan) { jobsNote(result.payload.message || 'Geen voorstel gekregen.', 'bad'); return; }
  jobsNote(plan.message || 'Voorstel van Runner 2.', plan.decision === 'resolved' ? 'ok' : null);
  const reason = plan.decision === 'choice_needed' ? 'repo_choice_needed' : plan.decision === 'unresolved' ? 'repo_unknown'
    : plan.split ? 'repo_split_confirm' : null;
  showRepoDecision(reason, plan, body);
}

function jobsFormChanged() {
  jobsPage.requestId = null;   // een gewijzigd formulier is een nieuwe verzending
  jobsPage.repoExtra = null;   // een antwoord op een repositoryvraag hoorde bij de vorige tekst
  clearRepoDecision();
}

function jobsFormValues() {
  return {
    prompt: el('jobs-prompt').value, title: el('jobs-title').value, repository: el('jobs-repository').value,
    node: el('jobs-node').value, model: el('jobs-model').value, supervisor: el('jobs-supervisor').checked,
    require_check: el('jobs-require-check').checked, approval: el('jobs-approval').value, held: el('jobs-held').checked,
    depends_on: Array.from(jobsPage.deps), by: jobsCurrentUser(),
  };
}

async function submitJob(event) {
  if (event && typeof event.preventDefault === 'function') event.preventDefault();
  if (jobsPage.busy) return;
  if (!jobsPage.requestId) jobsPage.requestId = newRequestId();
  const form = jobsFormValues();
  const choices = repoPaths(jobsPage.meta, form.node);
  const built = jobFormBody(form, newNonce(), jobsPage.requestId, choices === null ? [] : choices, jobsPage.repoExtra);
  if (built.error) { jobsNote(built.error, 'bad'); return; }
  jobsPage.busy = true;
  el('jobs-submit').disabled = true;
  jobsNote('Bezig…', null);
  let result;
  try {
    result = await post('/api/jobs', built.body);
  } catch (err) {
    result = { ok: false, payload: { message: 'Geen antwoord: ' + err.message + ' — controleer de wachtrij voor je opnieuw verstuurt.' } };
  }
  jobsPage.busy = false;
  el('jobs-submit').disabled = false;
  jobsNote(result.payload.message || (result.ok ? 'Opdracht in de wachtrij gezet.' : 'Er is niets uitgevoerd.'), result.ok ? 'ok' : 'bad');
  if (!result.ok && REPO_QUESTIONS.includes(result.payload.reason)) {
    showRepoDecision(result.payload.reason, (result.payload.data || {}).resolution, built.body);
    return;
  }
  if (result.ok) {
    jobsPage.requestId = null;
    clearRepoDecision();
    jobsPage.repoExtra = null;
    rememberRepo(built.body.node, built.body.repository);
    el('jobs-prompt').value = '';
    el('jobs-title').value = '';
    el('jobs-held').checked = false;
    el('jobs-approval').value = '';
    jobsPage.deps.clear();
    saveJobsDraft();
  }
  jobsPage.signature = null;
  renderJobs();
  pollJobs();
}

function jobsUserChanged() {
  const name = jobsCurrentUser();
  jobsPage.user = name || null;
  if (name) writeStored(JOBS_USER_KEY, name);
}

/* ---- detailvenster en rapport van een opdracht ---- */

async function openJobDialog(jobId) {
  const state = { kind: 'job', node: null, jobId, status: 'loading', job: null, error: null, note: null, tone: null,
    editing: false, busy: false, draft: undefined };
  page.dialog = state;
  const dialog = el('dialog');
  dialog.dataset.kind = 'job';
  refreshDialog(true);
  if (!dialog.open) dialog.showModal();
  try {
    const response = await fetch('/api/jobs?' + new URLSearchParams({ job: jobId }).toString(), { cache: 'no-store' });
    let payload = {};
    try { payload = await response.json(); } catch (err) { payload = {}; }
    if (page.dialog !== state) return;
    if (response.ok && payload.job) { state.status = 'ok'; state.job = payload.job; }
    else { state.status = 'error'; state.error = payload.message || 'De opdracht kon niet worden opgehaald (HTTP ' + response.status + ').'; }
  } catch (err) {
    if (page.dialog !== state) return;
    state.status = 'error';
    state.error = 'Geen antwoord van Monitor 2: ' + err.message;
  }
  refreshDialog(true);
}

/** Hoe de repository van een opdracht gekozen is; leeg bij een expliciet pad (dat is de gewone situatie). */
function repoChoiceText(job) {
  const choice = job && job.repo_choice;
  if (!choice || choice.mode !== 'auto') return null;
  const names = (choice.projects || []).map((p) => p.name + (p.component ? ' (' + p.component + ')' : ''));
  return 'automatisch bepaald' + (names.length ? ': ' + names.join(' + ') : '') + (choice.split ? ' — gesplitst per repository' : '');
}

function jobFactRows(job) {
  const approval = job.approval && job.approval.required
    ? job.approval.kind + ' (' + ({ open: 'open', approved: 'goedgekeurd', rejected: 'geweigerd' }[job.approval.state] || job.approval.state) + ')' : null;
  const check = job.check ? job.check.state + ' door ' + job.check.by + ' (' + job.check.source + ')'
    + (job.check.note ? ': ' + job.check.note : '') : null;
  return [
    { label: 'Status', value: job.label }, { label: 'Toelichting', value: job.why },
    { label: 'Opdracht-id', value: job.id, mono: true }, { label: 'Runner', value: job.node },
    { label: 'Repository', value: job.repo, mono: true },
    { label: 'Repository gekozen', value: repoChoiceText(job) },
    { label: 'Gesplitst met', value: (job.siblings || []).join(', ') },
    { label: 'Model', value: job.model || job.model_choice, mono: true },
    { label: 'Supervisor', value: job.supervisor ? 'ja' : null },
    { label: 'Wacht op', value: (job.depends_on || []).join(', ') },
    { label: 'Controle vereist', value: job.require_check ? 'ja' : null }, { label: 'Goedkeuring', value: approval },
    { label: 'Taak-id', value: job.task_id, mono: true }, { label: 'Run', value: job.run },
    { label: 'Aangemaakt door', value: job.created_by }, { label: 'Aangemaakt', value: job.created_at },
    { label: 'Gestart', value: job.started_at }, { label: 'Afgerond', value: job.ended_at },
    { label: 'Uitkomst', value: job.outcome && (job.outcome.text || job.outcome.kind) }, { label: 'Controle', value: check },
    { label: 'Laatste fout', value: job.last_error && (job.last_error.message || job.last_error.code) },
  ];
}

function dialogJob(body, state) {
  el('dialog-title').textContent = 'Opdracht — ' + ((state.job && state.job.title) || state.jobId);
  if (state.status === 'loading') { body.appendChild(make('p', 'dialog-why', 'Opdracht wordt opgehaald…')); return; }
  if (state.status === 'error') {
    const error = make('p', 'dialog-note', state.error);
    error.dataset.tone = 'bad';
    body.appendChild(error);
    return;
  }
  const job = state.job;
  const bar = make('div', 'dialog-controls');
  if (jobCanReport(job)) {
    const report = make('button', 'btn btn--report btn--small', 'Lees rapport');
    report.type = 'button';
    report.addEventListener('click', () => openJobReport(job));
    bar.appendChild(report);
  }
  const edit = state.edit;
  if (jobTextEditable(job) && !edit) {
    const open = make('button', 'btn btn--ghost btn--small', 'Tekst bewerken');
    open.type = 'button';
    open.id = 'job-edit-open';
    open.disabled = !jobsEnabled();
    if (open.disabled) open.title = 'Monitor 2 draait zonder bediening (alleen lezen).';
    open.addEventListener('click', () => {
      state.edit = { draft: job.prompt || '', busy: false, note: null, tone: null };
      state.editing = true;
      refreshDialog(true);
    });
    bar.appendChild(open);
  }
  const close = make('button', 'btn btn--ghost btn--small', 'Sluiten');
  close.type = 'button';
  close.addEventListener('click', closeDialog);
  bar.appendChild(close);
  body.appendChild(bar);
  body.appendChild(facts(jobFactRows(job)));
  body.appendChild(make('p', 'dialog-section-title', 'Opdracht'));
  if (edit) jobTextEditor(body, state, job, edit);
  else body.appendChild(make('div', 'job-prompt', job.prompt || ''));
  body.appendChild(make('p', 'dialog-section-title', 'Logboek'));
  const log = make('ul', 'job-log');
  (job.log || []).forEach((entry) => {
    log.appendChild(make('li', null, [entry.at, entry.by, entry.text].filter(Boolean).join(' · ')));
  });
  if (!(job.log || []).length) log.appendChild(make('li', null, 'Nog geen regels.'));
  body.appendChild(log);
  dialogNote(body);
}

/** Alleen een wachtende opdracht heeft nog een tekst die kan veranderen; actief of afgerond nooit (de server weigert ook). */
function jobTextEditable(job) { return Boolean(job) && job.status === 'queued' && typeof job.prompt_sha256 === 'string'; }

/** Tekstvak met Opslaan/Annuleren in het detailvenster. Alleen de tekst verandert: volgorde, voorgangers en vasthouden blijven. */
function jobTextEditor(body, state, job, edit) {
  const area = make('textarea', 'job-edit-text');
  area.id = 'job-edit-text';
  area.setAttribute('aria-label', 'Tekst van de opdracht');
  area.value = edit.draft;
  area.disabled = edit.busy;
  area.addEventListener('input', () => { edit.draft = area.value; });
  body.appendChild(area);
  body.appendChild(make('p', 'dialog-why',
    'Alleen de tekst verandert; plek in de wachtrij, voorgangers en vasthouden blijven. '
    + (job.approval && job.approval.required && job.approval.state === 'approved'
      ? 'De goedkeuring geldt voor de oude tekst en moet daarna opnieuw gegeven worden.' : 'Er wordt vooraf een back-up gemaakt.')));
  const row = make('div', 'dialog-controls');
  const save = make('button', 'btn btn--warm btn--small', edit.busy ? 'Bezig…' : 'Opslaan');
  save.type = 'button';
  save.id = 'job-edit-save';
  save.disabled = edit.busy || !jobsEnabled();
  save.addEventListener('click', () => saveJobText(state, job, edit));
  const cancel = make('button', 'btn btn--ghost btn--small', 'Annuleren');
  cancel.type = 'button';
  cancel.id = 'job-edit-cancel';
  cancel.disabled = edit.busy;
  cancel.addEventListener('click', () => { state.edit = null; state.editing = false; refreshDialog(true); });
  row.appendChild(save);
  row.appendChild(cancel);
  body.appendChild(row);
  if (edit.note) {
    const note = make('p', 'dialog-note', edit.note);
    if (edit.tone) note.dataset.tone = edit.tone;
    body.appendChild(note);
  }
}

async function saveJobText(state, job, edit) {
  if (edit.busy) return;
  const by = jobsCurrentUser();
  const text = String(edit.draft || '');
  const fail = (message) => { edit.busy = false; edit.note = message; edit.tone = 'bad'; refreshDialog(true); };
  if (!by) return fail('Kies eerst wie je bent (bovenaan bij Opdrachten).');
  if (!text.trim()) return fail('De tekst mag niet leeg zijn.');
  if (text === job.prompt) return fail('De tekst is niet veranderd.');
  edit.busy = true;
  edit.note = 'Bezig…';
  edit.tone = null;
  refreshDialog(true);
  let result;
  try {
    result = await post('/api/jobs', { action: 'set_prompt', job_id: job.id, prompt: text, old_sha256: job.prompt_sha256, by,
      nonce: newNonce() });
  } catch (err) {
    result = { ok: false, payload: { message: 'Geen antwoord: ' + err.message + ' \u2014 controleer of de tekst is opgeslagen.' } };
  }
  if (page.dialog !== state) return;
  if (!result.ok) return fail(result.payload.message || 'De tekst is niet opgeslagen.');
  jobsNote(result.payload.message || 'Opdrachttekst opgeslagen.', 'ok');
  jobsPage.signature = null;
  pollJobs();
  state.edit = null;
  state.editing = false;
  await openJobDialog(state.jobId);
  if (page.dialog && page.dialog.jobId === state.jobId) {
    page.dialog.note = result.payload.message || null;
    page.dialog.tone = 'ok';
    refreshDialog(true);
  }
}

function openJobReport(job) {
  return openReportTask(String(job.node || '').toLowerCase(), String(job.node || ''), job.task_id, job.title || null,
    job.ended_at ? isoFromRunner2(job.ended_at) : null);
}

/** Runner-2-tijd (``2026-09-30T10:00:00+0200``) naar iets wat elke browser leest; onleesbaar blijft null. */
function isoFromRunner2(text) {
  const match = /^(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)([+-]\d\d)(\d\d)?$/.exec(String(text || ''));
  return match ? match[1] + match[2] + ':' + (match[3] || '00') : null;
}

/** Andere Runner: meteen de lijst van die Runner (met zijn onthouden keuze). */
function jobsNodeChanged() {
  jobsPage.repoSignature = null;
  renderRepoChoices();
}

function jobsRepoChanged() {
  rememberRepo(el('jobs-node').value, el('jobs-repository').value);
  syncRepoPlanButton();
}

/** "Toon wat Runner 2 kiest" bestaat alleen bij Automatisch bepalen. */
function syncRepoPlanButton() {
  const button = document.getElementById('jobs-repo-plan');
  if (button) button.hidden = el('jobs-repository').value !== REPO_AUTO;
}

/** Prepares an editable draft only; the existing submit button remains the sole queue writer. */
function prepareJobDraft(draft) {
  if (jobsPage.busy) { jobsNote('Wacht tot de huidige opdrachtverzending is afgerond en probeer de overdracht daarna opnieuw.', 'wacht');return false; }
  if (!draft || typeof draft.prompt !== 'string' || !['GERARD', 'LINUX'].includes(draft.node) || !draft.repository || !draft.source) return false;
  const current = el('jobs-prompt').value.trim();
  if (current && current !== draft.prompt && !window.confirm('Je hebt al een conceptopdracht. Vervangen door dit besproken ontwerp?')) return false;
  el('jobs-node').value = draft.node; jobsNodeChanged();
  el('jobs-repository').value = draft.repository; jobsPage.draftRepo = {node: draft.node, repo: draft.repository};
  el('jobs-prompt').value = draft.prompt;
  el('jobs-title').value = 'Uit overleg ' + draft.source.conversation_id;
  el('jobs-supervisor').checked = false; el('jobs-require-check').checked = true;
  jobsPage.requestId = null; jobsPage.repoExtra = null; jobsPage.deps.clear();
  jobsFormChanged(); saveJobsDraft();
  jobsNote('Concept uit overleg klaargezet. Lees en bewerk de tekst en zet hem daarna zelf in de wachtrij.' + (draft.prompt.length > 60000 ? ' Kort dit lange concept eerst in tot maximaal 60.000 tekens.' : ''), 'ok');
  return true;
}
if (typeof window !== 'undefined') window.obfJobs = {prepareJobDraft};

function startJobs() {
  jobsPage.user = jobsReadUser();
  restoreJobsDraft();
  el('jobs-by').addEventListener('change', jobsUserChanged);
  el('jobs-form').addEventListener('submit', submitJob);
  // Vóór het opslaan van het concept, zodat het concept de repository van de nieuwe Runner bevat.
  el('jobs-node').addEventListener('change', jobsNodeChanged);
  el('jobs-repository').addEventListener('change', jobsRepoChanged);
  el('jobs-repo-plan').addEventListener('click', planRepo);
  JOBS_DRAFT_FIELDS.concat(JOBS_DRAFT_CHECKS).forEach((id) => {
    el(id).addEventListener('input', () => { jobsFormChanged(); saveJobsDraft(); });
    el(id).addEventListener('change', () => { jobsFormChanged(); saveJobsDraft(); });
  });
  el('jobs-pause-all').addEventListener('click', toggleAll);
  pollJobs();
}

/* ================================================================ Schermen */

/* Twee schermen in één pagina: Monitor (de machines) en Opdrachten (invoer en wachtrijen). Wisselen verbergt het ene
 * en toont het andere; er wordt niets herladen en beide blijven pollen, dus een half ingevulde opdracht blijft staan.
 * Het gekozen scherm staat in het adres (#opdrachten), zodat het een herlaadmoment (nieuwe servergeneratie) en de
 * terugknop van de browser overleeft. #jobs (het oude ankertje) opent ook Opdrachten. */

const VIEWS = { monitor: 'view-monitor', opdrachten: 'view-jobs' };
// Concept van het opdrachtformulier, per tabblad (sessionStorage): overleeft een herlaadmoment, niet het sluiten.
const JOBS_DRAFT_KEY = 'obf-monitor2-jobs-draft';
const JOBS_DRAFT_FIELDS = ['jobs-prompt', 'jobs-title', 'jobs-repository', 'jobs-node', 'jobs-model', 'jobs-approval'];
const JOBS_DRAFT_CHECKS = ['jobs-supervisor', 'jobs-require-check', 'jobs-held'];

/** Welk scherm hoort bij een adres-hash; alles wat geen Opdrachten is, is de monitor. */
function viewFromHash(hash) {
  const key = String(hash || '').replace(/^#/, '').toLowerCase();
  return key === 'opdrachten' || key === 'jobs' ? 'opdrachten' : 'monitor';
}

/** Toon één scherm. Bij een echte wissel begint het nieuwe scherm bovenaan en wordt het meteen ververst. */
function showView(name, opts) {
  const view = VIEWS[name] ? name : 'monitor';
  const options = opts || {};
  const changed = page.view !== view;
  page.view = view;
  Object.keys(VIEWS).forEach((key) => { el(VIEWS[key]).hidden = key !== view; });
  // De paginatitel blijft bewust gelijk (geen titelteller, 2026-10-05); alleen het scherm wisselt.
  if (typeof document !== 'undefined' && document.body && document.body.dataset) document.body.dataset.view = view;
  if (!changed && !options.top) return view;
  if (typeof window !== 'undefined' && typeof window.scrollTo === 'function') window.scrollTo(0, 0);
  if (options.focus) {
    // De ingedrukte knop is nu verborgen; het toetsenbordfocus gaat naar de terugknop van het nieuwe scherm.
    const back = el(view === 'opdrachten' ? 'nav-monitor' : 'nav-jobs');
    if (back && typeof back.focus === 'function') back.focus({ preventScroll: true });
  }
  if (changed && !options.initial) {
    if (view === 'opdrachten') pollJobs(); else poll();
  }
  return view;
}

/** Klik op de knop rechtsboven: wissel van scherm en zet het in het adres (de terugknop werkt dan ook). */
function goView(name, event) {
  if (event && typeof event.preventDefault === 'function') event.preventDefault();
  const view = VIEWS[name] ? name : 'monitor';
  try {
    if (window.history && typeof window.history.pushState === 'function' && window.location.hash !== '#' + view) {
      window.history.pushState(null, '', '#' + view);
    }
  } catch (err) { /* geen geschiedenis (bijv. file://): het scherm wisselt toch */ }
  return showView(view, { top: true, focus: true });
}

/** Het aantal opdrachten dat op een goedkeuring wacht: zichtbaar op de knop Opdrachten →, ook vanaf de monitor. */
function jobsApprovalCount(data) {
  let count = 0;
  Object.values((data && data.nodes) || {}).forEach((node) => {
    ['active', 'queue'].forEach((part) => ((node && node[part]) || []).forEach((job) => {
      const open = job && job.approval && job.approval.required && job.approval.state === 'open';
      if (job && (job.state === 'wacht_op_goedkeuring' || (jobIsQueued(job) && open))) count += 1;
    }));
  });
  return count;
}

function renderNavCount(data) {
  const count = jobsApprovalCount(data);
  const badge = el('nav-jobs-count');
  badge.hidden = !count;
  badge.textContent = count ? String(count) : '';
  const text = count === 1 ? '1 opdracht wacht op goedkeuring' : count + ' opdrachten wachten op goedkeuring';
  el('nav-jobs').title = count ? text : 'Naar de opdrachten';
}

function sessionStore() {
  try { return window.sessionStorage || null; } catch (err) { return null; }
}

/** Het formulier (behalve "wie ben je", dat al apart bewaard wordt) als concept in dit tabblad bewaren. */
function saveJobsDraft() {
  const store = sessionStore();
  if (!store) return;
  const draft = { fields: {}, checks: {}, deps: Array.from(jobsPage.deps) };
  JOBS_DRAFT_FIELDS.forEach((id) => { draft.fields[id] = String(el(id).value || ''); });
  JOBS_DRAFT_CHECKS.forEach((id) => { draft.checks[id] = Boolean(el(id).checked); });
  try { store.setItem(JOBS_DRAFT_KEY, JSON.stringify(draft)); } catch (err) { /* vol of geblokkeerd */ }
}

function restoreJobsDraft() {
  const store = sessionStore();
  if (!store) return;
  let draft = null;
  try { draft = JSON.parse(store.getItem(JOBS_DRAFT_KEY) || 'null'); } catch (err) { draft = null; }
  if (!draft || typeof draft !== 'object') return;
  JOBS_DRAFT_FIELDS.forEach((id) => {
    const value = draft.fields && draft.fields[id];
    if (typeof value === 'string' && (value || id === 'jobs-approval')) el(id).value = value;
  });
  // Het menu heeft nog geen keuzes (die komen met de eerste poll): de repository van het concept apart bewaren.
  const fields = draft.fields || {};
  if (typeof fields['jobs-repository'] === 'string' && fields['jobs-repository']) {
    jobsPage.draftRepo = { node: String(fields['jobs-node'] || ''), repo: fields['jobs-repository'] };
  }
  JOBS_DRAFT_CHECKS.forEach((id) => {
    if (draft.checks && typeof draft.checks[id] === 'boolean') el(id).checked = draft.checks[id];
  });
  if (Array.isArray(draft.deps)) draft.deps.forEach((id) => { if (typeof id === 'string') jobsPage.deps.add(id); });
}

function startViews() {
  el('nav-jobs').addEventListener('click', (event) => goView('opdrachten', event));
  el('nav-monitor').addEventListener('click', (event) => goView('monitor', event));
  const follow = () => showView(viewFromHash(window.location && window.location.hash));
  window.addEventListener('popstate', follow);
  window.addEventListener('hashchange', follow);
  showView(viewFromHash(window.location && window.location.hash), { initial: true, top: true });
}

/* --------------------------------------------------------------- start */

if (typeof document !== 'undefined' && typeof window !== 'undefined' && document.getElementById('cards')) {
  page.sound = normalizeSound(readStored(SOUND_KEY, null));
  page.notify = normalizeNotifyMemory(readStored(NOTIFY_KEY, null));
  unlockAudio(false);
  el('dialog-close').addEventListener('click', closeDialog);
  el('dialog').addEventListener('close', () => { page.dialog = null; });
  el('settings-open').addEventListener('click', () => openDialog('settings', null));
  el('history-open').addEventListener('click', () => openActionHistory(page.dialog && page.dialog.node));
  el('remote-switch').addEventListener('click', toggleRemote);
  el('sound-switch').addEventListener('click', () => {
    page.sound = normalizeSound({ ...page.sound, sound: !page.sound.sound });
    writeStored(SOUND_KEY, page.sound);
    renderNotifications(page.last && page.last.notifications);
  });
  el('speech-switch').addEventListener('click', () => {
    page.sound = normalizeSound({ ...page.sound, speech: !page.sound.speech });
    writeStored(SOUND_KEY, page.sound);
    renderNotifications(page.last && page.last.notifications);
  });
  el('test-sound').addEventListener('click', () => { unlockAudio(true); setTimeout(() => playTone('human'), 60); });
  el('test-speech').addEventListener('click', () => { unlockAudio(true); speak('Test. Claude wacht op een menselijke actie.'); });
  el('audio-gate').addEventListener('click', () => unlockAudio(true));
  // Elk gebaar ontgrendelt (iOS rekent alleen click/touchend, andere browsers ook pointerdown/keydown), ook na een herlaadmoment.
  ['pointerdown', 'keydown', 'click', 'touchend'].forEach((name) => window.addEventListener(name, () => unlockAudio(true), { passive: true }));
  // Een tabblad op de achtergrond pollt trager: bij terugkeer direct bijwerken en wachtende meldingen laten klinken.
  document.addEventListener('visibilitychange', () => { if (!document.hidden) { poll(); flushAudible(); } });
  window.addEventListener('online', () => { poll(); });
  tickClock();
  setInterval(tickClock, 5000);
  setInterval(() => { if (page.last && page.last.nodes) page.last.nodes.forEach(node => { if (cardNodes[node.id]) renderBackup(cardNodes[node.id].backup, node); }); }, 5000);
  updateAudioNotice();
  startViews();
  poll();
  startJobs();
}

if (typeof module !== 'undefined' && module.exports) {
  module.exports = {
    poll,
    backupBadge, renderBackup, dashboardQuery, nodeState, stateTone, pillFor, delayText, cardButtons, actionBody, durationLabel,
    recoveryText, summaryText, machineMarker, machineMarkers, machineStatus, MACHINE_STATUS_TEXT, reportButton, reportUrl, openReport, copyText, taskFacts, parseClock, inQuietHours,
    planNotifications, normalizeNotifyMemory, processNotifications, flushAudible, unlockAudio, audio, audioBlocked, quietAt, playTone, speak, CATCHUP_MAX_MS, PENDING_MAX_MS, NOTIFY_KEY,
    normalizeSound, agoLabel, resetText, usageSeverity, webserverView, renderWebserver, page, render, openDialog, closeDialog,
    sendAction, openActions, openActionItems, openActionHistory, actionHistoryUrl, actionBoardGroups, openActionReview, actionReviewUrl, actionReviewGroups, openSupervision, supervisionItems,
    jobBadgeClass, jobButtons, jobCanReport, jobReportUrl, pickableDeps, newRequestId, jobFormBody, jobMetaText, jobProgress, jobsHeadline,
    viewFromHash, showView, goView, jobsApprovalCount, renderNavCount, saveJobsDraft, restoreJobsDraft, JOBS_DRAFT_KEY, VIEWS,
    repoPaths, pickRepo, repoNote, renderRepoChoices, renderJobsMeta, rememberRepo, JOBS_REPO_KEY,
    REPO_AUTO, repoOptionText, showRepoDecision, clearRepoDecision, planRepo, repoChoiceText, jobsFormChanged,
    nodeName, pauseText, jobTitleOf, jobTextEditable, jobQueueSummary, jobsPage, JOB_QUEUE_NODES, JOB_NODE_TITLE, jobNodeLabel, renderJobs, sendJob, submitJob, prepareJobDraft, jobCard, openJobDialog, openJobReport, isoFromRunner2, JOB_NODES, STATE_TEXT, clockLabel, doneClock, supervisorText, actionOpenCount, actionOpenText, supervisionAttention, buildCard, updateCard,
  };
}
