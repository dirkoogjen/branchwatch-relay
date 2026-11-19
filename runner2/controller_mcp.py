"""Externe controller-MCP (stdio): dunne adapter waarmee ChatGPT Runner 2 bedient. Geen eigen logica of state.

Los van de interne sessie-MCP (`sessiontools`, voor Claude binnen een taak). Alles gaat via `controller`.
Bewust geen terugval op Runner 1: elke fout komt als duidelijke foutmelding terug.
"""
import json
import sys
import threading

from . import controller, dossier, jobs, remote, repos, supervision

STR = {"type": "string"}
NODE = {"type": "string", "enum": ["MAC", "LINUX", "GERARD", "WINDOWS"],
        "description": "machine waarop de taak draait; standaard de default-node van de control plane (GERARD). Expliciet MAC, LINUX of WINDOWS "
                       "alleen kiezen voor werk dat daar echt nodig is (macOS-specifiek, Linux-/Windows-test). Bij een bestaande taak niet nodig."}
TID = {"type": "string", "description": "Runner 2 task_id, bijvoorbeeld R2-1a2b3c4d5e6f"}


def _t(name, desc, props, required=()):
    return {"name": name, "description": desc, "inputSchema": {
        "type": "object", "properties": props, "required": list(required), "additionalProperties": False}}


JOB_ACTIONS = ("list", "add", "plan", "move", "set_node", "set_deps", "hold", "release", "approve", "reject", "cancel",
               "check", "retry", "resume", "archive", "pause_queue", "resume_queue", "repair")


TOOLS = [
    _t("runner2_start", "Start een Runner-2-programmeertaak op een node (MAC, LINUX, GERARD of WINDOWS; één actieve taak per node). "
       "Claude voert de opdracht uit in een eigen git-worktree op die node; er wordt niets naar main geïntegreerd en niets gepusht. "
       "Standaard is dit een eenmalige taak die bij completed stopt. Met continuation=true wordt het een keten van opeenvolgende "
       "taken: na elke voltooide schakel kiest Claude de volgende logische taak en Runner 2 start die op de commits van de vorige "
       "schakel (nog steeds één actieve taak per node). De keten wacht bij waiting_for_human/paused/failed, stopt bij runner2_stop of "
       "een limiet (max_tasks, max_hours, max_no_progress) en pusht of integreert nooit; status/result tonen het blok 'chain'. "
       "Beantwoord vragen, hervat en stop via de gewone tools op de taak-id van de lopende schakel. "
       "Met supervisor=true (opt-in, impliceert continuation) komt daar een beoordelingslaag boven: meldt een keten goal_reached "
       "of eindigt hij op een limiet, dan beoordeelt een aparte Claude-sessie in de review-rol (de judge) het ketenresultaat tegen het "
       "projectdoel en kiest continue_new_chain (een nieuwe keten op de tip van de vorige met een concreet vervolgdoel), project_complete "
       "(alleen met bewijs), needs_human (de beoordelingstaak wacht op runner2_answer) of blocked. De supervisor stopt op max_chains, "
       "supervisor_max_hours of supervisor_max_no_progress ketens zonder echte vooruitgang, pusht of integreert nooit en loopt als blok "
       "'supervisor' door status/result. Het kwaliteitsbeleid is risicogestuurd: gewone ketens draaien gerichte tests, de judge kan een change-scoped "
       "review-keten (quality) of gerichte reparatie (repair) kiezen, en een volledige suite alleen bij concrete brede impact met motivering; "
       "status toont wanneer de volgende keten een quality milestone is. runner2_stop stopt de hele supervisorrun.",
       {"repository": {"type": "string", "description": "pad naar de git-repository op de gekozen node, bijvoorbeeld ~/StudioOBF (Windows: C:\\Users\\operator\\repos\\naam of een pad met schuine strepen)"},
        "prompt": {"type": "string", "description": "de volledige programmeeropdracht"},
        "node": NODE,
        "task_id": {"type": "string", "description": "optioneel vooraf toegekend R2-id (R2- plus 12 hexcijfers) voor een idempotente start van elke soort "
                    "opdracht (eenmalig, keten of supervisor): bestaat de taak al, dan komt die terug (already_started) en start er niets een tweede keer. "
                    "Gebruikt door de supervisie en de opdrachtwachtrij."},
        "title": {"type": "string", "description": "korte omschrijving"},
        "role": {"type": "string", "enum": list(controller.ROLES), "description": "modelrol; standaard implement"},
        "model": {"type": "string", "description": "optioneel model-id (bijv. claude-opus-5-5 of claude-sonnet-5-5); standaard het model van de rol uit machine.toml"},
        "continuation": {"type": "boolean", "description": "opt-in keten van opeenvolgende taken; standaard false (eenmalige taak)"},
        "goal": {"type": "string", "description": "alleen met continuation: overkoepelend doel van de keten; standaard de prompt"},
        "max_tasks": {"type": "integer", "description": "alleen met continuation: maximaal aantal schakels (standaard 5, plafond 25)"},
        "max_hours": {"type": "number", "description": "alleen met continuation: na zoveel uur start er geen nieuwe schakel meer (standaard 12, plafond 48)"},
        "max_no_progress": {"type": "integer", "description": "alleen met continuation: stop na zoveel opeenvolgende schakels zonder nieuwe commits (standaard 2)"},
        "supervisor": {"type": "boolean", "description": "opt-in beoordelingslaag boven ketens (impliceert continuation); standaard false"},
        "project_goal": {"type": "string", "description": "alleen met supervisor: overkoepelend projectdoel dat de judge beoordeelt; standaard goal of de prompt"},
        "max_chains": {"type": "integer", "description": "alleen met supervisor: maximaal aantal ketens (standaard 4, plafond 12)"},
        "supervisor_max_hours": {"type": "number", "description": "alleen met supervisor: totale looptijd over alle ketens in uren (standaard 24, plafond 72)"},
        "supervisor_max_no_progress": {"type": "integer", "description": "alleen met supervisor: stop na zoveel opeenvolgende ketens zonder echte vooruitgang (standaard 2, plafond 5)"},
        "status_files": {"type": "array", "items": STR, "description": "alleen met supervisor: project-/statusbestanden (pad in de repo) die de judge meeleest"},
        "max_quality_chains": {"type": "integer", "description": "alleen met supervisor: maximaal aantal aparte change-scoped review-/kwaliteitsketens (standaard 2, plafond 4, 0 = geen)"},
        "full_verification": {"type": "boolean", "description": "alleen met supervisor: expliciete gebruikerswens dat de judge een volledige repository-/testsuite als brede controle mag kiezen; anders alleen bij concrete brede impact"}},
       ("repository", "prompt")),
    _t("runner2_status", "Alleen lezen: toestand van één taak (met task_id) of een compact overzicht (zonder). "
       "Toont bij waiting_for_human de vraag van Claude en, bij een keten, het blok 'chain' "
       "(fase, schakel x van max, limieten, reden van stoppen) en, onder een supervisor, het blok 'supervisor' "
       "(keten x van max, fase, laatste beslissing, volgende doel, deadline).", {"task_id": TID, "node": NODE}),
    _t("runner2_answer", "Beantwoord de vraag van een taak in status waiting_for_human; dezelfde Claude-conversatie gaat verder.",
       {"task_id": TID, "node": NODE, "answer": STR}, ("task_id", "answer")),
    _t("runner2_pause", "Pauzeer een lopende taak zonder werkverlies; de watch hervat hem niet.", {"task_id": TID, "node": NODE}, ("task_id",)),
    _t("runner2_resume", "Hervat een gepauzeerde (of mislukte) taak in dezelfde conversatie en worktree.",
       {"task_id": TID, "node": NODE, "note": {"type": "string", "description": "optionele korte instructie"}}, ("task_id",)),
    _t("runner2_stop", "Stop een taak definitief (niet hervatbaar). Worktree, transcript en commits blijven bestaan. "
       "Zit de taak in een keten, dan stopt dit ook de keten (ook als er net geen schakel draait).",
       {"task_id": TID, "node": NODE, "reason": STR}, ("task_id",)),
    _t("runner2_result", "Alleen lezen: eindresultaat van een voltooide, mislukte of gestopte taak; bij een keten met overzicht van alle schakels, "
       "de tip-branch en waarom de keten stopte.", {"task_id": TID, "node": NODE}, ("task_id",)),
    _t("runner2_actions", "Openstaande menselijke actiepunten: beslis-/actiepunten die een afgeronde of doorlopende run heeft achtergelaten "
       "zonder erop te wachten (Monitor 2 toont alleen 'ACTIE OPEN · N'). Zonder resolve: lijst de open punten met id, interne tekst, reden "
       "en bron (taak/keten/supervisor) - van de run van task_id, anders van de hele node; dat wijzigt niets. Met resolve (lijst id's): sluit die punten zodat "
       "de teller daalt (idempotent), met optioneel een korte note. Met review=true: DRY RUN van het actiebeheer op die node (zou sluiten / "
       "twijfel / vereist beslissing, met regel en bewijs per punt); dat wijzigt niets en sluit nooit iets. Met closure=true: rapport van de bewijsgerichte "
       "afsluiting (alleen lezen). Met reassess=true: het laatst bewaarde rapport van de periodieke herbeoordeling van de open acties van alle nodes "
       "(klasse per punt, werkelijk nieuwe menselijke beslissingen, voorbereid technisch vervolgwerk; alleen lezen, sluit niets). Met claim {action_id, job_id, besluit?, soort?}: koppelt een open punt aan de opdracht die het moet uitvoeren; dat sluit niets: "
       "Runner 2 sluit het punt pas zelf als besluit, uitvoering, controle (jobs/check) en waar relevant integratie aantoonbaar zijn. Een vraag die een run echt blokkeert hoort hier niet bij: dat is "
       "waiting_for_human (WACHT OP ACTIE) en wordt met runner2_answer beantwoord.",
       {"task_id": TID, "node": NODE, "resolve": {"type": "array", "items": STR, "description": "id's van open actiepunten om te sluiten"},
        "note": {"type": "string", "description": "alleen met resolve: korte reden of beslissing"},
        "include_resolved": {"type": "boolean", "description": "ook gesloten punten tonen (standaard alleen open)"},
        "review": {"type": "boolean", "description": "DRY RUN van het actiebeheer: welke punten zouden sluiten en waarom (alleen lezen)"},
        "closure": {"type": "boolean", "description": "rapport van de bewijsgerichte afsluiting per gekoppeld punt (alleen lezen)"},
        "reassess": {"type": "boolean", "description": "laatst bewaarde rapport van de periodieke herbeoordeling van open acties (alleen lezen)"},
        "claim": {"type": "object", "description": "koppel een open punt aan de opdracht die het uitvoert: {action_id, job_id, besluit?: nummer in het "
                  "beslissingenregister, soort?: 'technisch', note?}. Er is een besluit of soort=technisch nodig; het sluit niets"}}),
]

OBJ = {"type": "object"}
OBJS = {"type": "array", "items": {"type": "object"}}
SUP_PID = {"type": "string", "description": "supervisie-project-id, bijvoorbeeld P-1a2b3c4d5e6f"}
TOOLS += [
    _t("runner2_supervision_status", "Alleen lezen: externe supervisie (ChatGPT als regisseur boven de Claude-runners). Zonder project_id: overzicht van alle "
       "supervisieprojecten met label (SUPERVISIE NODIG, SUPERVISIE GEBLOKKEERD, WACHT OP BEHEERDER, RUN LOOPT, ...) en volgende stap; een project dat "
       "aandacht vraagt heeft een blok point (Runner, opdracht, taak-id, reden, status/blokkade, acties, eindrapport). Met project_id: "
       "ook recente beslissingen, opdrachten en events. Lokaal op de control plane; leest alleen het dossier, geen chatgeheugen nodig.",
       {"project_id": SUP_PID}),
    _t("runner2_supervision_claim", "Claim het oudste openstaande supervisie-event (bijvoorbeeld: een run is afgelopen of meldde completed). "
       "Geeft event_id, een token en de volledige compacte context uit het dossier, of claimed=false. Een claim heeft een lease; verloopt die zonder "
       "beslissing, dan mag een andere sessie het event overnemen (na te veel pogingen escaleert het naar SUPERVISIE GEBLOKKEERD). "
       "Beslis daarna met runner2_supervision_decide met het token.",
       {"project_id": SUP_PID, "claimer": {"type": "string", "description": "naam van de beslisser, standaard chatgpt"},
        "lease_s": {"type": "integer", "description": "leaseduur in seconden (30 t/m 3600, standaard uit de config)"}}),
    _t("runner2_supervision_context", "Alleen-lezen: dossiercontext van een project (doel, bouwlijst, kennis, backlog, blokkades, laatste "
       "opdrachten en beslissingen, limieten, checkpoint), zonder te claimen. Voor een verse sessie die zich wil oriënteren.",
       {"project_id": SUP_PID, "event_id": STR}, ("project_id",)),
    _t("runner2_supervision_decide", "Leg het oordeel over een geclaimd event vast: precies één van drie uitkomsten in decision.kind. "
       "goal_complete (echt projectdoel bereikt; evidence[] verplicht; geweigerd zolang uitvoerbaar backlogwerk openstaat), "
       "work_remaining (reason, why_next, next_goal, next_prompt; optioneel node, repository uit de toegestane targets, backlog_ids, chain_limits; "
       "Runner 2 start of queuet precies één taak op die node/repo) of human_blocked (human_blockers[] of question; geweigerd zolang uitvoerbaar werk "
       "zonder menselijke blokkade resteert). Optioneel: backlog_updates, status_updates, action_classes, resolve_actions, knowledge_add. Dezelfde "
       "beslissing opnieuw sturen is veilig (idempotent); een andere inhoud voor een al beslist event wordt geweigerd. Houdt limieten, pauze/stop en "
       "één lopende opdracht per project af.",
       {"project_id": SUP_PID, "event_id": STR, "token": STR, "decision": {"type": "object", "description": "zie beschrijving"}},
       ("project_id", "event_id", "token", "decision")),
    _t("runner2_supervision_admin", "Beheer van supervisieprojecten (Beheerder/ChatGPT). action: register (title, goal, targets[{node, repository}], "
       "optioneel master_list{refs,text}, backlog[], knowledge{architecture[],beheerder_decisions[]}, limits, deadline_hours, idempotency_key), assign "
       "(handmatige opdracht: goal, prompt, node, repository), pause, stop (stopt ook de lopende run), resume, set_limits (limits, reset_no_progress, deadline_hours = nieuwe deadline vanaf nu), amend_goal (goal, reason), "
       "note (kind, text), backlog (nieuwe items), request_review, release_event (event_id), resolve_blocker (blocker_ids, note), "
       "repair (mode restore_prev|reseal met confirm=true|accept), reconcile (nu een tik uitvoeren).",
       {"action": {"type": "string", "enum": ["register", "assign", "pause", "stop", "resume", "set_limits", "amend_goal", "note", "backlog",
                                              "request_review", "release_event", "resolve_blocker", "repair", "reconcile"]},
        "project_id": SUP_PID, "title": STR, "goal": STR, "targets": OBJS, "master_list": OBJ, "backlog": OBJS, "knowledge": OBJ, "limits": OBJ,
        "deadline_hours": {"type": "number"}, "idempotency_key": STR, "mode": STR, "confirm": {"type": "boolean"}, "note": STR, "by": STR,
        "reason": STR, "kind": STR, "text": STR, "event_id": STR, "blocker_ids": {"type": "array", "items": STR}, "node": NODE, "repository": STR,
        "prompt": STR, "backlog_ids": {"type": "array", "items": STR}, "chain_limits": OBJ, "reset_no_progress": {"type": "boolean"}},
       ("action",)),
    _t("runner2_jobs", "Opdrachtwachtrij op de control plane: één invoer voor opdrachten aan de Runners MAC, GERARD, LINUX en WINDOWS. "
       "Per Runner een persistente wachtrij; de watch start automatisch de volgende opdracht zodra een Runner vrij is (één taak per Runner; "
       "verschillende Runners tegelijk). depends_on laat een opdracht pas starten als alle voorgangers geslaagd zijn (CLAUDE KLAAR, of "
       "gecontroleerd als de voorganger require_check heeft); faalt een voorganger, dan is de opdracht GEBLOKKEERD. approval "
       "(start, integratie, publicatie, risico) houdt een opdracht vast tot een mens haar goedkeurt. Elke start gebruikt een vooraf "
       "toegekend taak-id, dus nooit een dubbele start. repository mag het woord auto zijn: Runner 2 bepaalt dan uit de tekst (en de "
       "projectafspraken/ontdekte repositories van de node) welke repository hoort; is dat niet ondubbelzinnig dan volgt een fout "
       "repo_choice_needed (één vraag met opties; beantwoord met project_repos {project: repository}) of repo_unknown, en bij een "
       "opdracht die over meerdere repositories gaat repo_split_confirm (herhaal met split=true om per repository een aparte opdracht "
       "te maken). action list en plan zijn alleen lezen (plan toont wat add zou doen); de andere handelingen wijzigen de wachtrij. action: list (overzicht; job_id voor één opdracht), add (prompt, repository, node, "
       "title, model standaard|opus|sonnet, supervisor, continuation, depends_on, require_check, approval, held, position top, request_id), "
       "move (job_id, to top|bottom|up|down|positie), set_node, set_deps, hold, release, approve, reject, cancel (ook een lopende taak), "
       "check (verdict ok|afgekeurd, source chatgpt|mens: na CLAUDE KLAAR de aparte status GECONTROLEERD), retry (nieuwe run), resume (de bestaande taak hervatten), archive, "
       "pause_queue/resume_queue (node of alle), repair (mode restore_prev). by = wie dit doet (naam). Pusht of integreert nooit zelf.",
       {"action": {"type": "string", "description": "handeling: " + " | ".join(JOB_ACTIONS)},
        "by": {"type": "string", "description": "naam van wie de handeling doet, bijv. Beheerder, Gerard of ChatGPT"},
        "job_id": STR, "prompt": STR, "repository": STR, "title": STR,
        "node": {"type": "string", "enum": ["MAC", "LINUX", "GERARD", "WINDOWS", "ALL"], "description": "doel-Runner van de opdracht (of de wachtrij bij pause_queue)"},
        "model": {"type": "string", "enum": list(jobs.MODEL_CHOICES)}, "supervisor": {"type": "boolean"}, "continuation": {"type": "boolean"},
        "project_goal": STR, "max_chains": {"type": "integer"}, "supervisor_max_hours": {"type": "number"},
        "depends_on": {"type": "array", "items": STR}, "require_check": {"type": "boolean"},
        "approval": {"type": "string", "enum": ["geen", *jobs.APPROVAL_KINDS]}, "held": {"type": "boolean"},
        "position": {"type": "string", "enum": ["top", "bottom"]}, "request_id": STR,
        "to": {"type": "string", "description": "top, bottom, up, down of een positie als getal in tekst (0 = eerst)"}, "verdict": {"type": "string", "enum": ["ok", "afgekeurd"]},
        "source": {"type": "string", "enum": list(jobs.CHECK_SOURCES)}, "note": STR, "reason": STR,
        "include_archived": {"type": "boolean"}, "mode": {"type": "string", "enum": ["restore_prev"]},
        "split": {"type": "boolean", "description": "bij repository=auto: bevestigt dat een opdracht over meerdere repositories "
                  "in aparte opdrachten (één per repository) gesplitst mag worden"},
        "project_repos": {"type": "object", "description": "bij repository=auto: antwoord op een eerdere vraag, {project-id: repository}; "
                          "alleen kandidaten van dat project zijn geldig"}},
       ("action",)),
    _t("runner2_repos", "Repositorykeuze (alleen lezen): welke git-repositories een node heeft en bij welk project ze horen. "
       "action: discover (node; live inspectie van de eigen machine of via het node-transport, met terugval op bekende repositories), "
       "catalog (alle nodes plus de projectenlijst), resolve (text, node, optioneel project_repos: welke repository hoort bij de tekst; "
       "geeft decision resolved | choice_needed | unresolved). Ontdekt alleen via projectafspraken (projects.toml), repositories die Runner 2 "
       "al kent en de ingestelde roots op diepte 1; geen vrije paden, geen schrijfacties.",
       {"action": {"type": "string", "description": "discover | catalog | resolve"}, "node": NODE, "text": STR,
        "refresh": {"type": "boolean"}, "project_repos": {"type": "object"}},
       ("action",)),
]

# MCP-annotations (tool-hints, spec 2025-03-26) zodat de client (ChatGPT) alleen-lezen-tools onderscheidt van tools die iets
# wijzigen. Het zijn hints, geen autorisatie: validatie en de regels in controller/jobs/supervision blijven bepalend. Per tool telt
# de zwaarste handeling: runner2_jobs (cancel/reject/archive) en runner2_supervision_admin (stop/repair) zijn dus destructive.
# openWorldHint=false: alleen de eigen control plane en nodes; true waar een Claude-taak start of verder werkt.
_RO = {"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": False}


def _rw(title, destructive=False, idempotent=False, open_world=False):
    return {"title": title, "readOnlyHint": False, "destructiveHint": destructive, "idempotentHint": idempotent, "openWorldHint": open_world}


ANNOTATIONS = {
    "runner2_start": _rw("Taak starten", open_world=True),
    "runner2_status": {"title": "Status bekijken", **_RO},
    "runner2_answer": _rw("Vraag van een taak beantwoorden", open_world=True),
    "runner2_pause": _rw("Taak pauzeren"),
    "runner2_resume": _rw("Taak hervatten", open_world=True),
    "runner2_stop": _rw("Taak definitief stoppen", destructive=True, idempotent=True),
    "runner2_result": {"title": "Eindresultaat bekijken", **_RO},
    "runner2_actions": _rw("Open actiepunten bekijken of sluiten", idempotent=True),
    "runner2_supervision_status": {"title": "Supervisie: overzicht", **_RO},
    "runner2_supervision_claim": _rw("Supervisie: event claimen"),
    "runner2_supervision_context": {"title": "Supervisie: dossiercontext", **_RO},
    "runner2_supervision_decide": _rw("Supervisie: beslissing vastleggen", idempotent=True, open_world=True),
    "runner2_supervision_admin": _rw("Supervisie: projectbeheer", destructive=True, open_world=True),
    "runner2_jobs": _rw("Opdrachtwachtrij", destructive=True, open_world=True),
    "runner2_repos": {"title": "Repositorykeuze bekijken", **_RO},
}
TOOLS += [
    _t('runner2_discussions', 'Overlegvragen en antwoorden op de primaire control plane; geen programmeeropdracht.',
       {'action': STR, 'by': STR, 'nonce': STR, 'conversation_id': STR, 'turn_id': STR, 'version': {'type': 'integer'},
        'node': NODE, 'repository': STR, 'model': STR, 'question': STR, 'title': STR}, ['action'])]
ANNOTATIONS['runner2_discussions'] = _rw('Met Claude overleggen', destructive=True, open_world=True)
for _tool in TOOLS:
    _tool["annotations"] = ANNOTATIONS[_tool["name"]]
_BY_NAME = {t["name"]: t for t in TOOLS}


def validate(tool: dict, args: dict) -> None:
    schema = tool["inputSchema"]
    if not isinstance(args, dict):
        raise controller.ControllerError("invalid_input", "arguments moet een object zijn")
    for k in schema["required"]:
        if args.get(k) in (None, ""):
            raise controller.ControllerError("invalid_input", f"ontbrekend veld: {k}")
    for k, v in args.items():
        spec = schema["properties"].get(k)
        if spec is None:
            raise controller.ControllerError("invalid_input", f"onbekend veld: {k}")
        kind = spec.get("type", "string")
        ok = {"string": isinstance(v, str), "boolean": isinstance(v, bool),
              "integer": isinstance(v, int) and not isinstance(v, bool),
              "number": isinstance(v, (int, float)) and not isinstance(v, bool),
              "object": isinstance(v, dict),
              "array": isinstance(v, list) and all(isinstance(i, dict if (spec.get("items") or {}).get("type") == "object" else str) for i in v)}[kind]
        if not ok:
            raise controller.ControllerError("invalid_input", f"veld {k} moet " + {
                "string": "tekst", "boolean": "true of false", "integer": "een geheel getal", "number": "een getal",
                "object": "een object", "array": "een lijst van " + ("objecten" if (spec.get("items") or {}).get("type") == "object" else "teksten")}[kind] + " zijn")
        if "enum" in spec and v not in spec["enum"]:
            raise controller.ControllerError("invalid_input", f"veld {k} moet een van {spec['enum']} zijn")


def dispatch_local(name: str, a: dict) -> dict:
    if name == "runner2_start":
        return controller.start(a["repository"], a["prompt"], node=a.get("node"), title=a.get("title"),
                                role=a.get("role", "implement"), continuation=a.get("continuation", False),
                                goal=a.get("goal"), max_tasks=a.get("max_tasks"), max_hours=a.get("max_hours"),
                                max_no_progress=a.get("max_no_progress"), supervisor=a.get("supervisor", False),
                                project_goal=a.get("project_goal"), max_chains=a.get("max_chains"),
                                supervisor_max_hours=a.get("supervisor_max_hours"),
                                supervisor_max_no_progress=a.get("supervisor_max_no_progress"),
                                status_files=a.get("status_files"), max_quality_chains=a.get("max_quality_chains"),
                                full_verification=a.get("full_verification"), task_id=a.get("task_id"), model=a.get("model"))
    if name == "runner2_status":
        return controller.status(a.get("task_id"))
    if name == "runner2_answer":
        return controller.answer(a["task_id"], a["answer"])
    if name == "runner2_pause":
        return controller.pause(a["task_id"])
    if name == "runner2_resume":
        return controller.resume(a["task_id"], a.get("note", ""))
    if name == "runner2_stop":
        return controller.stop(a["task_id"], a.get("reason", ""))
    if name == "runner2_actions":
        if a.get("reassess") and any(a.get(k) for k in ("review", "closure", "claim", "resolve")):
            raise controller.ControllerError("invalid_input", "reassess gaat niet samen met review, closure, claim of resolve")
        if (a.get("closure") or a.get("claim")) and sum(bool(a.get(k)) for k in ("review", "closure", "claim", "resolve")) > 1:
            raise controller.ControllerError("invalid_input", "review, closure, claim en resolve gaan niet samen")
        if a.get("closure"):
            return controller.closure_report()
        if a.get("reassess"):
            if any(a.get(k) for k in ("review", "claim", "resolve")):
                raise controller.ControllerError("invalid_input", "reassess gaat niet samen met review, claim of resolve")
            return controller.reassess_report()
        if a.get("claim"):
            c = a["claim"]
            if not isinstance(c, dict):
                raise controller.ControllerError("invalid_input", "claim moet een object zijn")
            return controller.claim_action(c.get("action_id"), c.get("job_id"), besluit=c.get("besluit"), soort=c.get("soort"),
                                           note=c.get("note") or "", by="controller (ChatGPT)")
        if a.get("review"):
            if a.get("resolve"):
                raise controller.ControllerError("invalid_input", "review en resolve gaan niet samen: de dry run sluit nooit iets")
            return controller.review_actions()
        if a.get("resolve"):
            return controller.resolve_actions(a["resolve"], a.get("note", ""), by="controller (ChatGPT)")
        return controller.list_actions(a.get("task_id"), bool(a.get("include_resolved")))
    return controller.result(a["task_id"])


_channel = threading.local()   # "chatgpt" voor de stdio-MCP (handle); de rpc zet "rpc"; anders "lokaal"


def gate_direct(name: str, a: dict, channel: str | None = None) -> None:
    """Menselijke goedkeuringspoort voor ingangen die niet via de wachtrij lopen (directe start, antwoord, hervatten).

    Wat ChatGPT hier rechtstreeks instuurt kan niet wachten op goedkeuring; een risicovolle inhoud wordt dus geweigerd
    en moet als opdracht (runner2_jobs add) worden ingediend, waar een mens goedkeurt. Menselijke kanalen en de
    dispatcher (gaat niet via call_tool) worden niet gepoort."""
    from . import riskpolicy
    if not riskpolicy.enabled(controller.config.load_config()):
        return
    channel = channel if channel is not None else (getattr(_channel, "name", None) or "lokaal")
    if riskpolicy.is_human_channel(channel):
        return
    fields = {"runner2_start": ("prompt", "title", "goal", "project_goal"), "runner2_answer": ("answer",),
              "runner2_resume": ("note",)}.get(name)
    if not fields:
        return
    texts = [a.get(f) for f in fields]
    v = (riskpolicy.assess(channel, texts, a.get("repository"), controller.config.load_config()) if name == "runner2_start"
         else riskpolicy.classify(*texts))
    if v["risky"]:
        raise controller.ControllerError("approval_required", "Dit raakt productie/publicatie, verwijdert gegevens of wijzigt beveiliging/"
                                         "toegang en vraagt menselijke goedkeuring. Dien het in als opdracht (runner2_jobs add); "
                                         "een mens keurt goed in Monitor 2. Reden: " + "; ".join(v["reasons"]),
                                         {"reasons": v["reasons"], "categories": v["categories"]})



def dispatch_jobs(a: dict) -> dict:
    """Opdrachtwachtrij: altijd op de control plane zelf; `node` is hier de doel-Runner van een opdracht, geen routering."""
    args = {k: v for k, v in a.items() if k not in ("action", "by")}
    return jobs.act(a["action"], args, a.get("by") or "", channel=getattr(_channel, "name", None) or "lokaal")


def dispatch_supervision(name: str, a: dict) -> dict:
    """Supervisie draait op de control plane; `node` (assign) is daar een doelnode, geen routering."""
    if name == "runner2_supervision_status":
        return supervision.status(a.get("project_id"))
    if name == "runner2_supervision_claim":
        return supervision.claim(a.get("project_id"), a.get("claimer") or "chatgpt", a.get("lease_s"))
    if name == "runner2_supervision_context":
        return supervision.context(a["project_id"], a.get("event_id"))
    if name == "runner2_supervision_decide":
        return supervision.decide(a["project_id"], a["event_id"], a["token"], a["decision"])
    return supervision.admin(a.get("project_id"), a["action"], **{k: v for k, v in a.items() if k not in ("project_id", "action")})


def dispatch_repos(a: dict, fanout: bool) -> dict:
    action = a["action"]
    node = (a.get("node") or "").upper() or repos.local_name()
    if action == "discover":
        if not fanout or node == repos.local_name():
            return repos.discover_local()   # live; de ontvangende kant antwoordt alleen over zichzelf
        return repos.discover(node, refresh=bool(a.get("refresh")))
    if action == "catalog":
        if not fanout:
            return {"nodes": {repos.local_name(): repos.discover_local()}, "local": repos.local_name(), "projects": []}
        return repos.catalog(refresh=bool(a.get("refresh")))
    if action == "resolve":
        text = a.get("text")
        if not isinstance(text, str) or not text.strip():
            raise controller.ControllerError("invalid_input", "text ontbreekt")
        return repos.resolve(text, node, choices=a.get("project_repos"), live_remote=fanout)
    raise controller.ControllerError("invalid_input", f"onbekende handeling {action!r}; kies discover, catalog of resolve")


def dispatch(name: str, a: dict, fanout: bool = True) -> dict:
    """Kiest de node (expliciet, of via de taak-id) en voert lokaal uit of via het node-transport.

    fanout=False (de ontvangende kant, rpc.py): antwoord alleen over deze machine; geen doorrouteren naar andere nodes
    en geen nodeoverzicht, anders wacht een node op zijn eigen (oude/onbereikbare) routes en loopt een aanroep vast.
    """
    if name == 'runner2_discussions':
        from . import cplane, discussions
        cplane.require_primary()
        try:return discussions.handle({k: v for k, v in a.items() if k != 'by'}, a.get('by', ''))
        except ValueError as exc:raise controller.ControllerError('invalid_input', str(exc)) from None
    if name == 'runner2_discussion_execution':
        from . import discussion_worker as worker
        allowed = {'action', 'node', 'conversation_id', 'turn_id', 'execution_id', 'repository', 'model', 'context'}
        if not isinstance(a, dict) or set(a) - allowed or a.get('node') not in ('GERARD', 'LINUX'):
            raise controller.ControllerError('invalid_input', 'Ongeldig intern overlegverzoek.')
        node = a.get('node')
        if fanout and node != controller.config.local_node():return remote.call(node, name, a)
        if node != controller.config.local_node():raise controller.ControllerError('unsupported_node', 'Verkeerde overlegnode.')
        try:
            if a['action'] == 'start':return worker.start({k: v for k, v in a.items() if k != 'action'})
            if a['action'] == 'stop':return worker.stop_request({k: v for k, v in a.items() if k != 'action'})
            if a['action'] == 'status':
                if not (worker.execution_dir(a['execution_id']) / 'state.json').exists():
                    raise controller.ControllerError('not_found', 'Overleguitvoering niet gevonden.')
                return worker.status(a['execution_id'])
            raise ValueError('Onbekende overlegactie.')
        except ValueError as exc:raise controller.ControllerError('invalid_input', str(exc)) from None
    if name.startswith("runner2_supervision"):
        from . import cplane
        cplane.require_primary()
        return dispatch_supervision(name, a)
    if name == "runner2_jobs":
        from . import cplane
        if fanout and a.get("action") == "list" and not cplane.is_primary():
            primary = str((cplane.role() or {}).get("primary_host") or "").upper()
            if primary:
                return remote.call(primary, name, a)
        cplane.require_primary()
        return dispatch_jobs(a)
    if name == "runner2_repos":
        return dispatch_repos(a, fanout)
    if not fanout:
        return dispatch_local(name, a)
    node = (a.get("node") or "").upper() or (controller.config.default_node() if name == "runner2_start" else remote.locate(a.get("task_id")))
    if node and node != controller.config.local_node():
        return remote.call(node, name, {k: v for k, v in a.items() if k != "node"})
    out = dispatch_local(name, a)
    if name == "runner2_status" and not a.get("task_id") and not a.get("node") and remote.nodes():
        out["nodes"] = remote.overview()
    return out


def call_tool(name: str, args: dict, fanout: bool = True) -> tuple[str, bool]:
    """Geeft (compacte JSON-tekst, is_error)."""
    tool = _BY_NAME.get(name)
    try:
        if not tool:
            raise controller.ControllerError("invalid_input", f"onbekende tool {name}")
        normalized = dict(args or {})
        if name == "runner2_jobs" and normalized.get("approval") == "":
            normalized["approval"] = "geen"
        validate(tool, normalized)
        gate_direct(name, normalized)
        if name == "runner2_jobs" and normalized["action"] not in JOB_ACTIONS:
            raise controller.ControllerError("invalid_input", f"onbekende handeling {normalized['action']!r}")
        return json.dumps(dispatch(name, normalized, fanout), ensure_ascii=False, separators=(",", ":")), False
    except (controller.ControllerError, supervision.SupervisionError, dossier.DossierError, jobs.JobsError, repos.ReposError) as e:
        body = {"error": e.code, "message": e.message}
        if getattr(e, "data", None):
            body["data"] = e.data
        return json.dumps(body, ensure_ascii=False), True
    except Exception as e:  # nooit een stacktrace of een stille terugval; wel een duidelijke fout
        return json.dumps({"error": "runner2_unavailable", "message": f"{type(e).__name__}: {e}"}), True


def handle(msg: dict) -> dict | None:
    method, mid = msg.get("method"), msg.get("id")
    if mid is None:
        return None
    if method == "initialize":
        result = {"protocolVersion": (msg.get("params") or {}).get("protocolVersion", "2024-11-05"),
                  "capabilities": {"tools": {}}, "serverInfo": {"name": "runner2-controller", "version": "1"}}
    elif method == "tools/list":
        from . import cplane
        result = {"tools": cplane.tools_for_role(TOOLS)}
    elif method == "tools/call":
        p = msg.get("params") or {}
        _channel.name = "chatgpt"   # de stdio-MCP is de ingang van ChatGPT (connector via de tunnel)
        try:
            text, err = call_tool(p.get("name", ""), p.get("arguments") or {})
        finally:
            _channel.name = None
        result = {"content": [{"type": "text", "text": text}], "isError": err}
    elif method == "ping":
        result = {}
    else:
        return {"jsonrpc": "2.0", "id": mid, "error": {"code": -32601, "message": "method not found"}}
    return {"jsonrpc": "2.0", "id": mid, "result": result}


def main() -> None:
    for line in sys.stdin:
        if not line.strip():
            continue
        try:
            reply = handle(json.loads(line))
        except ValueError:
            reply = {"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "parse error"}}
        if reply:
            sys.stdout.write(json.dumps(reply) + "\n")
            sys.stdout.flush()


if __name__ == "__main__":
    main()
