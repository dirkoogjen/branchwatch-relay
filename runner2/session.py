"""Schone omgeving (W2), opdrachtregel, init-controle (W14), hervatberichten (W4/W5) en transcript-back-up (W7)."""
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

from . import osadapter, store
from .filelock import replace

DROP_PREFIXES = ("CLAUDE", "ANTHROPIC_", "AI_AGENT")
DROP_EXACT = {"TERM", "TERM_PROGRAM", "TERM_SESSION_ID", "SSH_AUTH_SOCK", "OLDPWD", "PWD", "SHLVL", "_"}
WINDOWS_DROP_EXACT = {"SSH_CLIENT", "SSH_CONNECTION", "SSH_TTY"}

AGREEMENT = """\
Je werkt via Runner 2, onbewaakt: er kijkt niemand live mee.
- Werk autonoom door: onderzoek technische problemen zelf, voer relevante tests zelf uit en herstel testfouten zelf.
- Stel geen menselijke vraag voor normale programmeerproblemen. Kies een redelijke standaard en noteer die in je rapport.
- Roep mcp__r2__runner_ask alleen aan als informatie of een beslissing werkelijk ontbreekt, en beëindig daarna je beurt.
- Roep mcp__r2__runner_finish aan zodra de opdracht volledig klaar en getest is (commit eerst je werk). Stop nooit met alleen een tekstueel eindrapport.
- Beëindig je beurt nooit om op achtergrondwerk (tests) te wachten; wacht op het resultaat.
- Start lange tests op de voorgrond met begrensde timeout, of via Bash run_in_background en bewaar de verkregen task_id. Lees voltooiing en exitcode via TaskOutput voor precies die taak. Wacht nooit met while/until en pgrep -f op testvoltooiing: zo vindt een shell zichzelf of ander werk. Start geen tweede test als de eerste al draait; behoud het bestaande taakresultaat.
- Laat je een beslis- of actiepunt voor een mens achter waarop je NIET hoeft te wachten (het werk is af of gaat door), meld het dan bij runner_finish in open_actions; verwerk het niet alleen in de tekst van je rapport. Ontbreekt iets waardoor je niet verder kunt, gebruik dan runner_ask.
- Regels voor open_actions (de lijst wordt door een niet-programmerende gebruiker gelezen): text = één korte vraag of opdracht van 1-2 zinnen in gewoon Nederlands, zonder jargon, zonder commit-hashes, branchnamen, taak-ids of kenmerken als H1/H2. Technische toelichting hoort in reason en details (die toont de gebruiker alleen onder 'Details'). Geef een stabiele topic mee (korte slug van de vraag, bv. media-map). Meld alleen wat de gebruiker zelf moet beslissen of doen; technische vervolgstappen die jij kunt uitvoeren, een verslag of mededeling over afgerond werk, of iets wat in de prompt al als BESLOTEN of AL OPEN staat is GEEN open_action. Is een eerdere vraag al beantwoord maar ontbreekt er een nieuw, ander deel van het besluit, meld dan alleen dat nieuwe deel (en vul new_info in als het onder dezelfde topic valt).
- Werk in deze worktree en commit op de taakbranch; blijf binnen de worktree tenzij de opdracht machinebrede handelingen vraagt.
- Het klaar-signaal uit je persoonlijke instructies geldt niet voor Runner 2-taken: zet nooit een klaar-signaalvlag.
"""

CHAIN_AGREEMENT = """\

Autonome keten (deze taak is één schakel in een keten naar een overkoepelend doel):
- Werk één afgeronde, geteste stap af, commit die, en sluit af met mcp__r2__runner_finish.
- Geef bij runner_finish altijd chain_status: "goal_reached" als het overkoepelende doel volledig bereikt en getest is, anders "continue" met next_step: een concrete, zelfstandig uitvoerbare beschrijving van de volgende schakel.
- Laat geen ongecommitte wijzigingen achter: de volgende schakel start op jouw laatste commit en alleen wat gecommit is gaat mee.
- Push nooit, deploy of publiceer nooit, en merge, reset of verplaats zelf niets op main of een andere branch. Integreren naar de lokale main doet Monitor 2 (de controller), alleen voor een repository waarvoor de gebruiker dat eenmalig heeft ingesteld; jij doet daarvoor alleen een aanvraag. Staat `scripts/obf_monitor2_integrate.py` in de repository en moet je werk naar main, commit dan eerst alles en vraag het vanuit je worktree aan met `python scripts/obf_monitor2_integrate.py request --test <testbestand> [--test ...]` (kies gerichte tests voor wat je wijzigde; daarna geen commits meer). Noem het aanvraag-id (R-…) in de summary van runner_finish: Monitor 2 integreert alleen een taak die uitdrukkelijk is afgerond en haar aanvraag noemt. Het aanvraag-bestand geeft zelf geen recht: Monitor 2 controleert je afronding, de werkboom en de tests, en zet bij twijfel, conflicten of beschermde bestanden niets door maar een open actie voor de gebruiker neer. Raak de map met toestemmingen en de inbox van Monitor 2, het integratiebeleid, `~/.runner2` en de Monitor 2-configuratie nooit aan en voer nooit zelf `git merge`, `reset`, `checkout`, `update-ref`, `branch -f` of `push` uit op main.
- Omzeil nooit een veiligheids- of toegangsgrens (ontbrekende rechten of credentials, destructieve of onomkeerbare acties, bestanden buiten de worktree, publieke acties): gebruik mcp__r2__runner_ask; de keten wacht dan op een mens.
- Kies niet "continue" om alleen maar door te werken: zonder concrete vooruitgang stopt de keten vanzelf. Is er niets zinnigs meer te doen, geef dan "goal_reached" met uitleg in summary.
- "goal_reached" is een bewering die Runner 2 onafhankelijk laat controleren aan de hand van de roadmap/backlog/documentatie in de repository: staat er nog uitvoerbaar werk binnen het doel open, dan wordt het geweigerd en gaat de keten door. Meld dus alleen "goal_reached" als je dat zelf aantoonbaar hebt nagelopen; een punt dat alleen een mens kan doen is geen reden om "continue" te kiezen en hoort in open_actions.
"""

SUPERVISOR_CHAIN_NOTE = """\
- Deze keten draait onder een supervisor die na jouw keten het projectdoel beoordeelt. "goal_reached" betekent: het doel van DEZE keten is bereikt en getest; de supervisor bepaalt of er nog een vervolgketen nodig is. Je hoeft dus geen werk te verzinnen om door te gaan.
- Testbeleid onder de supervisor: draai alleen gerichte tests voor de modules die je wijzigt en hun directe afhankelijkheden; geen volledige repository- of testsuite en geen brede audit tenzij je opdracht dat met een concrete reden uitdrukkelijk toestaat. De vaste veiligheidsregels en "getest" (gericht) vóór runner_finish blijven altijd gelden.
"""

GATE_AGREEMENT = """\

Voltooiingscontrole (jij bent een onafhankelijke controleur van de bewering van een ketenschakel dat het overkoepelende doel af is, geen uitvoerder):
- Je worktree staat op de eindcommit van die schakel. Je mag lezen, git-geschiedenis en diffs bekijken en gerichte tests draaien; wijzig of commit niets en maak geen nieuwe bestanden in de repository.
- Neem de bewering niet over. Lees de roadmap, backlog, projectstatus en documentatie in de repository en vergelijk die met het ketendoel en met wat er werkelijk is gebouwd.
- Beslis via mcp__r2__runner_finish met verdict: goal_complete (alleen met evidence), work_remaining (met remaining_tasks en een concrete next_step) of human_blocked (alleen als er GEEN uitvoerbaar werk meer is omdat alleen een mens verder kan; geef human_blockers en question).
- Verantwoord elk item van de BACKLOGSCAN uit de prompt in remaining_tasks (met scan_id) of in dismissed (met category en reden).
- Een niet-blokkerend punt voor een mens dwingt niets af en houdt ander uitvoerbaar werk niet tegen; meld het in open_actions. Verzin geen werk en bevestig geen doel dat niet af is.
- Push nooit en omzeil nooit een veiligheids- of toegangsgrens.
"""

JUDGE_AGREEMENT = """\

Supervisor-beoordeling (jij bent de onafhankelijke beoordelaar van een of meer afgeronde Runner 2-ketens, geen uitvoerder):
- Je worktree staat op de tip van de laatste keten. Je mag lezen, git-geschiedenis en diffs bekijken en gerichte tests draaien; wijzig of commit niets en maak geen nieuwe bestanden in de repository.
- Beoordeel op PROJECTNIVEAU of het overkoepelende projectdoel bereikt is. Controleer claims van de keten steekproefsgewijs zelf; neem samenvattingen niet blind over.
- Beslis via mcp__r2__runner_finish met decision: continue_new_chain (projectdoel nog niet bereikt; geef een concreet, zelfstandig uitvoerbaar next_goal en next_prompt), project_complete (alleen met evidence van wat je zelf controleerde), needs_human (alleen bij een echte productkeuze, ontbrekende credential of verboden/infrastructuuractie; geef question) of blocked (het project kan niet verder, met reden).
- Staan er OPENSTAANDE MENSELIJKE ACTIEPUNTEN in de prompt, sluit dan met resolve_actions (lijst van id's) de punten die nu beslist of vervallen zijn; meld nieuwe niet-blokkerende beslispunten in open_actions. Bij needs_human laat je open_actions weg.
- Maak nooit een nieuw punt aan als samenvatting of mededeling over punten die je met resolve_actions sluit of over werk dat af is; vraag niets opnieuw wat onder AL BESLOTEN staat.
- Geef chain_progress: substantive (de laatste keten leverde echte inhoudelijke vooruitgang), cosmetic (alleen kosmetische/herhalende wijzigingen) of none.
- Kies niet continue_new_chain voor cosmetische churn, nog een audit zonder concreet doel, of werk dat al gedaan is: de supervisor stopt zonder vooruitgang vanzelf. Is het doel bereikt, kies dan project_complete.
- Kwaliteitsbeleid (zie ook het KWALITEITSBELEID in je opdracht): je eigen controle is gericht (git diff, gewijzigde bestanden, gerichte tests), geen volledige suite. Bij continue_new_chain kies je chain_kind en verification: gewone ketens draaien alleen gerichte tests; een aparte review (quality, change_scoped) beperk je tot werkelijk gewijzigde bestanden en hun directe afhankelijkheden, nooit twee reviews achter elkaar; een brede controle (broad, volledige suite) alleen bij concrete brede impact (gedeelde kern/architectuur, brede migratie, release/eindmijlpaal, aanwijzing voor cross-cutting regressie, expliciete gebruikerswens) en altijd met motivering. Reviewbevindingen leiden eerst tot een gerichte reparatie (repair) met relevante hertests.
- Schrijf een vervolgopdracht nooit zo dat die een veiligheidsgrens omzeilt: pushen, integreren naar main, secrets, destructieve of infrastructuuracties blijven verboden en kunnen niet door een supervisor worden vrijgegeven.
"""


def windows_git_bash(env: dict) -> str | None:
    """Pad naar bash.exe van Git for Windows (Claude Code draait daar zijn Bash-tool en hooks in), afgeleid van git op het PATH."""
    if env.get("CLAUDE_CODE_GIT_BASH_PATH"):
        return env["CLAUDE_CODE_GIT_BASH_PATH"]
    git = shutil.which("git", path=env.get("PATH"))
    if not git:
        return None
    root = Path(git).resolve().parent.parent   # ...\Git\cmd\git.exe -> ...\Git
    for rel in ("bin/bash.exe", "usr/bin/bash.exe"):
        if (root / rel).is_file():
            return str(root / rel)
    return None


def hook_command(py: str, name: str) -> str:
    """Hookopdracht; Claude Code voert die op Windows uit in Git Bash: pad met schuine strepen en tussen aanhalingstekens."""
    if osadapter.WINDOWS:
        return f'"{py.replace(chr(92), "/")}" -m runner2.hook {name}'
    return f"{py} -m runner2.hook {name}"


def _windows_env() -> dict:
    """Windows heeft geen login-shell: het (volledige) procesmilieu is de bron; HOME/USER voor Git Bash aanvullen."""
    env = dict(os.environ)
    env.setdefault("HOME", str(Path.home()))
    env.setdefault("USER", env.get("USERNAME", ""))
    return {k: v for k, v in env.items() if k not in WINDOWS_DROP_EXACT}


def build_env(cfg: dict, state: dict, turn: int) -> dict:
    """Bouwt de omgeving expliciet op; erft niets van het startende proces."""
    home = os.environ.get("HOME", str(Path.home()))
    user = os.environ.get("USER", "")
    base = {"HOME": home, "USER": user, "LOGNAME": user, "SHELL": os.environ.get("SHELL", osadapter.DEFAULT_SHELL)}
    if osadapter.WINDOWS:
        env = _windows_env()
    elif cfg["login_shell_env"]:
        r = subprocess.run([base["SHELL"], "-l", "-c", "env -0"], env=base, capture_output=True, timeout=10)
        env = dict(kv.split("=", 1) for kv in r.stdout.decode(errors="replace").split("\0") if "=" in kv)
    else:
        env = dict(base, PATH=os.environ["PATH"])
    env = {k: v for k, v in env.items()
           if k not in DROP_EXACT and not k.startswith(DROP_PREFIXES)}
    tdir = store.task_dir(state["task_id"])
    if state.get("gate_role") == "gate":
        env["R2_GATE"] = "1"
    elif state.get("chain_id"):
        env["R2_CHAIN_ID"] = state["chain_id"]
    if state.get("supervisor_role") == "judge":
        env["R2_JUDGE"] = "1"
    env.update({
        "DISABLE_AUTOUPDATER": "1",
        "R2_TASK_ID": state["task_id"], "R2_TASK_DIR": str(tdir), "R2_TURN": str(turn),
        "R2_HOME": os.environ.get("R2_HOME", ""),
        "BASH_DEFAULT_TIMEOUT_MS": str(cfg["bash_default_timeout_ms"]),
        "BASH_MAX_TIMEOUT_MS": str(cfg["bash_max_timeout_ms"]),
        "PYTHONPATH": str(Path(__file__).resolve().parent.parent),
    })
    if osadapter.WINDOWS:
        env["PYTHONUTF8"] = "1"
    env.update({k: str(v) for k, v in cfg["env"].items()})
    if osadapter.WINDOWS and (bash := windows_git_bash(env)):
        env["CLAUDE_CODE_GIT_BASH_PATH"] = bash
    return env


def claude_binary(cfg: dict, env: dict) -> str:
    return cfg["claude_path"] or shutil.which("claude", path=env.get("PATH")) or "claude"


def executable_version(binary: str, env: dict) -> str:
    r = subprocess.run([binary, "--version"], env=env, capture_output=True, text=True, timeout=30)
    return (r.stdout.split() or ["?"])[0]


def write_session_files(cfg: dict, state: dict) -> dict:
    """Werkafspraak, MCP-config en hook-instellingen in de taakmap; geeft de paden terug."""
    d = store.task_dir(state["task_id"])
    py = sys.executable
    text = AGREEMENT
    if state.get("gate_role") == "gate":
        text += GATE_AGREEMENT
    elif state.get("supervisor_role") == "judge":
        text += JUDGE_AGREEMENT
    elif state.get("chain_id"):
        text += CHAIN_AGREEMENT + (SUPERVISOR_CHAIN_NOTE if state.get("supervisor_id") else "")
    (d / "werkafspraken.md").write_text(text, encoding="utf-8")
    mcp_env = {"PYTHONPATH": str(Path(__file__).resolve().parent.parent)}
    if osadapter.WINDOWS:
        mcp_env["PYTHONUTF8"] = "1"
    (d / "mcp.json").write_text(json.dumps({"mcpServers": {"r2": {
        "command": py, "args": ["-m", "runner2.sessiontools"], "env": mcp_env}}}))
    hook = lambda name: {"hooks": [{"type": "command", "command": hook_command(py, name)}]}
    (d / "settings.json").write_text(json.dumps({"hooks": {"Stop": [hook("stop")], "SessionStart": [hook("start")], "PreToolUse": [{"matcher": "Bash", **hook("pretool")}]}}))
    return {"agreement": d / "werkafspraken.md", "mcp": d / "mcp.json", "settings": d / "settings.json"}


def build_cmd(cfg: dict, state: dict, binary: str, files: dict, resume: bool) -> list:
    if cfg.get("allow_unattended_execution") is not True:
        raise ValueError("Unattended execution has not been enabled.")
    return [binary, "-p", "--output-format", "stream-json", "--verbose",
            "--resume" if resume else "--session-id", state["session_id"],
            "--model", state["model"], "--effort", cfg["effort"],
            "--permission-mode", "bypassPermissions", "--permission-prompts", "none",
            "--append-system-prompt-file", str(files["agreement"]),
            "--mcp-config", str(files["mcp"]), "--settings", str(files["settings"])]


def check_init(ev: dict, expected_version: str) -> list:
    """Harde signalen tegen --bare/--simple en versiedrift; lege lijst = in orde."""
    bad = []
    if ev.get("claude_code_version") != expected_version:
        bad.append(f"claude_code_version {ev.get('claude_code_version')!r}, verwacht {expected_version!r}")
    if not (ev.get("memory_paths") or {}).get("auto"):
        bad.append("memory_paths.auto ontbreekt")
    if not any(s.get("name") == "r2" and s.get("status") == "connected" for s in ev.get("mcp_servers") or []):
        bad.append("MCP-server r2 niet connected")
    tools = ev.get("tools") or []
    if len(tools) < 20 or not ({"Agent", "Task"} & set(tools)):
        bad.append(f"normale toolset ontbreekt ({len(tools)} tools, geen Agent)")
    if ev.get("permissionMode") != "bypassPermissions":
        bad.append(f"permissionMode {ev.get('permissionMode')!r}")
    return bad


# --- hervatberichten ---

def build_prompt(state: dict, orphan_report: list) -> str:
    note, parts = state.get("resume_note"), []
    if note:
        kind = note["kind"]
        if kind == "interrupted":
            tools = ", ".join(note.get("tools") or []) or "onbekend"
            parts.append(
                "Je vorige beurt werd onderbroken (het Claude-proces is beëindigd). "
                f"Actieve toolaanroep(en): {tools}. "
                + (" ".join(orphan_report) + " " if orphan_report else "")
                + "Controleer de git- en worktree-status zelf en ga verder vanaf de bestaande toestand; "
                "begin niet opnieuw.")
        elif kind in ("api_error", "auth_error"):
            parts.append("De verbinding/modelcall werd onderbroken. "
                         "Ga verder met het laatste bericht dat al in deze conversatie staat.")
        elif kind == "paused":
            parts.append("Je werd gepauzeerd. Ga verder waar je was.")
    parts.extend(state["in_flight"])
    return "\n\n".join(parts)


# --- transcript ---

def find_transcript(cfg: dict, session_id: str) -> Path | None:
    hits = list(Path(cfg["projects_dir"]).glob(f"*/{session_id}.jsonl"))
    return hits[0] if hits else None


def _truncate_to_newline(path: Path) -> None:
    data = path.read_bytes()
    cut = data.rfind(b"\n") + 1
    if cut != len(data):
        with open(path, "r+b") as f:
            f.truncate(cut)
            os.fsync(f.fileno())


def backup_transcript(cfg: dict, state: dict) -> str | None:
    src = find_transcript(cfg, state["session_id"])
    if not src:
        return None
    dst = store.task_dir(state["task_id"]) / "transcript.backup.jsonl"
    tmp = dst.with_name(dst.name + ".tmp")
    shutil.copyfile(src, tmp)
    _truncate_to_newline(tmp)
    with open(tmp, "r+b") as f:   # fsync vraagt een schrijfbare handle (Windows: EBADF op "rb")
        os.fsync(f.fileno())
    replace(tmp, dst)
    state["transcript_path"] = str(src)
    return str(dst)


def restore_transcript(cfg: dict, state: dict) -> bool:
    """Zet de back-up terug op de oorspronkelijke plek als het transcript weg is; daarna --resume <id>."""
    backup = store.task_dir(state["task_id"]) / "transcript.backup.jsonl"
    if find_transcript(cfg, state["session_id"]) or not backup.exists() or not state.get("transcript_path"):
        return False
    dst = Path(state["transcript_path"])
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(backup, dst)
    _truncate_to_newline(dst)
    return True
