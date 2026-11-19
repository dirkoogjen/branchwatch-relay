"""Minimale stdio-MCP-server met de twee sessietools: runner_finish en runner_ask."""
import json
import os
import sys
from pathlib import Path

from . import actions, gate, quality, store, supervisor

STR = {"type": "string"}
LIST = {"type": "array", "items": STR}
OPEN_ACTIONS = {"type": "array", "description":
                "niet-blokkerende punten die een mens nog moet beslissen of doen terwijl jouw werk wel af is of doorgaat "
                "(max 10). Geschreven voor een niet-programmerende gebruiker: text = één korte vraag/opdracht (1-2 zinnen, gewoon "
                "Nederlands, geen jargon, geen hashes/branches/ids/H1-kenmerken); reason en details = technische toelichting (alleen onder "
                "'Details' zichtbaar); topic = stabiele slug van de vraag. Alleen echte beslis-/actiepunten van een mens: geen verslag, geen "
                "technische stap die jij zelf kunt doen, niets wat al beantwoord of al open is (zie BESLOTEN/AL OPEN in de prompt). "
                "Een punt waarop je NIET kunt doorwerken is geen open_action maar een vraag via runner_ask.",
                "items": {"type": "object", "required": ["text"], "properties": {
                    "text": {**STR, "description": "korte gebruikersvraag, 1-2 zinnen"},
                    "reason": {**STR, "description": "waarom een mens dit moet beslissen (technisch mag)"},
                    "details": {**STR, "description": "technische context, optioneel"},
                    "topic": {**STR, "description": "stabiele korte slug van deze vraag, bv. media-map"},
                    "new_info": {**STR, "description": "alleen als er een nieuwe omstandigheid is die een eerder besluit over deze topic niet dekt"}}}}
TOOLS = [
    {"name": "runner_finish",
     "description": "Meld dat de opdracht volledig klaar en getest is. Beëindig daarna je beurt.",
     "inputSchema": {"type": "object", "required": ["summary"], "properties": {
         "summary": STR, "changes": LIST, "tests_run": LIST, "test_results": STR, "attention": LIST, "open_actions": OPEN_ACTIONS,
         "chain_status": {"type": "string", "enum": ["continue", "goal_reached"],
                          "description": "alleen in een autonome keten (verplicht daar): goal_reached of continue"},
         "next_step": {"type": "string", "description": "alleen in een autonome keten: concrete volgende schakel (verplicht bij continue)"}}}},
    {"name": "runner_ask",
     "description": "Stel een vraag aan de mens, alleen als informatie of een beslissing werkelijk ontbreekt. "
                    "Beëindig daarna je beurt; het antwoord komt als volgend bericht.",
     "inputSchema": {"type": "object", "required": ["question", "why_needed"], "properties": {
         "question": STR, "why_needed": STR,
         "kind": {"type": "string", "enum": ["decision", "permission", "information", "credentials"]},
         "recommendation": STR, "alternatives": LIST}}},
]

JUDGE_FINISH = {
    "name": "runner_finish",
    "description": "Geef je beoordeling van de afgeronde keten(s) op projectniveau. Beëindig daarna je beurt.",
    "inputSchema": {"type": "object", "required": ["decision", "reason", "chain_progress"], "properties": {
        "decision": {"type": "string", "enum": list(supervisor.DECISIONS),
                     "description": "continue_new_chain | project_complete | needs_human | blocked"},
        "reason": {"type": "string", "description": "onderbouwing van je beslissing"},
        "chain_progress": {"type": "string", "enum": list(supervisor.PROGRESS),
                           "description": "echte inhoudelijke vooruitgang van de laatste keten?"},
        "next_goal": {"type": "string", "description": "verplicht bij continue_new_chain: concreet doel van de vervolgketen"},
        "next_prompt": {"type": "string", "description": "verplicht bij continue_new_chain: volledige opdracht voor de vervolgketen"},
        "chain_kind": {"type": "string", "enum": list(quality.KINDS),
                       "description": "alleen bij continue_new_chain; standaard feature. quality = aparte, change-scoped review-mijlpaal "
                                      "(nooit twee achter elkaar); repair = gerichte reparatie van reviewbevindingen (vraagt findings)"},
        "verification": {"type": "object", "description":
                         "alleen bij continue_new_chain: controleoppervlak van de volgende keten. Standaard targeted (gerichte tests). "
                         "quality: scope change_scoped + targets (gewijzigde bestanden) + rationale. broad (volledige suite) alleen met een "
                         "concrete brede impact in risk_factors en een rationale.",
                         "properties": {
                             "scope": {"type": "string", "enum": list(quality.SCOPES)},
                             "rationale": STR,
                             "targets": {**LIST, "description": "concrete gewijzigde bestanden/modules/tests"},
                             "dependencies": {**LIST, "description": "ongewijzigde afhankelijkheden die toch mee moeten (vraagt dependency_rationale)"},
                             "dependency_rationale": STR,
                             "risk_factors": {"type": "array", "items": {"type": "string", "enum": list(quality.RISKS)}}}},
        "findings": {**LIST, "description": "verplicht bij chain_kind repair: de concrete reviewbevindingen"},
        "evidence": {**LIST, "description": "verplicht bij project_complete: wat je zelf controleerde"},
        "question": {"type": "string", "description": "verplicht bij needs_human: de concrete vraag aan de mens"},
        "open_actions": OPEN_ACTIONS,
        "resolve_actions": {**LIST, "description": "id's van openstaande menselijke actiepunten (zie de prompt) die met deze beoordeling zijn beslist of vervallen"},
        "summary": STR}}}


GATE_FINISH = gate.finish_tool(OPEN_ACTIONS)


def tools() -> list:
    if os.environ.get("R2_GATE"):
        return [GATE_FINISH, TOOLS[1]]
    return [JUDGE_FINISH, TOOLS[1]] if os.environ.get("R2_JUDGE") else TOOLS


def _gate_finish(tdir: Path, turn: int, args: dict) -> tuple[str, bool]:
    st = store.load(tdir.name) if (tdir / "state.json").exists() else {}
    if actions.validate(args.get("open_actions")):
        return f"Ongeldige open_actions: {actions.validate(args.get('open_actions'))}. Roep runner_finish opnieuw aan.", True
    err = gate.validate_decision(args, st.get("gate_scan_ids") or [])
    if err:
        return f"Ongeldig oordeel: {err}. Roep runner_finish opnieuw aan.", True
    if args["verdict"] != "human_blocked":
        e = store.record_signal(tdir, turn, "finish", args)
        return (e, True) if e else ("Oordeel geregistreerd. Beëindig je beurt.", False)
    # human_blocked wordt een gewone vraag: de taak gaat naar waiting_for_human en een antwoord hervat dezelfde conversatie.
    ask = {"question": args["question"], "why_needed": args["reason"], "kind": "decision", "gate_decision": "human_blocked",
           "human_blockers": [str(b)[:300] for b in args["human_blockers"] if isinstance(b, str)][:10]}
    e = store.record_signal(tdir, turn, "ask", ask)
    return (e, True) if e else ("Vraag aan de mens geregistreerd; de keten wacht. Beëindig je beurt; het antwoord komt als volgend bericht.", False)


def _judge_finish(tdir: Path, turn: int, args: dict) -> tuple[str, bool]:
    err = supervisor.validate_decision(args, supervisor.for_task(store.load(tdir.name)) if (tdir / "state.json").exists() else None)
    if err:
        return f"Ongeldige beoordeling: {err}. Roep runner_finish opnieuw aan.", True
    if args["decision"] != "needs_human":
        e = store.record_signal(tdir, turn, "finish", args)
        return (e, True) if e else ("Beoordeling geregistreerd. Beëindig je beurt.", False)
    # needs_human wordt een gewone vraag: de taak gaat naar waiting_for_human en een antwoord hervat dezelfde conversatie.
    ask = {"question": args["question"], "why_needed": args["reason"], "kind": "decision",
           "supervisor_decision": "needs_human", "chain_progress": args["chain_progress"]}
    e = store.record_signal(tdir, turn, "ask", ask)
    return (e, True) if e else ("Vraag aan de mens geregistreerd; de supervisor wacht. Beëindig je beurt; het antwoord komt als volgend bericht.", False)


def call_tool(name: str, args: dict) -> tuple[str, bool]:
    """Geeft (tekst, is_error). Werkt op R2_TASK_DIR en R2_TURN uit de omgeving."""
    tdir, turn = Path(os.environ["R2_TASK_DIR"]), int(os.environ.get("R2_TURN", "0"))
    tool = next((t for t in tools() if t["name"] == name), None)
    if not tool:
        return f"Onbekende tool {name}", True
    if name == "runner_finish" and os.environ.get("R2_GATE"):
        return _gate_finish(tdir, turn, args)
    if name == "runner_finish" and os.environ.get("R2_JUDGE"):
        return _judge_finish(tdir, turn, args)
    missing = [k for k in tool["inputSchema"]["required"] if not args.get(k)]
    if missing:
        return f"Ontbrekende velden: {', '.join(missing)}", True
    if name == "runner_finish" and actions.validate(args.get("open_actions")):
        return f"Ongeldige open_actions: {actions.validate(args.get('open_actions'))}. Roep runner_finish opnieuw aan.", True
    if name == "runner_finish" and os.environ.get("R2_CHAIN_ID"):
        if args.get("chain_status") not in ("continue", "goal_reached"):
            return "Deze taak zit in een autonome keten: geef chain_status ('continue' of 'goal_reached') mee en roep runner_finish opnieuw aan.", True
        if args["chain_status"] == "continue" and not str(args.get("next_step") or "").strip():
            return "Bij chain_status 'continue' is next_step verplicht: beschrijf de volgende schakel concreet en roep runner_finish opnieuw aan.", True
    err = store.record_signal(tdir, turn, "finish" if name == "runner_finish" else "ask", args)
    if err:
        return err, True
    if name == "runner_finish":
        return "Afronding geregistreerd. Beëindig je beurt.", False
    return "Vraag geregistreerd. Beëindig je beurt; het antwoord komt als volgend bericht.", False


def handle(msg: dict) -> dict | None:
    method, mid = msg.get("method"), msg.get("id")
    if mid is None:
        return None  # notificatie
    if method == "initialize":
        result = {"protocolVersion": msg["params"].get("protocolVersion", "2024-11-05"),
                  "capabilities": {"tools": {}}, "serverInfo": {"name": "r2", "version": "1"}}
    elif method == "tools/list":
        result = {"tools": tools()}
    elif method == "tools/call":
        text, is_err = call_tool(msg["params"]["name"], msg["params"].get("arguments") or {})
        result = {"content": [{"type": "text", "text": text}], "isError": is_err}
    elif method == "ping":
        result = {}
    else:
        return {"jsonrpc": "2.0", "id": mid, "error": {"code": -32601, "message": "method not found"}}
    return {"jsonrpc": "2.0", "id": mid, "result": result}


def main() -> None:
    for line in sys.stdin:
        if not line.strip():
            continue
        reply = handle(json.loads(line))
        if reply:
            sys.stdout.write(json.dumps(reply) + "\n")
            sys.stdout.flush()


if __name__ == "__main__":
    main()
