"""Ontvangende kant van het node-transport: één controllerhandeling per aanroep, JSON in via stdin, JSON uit via stdout.

Draait op de node zelf (bijv. via ssh) en gebruikt dezelfde tooldefinities en controllerfuncties als de externe MCP.
"""
import json
import sys

from . import controller, controller_mcp


def handle(req: dict) -> dict:
    name, args = req.get("tool", ""), req.get("args") or {}
    if name == "gerard_scoped_jobs":
        from . import gerard_queue as scoped
        try:
            args = dict(args)
            source = args.pop("source", None)
            if source not in ("monitor", "chatgpt") or "by" in args:
                raise ValueError("onbekende bron of identiteit")
            if args.get("action") == "set_prompt":
                if source != "monitor" or set(args) != {"action","job_id","prompt","old_sha256"}:
                    raise ValueError("ongeldig tekstwijzigingsverzoek")
                return {"ok": True, "result": scoped.edit_prompt({k:v for k,v in args.items() if k!="action"})}
            controller_mcp.validate(controller_mcp._BY_NAME["runner2_jobs"], args)
            action = args.pop("action")
            result = scoped.act(action, args, "Gerard", channel="chatgpt" if source == "chatgpt" else "lokaal")
            return {"ok":True,"result":result}
        except Exception as exc:
            return {"ok":False,"error":getattr(exc,"code","access_denied"),"message":str(exc)[:200]}
    if name == "guardian_node_services":
        from . import guardian_node
        action = args.get("action")
        if action == "integrator_status" and set(args) == {"action"}:
            from pathlib import Path
            path = Path.home()/".config/studioobf-integrator/gerard-integrator-status.json"
            if path.is_symlink() or not path.is_file() or path.stat().st_size > 1048576:
                return {"ok": False, "error": "unavailable", "message": "integrator status unavailable"}
            status = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(status, dict):
                return {"ok": False, "error": "invalid_input", "message": "invalid integrator status"}
            return {"ok": True, "result": status}
        if action == "services" and set(args) == {"action"}:
            return {"ok": True, "result": guardian_node.dispatch(args)}
        if (action == "restart_service" and set(args) == {"action", "unit", "expected"}
                and args.get("unit") in guardian_node.UNITS and isinstance(args.get("expected"), dict)):
            return {"ok": True, "result": guardian_node.dispatch(args)}
        return {"ok": False, "error": "invalid_input", "message": "Unsupported service operation"}
    if name == "guardian_event":
        # Private NUC signal via the existing restricted SSH gate; no caller instructions execute.
        from .guardian_events import receive
        return {"ok": True, "result": receive(args)}
    if name == "integration_completion_proof":
        from .integration_check import verify
        return {"ok": True, "result": verify(args)}
    if name == 'runner2_discussion_execution':
        # Private worker transport; deliberately absent from the public MCP tool list.
        try:
            return {'ok': True, 'result': controller_mcp.dispatch(name, args, fanout=False)}
        except controller.ControllerError as exc:
            return {'ok': False, 'error': exc.code, 'message': exc.message}
    if name == "bundle":
        return {"ok": True, "result": controller.bundle()}
    controller_mcp._channel.name = "rpc"   # node-transport (o.a. Monitor 2 op een andere machine), niet ChatGPT
    try:
        text, is_error = controller_mcp.call_tool(name, args, fanout=False)
    finally:
        controller_mcp._channel.name = None
    body = json.loads(text)
    return {"ok": False, "error": body.get("error"), "message": body.get("message"), "data": body.get("data")} if is_error else {"ok": True, "result": body}


def main() -> int:
    try:
        reply = handle(json.loads(sys.stdin.buffer.read().decode("utf-8")))
    except Exception as e:  # noqa: BLE001 -- de aanroeper krijgt altijd een JSON-antwoord
        reply = {"ok": False, "error": "runner2_unavailable", "message": f"{type(e).__name__}: {e}"}
    # Bytes i.p.v. tekst: altijd UTF-8 en LF, ook op Windows (daar is de standaard codepagina en CRLF-vertaling anders).
    sys.stdout.buffer.write((json.dumps(reply, ensure_ascii=False) + "\n").encode("utf-8"))
    sys.stdout.buffer.flush()
    return 0


if __name__ == "__main__":
    sys.exit(main())

