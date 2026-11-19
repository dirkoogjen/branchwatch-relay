"""r2 run | send | status | pause | stop"""
import argparse
import json
import os
import sys
import time
from pathlib import Path

from . import chains, controller, store, supervisor, taskproc
from .procs import alive


def _wait(tid: str) -> None:
    time.sleep(1)
    while True:
        s = store.load(tid)
        if s["status"] != "running" and store.MachineLock("-")._holder() is None:
            return
        time.sleep(1)


def _summary(s: dict) -> str:
    claude = alive(s["claude_pid"], s["claude_pid_start"]) if s["claude_pid"] else False
    started = time.mktime(time.strptime(s["created"], "%Y-%m-%dT%H:%M:%S%z"))
    lines = [f"{s['task_id']}  {s['status']}  beurt {s['turn']}  verstreken {int(time.time() - started)} s",
             f"  repo {s['repo']}  worktree {s['worktree']}",
             f"  claude actief: {'ja (pid %s)' % s['claude_pid'] if claude else 'nee'}  session {s['session_id']}"]
    if s["question"]:
        q = s["question"]
        lines.append(f"  vraag [{q.get('kind', '?')}]: {q.get('question')}  ({q.get('why_needed', '')})")
    if s["finish"]:
        lines.append(f"  finish: {s['finish'].get('summary')}")
    if s["failure"]:
        lines.append(f"  fout [{s['failure']['kind']}]: {s['failure']['message']}")
    if s["last_result"]:
        lines.append(f"  laatste result: is_error={s['last_result']['is_error']} {s['last_result']['result'][:120]!r}")
    if ch := chains.for_task(s):
        d = chains.describe(ch, s["task_id"])
        lines.append(f"  keten {d['id']} {d['state']} ({d['phase']}) schakel {d['link']}/{d['max_tasks']}"
                     + (f"  gestopt: {d['stopped_because']['kind']} - {d['stopped_because']['message']}" if d.get("stopped_because") else ""))
        if (g := d.get("completion_gate")) and g.get("last"):
            lines.append(f"  voltooiingscontrole: {g['last']['summary']}")
    if sup := supervisor.for_task(s):
        d = supervisor.describe(sup, s["task_id"])
        lines.append(f"  supervisor {d['id']} {d['state']} ({d['phase']}) keten {d['chain']}/{d['max_chains']}"
                     + (f"  laatste beslissing: {d['last_decision']['decision']}" if d.get("last_decision") else "")
                     + (f"  gestopt: {d['stopped_because']['kind']} - {d['stopped_because']['message']}" if d.get("stopped_because") else ""))
    procs = s["children"] + s["orphans"]
    if procs:
        lines.append("  processen: " + "; ".join(f"{p['pid']} {p['cmd'][:50]}" for p in procs[:6]))
    if s["integration"]:
        i = s["integration"]
        lines.append(f"  integratie (droog): fast-forward={i.get('fast_forward')} conflicten={i.get('conflicts')} "
                     f"commits={i.get('commits_ahead')} schoon={i.get('worktree_clean')}")
    return "\n".join(lines)


def _call(fn, *args, **kw):
    try:
        return fn(*args, **kw)
    except controller.ControllerError as e:
        print(e.message, file=sys.stderr)
        return None


def cmd_run(a) -> int:
    prompt = Path(a.prompt_file).expanduser().read_text() if a.prompt_file else a.prompt
    r = _call(controller.start, a.repo, prompt, model=a.model, continuation=a.continuation, goal=a.goal,
              max_tasks=a.max_tasks, max_hours=a.max_hours, max_no_progress=a.max_no_progress,
              supervisor=a.supervisor, project_goal=a.project_goal, max_chains=a.max_chains,
              supervisor_max_hours=a.supervisor_max_hours, supervisor_max_no_progress=a.supervisor_max_no_progress,
              status_files=a.status_file or None, max_quality_chains=a.max_quality_chains,
              full_verification=True if a.full_verification else None)
    if r is None:
        return 3
    print(f"task {r['task_id']}\nworktree {r['worktree']}\nsession {r['session_id']}\nstatus {r['status']}")
    if r.get("chain"):
        print(f"keten {r['chain']['id']} (max {r['chain']['max_tasks']} schakels)")
    if r.get("supervisor"):
        print(f"supervisor {r['supervisor']['id']} (max {r['supervisor']['max_chains']} ketens, "
              f"{r['supervisor']['max_hours']} u, deadline {r['supervisor']['deadline']})")
    if a.wait:
        _wait(r["task_id"])
        print(_summary(store.load(r["task_id"])))
    return 0


def cmd_send(a) -> int:
    s = store.load(a.task)
    msg = Path(a.file).read_text() if a.file else " ".join(a.message)
    if not msg and s["status"] in ("waiting_for_human", "completed"):
        print("een bericht is nodig (antwoord of vervolgopdracht)", file=sys.stderr)
        return 2
    if s["status"] == "stopped":
        print("taak is gestopt", file=sys.stderr)
        return 2
    if busy := controller.busy():
        print(f"taak {busy['task_id']} draait al (pid {busy['pid']}); één actieve taak per machine", file=sys.stderr)
        return 3
    store.update(a.task, inbox=s["inbox"] + ([msg] if msg else []))
    taskproc.spawn(a.task)
    print(f"{a.task} beurt gestart")
    if a.wait:
        _wait(a.task)
        print(_summary(store.load(a.task)))
    return 0


def cmd_status(a) -> int:
    for s in [store.load(a.task)] if a.task else store.all_tasks():
        print(_summary(s))
    return 0


def cmd_pause(a) -> int:
    r = _call(controller.pause, a.task)
    if r is None:
        return 2
    print("pauze aangevraagd (SIGINT volgt bij de volgende controle)")
    return 0


def cmd_stop(a) -> int:
    r = _call(controller.stop, a.task, a.reason or "")
    if r is None:
        return 2
    print(f"{a.task} {r['status']}" + (f" ({r['processes_stopped']} processen gestopt)" if r["changed"] else f" ({r['note']})"))
    return 0


def cmd_health(a) -> int:
    h = controller.health()
    print(f"watch: {'gezond' if h['watch']['healthy'] else 'NIET gezond'} (laatste scan {h['watch']['last_scan_s_ago']} s geleden)")
    print(f"controller-MCP: {'ok, ' + str(len(h['tools'].get('names', []))) + ' tools' if h['tools']['ok'] else 'FOUT ' + str(h['tools'])}")
    print(f"tunnel (8766): {'ready' if h['tunnel']['ready'] else 'niet ready'}")
    return 0 if h["ok"] else 1


def cmd_review(a) -> int:
    """`r2 actions --review`: DRY RUN van het actiebeheer (deze node, of met --nodes alle nodes vanaf de control plane)."""
    from . import actionreview
    if a.nodes:
        r = actionreview.review_all(None if a.nodes == ["all"] else a.nodes)
    else:
        r = _call(controller.review_actions)
        if r is None:
            return 2
    if a.out:
        Path(a.out).expanduser().write_text(json.dumps(r, indent=1, ensure_ascii=False))
    print(json.dumps(r, indent=1, ensure_ascii=False) if a.json else actionreview.render(r), end="")
    return 0


def cmd_closure(a) -> int:
    """`r2 actions --closure` (rapport), `--claim A-… --job J-… [--besluit N | --technisch]` (koppelen), `--closure-undo A-… --note REDEN`."""
    from . import actionclosure
    if a.closure_undo:
        if not a.note:
            print("--closure-undo vraagt --note met de reden", file=sys.stderr)
            return 2
        try:
            r = actionclosure.undo(a.closure_undo, "cli", a.note)
        except actionclosure.ClosureError as e:
            print(f"fout ({e.code}): {e.message}", file=sys.stderr)
            return 2
        print(f"teruggezet {len(r['reopened'])}, ongewijzigd {len(r['untouched'])}, onbekend {len(r['unknown'])}")
        return 0
    if a.claim:
        if not a.job:
            print("--claim vraagt --job J-…", file=sys.stderr)
            return 2
        r = _call(controller.claim_action, a.claim, a.job, besluit=a.besluit, soort="technisch" if a.technisch else None,
                  note=a.note or "", by=f"task {os.environ['R2_TASK_ID']}" if os.environ.get("R2_TASK_ID") else "cli")
        if r is None:
            return 2
        print(f"{r['action_id']} gekoppeld aan {r['claim']['job_id']} ({'nieuw' if r['changed'] else 'al zo vastgelegd'}); dit sluit niets")
        return 0
    r = _call(controller.closure_report)
    if r is None:
        return 2
    print(json.dumps(r, indent=1, ensure_ascii=False) if a.json else actionclosure.render(r), end="")
    return 0


def cmd_reassess(a) -> int:
    """`r2 actions --reassess` (nu berekenen, bewaart niets), `--reassess-last` (laatst bewaarde rapport), `--reassess-run` (volledige ronde zoals de watch)."""
    from . import actionreassess, cplane
    try:
        if a.reassess_run:
            if not cplane.is_primary():
                print("--reassess-run draait alleen op de primaire control plane", file=sys.stderr)
                return 2
            r = actionreassess.run()
        elif a.reassess_last:
            r = _call(controller.reassess_report)
            if r is None:
                return 2
        else:
            r = actionreassess.run(persist=False)
    except actionreassess.ReassessError as e:
        print(f"fout ({e.code}): {e.message}", file=sys.stderr)
        return 2
    print(json.dumps(r, indent=1, ensure_ascii=False) if a.json else actionreassess.render(r), end="")
    return 0


def cmd_reassess_archive_repair(a) -> int:
    """`r2 reassess archief-herstel [--bron BESTAND ...]`: herstelt het archief van verwerkte events alleen als de geschiedenis aantoonbaar volledig is (manifest); anders blijft de verwerking geblokkeerd (exit 2). Geen herverwerking."""
    from . import actionreassess
    try:
        r = actionreassess.repair_archive(getattr(a, "bron", None))
    except actionreassess.ReassessError as e:
        print(f"fout ({e.code}): {e.message}", file=sys.stderr)
        return 2
    print(json.dumps(r, ensure_ascii=False))
    return 2 if r.get("geblokkeerd") else 0


def cmd_reassess_ledger_restore(a) -> int:
    """`r2 reassess grootboek-herstel --bron BACKUP`: zet een verdwenen grootboek alleen terug als de historie daarmee aantoonbaar compleet is; anders blijft de verwerking geblokkeerd (exit 2). Geen herverwerking."""
    from . import actionreassess
    try:
        r = actionreassess.restore_ledger(a.bron)
    except actionreassess.ReassessError as e:
        print(f"fout ({e.code}): {e.message}", file=sys.stderr)
        return 2
    print(json.dumps(r, ensure_ascii=False))
    return 2 if r.get("geblokkeerd") else 0


def cmd_reassess_event(a) -> int:
    """`r2 reassess event <soort> <ref>`: eventgestuurde herbeoordeling (idempotent per soort+ref; alleen primaire control plane)."""
    from . import actionreassess
    try:
        r = actionreassess.notify_event(None, a.soort, a.ref)
    except actionreassess.ReassessError as e:
        print(f"fout ({e.code}): {e.message}", file=sys.stderr)
        return 2
    if r.get("overgeslagen"):
        print("herbeoordeling per event draait alleen op de primaire control plane", file=sys.stderr)
        return 2
    if r.get("uitgesteld"):
        print(f"event {r['sleutel']} vastgelegd; de volgende ronde verwerkt het (er liep al een herbeoordeling)")
        return 0
    if r.get("al_verwerkt"):
        print(f"event {r['sleutel']} was al verwerkt in ronde {r.get('ronde')}; niets opnieuw gemeld")
        return 0
    print(json.dumps(r, indent=1, ensure_ascii=False) if a.json else actionreassess.render(r), end="")
    return 0


def cmd_actions(a) -> int:
    if a.reassess or a.reassess_last or a.reassess_run:
        if a.resolve or a.review or a.closure or a.claim or a.closure_undo:
            print("--reassess gaat niet samen met --resolve, --review, --closure, --claim of --closure-undo", file=sys.stderr)
            return 2
        return cmd_reassess(a)
    if a.closure or a.claim or a.closure_undo:
        if a.resolve or a.review:
            print("--closure, --claim en --closure-undo gaan niet samen met --resolve of --review", file=sys.stderr)
            return 2
        return cmd_closure(a)
    if a.review:
        if a.resolve:
            print("--review en --resolve gaan niet samen: de dry run sluit nooit iets", file=sys.stderr)
            return 2
        return cmd_review(a)
    if a.resolve:
        by = f"task {os.environ['R2_TASK_ID']}" if os.environ.get("R2_TASK_ID") else "cli"
        r = _call(controller.resolve_actions, a.resolve, note=a.note or "", by=by)
        if r is None:
            return 2
        print(f"gesloten {len(r['resolved'])}, al gesloten {len(r['already_resolved'])}, onbekend {len(r['unknown'])}; nog open op de node: {r['open_remaining']}")
        return 0
    r = _call(controller.list_actions, a.task, a.all)
    if r is None:
        return 2
    print(f"{r['open']} open, {r['resolved']} gesloten, {r['suppressed']} onderdrukt (scope {r['scope']})")
    for i in r["items"]:
        print(f"- {i['id']} [{i['status']}] {i['text']}" + (f"  → {i['resolved_note']}" if i.get("resolved_note") else ""))
    return 0


def cmd_jobs(a) -> int:
    """Opdrachtwachtrij zonder Monitor of ChatGPT: `r2 jobs` (overzicht), `r2 jobs add --node GERARD --repo ~/StudioOBF
    --prompt-file p.md --by Beheerder` (`--repo auto` laat Runner 2 de repository bepalen; `r2 jobs plan` toont wat er zou gebeuren), of elke handeling met `r2 jobs ACTIE --job J-... --by Beheerder [--json '{...}']`."""
    from . import cplane, jobs
    from .supervision import SupervisionError
    try:
        cplane.require_primary()   # een standby-machine neemt geen opdrachten aan (die zouden daar nooit starten)
    except SupervisionError as e:
        print(f"fout ({e.code}): {e.message}", file=sys.stderr)
        return 2
    args = json.loads(a.json) if a.json else {}
    if a.job:
        args["job_id"] = a.job
    if a.action == "add":
        args.update({k: v for k, v in (("node", a.node), ("repository", a.repo), ("title", a.title), ("model", a.model)) if v})
        if a.prompt_file:
            args["prompt"] = Path(a.prompt_file).expanduser().read_text()
        if a.after:
            args["depends_on"] = a.after
        if a.supervisor:
            args["supervisor"] = True
        if a.split:
            args["split"] = True
    elif a.action == "plan":
        args.update({k: v for k, v in (("node", a.node), ("repository", a.repo or "auto")) if v})
        if a.prompt_file:
            args["prompt"] = Path(a.prompt_file).expanduser().read_text()
    try:
        r = jobs.act(a.action, args, a.by or os.environ.get("USER", ""))
    except jobs.JobsError as e:
        print(f"fout ({e.code}): {e.message}", file=sys.stderr)
        if getattr(e, "data", None):
            print(json.dumps(e.data, ensure_ascii=False, indent=1), file=sys.stderr)
        return 2
    if a.action == "list" and "nodes" in r:
        for node, n in r["nodes"].items():
            print(f"{node}{' (gepauzeerd)' if n['paused'] else ''}: {n['counts']['active']} actief, {n['counts']['queued']} in wachtrij")
            for j in n["active"] + n["queue"]:
                print(f"  {j['id']} [{j['label']}] {j['title'][:60]}  — {j['why'][:80]}")
        return 0
    print(json.dumps(r, ensure_ascii=False, indent=1))
    return 0


def cmd_repos(a) -> int:
    """Repositorykeuze: `r2 repos` (alle nodes), `r2 repos --node GERARD`, `r2 repos --resolve "verander project.example.invalid" --node GERARD`."""
    from . import repos
    try:
        if a.resolve:
            r = repos.resolve(a.resolve, (a.node or repos.local_name()).upper())
            print(f"{r['decision']}: {r.get('message', '')}")
            for g in r.get("groups", []):
                print(f"  {g['node']}  {g['repository']}  [{', '.join(p['name'] + (' → ' + p['component'] if p['component'] else '') for p in g['projects'])}]")
            q = r.get("question")
            if q:
                print(f"  vraag: {q['text']}")
                for o in q["options"]:
                    print(f"    - {o['repository']}  ({o['label']})")
            return 0 if r["decision"] == "resolved" else 3
        res = {a.node.upper(): repos.discover(a.node, refresh=True)} if a.node else repos.catalog(refresh=True)["nodes"]
    except repos.ReposError as e:
        print(f"fout ({e.code}): {e.message}", file=sys.stderr)
        return 2
    for node, inv in res.items():
        print(f"{node} ({inv.get('checked')}){' — ' + inv['note'] if inv.get('note') else ''}")
        for r in inv.get("repos", []):
            print(f"  {r['path']}  [{', '.join(p['name'] for p in r['projects']) or 'geen project herkend'}]"
                  f"{'' if r['verified'] else '  (niet gecontroleerd)'}")
        for h in inv.get("hidden", []):
            print(f"  niet getoond: {h['path']} ({h['why']})")
    return 0


def cmd_service(a) -> int:
    from . import service
    if a.action == "install":
        print(service.install(Path(__file__).resolve().parent.parent))
    elif a.action == "uninstall":
        print(service.uninstall())
    print(service.status())
    return 0


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="r2")
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--repo", required=True)
    g = r.add_mutually_exclusive_group(required=True)
    g.add_argument("--prompt-file")
    g.add_argument("--prompt")
    r.add_argument("--model")
    r.add_argument("--wait", action="store_true")
    r.add_argument("--continuation", action="store_true", help="autonome keten (opt-in)")
    r.add_argument("--goal")
    r.add_argument("--max-tasks", type=int)
    r.add_argument("--max-hours", type=float)
    r.add_argument("--max-no-progress", type=int)
    r.add_argument("--supervisor", action="store_true", help="autopilot boven ketens (opt-in, impliceert --continuation)")
    r.add_argument("--project-goal")
    r.add_argument("--max-chains", type=int)
    r.add_argument("--supervisor-max-hours", type=float)
    r.add_argument("--supervisor-max-no-progress", type=int)
    r.add_argument("--status-file", action="append", help="project-/statusbestand dat de judge meeleest (herhaalbaar)")
    r.add_argument("--max-quality-chains", type=int, help="maximaal aantal aparte review-/kwaliteitsketens onder de supervisor (0 = geen)")
    r.add_argument("--full-verification", action="store_true", help="expliciete wens: de judge mag een volledige suite als brede controle kiezen")
    r.set_defaults(fn=cmd_run)
    s = sub.add_parser("send")
    s.add_argument("task")
    s.add_argument("message", nargs="*")
    s.add_argument("--file")
    s.add_argument("--wait", action="store_true")
    s.set_defaults(fn=cmd_send)
    st = sub.add_parser("status")
    st.add_argument("task", nargs="?")
    st.set_defaults(fn=cmd_status)
    pa = sub.add_parser("pause")
    pa.add_argument("task")
    pa.set_defaults(fn=cmd_pause)
    sp = sub.add_parser("stop")
    sp.add_argument("task")
    sp.add_argument("--reason")
    sp.set_defaults(fn=cmd_stop)
    ac = sub.add_parser("actions", help="menselijke actiepunten tonen of sluiten")
    ac.add_argument("task", nargs="?")
    ac.add_argument("--all", action="store_true", help="ook gesloten punten (de besluiten)")
    ac.add_argument("--resolve", nargs="+", metavar="ID")
    ac.add_argument("--note", help="bij --resolve: het besluit of de reden")
    ac.add_argument("--review", action="store_true", help="DRY RUN van het actiebeheer: wat zou sluiten / wat vraagt een beslissing (sluit niets)")
    ac.add_argument("--nodes", nargs="+", metavar="NODE", help="bij --review: deze nodes (of 'all') vanaf de control plane")
    ac.add_argument("--json", action="store_true", help="bij --review: volledig plan als JSON")
    ac.add_argument("--out", metavar="PAD", help="bij --review: plan ook als JSON naar dit bestand")
    ac.add_argument("--closure", action="store_true", help="rapport van de bewijsgerichte afsluiting (besluit 43; sluit niets)")
    ac.add_argument("--claim", metavar="ID", help="koppel dit open punt aan de opdracht --job die het uitvoert (sluit niets)")
    ac.add_argument("--job", metavar="J-ID", help="bij --claim: de opdracht die het punt uitvoert")
    ac.add_argument("--besluit", type=int, metavar="NR", help="bij --claim: nummer in het beslissingenregister dat dit punt noemt")
    ac.add_argument("--technisch", action="store_true", help="bij --claim: technisch vervolgpunt (geen menselijk besluit nodig)")
    ac.add_argument("--closure-undo", nargs="+", metavar="ID", help="zet door bewijs gesloten punten terug naar open en trek de claim in (vraagt --note)")
    ac.add_argument("--reassess", action="store_true", help="herbeoordeling van de open acties van alle nodes nu berekenen (besluit 46; bewaart en sluit niets)")
    ac.add_argument("--reassess-last", action="store_true", help="laatst bewaarde rapport van de periodieke herbeoordeling")
    ac.add_argument("--reassess-run", action="store_true", help="een volledige herbeoordelingsronde zoals de watch (bewaart grootboek en rapport; alleen primaire control plane; sluit niets)")
    ac.set_defaults(fn=cmd_actions)
    ra = sub.add_parser("reassess", help="eventgestuurde herbeoordeling van open acties (alleen primaire control plane; sluit niets)")
    ras = ra.add_subparsers(dest="reassess_cmd", required=True)
    rae = ras.add_parser("event", help="verwerk een gebeurtenis (idempotent per soort + ref)")
    rae.add_argument("soort")
    rae.add_argument("ref")
    rae.add_argument("--json", action="store_true")
    rae.set_defaults(fn=cmd_reassess_event)
    raa = ras.add_parser("archief-herstel", help="zonder herverwerking: herstel het archief van verwerkte events, alleen als de geschiedenis aantoonbaar volledig is")
    raa.add_argument("--bron", action="append", help="extra bestand met ontbrekende sleutels (back-up of afgezonderd bestand); herhaalbaar")
    raa.set_defaults(fn=cmd_reassess_archive_repair)
    rag = ras.add_parser("grootboek-herstel", help="zonder herverwerking: zet een verdwenen grootboek terug uit een back-up, alleen als de historie daarmee aantoonbaar compleet is")
    rag.add_argument("--bron", required=True, help="back-up van ledger.json")
    rag.set_defaults(fn=cmd_reassess_ledger_restore)
    jb = sub.add_parser("jobs", help="opdrachtwachtrij (alleen op de control plane)")
    jb.add_argument("action", nargs="?", default="list")
    jb.add_argument("--job")
    jb.add_argument("--by", help="wie dit doet (naam)")
    jb.add_argument("--node")
    jb.add_argument("--repo")
    jb.add_argument("--title")
    jb.add_argument("--model", choices=["standaard", "opus", "sonnet"])
    jb.add_argument("--prompt-file")
    jb.add_argument("--after", nargs="+", metavar="JOB", help="voorgangers")
    jb.add_argument("--supervisor", action="store_true")
    jb.add_argument("--json", help="extra velden als JSON-object")
    jb.add_argument("--split", action="store_true", help="bij --repo auto: een opdracht over meerdere repositories splitsen")
    jb.set_defaults(fn=cmd_jobs)
    rp = sub.add_parser("repos", help="repositorykeuze: gevonden repositories en projecten per node (alleen lezen)")
    rp.add_argument("--node")
    rp.add_argument("--resolve", metavar="TEKST", help="welke repository hoort bij deze opdrachttekst?")
    rp.set_defaults(fn=cmd_repos)
    sub.add_parser("health").set_defaults(fn=cmd_health)
    sv = sub.add_parser("service")
    sv.add_argument("action", choices=["install", "uninstall", "status"])
    sv.set_defaults(fn=cmd_service)
    a = p.parse_args(argv)
    return a.fn(a)
