"""Trusted per-node workflow settings; never inferred from a task's prompt."""
import shlex
from pathlib import Path


def for_job(job, cfg):
    workflows = cfg.get('jobs_integration_workflows') or {}
    if not isinstance(workflows, dict):
        raise ValueError('jobs_integration_workflows moet een mapping zijn')
    entries = workflows.get(job['node'], {})
    if not isinstance(entries, dict):
        raise ValueError('workflow per node moet een repositorymapping zijn')
    for repo, value in entries.items():
        if not isinstance(repo, str) or not Path(repo).is_absolute() or not isinstance(value, dict):
            raise ValueError('workflowrepository moet absoluut en expliciet zijn')
        if any(not isinstance(value.get(k), str) or not Path(value[k]).is_absolute() for k in ('request_cli', 'inbox')):
            raise ValueError('workflow request_cli en inbox moeten absoluut zijn')
    if job.get('continuation') or job.get('supervisor'):
        return None
    value = entries.get(job['repo'])
    if entries and value is None:
        raise ValueError('Gebruik een exact geconfigureerd absoluut repositorypad voor deze automatische keten; kies anders eerst een projectprofiel')
    return {'repo': job['repo'], **value} if value else None


def instructions(job, workflow):
    cli, repo, inbox = (shlex.quote(workflow[k]) for k in ('request_cli', 'repo', 'inbox'))
    return (f"\n\nAutomatische integratieketen — taak {job['task_id']}. "
            f"Werk uitsluitend op branch r2/{job['task_id']} in de toegewezen worktree. "
            "Voer gerichte tests uit en commit al het taakwerk. Dien daarna een ff-only aanvraag in via "
            f"python3 {cli} --inbox {inbox} request --repo {repo} --branch r2/{job['task_id']} "
            "--mode ff-only --test PAD_VAN_ECHT_GERICHT_TESTBESTAND (herhaal --test waar nodig). "
            "Vermeld het ontvangen R-aanvraagnummer letterlijk in runner_finish.summary. "
            "Bij een geweigerde aanvraag rapporteer je de concrete oorzaak; verzin geen aanvraagnummer. "
            "De bestaande integrator voert integratie en nacontroles uit. Geen main-wijziging, grant, check-ok, push of productuitrol uitvoeren.")
