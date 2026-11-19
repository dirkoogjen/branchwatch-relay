"""Machine-instellingen (machine.toml) en vaste paden."""
import os
import tomllib
from pathlib import Path

from . import osadapter

DEFAULT_TOML = """\
# Runner 2 machine-instellingen.
# expected_claude_version wijzigen is een bewuste beheeractie (na een korte rooktest).
expected_claude_version = ""
allow_unattended_execution = false
# claude_path = "/usr/local/bin/claude"   # leeg = zoeken op het PATH van de schone omgeving
# Modelbeleid per rol: implementatie/programmeren, zware audit, kritische review.
model_implement = "claude-sonnet-5-5"
model_audit = "claude-opus-5-5"
model_review = "claude-opus-5-5"
effort = "high"
bash_default_timeout_ms = 900000
bash_max_timeout_ms = 1800000
orphan_grace_s = 10
first_event_timeout_s = 180   # geen enkel event binnen deze tijd = hangende Claude
poll_interval_s = 2.0
login_shell_env = true
watch_poll_s = 3.0
retry_base_s = 5              # backoff: base * 2^pogingen, max retry_max_s
retry_max_s = 300
max_recovery_attempts = 5
# Niet (meer) aangemeld: geen blinde herhaling. Een verlopen login gaat meteen als vraag naar de gebruiker; alleen
# 'Not logged in' wordt op de Mac één keer via de watch herhaald (ssh kan de inlogsleutelhanger niet lezen).
# auth_retry_attempts = 1          # standaard 1 op de Mac, 0 op Linux en Windows
auth_retry_delay_s = 30
# default_node = "GERARD"                  # control plane: nieuwe opdrachten zonder node gaan hierheen (leeg = deze machine)
# Autonome ketens (alleen actief als een start expliciet continuation=true meegeeft).
chain_default_max_tasks = 5
chain_default_max_hours = 12
chain_default_max_no_progress = 2
chain_cap_max_tasks = 25      # harde bovengrens; een start kan er niet overheen
chain_cap_max_hours = 48
# Voltooiingscontrole: meldt een schakel goal_reached, dan beoordeelt een aparte controletaak onafhankelijk of het doel echt af is.
# false = oud gedrag (de bewering van de schakel is voldoende). Geldt niet voor ketens onder een supervisor (die heeft een judge).
chain_completion_gate = true
# Supervisor (autopilot boven ketens; alleen actief als een start expliciet supervisor=true meegeeft).
supervisor_default_max_chains = 4
supervisor_default_max_hours = 24
supervisor_default_max_no_progress = 2
supervisor_cap_max_chains = 12      # harde bovengrens; een start kan er niet overheen
supervisor_cap_max_hours = 72
supervisor_cap_max_no_progress = 5
# Kwaliteitsbeleid van de supervisor (risicogestuurd): aparte review-mijlpalen zijn change-scoped en begrensd,
# een volledige repository-/testsuite alleen bij concrete brede impact.
quality_default_max_chains = 2      # aparte review-/kwaliteitsketens per supervisorrun
quality_cap_max_chains = 4          # harde bovengrens; een start kan er niet overheen
quality_max_broad = 1               # brede controles (volledige suite) per run
quality_min_commits = 3             # minimale serie voor een review zonder hoog risico
quality_review_after_chains = 3     # vanaf dit aantal ketens (of commits) wijst de judge-prompt op een mijlpaal
quality_review_after_commits = 12
quality_chain_max_hours = 2         # tijdbudget van een change-scoped kwaliteitsketen
quality_chain_max_tasks = 3
quality_broad_chain_max_hours = 4   # tijdbudget van een brede kwaliteitsketen
# Externe supervisie (ChatGPT boven de runners; alleen actief voor expliciet geregistreerde projecten).
supervision_enabled = false          # kill switch: false = geen enkele supervisie-actie start nog een taak
supervision_lease_s = 900           # hoe lang een geclaimd event van de claimer is voordat een ander het mag overnemen
supervision_lease_cap_s = 3600
supervision_max_claims = 5          # na zoveel claims zonder beslissing wordt een event geëscaleerd (SUPERVISIE GEBLOKKEERD)
supervision_poll_s = 10             # hoe vaak de watch de supervisie-tik draait
supervision_default_max_assignments = 10
supervision_cap_max_assignments = 50
supervision_default_max_hours = 48
supervision_cap_max_hours = 168
supervision_default_max_no_progress = 2
supervision_cap_max_no_progress = 5
supervision_start_retry_s = 15      # wachttijd tussen startpogingen van een geplande opdracht (node bezet/onbereikbaar)
supervision_max_start_failures = 6  # echte startfouten (geen "bezet") voordat een opdracht als mislukt telt
supervision_planned_max_h = 6       # een geplande opdracht die zolang niet gestart kan worden telt als mislukt
supervision_webhook_url = ""        # optioneel: wekt een always-on trigger (https, of http op localhost); leeg = uit
supervision_webhook_secret_env = "" # NAAM van de omgevingsvariabele met het HMAC-geheim (nooit het geheim zelf hier)
supervision_webhook_timeout_s = 5
supervision_webhook_max_attempts = 8
# Opdrachtwachtrij (Monitor 2 / runner2_jobs; draait alleen op de primaire control plane).
jobs_enabled = false                 # kill switch: false = de wachtrij start niets meer (bediening en overzicht blijven werken)
jobs_poll_s = 10                    # hoe vaak de watch de wachtrij-tik draait
chains_on_standby = false           # true = ook op een expliciete standby-machine ketens/supervisors doorschakelen (normaal nooit: split-brain-hek)
jobs_start_retry_s = 20             # wachttijd tussen startpogingen (Runner bezet of onbereikbaar)
jobs_max_start_failures = 6         # echte startfouten (geen "bezet") voordat een opdracht als mislukt telt
jobs_model_opus = "claude-opus-5-5"
jobs_model_sonnet = "claude-sonnet-5-5"
jobs_approvers = []   # alleen deze namen kunnen goedkeuren/weigeren (en nooit via het ChatGPT-kanaal)
jobs_missing_max = 3                # zoveel rondes "taak niet gevonden" achter elkaar = opdracht mislukt (de Runner komt vrij)
jobs_created_grace_s = 90           # een taak die zo lang op 'created' blijft, wordt idempotent opnieuw gestart
jobs_orphan_watch_s = 900           # na annuleren tijdens het starten: zo lang nog bewaken of de taak alsnog verschijnt
# Actiebeheer (open actiepunten nalopen). Fase 1: alleen DRY RUN-rapporten; automatisch sluiten vraagt aparte toestemming.
action_review_interval_h = 0        # 0 = uit; bijv. 24 = de watch bewaart dagelijks een DRY RUN-rapport (sluit niets)
action_review_apply = false         # automatisch sluiten; blijft false tot de gebruiker daar afzonderlijk toestemming voor geeft
action_review_max_close = 25        # stroomonderbreker: meer sluitingen in één ronde = niets sluiten
action_review_max_fraction = 0.5   # ... of meer dan dit deel van de open punten
action_closure_apply = false         # bewijsgerichte afsluiting (besluit 43): sluit alleen punten met een claim EN sluitend bewijs; false = alleen rapport
action_closure_interval_s = 120     # hoogstens elke zoveel seconden een ronde in de watch (0 = uit)
action_closure_max = 10             # stroomonderbreker: meer sluitingen in één ronde = niets sluiten
action_register_directory = ""     # canoniek beslissingenregister; leeg = meegeleverde momentopname
# Periodieke herbeoordeling van de open acties (besluit 46): alleen lezen en rapporteren; sluit niets, zet niets in de wachtrij, geen betaalde dienst.
action_reassess_interval_h = 0     # herbeoordeling aan/uit en terugvalinterval in uren (0 = uit); besluit 47: eenmaal per dag
action_reassess_daily_at = "05:00"  # vast dagelijks moment (HH:MM, lokale tijd van de primaire control plane; GERARD = UTC); leeg = alleen het interval
action_reassess_nodes = ["GERARD", "MAC", "LINUX"]   # leeg = alle geconfigureerde nodes
action_reassess_on_events = false  # eventgestuurde herbeoordeling vanuit afronding/controle/integratie/besluit (uitrol = bewust aanzetten; alleen primary)
action_reassess_queue_reserve = 30  # een voorbereid voorstel wacht als er minder dan zoveel plaatsen onder het wachtrijmaximum (300) vrij zijn
"""

DEFAULTS = {
    "expected_claude_version": "",
    "allow_unattended_execution": False,
    "claude_path": "",
    "model_implement": "claude-sonnet-5-5",
    "model_audit": "claude-opus-5-5",
    "model_review": "claude-opus-5-5",
    "effort": "high",
    "bash_default_timeout_ms": 900000,
    "bash_max_timeout_ms": 1800000,
    "orphan_grace_s": 10,
    "watch_poll_s": 3.0,
    "retry_base_s": 5,
    "retry_max_s": 300,
    "max_recovery_attempts": 5,
    "auth_retry_attempts": 0 if (osadapter.LINUX or osadapter.WINDOWS) else 1,
    "auth_retry_delay_s": 30,
    "chain_default_max_tasks": 5,
    "chain_default_max_hours": 12,
    "chain_default_max_no_progress": 2,
    "chain_cap_max_tasks": 25,
    "chain_cap_max_hours": 48,
    "chain_completion_gate": True,
    "supervisor_default_max_chains": 4,
    "supervisor_default_max_hours": 24,
    "supervisor_default_max_no_progress": 2,
    "supervisor_cap_max_chains": 12,
    "supervisor_cap_max_hours": 72,
    "supervisor_cap_max_no_progress": 5,
    "quality_default_max_chains": 2,
    "quality_cap_max_chains": 4,
    "quality_max_broad": 1,
    "quality_min_commits": 3,
    "quality_review_after_chains": 3,
    "quality_review_after_commits": 12,
    "quality_chain_max_hours": 2,
    "quality_chain_max_tasks": 3,
    "quality_broad_chain_max_hours": 4,
    "supervision_enabled": False,
    "supervision_lease_s": 900,
    "supervision_lease_cap_s": 3600,
    "supervision_max_claims": 5,
    "supervision_poll_s": 10,
    "supervision_default_max_assignments": 10,
    "supervision_cap_max_assignments": 50,
    "supervision_default_max_hours": 48,
    "supervision_cap_max_hours": 168,
    "supervision_default_max_no_progress": 2,
    "supervision_cap_max_no_progress": 5,
    "supervision_start_retry_s": 15,
    "supervision_max_start_failures": 6,
    "supervision_planned_max_h": 6,
    "supervision_webhook_url": "",
    "supervision_webhook_secret_env": "",
    "supervision_webhook_timeout_s": 5,
    "supervision_webhook_max_attempts": 8,
    "jobs_enabled": False,
    "jobs_poll_s": 10,
    "chains_on_standby": False,
    "jobs_start_retry_s": 20,
    "jobs_max_start_failures": 6,
    "jobs_model_opus": "claude-opus-5-5",
    "jobs_model_sonnet": "claude-sonnet-5-5",
    "jobs_approvers": [],
    "jobs_missing_max": 3,
    "jobs_created_grace_s": 90,
    "jobs_orphan_watch_s": 900,
    "action_review_interval_h": 0,
    "action_review_apply": False,
    "action_review_max_close": 25,
    "action_review_max_fraction": 0.5,
    "action_review_max_plan_age_s": 3600,
    "action_closure_apply": False,
    "action_closure_interval_s": 120,
    "action_closure_max": 10,
    "action_register_directory": "",
    "action_reassess_interval_h": 0,
    "action_reassess_daily_at": "05:00",
    "action_reassess_nodes": ["GERARD", "MAC", "LINUX"],
    "action_reassess_on_events": False,
    "action_reassess_queue_reserve": 30,
    "chain_start_retry_s": 10,
    "chain_max_start_failures": 3,
    "caffeinate_path": "caffeinate",
    "node": "",   # nodenaam (MAC/LINUX/...); leeg = afgeleid van het besturingssysteem
    "default_node": "",   # node voor een nieuwe opdracht zonder expliciete node (control plane); leeg = de lokale node
    "first_event_timeout_s": 180,
    "poll_interval_s": 2.0,
    "login_shell_env": True,
    "projects_dir": "",
    "env": {},
}


def r2_home() -> Path:
    return Path(os.environ.get("R2_HOME") or Path.home() / ".runner2")


def tasks_dir() -> Path:
    return r2_home() / "tasks"


def chains_dir() -> Path:
    return r2_home() / "chains"


def supervisors_dir() -> Path:
    return r2_home() / "supervisors"


def supervision_dir() -> Path:
    return r2_home() / "supervision"


def worktrees_dir() -> Path:
    return r2_home() / "worktrees"


def local_node() -> str:
    return (load_config()["node"] or osadapter.node_name()).upper()


def default_node() -> str:
    """Standaard-uitvoeringsnode voor een start zonder expliciete node; expliciete MAC/LINUX/GERARD/WINDOWS blijft gelden."""
    cfg = load_config()
    return (cfg["default_node"] or cfg["node"] or osadapter.node_name()).upper()


def load_config() -> dict:
    """Leest machine.toml (maakt het bij de eerste keer aan) en vult defaults aan."""
    path = r2_home() / "config" / "machine.toml"
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(DEFAULT_TOML)
    cfg = dict(DEFAULTS)
    cfg.update(tomllib.loads(path.read_text()))
    if not cfg["projects_dir"]:
        cfg["projects_dir"] = str(Path.home() / ".claude" / "projects")
    return cfg
