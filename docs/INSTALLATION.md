# Installation / Installatie

Read [Security](../SECURITY.md) before enabling tasks. Lees [Beveiliging](../SECURITY.md) voordat je taken inschakelt. Use a dedicated unprivileged account and a new data directory. Gebruik een apart account zonder beheerrechten en een nieuwe gegevensmap.

## macOS and Ubuntu/Linux

Install Python and Git independently. The verified Python versions are listed in [Verification](VERIFICATION.md). Open a terminal in the extracted archive. Run these commands in that directory:

```sh
export R2_HOME="$PWD/local-state"
python3 -m runner2 status
python3 -m runner2.watch --once
python3 scripts/obf_monitor2.py --port 8772
```

Open http://127.0.0.1:8772. Use the same data directory in each shell. For a foreground watch that keeps running, omit `--once`. Stop it with Ctrl+C when the test is over. On Linux, real task execution needs a working systemd user session. Check it with `systemctl --user status`. On macOS, run under the account with the Claude Code login session if you later activate execution. Use an absolute path for the Claude executable if it is not on the service's PATH.

Op macOS en Ubuntu voer je dezelfde commando's uit vanuit de uitgepakte map. Een voortdurend draaiende watch start je zonder `--once`. Open daarna de monitor in een tweede terminal met dezelfde instelling voor `R2_HOME`. Op Ubuntu heeft taakuitvoering een werkende systemd-gebruikerssessie nodig. Op de Mac kan de inlogsessie van hetzelfde account nodig zijn voor Claude Code.

If using two Linux nodes, edit the second node's `local-state/config/machine.toml` and set `node = "GERARD"`. This is the existing identifier for a second Linux node. Use a different data directory on each host. Stel bij twee Linux-machines op de tweede `node = "GERARD"` in.

## Windows

Use native Python and Git for Windows. Start PowerShell in the extracted directory:

```powershell
$env:R2_HOME = Join-Path $PWD 'local-state'
python -m runner2 status
python -m runner2.watch --once
python scripts/obf_monitor2.py --port 8772
```

Open http://127.0.0.1:8772. For a longer-running watch, omit `--once`. Claude Code may require the Git for Windows Bash path when configured for unattended tasks. Keep the extracted directory and state on a disk writable by the runner account. Use one consistent interpreter and avoid moving a path while a service points to it.

Gebruik native Python en Git voor Windows. Open PowerShell in de uitgepakte map. Gebruik voor een voortdurend draaiende watch hetzelfde commando zonder `--once`. Gebruik hetzelfde pad naar Python en dezelfde gegevensmap in alle vensters.

## Automatic startup / Automatisch starten

The built-in `python -m runner2 service install` uses a LaunchAgent on macOS, a systemd user unit on Linux and an on-logon Scheduled Task on Windows. The service identifiers are fixed, so use this only on a fresh account after checking `python -m runner2 service status`. The watch and monitor are separate. The [verification record](VERIFICATION.md) describes the tested operating systems and startup conditions.

De watch kan per account automatisch starten. De ingebouwde opdracht gebruikt een vaste servicenaam. Gebruik die alleen op een apart account zonder bestaande Runner 2-service. Op Windows start de taak bij aanmelding. Op Linux hangt automatisch starten vóór aanmelding af van de gebruikerssessie en het systeembeheer. De monitor start je afzonderlijk. Het [verificatierapport](VERIFICATION.md) vermeldt de geteste systemen en opstartvoorwaarden.

## Configure execution / Uitvoering instellen

1. Install and authenticate Claude Code under the dedicated account. Check its exact version with `claude --version`. The runtime, account and any usage charges are provided separately.
2. Edit `local-state/config/machine.toml`. Set `expected_claude_version` to the exact version token and configure the role models for your account. Set `claude_path` if necessary. Review the security implications before setting `allow_unattended_execution = true`.
3. Start the foreground watch and try one trusted task in a disposable repository. Use `python -m runner2 run --repo PATH --prompt-file FILE`. Use `python3` on systems where that is the interpreter command.
4. For multiple nodes, verify SSH host keys through a trusted channel before populating `config/nodes.toml`. Use absolute executable and code paths and a dedicated key. See [example](../examples/nodes.toml.example). The example assumes the remote account uses the default `.runner2` state location. Validate each node's read-only status before activating the queue. Enable `jobs_enabled` only on the central controller.

Installeer Claude Code onder het aparte account, zet de exacte CLI-versie in `machine.toml` en kies beschikbare modellen. Schakel onbeheerde uitvoering alleen bewust in. Begin met één opdracht in een wegwerprepository. Controleer SSH-hostsleutels vooraf en activeer de wachtrij pas nadat de status van alle gekozen uitvoermachines klopt.
