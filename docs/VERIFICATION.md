# Verification / Verificatie

Scope: local release candidate, 19 November 2026. Every host ran the same Python and static source files from a freshly extracted temporary archive. The second Linux host used its own node identifier. The tests used empty runner state, a disposable Git repository and an ephemeral loopback port.

| Host | Native Python | Native Git | Fresh local installation checks |
| --- | --- | --- | --- |
| macOS | 3.13.12 | 2.50.1 | 11 passed |
| Ubuntu/Linux controller | 3.14.4 | 2.53.0 | 11 passed |
| Second Linux node | 3.14.4 | 2.53.0 | 11 passed |
| Windows | 3.14.9 | 2.56.0.windows.3 | 11 passed |

On each host the checks covered Python compilation, empty RPC status, fresh configuration, watch heartbeat, refusal to start Claude under defaults, generation of the platform service definition, creation of an isolated Git worktree, serving the browser page, health API, dashboard and rejection of a browser write request. HTTP responses were checked directly. Browser interaction, production service replacement, recovery after reboot, authenticated execution with Claude Code and optional integrations remain outside these tests. A generated service definition is not proof of successful installation or automatic startup.

Op elk systeem is dezelfde lokale proef uitgevoerd. Er is geen echte Claude-opdracht gestart. De geïnstalleerde diensten zijn voor deze proef niet vervangen. Het pakket is bedoeld voor beoordeling. Een volledig operationeel profiel vereist nog tests met een apart ingelogd account, de automatische start en de optionele functies die men publiek wil ondersteunen.

## Follow-up checks

A real, single-turn Claude Code task completed on macOS, both Linux hosts and Windows in new temporary worktrees under the existing authenticated accounts. Each result returned the expected marker. Windows initially failed while refreshing its sign-in token and passed the later retry. The tests used the preceding candidate runtime. The Branchwatch Relay update changes the public name, documents and window title.

An isolated launchd service on macOS and isolated systemd services on both Linux hosts automatically started fresh watch processes after the test processes were stopped. Windows refused registration of a temporary logon task under the available account. These process-recovery tests left the hosts running.

One candidate controller collected four fresh native RPC replies through temporary test routes. Each node reported its own identity and a healthy watch. The test used the established authorized connections to start isolated code on each host. It did not cover the setup of a new cluster with its own SSH keys, queue dispatch or failover.

Eén echte Claude-taak per machine slaagde. De tijdelijke watch herstelde automatisch op Mac en beide Linux-machines. De Windows-testaccount kreeg geen toestemming om een nieuwe geplande taak te registreren. Hieronder staan de vervolgproeven in aparte virtuele machines.

## Separate reboot and queue lab

A new Ubuntu 24.04 virtual machine ran the normal service installation with systemd user lingering enabled. A full guest reboot changed the OS boot ID. The watch recorded a fresh heartbeat under its new process approximately nine seconds after boot and before the first SSH login.

Two independent Ubuntu guests then formed a new cluster. Each guest had its own repository, task state, watch service and SSH identity. The controller used a dedicated test key and the worker host key was pinned. The actual queue started one task locally and an explicitly dependent task on the remote worker. The worker stayed queued until the first task finished. Both native task processes finished with the expected marker. The test replaced the model executable with a deterministic local test double. It used the regular queue dispatcher, SSH client, RPC, watch, Git worktrees, task processes and result reads.

The worker was also rebooted. Its boot ID changed and the controller could again read a healthy watch over SSH. This worker observation did not establish that the watch started before the first login.

Op een verse Ubuntu 24.04-VM installeerde de reguliere installatieopdracht de systemd-gebruikersdienst. Met gebruikersdiensten actief zonder aanmelding startte de watch ongeveer negen seconden na een volledige herstart en vóór de eerste SSH-aanmelding. De gewijzigde opstartsessie en de verse hartslag van het nieuwe proces bevestigden dit.

Twee aparte Ubuntu-VM’s vormden vervolgens een nieuw cluster met elk een eigen repository, wachtdienst, taakgegevens en SSH-identiteit. De centrale wachtrij voerde een taak lokaal uit en hield de afhankelijke taak vast tot die klaar was. Daarna voerde de tweede VM de afhankelijke taak uit. Beide taken leverden de verwachte uitkomst op. De proef gebruikte de echte opdrachtverdeling, SSH-verbinding, RPC en Git-worktrees, met een lokaal testprogramma op de plaats van het model. Na een reboot van de tweede VM kreeg de controller opnieuw een gezonde watch-status. Het startmoment van die tweede watch ten opzichte van de eerste aanmelding is niet vastgesteld.

## Windows and macOS reboot / Volledige herstart op Windows en macOS

A fresh, offline Windows Server 2022 Desktop Experience guest (build 20348) passed. The regular service installer registered a scheduled task triggered by logon. After a full guest reboot and normal test-account logon, a new Windows Task Scheduler action-started event for that task and a fresh heartbeat from a live watch process confirmed automatic startup. The reboot test covers Windows Server 2022. Windows 11 remains unverified for full reboot.

Een aparte, offline virtuele machine met Windows Server 2022 Desktop Experience (build 20348) doorstond de proef. Na een volledige reboot en normale aanmelding startte Windows Taakplanner de watch automatisch. Een nieuwe Taakplanner-gebeurtenis en een verse hartslag van het actieve proces bevestigden dit. Een volledige reboot op Windows 11 blijft onbevestigd.

An offline Intel virtual machine running macOS Tahoe 26.7.3 (build 25G342) passed. The regular service installer registered a user LaunchAgent with `RunAtLoad`. After a full macOS restart and normal login, a different boot session ID and a fresh heartbeat from a new, live watch process verified automatic startup. No manual watch start occurred after the reboot. Apple silicon and earlier macOS releases remain unverified for full reboot. The test covers the watch service. The monitor starts separately.

Een aparte, offline Intel-VM met macOS Tahoe 26.7.3 (build 25G342) doorstond de proef. De reguliere installatie registreerde een LaunchAgent met `RunAtLoad`. Na een volledige herstart en normale aanmelding bevestigden een nieuwe opstartsessie en een verse hartslag van een nieuw actief watch-proces de automatische start. De watch werd na de herstart niet handmatig gestart. Een volledige reboot met automatische start op Apple Silicon en oudere macOS-versies blijft onbevestigd. De proef omvat de wachtdienst. De monitor wordt afzonderlijk gestart.
