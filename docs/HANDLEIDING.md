# Claude Runner 2 en Monitor 2 — gebruikershandleiding

Deze handleiding hoort bij de lokale releasekandidaat. Zie [Installatie](INSTALLATION.md) en [Verificatie](VERIFICATION.md) voor het geteste profiel.

## Doel en onderdelen

Runner 2 beheert programmeertaken die door Claude Code worden uitgevoerd. Een taak krijgt een eigen Git-worktree, zodat wijzigingen apart van de oorspronkelijke werkmap kunnen ontstaan. De controller beheert de taak; de watch signaleert onder meer voortgang en herstel; een centrale wachtrij kan opdrachten over verschillende machines verdelen. Monitor 2 biedt een webinterface voor de beschikbare status en bediening. De monitor is afhankelijk van de runner en aanvullende modules; een los gekopieerde monitor is geen volledige installatie.

## Belangrijk vóór gebruik

Na expliciete activering start de runner Claude Code met `bypassPermissions` en zonder interactieve toestemmingsvragen. Een taak kan daardoor handelen met de rechten van het gebruikersaccount waarop zij draait. Een Git-worktree is **geen beveiligingssandbox**: andere bestanden, netwerktoegang en beschikbare inloggegevens kunnen bereikbaar zijn. Gebruik uitsluitend vertrouwde opdrachten en repositories; kies een apart account of passende isolatie en minimale rechten. Controleer de bevoegdheden, externe diensten, kosten en veiligheidsinstellingen voordat je een taak start. Stel de monitor en de controller niet zonder passende toegangsbeveiliging aan een netwerk of internet bloot.

## Opdrachten en taken

Een opdracht kiest een machine, repository, instructie en eventueel een model en afhankelijkheden. De wachtrij kan afhankelijk van de instellingen wachten op eerdere opdrachten of een menselijke goedkeuring. De watch kan een volgende opdracht starten als de betreffende runner vrij is. Welke keuzes in een installatie zichtbaar of beschikbaar zijn, hangt af van configuratie en versie.

Een taak kan actief zijn, wachten op een antwoord, gepauzeerd zijn, voltooid zijn, mislukken of gestopt zijn. Via de beschikbare bediening kun je status en resultaat bekijken, een vraag beantwoorden, pauzeren, hervatten of stoppen. Controleer het eindrapport en de wijzigingen in de worktree; een melding dat een taak klaar is bewijst niet op zichzelf dat de wijzigingen correct zijn of zijn geïntegreerd.

Een optionele keten kan meerdere opeenvolgende taken uitvoeren. Een optionele supervisor kan ketens beoordelen en vervolgstappen plannen binnen ingestelde grenzen. Daarnaast bestaat externe supervisie voor expliciet geregistreerde projecten. Limieten voor tijd, aantallen en voortgang horen bij de configuratie; die begrenzen het werk maar vormen geen garantie tegen fouten of kosten.

## Monitor

De onderzochte monitor heeft overzichten voor machines, opdrachten en aandachtspunten, en biedt afhankelijk van configuratie en rechten bediening en rapporten. De servercode van de onderzochte bron accepteert standaard alleen een lokaal bindadres; voor browseracties zijn extra controles aanwezig. Er zijn afzonderlijke routes voor specifieke netwerktoegang. De actuele release en alle routes moeten nog afzonderlijk worden beoordeeld. Behandel lokale toegang als toegang tot gevoelige gegevens en potentieel krachtige acties.

## Beheer en herstel

Bewaar configuratie, taakadministratie en sleutels gescheiden van publiek bronmateriaal. Logs en taaktranscripten kunnen vertrouwelijke opdrachten en resultaten bevatten. Controleer regelmatig backups en test terugzetten in een afzonderlijke omgeving. Verplaats of verwijder geen actieve worktrees en wijzig geen dienst tijdens een lopende taak zonder eerst de specifieke gevolgen te onderzoeken. Een herstelactie of herstart is afhankelijk van platform, versie en actuele status; deze concepttekst is geen draaiboek voor de bestaande installatie.

## Eerste gebruik

Volg [Installatie](INSTALLATION.md) voor jouw systeem. Open Monitor 2 en kijk of de lokale runner verschijnt. Een ontbrekende remote runner is te verwachten totdat je deze configureert. De monitor is standaard alleen-lezen. Gebruik `python -m runner2 status` om de taken te bekijken. Voor een bestaande taak toont `status TAAK-ID` het overzicht. Gebruik `pause TAAK-ID` voor een pauze, `send TAAK-ID` om een gepauzeerde taak te hervatten, of voeg bij een wachtende vraag het antwoord toe. Met `stop TAAK-ID` beëindig je de taak. Bekijk via `--help` welke opties jouw versie ondersteunt. Gebruik op macOS of Linux `python3` als dat jouw Python-commando is.

Bij een fout controleer je eerst Python, Git, de gegevensmap, de watch en de gekozen poort. Controleer bij taakstart ook de gepinde Claude-versie en of het uitvoeraccount toegang tot de repository heeft. Deel alleen geanonimiseerde diagnostiek.
