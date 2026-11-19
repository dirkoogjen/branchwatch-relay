"""Remote notificaties voor de Studio OBF Claude Runner.

Een *observerende* laag: ze leest de duurzame controller-/projectstatus (dezelfde
read-only bronnen als de webmonitor) en meldt alleen wanneer de bestaande
controller zelf zegt dat een mens nodig is (``human_action``) of dat een taak of
projectsessie definitief klaar/mislukt is. Ze schrijft nooit naar controller-,
project- of runnerstatus. Zie ``docs/obf_notify.md``.
"""
