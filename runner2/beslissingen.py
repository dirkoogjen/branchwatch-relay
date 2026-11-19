"""Centraal beslissingenregister (besluit 49): één duurzame bron voor de besluiten van Beheerder.

Canonieke locatie: docs/beslissingenregister/ in deze repository (GERARD-projectdossier):
- register.json  de enige bron van waarheid (per besluit: datum, tekst met herkomst, actie-ID's, opdracht-/job-ID's en
                 APARTE statussen voor documentatie, uitvoering en controle);
- audit.jsonl    append-only, hash-geketend auditspoor van elke wijziging (wie, waarom, bewijs, resultaat);
- register.md    leesbare weergave, gegenereerd uit register.json (nooit met de hand bewerken).
Wijzigen gaat uitsluitend via `apply` (of `refresh`/`bootstrap`): exclusieve lock, validatie vooraf, atomair schrijven,
idempotent op update-id. Het register leest de wachtrij alleen en sluit nooit een actie, opdracht of keten.

Gebruik: python3 -m runner2.beslissingen {validate|render|apply|refresh|adopt|bootstrap} (zie docs/beslissingenregister/README.md).
"""
import argparse
import copy
import hashlib
import json
import os
import sys
import time
from pathlib import Path

from . import config, dossier
from .filelock import lock_exclusive, replace

SCHEMA = "runner2.beslissingenregister.v1"
GENESIS = "0" * 64
ROOT = Path(__file__).resolve().parents[1]
REGISTER_DIR = ROOT / "docs" / "beslissingenregister"
BEWIJS_50 = ROOT / "docs" / "rapporten" / "2026-10-23-besluit-50-bewijs.json"
DEFAULT_QUEUE = config.r2_home() / "jobs" / "queue.json"

DIMENSIES = ("documentatie", "uitvoering", "controle")
STATUSSEN = {
    "documentatie": {"onbekend", "wacht", "bezig", "gedeeltelijk", "gedaan", "niet_van_toepassing"},
    "uitvoering": {"onbekend", "wacht", "bezig", "gedeeltelijk", "gedaan", "uitgesteld", "bewust_niet", "niet_van_toepassing"},
    "controle": {"onbekend", "wacht", "bezig", "ok", "mislukt", "niet_van_toepassing"},
}
BEWIJS_NODIG = {"gedaan", "ok"}  # een voltooiing zonder verwijzing naar bewijs is nooit toegestaan
ZEKERHEID = {"bewezen", "afgeleid", "onbekend"}
PROJECTEN = ("studio_obf", "gerard", "monitor2")
VERWIJZING_STATUS = {"onbekend", "ontbreekt", "aangebracht"}
OPS = {"status", "tekst", "koppel", "verwijzing", "opmerking"}


class RegisterError(Exception):
    pass


# --- bestanden: canonieke serialisatie, hash, atomair schrijven ---

def canon(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, indent=1, sort_keys=True) + "\n"


def sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S+0000", time.gmtime())


def _write_atomic(path: Path, text: str) -> None:
    tmp = path.with_name(f".{path.name}.tmp{os.getpid()}")
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text)
        f.flush()
        os.fsync(f.fileno())
    replace(tmp, path)


def load(directory=REGISTER_DIR):
    d = Path(directory)
    try:
        reg = json.loads((d / "register.json").read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise RegisterError(f"register.json ontbreekt in {d}; bootstrap is alleen voor de eerste aanmaak")
    audit = []
    p = d / "audit.jsonl"
    if p.exists():
        audit = [json.loads(r) for r in p.read_text(encoding="utf-8").splitlines() if r.strip()]
    return reg, audit


def _entry_hash(entry: dict) -> str:
    body = {k: v for k, v in entry.items() if k != "hash"}
    return sha(json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":")))


def _append_audit(d: Path, audit: list, entry: dict) -> dict:
    entry = dict(entry, seq=len(audit) + 1, prev=audit[-1]["hash"] if audit else GENESIS)
    entry["hash"] = _entry_hash(entry)
    with open(d / "audit.jsonl", "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False, sort_keys=True) + "\n")
        f.flush()
        os.fsync(f.fileno())
    audit.append(entry)
    return entry


# --- validatie ---

def check_audit(reg: dict, audit: list) -> list:
    """Controleert keten en koppeling met register.json; geeft een lijst fouten (leeg = gezond)."""
    fouten, prev, ids = [], GENESIS, set()
    for i, e in enumerate(audit, 1):
        if e.get("seq") != i:
            fouten.append(f"audit #{i}: volgnummer {e.get('seq')}")
        if e.get("prev") != prev:
            fouten.append(f"audit #{i}: keten verbroken (prev)")
        if e.get("hash") != _entry_hash(e):
            fouten.append(f"audit #{i}: hash klopt niet (regel gewijzigd)")
        if e.get("update_id") in ids:
            fouten.append(f"audit #{i}: update-id {e.get('update_id')} komt dubbel voor")
        ids.add(e.get("update_id"))
        prev = e.get("hash")
    if not audit:
        fouten.append("audit is leeg")
    elif audit[-1].get("register_sha256") != sha(canon(reg)):
        fouten.append("register.json wijkt af van de laatste auditregel (wijziging buiten de procedure om; zie `adopt`)")
    return fouten


def validate(reg: dict) -> list:
    f = []
    if reg.get("schema") != SCHEMA:
        f.append(f"schema {reg.get('schema')!r} is niet {SCHEMA}")
    nrs = [b.get("nr") for b in reg.get("besluiten", [])]
    if nrs != sorted(set(nrs)) or not nrs:
        f.append("besluitnummers moeten uniek en oplopend zijn")
    for b in reg.get("besluiten", []):
        n = b.get("nr")
        for k in ("onderwerp", "datum", "datum_bron", "nummer_zekerheid", "tekst", "jobs", "acties", "statussen"):
            if k not in b:
                f.append(f"besluit {n}: veld {k} ontbreekt")
        if b.get("nummer_zekerheid") not in ZEKERHEID:
            f.append(f"besluit {n}: ongeldige nummer_zekerheid")
        t = b.get("tekst") or {}
        if t.get("exact_van_beheerder") is None and t.get("opdrachttekst") is None and not t.get("herkomst"):
            f.append(f"besluit {n}: zonder tekst moet de herkomst/reden vermeld zijn")
        for j in b.get("jobs", []):
            if not (isinstance(j.get("job"), str) and j["job"].startswith("J-")):
                f.append(f"besluit {n}: ongeldig job-ID {j.get('job')!r}")
        for a in b.get("acties", []):
            if not (isinstance(a.get("id"), str) and a["id"].startswith("A-")):
                f.append(f"besluit {n}: ongeldig actie-ID {a.get('id')!r}")
        s = b.get("statussen", {})
        if set(s) != set(DIMENSIES):
            f.append(f"besluit {n}: statussen moeten exact {DIMENSIES} zijn")
        for dim in DIMENSIES:
            st = s.get(dim) or {}
            if st.get("status") not in STATUSSEN[dim]:
                f.append(f"besluit {n}: {dim}-status {st.get('status')!r} is ongeldig")
            if st.get("zekerheid") not in ZEKERHEID:
                f.append(f"besluit {n}: {dim}-zekerheid ongeldig")
            if st.get("status") in BEWIJS_NODIG and not [x for x in st.get("bewijs", []) if x.get("soort") and x.get("ref")]:
                f.append(f"besluit {n}: {dim}={st.get('status')} zonder bewijs")
    for p in PROJECTEN:
        v = (reg.get("projectdossiers") or {}).get(p)
        if not v or v.get("verwijzing_status") not in VERWIJZING_STATUS:
            f.append(f"projectdossier {p}: verwijzing_status ontbreekt of is ongeldig")
    return f


# --- mutaties ---

def mutation_hash(m: dict) -> str:
    return sha(json.dumps({k: v for k, v in m.items() if k != "reden_hergebruik"}, ensure_ascii=False, sort_keys=True))


def _besluit(reg: dict, nr):
    for b in reg["besluiten"]:
        if b["nr"] == nr:
            return b
    raise RegisterError(f"besluit {nr} bestaat niet in het register")


def _apply_one(reg: dict, m: dict, stamp: str) -> bool:
    """Past één mutatie toe op `reg` (in het geheugen). Geeft False als het register er niet door verandert."""
    op = m.get("op")
    if op not in OPS:
        raise RegisterError(f"{m.get('id')}: onbekende op {op!r}")
    if op == "verwijzing":
        proj = m.get("project")
        if proj not in PROJECTEN or m.get("verwijzing_status") not in VERWIJZING_STATUS:
            raise RegisterError(f"{m['id']}: ongeldig project of verwijzing_status")
        v = reg["projectdossiers"][proj]
        new = dict(v, verwijzing_status=m["verwijzing_status"], update_id=m["id"])
        for k in ("pad", "bewijs", "notitie"):
            if k in m:
                new[k] = m[k]
        if new["verwijzing_status"] == "aangebracht" and not new.get("bewijs"):
            raise RegisterError(f"{m['id']}: verwijzing 'aangebracht' vereist bewijs")
        changed = {k: v.get(k) for k in new if k != "update_id"} != {k: new.get(k) for k in new if k != "update_id"}
        reg["projectdossiers"][proj] = new
        return changed
    b = _besluit(reg, m.get("besluit"))
    if op == "status":
        dim, status = m.get("dimensie"), m.get("status")
        if dim not in DIMENSIES or status not in STATUSSEN.get(dim, ()):
            raise RegisterError(f"{m['id']}: ongeldige dimensie/status {dim!r}/{status!r}")
        zek = m.get("zekerheid", "bewezen" if m.get("bewijs") else "onbekend")
        if zek not in ZEKERHEID:
            raise RegisterError(f"{m['id']}: ongeldige zekerheid")
        bewijs = m.get("bewijs", [])
        if status in BEWIJS_NODIG and not [x for x in bewijs if x.get("soort") and x.get("ref")]:
            raise RegisterError(f"{m['id']}: status {status!r} vereist bewijs (lijst van {{soort, ref}})")
        huidig = b["statussen"][dim]
        if huidig["status"] in BEWIJS_NODIG and status != huidig["status"] and not m.get("reden"):
            raise RegisterError(f"{m['id']}: {dim} van besluit {b['nr']} staat op {huidig['status']!r}; terugzetten vraagt een reden")
        new = {"status": status, "zekerheid": zek, "bewijs": bewijs, "bron": m.get("bron", ""), "peildatum": m.get("peildatum", stamp),
               "notitie": m.get("notitie", ""), "update_id": m["id"]}
        changed = {k: huidig.get(k) for k in new if k not in ("update_id", "peildatum")} != {k: new[k] for k in new if k not in ("update_id", "peildatum")}
        b["statussen"][dim] = new
        return changed
    if op == "tekst":
        t = b["tekst"]
        nieuw = m.get("exact_van_beheerder")
        if not isinstance(nieuw, str) or not nieuw.strip() or not m.get("bron"):
            raise RegisterError(f"{m['id']}: exacte tekst en bron zijn verplicht")
        nieuw = dossier.redact(nieuw)
        if t.get("exact_van_beheerder") == nieuw:
            return False
        if t.get("exact_van_beheerder") and not m.get("reden"):
            raise RegisterError(f"{m['id']}: er staat al een exacte tekst; vervangen vraagt een reden (de oude blijft in tekst_historie)")
        if t.get("exact_van_beheerder"):
            t.setdefault("tekst_historie", []).append({"tekst": t["exact_van_beheerder"], "bron": t.get("exact_bron"), "update_id": t.get("update_id")})
        t.update(exact_van_beheerder=nieuw, exact_bron=m["bron"], update_id=m["id"])
        return True
    if op == "koppel":
        kind, ref = m.get("soort"), m.get("ref")
        if kind == "actie" and isinstance(ref, str) and ref.startswith("A-"):
            if any(a["id"] == ref for a in b["acties"]):
                return False
            b["acties"].append({"id": ref, "relatie": m.get("relatie", "gekoppeld"), "zekerheid": m.get("zekerheid", "bewezen"),
                                "bron": m.get("bron", ""), "update_id": m["id"]})
            return True
        if kind == "job" and isinstance(ref, str) and ref.startswith("J-"):
            if any(j["job"] == ref for j in b["jobs"]):
                return False
            b["jobs"].append({"job": ref, "node": m.get("node"), "taak": m.get("taak"), "titel": m.get("titel", ""), "rol": m.get("rol", ""),
                              "update_id": m["id"]})
            return True
        raise RegisterError(f"{m['id']}: koppel vraagt soort 'actie' (A-…) of 'job' (J-…)")
    if op == "opmerking":
        tekst = dossier.redact(m.get("tekst", ""))
        if not tekst or tekst in b.setdefault("opmerkingen", []):
            return False
        b["opmerkingen"].append(tekst)
        return True
    return False


def apply(mutations: list, actor: str, directory=REGISTER_DIR, stamp=None, reden_batch: str = "") -> dict:
    """Past een lijst mutaties atomair en idempotent toe. Resultaat: {toegepast, overgeslagen, ongewijzigd}."""
    d = Path(directory)
    stamp = stamp or now_iso()
    with open(d / ".lock", "a+") as lk:
        lock_exclusive(lk)
        reg, audit = load(d)
        fouten = check_audit(reg, audit)
        if fouten:
            raise RegisterError("register niet gezond, geen wijziging: " + "; ".join(fouten))
        seen = {e["update_id"]: e for e in audit}
        work, res, nieuw = copy.deepcopy(reg), {"toegepast": [], "overgeslagen": [], "ongewijzigd": []}, []
        batch_ids = set()
        for m in mutations:
            mid = m.get("id")
            if not isinstance(mid, str) or not mid:
                raise RegisterError("elke mutatie heeft een unieke tekst-id nodig")
            h = mutation_hash(m)
            if mid in batch_ids:
                raise RegisterError(f"{mid}: komt dubbel voor in dezelfde batch")
            batch_ids.add(mid)
            if mid in seen:
                if seen[mid].get("mutatie_sha256") != h:
                    raise RegisterError(f"{mid}: update-id is al gebruikt voor een andere mutatie")
                res["overgeslagen"].append(mid)
                continue
            if _apply_one(work, m, stamp):
                nieuw.append((m, h))
                res["toegepast"].append(mid)
            else:
                res["ongewijzigd"].append(mid)
        if not nieuw:
            return res
        work["bijgewerkt"] = stamp
        _write_atomic(d / "register.prev.json", canon(reg))
        _write_atomic(d / "register.json", canon(work))
        digest = sha(canon(work))
        for i, (m, h) in enumerate(nieuw):
            _append_audit(d, audit, {"ts": stamp, "update_id": m["id"], "type": "mutatie", "actor": actor, "reden": m.get("reden") or reden_batch,
                                     "mutatie": m, "mutatie_sha256": h, "register_sha256": digest if i == len(nieuw) - 1 else None})
        _write_atomic(d / "register.md", render(work))
    if directory == REGISTER_DIR:
        try:
            from . import actionreassess
            actionreassess.hook_event("nieuw_besluit", digest[:16])
        except Exception:   # noqa: BLE001 -- een herbeoordeling mag het register nooit raken
            pass
    return res


def adopt(reden: str, actor: str, directory=REGISTER_DIR, stamp=None) -> dict:
    """Legt een wijziging buiten de procedure om expliciet vast in het auditspoor (geen gegevens worden gewijzigd)."""
    if not reden.strip():
        raise RegisterError("adopt vraagt een reden")
    d, stamp = Path(directory), stamp or now_iso()
    with open(d / ".lock", "a+") as lk:
        lock_exclusive(lk)
        reg, audit = load(d)
        fouten = [x for x in check_audit(reg, audit) if "wijkt af" not in x]
        errs = validate(reg)
        if fouten or errs:
            raise RegisterError("adoptie kan niet: " + "; ".join(fouten + errs))
        digest = sha(canon(reg))
        if audit and audit[-1]["register_sha256"] == digest:
            return {"adoptie": None}
        e = _append_audit(d, audit, {"ts": stamp, "update_id": f"adopt-{digest[:16]}", "type": "adoptie", "actor": actor, "reden": reden,
                                     "mutatie": None, "mutatie_sha256": None, "register_sha256": digest})
        _write_atomic(d / "register.md", render(reg))
        return {"adoptie": e["update_id"]}


# --- wachtrij lezen (alleen lezen) en afgeleide mutaties ---

def load_queue(path=DEFAULT_QUEUE) -> dict:
    q = json.loads(Path(path).read_text(encoding="utf-8"))
    jobs = q["jobs"]
    return {"jobs": jobs if isinstance(jobs, dict) else {j["id"]: j for j in jobs}, "updated": q.get("updated", "")}


def _job_bewijs(j: dict) -> list:
    ev = ((j.get("check") or {}).get("evidence") or {})
    out = [{"soort": "job", "ref": j["id"], "node": j.get("node"), "taak": j.get("task_id"), "status": j.get("status")}]
    if ev.get("request"):
        out.append({"soort": "integratie", "ref": ev["request"], "grant": ev.get("grant"), "commit": ev.get("commit"), "repo": ev.get("repo")})
    return out


def refresh_mutations(reg: dict, queue: dict) -> list:
    """Leidt documentatie- en controlestatus af uit de wachtrij. Uitvoering wordt NOOIT afgeleid: een afgeronde
    vastleggingsopdracht is geen bewijs dat de technische uitvoering klaar is."""
    out, jobs, peil = [], queue["jobs"], queue.get("updated") or now_iso()
    for b in reg["besluiten"]:
        for jr in b["jobs"]:
            if jr.get("rol") != "besluitopdracht":
                continue
            j = jobs.get(jr["job"])
            if not j:
                continue
            ok = (j.get("check") or {}).get("state") == "ok" and ((j.get("check") or {}).get("evidence") or {}).get("request")
            if j["status"] == "done" and ok:
                doc, ctl = "gedaan", "ok"
            elif j["status"] == "done":
                # afgerond zonder sluitend integratiebewijs: vastgelegd op de taakbranch, nog niet in main bewezen
                doc, ctl = "gedeeltelijk", "wacht"
            elif j["status"] in ("running", "claimed", "starting", "started"):
                doc, ctl = "bezig", "wacht"
            elif j["status"] in ("queued", "held"):
                doc, ctl = "wacht", "wacht"
            else:
                continue  # onbekende/afwijkende uitkomst (stopped, cancelled, incomplete, done zonder bewijs): handmatig beoordelen
            bw = _job_bewijs(j)
            tag = (bw[1]["ref"] if len(bw) > 1 else j["status"])
            for dim, st in (("documentatie", doc), ("controle", ctl)):
                if b["statussen"][dim]["status"] == st:
                    continue
                if b["statussen"][dim]["status"] in BEWIJS_NODIG | {"gedeeltelijk"} and st not in BEWIJS_NODIG:
                    continue  # nooit een gedeeltelijke of voltooide status terugzetten op grond van de wachtrij
                out.append({"id": f"queue:{b['nr']}:{jr['job']}:{dim}:{st}:{tag}", "op": "status", "besluit": b["nr"], "dimensie": dim, "status": st,
                            "bewijs": bw, "zekerheid": "bewezen", "bron": f"wachtrij {peil} (alleen gelezen)",
                            "peildatum": peil, "notitie": f"afgeleid uit opdracht {jr['job']} ({j['status']})"
                            + ("" if ok or j["status"] != "done" else f"; nog geen sluitend integratiebewijs (laatste fout: {(j.get('last_error') or {}).get('code') or 'geen'})")})
    return out


# --- weergave ---

def render(reg: dict) -> str:
    L = ["# Beslissingenregister Runner 2 / Monitor 2 / Studio OBF", "",
         "**Gegenereerd uit `register.json`; niet met de hand bewerken** (wijzigen: zie README.md in deze map). "
         f"Bijgewerkt: {reg.get('bijgewerkt', '')}. Reeks: {reg.get('reeks', '')}.", "",
         "Drie gescheiden statussen per besluit: **documentatie** (vastgelegd), **uitvoering** (technisch gedaan) en **controle** "
         "(door de integrator/test bewezen). *Onbekend* betekent: niet aantoonbaar; het is nooit stilzwijgend 'gedaan'.", "",
         "| nr | datum | onderwerp | documentatie | uitvoering | controle | opdracht(en) | acties |", "|---|---|---|---|---|---|---|---|"]
    for b in reg["besluiten"]:
        s = b["statussen"]
        nr = f"{b['nr']}{'*' if b['nummer_zekerheid'] != 'bewezen' else ''}"
        L.append(f"| {nr} | {b['datum']} | {b['onderwerp']} | {s['documentatie']['status']} | {s['uitvoering']['status']} | {s['controle']['status']} | "
                 f"{', '.join(j['job'] for j in b['jobs']) or '-'} | {len(b['acties'])} |")
    L += ["", "\\* = nummer afgeleid, niet bevestigd.", "", "## Projectdossiers (verwijzingen naar dit register)", ""]
    for p, v in reg["projectdossiers"].items():
        L.append(f"* **{p}**: {v['verwijzing_status']}. {v.get('notitie', '')}")
    L += ["", "## Per besluit", ""]
    for b in reg["besluiten"]:
        L += [f"### Besluit {b['nr']}{'*' if b['nummer_zekerheid'] != 'bewezen' else ''} — {b['onderwerp']}", "",
              f"Datum: {b['datum']} ({b['datum_bron']})."]
        t = b["tekst"]
        if t.get("exact_van_beheerder"):
            L += ["", "Exacte tekst van Beheerder:", "", "> " + t["exact_van_beheerder"].replace("\n", "\n> ")]
        if t.get("opdrachttekst"):
            L += ["", f"Opdrachttekst (woordelijk uit de wachtrij, {t.get('herkomst', '')}):", "", "> " + t["opdrachttekst"].replace("\n", "\n> ")]
        if not t.get("exact_van_beheerder") and not t.get("opdrachttekst"):
            L += ["", f"Tekst: niet beschikbaar. {t.get('herkomst', '')}"]
        L += ["", "Opdrachten: " + ("; ".join(f"{j['job']} ({j.get('node')}, taak {j.get('taak')}, {j.get('rol') or '-'})" for j in b["jobs"]) or "geen"),
              "Acties: " + (", ".join(a["id"] for a in b["acties"]) or "geen")]
        for dim in DIMENSIES:
            st = b["statussen"][dim]
            bw = "; ".join(f"{x['soort']} {x['ref']}" + (f" ({x['commit'][:8]})" if x.get("commit") else "") for x in st["bewijs"]) or "geen"
            L.append(f"* {dim}: **{st['status']}** ({st['zekerheid']}, peil {st['peildatum']}) — bewijs: {bw}. {st['notitie']}".rstrip())
        for o in b.get("opmerkingen", []):
            L.append(f"* Opmerking: {o}")
        L.append("")
    return "\n".join(L).rstrip() + "\n"


# --- eenmalige aanmaak uit het bewijsbestand van besluit 50 en de wachtrij ---

# Uitvoering van besluiten met een afgeronde of lopende opdracht: bewust per besluit gekozen (alleen wat de bronnen dragen).
UITVOERING = {
    4: ("gedeeltelijk", "Drie Qt-acties zijn op 23-10 gesloten; de rest loopt via J-d81c13907f0e (besluit 42) en J-a4f6ebdfdef0 (besluit 45)."),
    6: ("gedeeltelijk", "Triage gedaan; de 19 vervolgtaken NV-01..19 staan niet in de wachtrij en geen Mac-opdracht is vrijgegeven of gesloten."),
    7: ("gedeeltelijk", "Alleen voorbereiding; de echte proefuitrol op de live mappen wacht op goedkeuring (A-1f39ea1252)."),
    17: ("bewust_niet", "Bewust niets uitgevoerd: geen masterwissel, geen sleutels, geen herstart; verhuisacties blijven uitgesteld."),
    19: ("gedeeltelijk", "Veilige onderdelen gebouwd; de technische inrichting is deels nog ontbrekend (zie het rapport van de opdracht)."),
    37: ("gedeeltelijk", "A-887b483d82 gesloten; de 17 Mac-markeringen zijn niet aangeraakt (apart voorstel)."),
    38: ("gedaan", "Vergelijking uitgevoerd (284 commits, 43 patches); niets verwijderd of gereset."),
    44: ("gedaan", "Drie categorieën en actiehistorie gebouwd in Monitor 2."),
    45: ("onbekend", "Opdracht afgerond en door de integrator bewezen; het uitvoeringsresultaat is door dit register niet beoordeeld."),
    49: ("bezig", "Dit register (opdracht J-3c21f0966cd8) wordt aangemaakt; de status wordt na integratie via `refresh` bijgewerkt."),
    50: ("gedeeltelijk", "Planning (volgorde van wachtende opdrachten) aangepast en overzicht opgesteld; de Studio OBF-migratie zelf loopt via de wachtrij."),
}
UITVOERING_BEWIJS = {38: "J-8552a146de70", 44: "J-f03cef87f7f2"}
GEDEELDE_VASTLEGGING = {1: "J-5b0bb823f3e1", 2: "J-5b0bb823f3e1", 3: "J-5b0bb823f3e1", 4: "J-5b0bb823f3e1"}


def bootstrap(directory=REGISTER_DIR, bewijs=BEWIJS_50, queue_path=DEFAULT_QUEUE, actor="R2-0aca77bdb735", stamp=None) -> None:
    d = Path(directory)
    if (d / "register.json").exists() or (d / "audit.jsonl").exists():
        raise RegisterError("register bestaat al; bootstrap is alleen voor de eerste aanmaak (gebruik apply)")
    d.mkdir(parents=True, exist_ok=True)
    stamp = stamp or now_iso()
    bw = json.loads(Path(bewijs).read_text(encoding="utf-8"))
    queue = load_queue(queue_path)
    jobs = queue["jobs"]
    peil = queue["updated"] or stamp
    acties_per_besluit: dict = {}
    for a in bw["open_acties"]:
        for n in a.get("besluiten", []):
            acties_per_besluit.setdefault(n, {})[a["id"]] = {
                "id": a["id"], "relatie": f"open-actie:{a['bucket']}", "zekerheid": a["zekerheid"], "bron": "bewijsbestand besluit 50 (peil 2026-10-23 09:20Z)",
                "status": "open", "node": a["node"]}
    besluiten = []
    for b in bw["besluiten"]:
        nr = b["nr"]
        job_ids = [b["opdracht"]["job"]] if b.get("opdracht") else []
        if nr in GEDEELDE_VASTLEGGING:
            job_ids.append(GEDEELDE_VASTLEGGING[nr])
        jr = []
        for jid in job_ids:
            j = jobs.get(jid)
            jr.append({"job": jid, "node": j.get("node") if j else None, "taak": j.get("task_id") if j else None, "titel": j.get("title", "") if j else "",
                       "rol": "besluitopdracht", "aangemaakt": j.get("created_at") if j else None})
        prompt = None
        for jid in job_ids:
            if jid in jobs:
                prompt = jobs[jid]["prompt"]
                break
        tekst = {"exact_van_beheerder": None, "opdrachttekst": dossier.redact(prompt) if prompt else None,
                 "opdrachttekst_sha256": sha(prompt) if prompt else None, "opdracht_job": job_ids[0] if prompt else None}
        if prompt:
            tekst["herkomst"] = ("opdracht " + tekst["opdracht_job"] + "; formulering van de ChatGPT-supervisor in de wachtrij, niet bewezen woordelijk van Beheerder"
                                 + ("; gedeelde opdrachttekst voor de besluiten " + ", ".join(str(k) for k in GEDEELDE_VASTLEGGING) if nr in GEDEELDE_VASTLEGGING else ""))
        else:
            tekst["herkomst"] = "geen eigen opdracht in de Runner 2-historie; de woordelijke tekst van Beheerder is nergens vastgelegd gevonden"
        acties = {a: {"id": a, "relatie": "genoemd-in-besluit", "zekerheid": "bewezen", "bron": "bewijsbestand besluit 50 (besluiten[].actie_ids)",
                      "status": (b.get("actie_status") or {}).get(a, "onbekend")} for a in b["actie_ids"]}
        for a, v in acties_per_besluit.get(nr, {}).items():
            acties.setdefault(a, v)
        besluiten.append({
            "nr": nr, "onderwerp": b["onderwerp"], "datum": "2026-10-23",
            "datum_bron": "opdracht 49 van Beheerder: beslissingen 1-49 van 23 oktober 2026; exact tijdstip van het besluit niet vastgelegd",
            "nummer_zekerheid": b["zekerheid_nummer"], "tekst": tekst, "jobs": jr,
            "acties": sorted(acties.values(), key=lambda x: x["id"]),
            "statussen": {dim: {"status": "onbekend", "zekerheid": "onbekend", "bewijs": [], "bron": "", "peildatum": peil, "notitie": ""} for dim in DIMENSIES},
            "opmerkingen": [b["opmerking"]] if b.get("opmerking") else [],
        })
    reg = {"schema": SCHEMA, "reeks": "besluiten van Beheerder, 23 oktober 2026 (nummers 1-50)", "aangemaakt": stamp, "bijgewerkt": stamp,
           "canonieke_locatie": "docs/beslissingenregister/register.json (repository runner2-src, GERARD-projectdossier)",
           "bronnen": {
               "bewijsbestand_besluit_50": "docs/rapporten/2026-10-23-besluit-50-bewijs.json (momentopname 2026-10-23 09:20Z; bron voor de eerste aanmaak)",
               "wachtrij": f"{DEFAULT_QUEUE} (alleen gelezen; peil {peil})",
               "toelichting_per_besluit": "docs/besluiten.md (verhalend; geen statusbron meer)",
           },
           "projectdossiers": {
               "gerard": {"verwijzing_status": "onbekend", "pad": "docs/besluiten.md", "notitie": ""},
               "studio_obf": {"verwijzing_status": "ontbreekt", "pad": "docs/projectdossier/PROJECTDOSSIER.md (Studio OBF-repository op de NUC)",
                              "notitie": "Bevat de besluiten als D 23-1..D 23-5 en BESLUITEN-18-OKTOBER.md (andere reeks); verwijzing naar dit register volgt via een aparte opdracht."},
               "monitor2": {"verwijzing_status": "ontbreekt", "pad": "docs/besluiten-monitor2.md (repository monitor2-app)",
                            "notitie": "Heeft een eigen kort register (beslissing 43, 44, persoonsrechten); verwijzing naar dit register volgt via een aparte opdracht."},
           },
           "besluiten": besluiten}
    _write_atomic(d / "register.json", canon(reg))
    _write_atomic(d / "register.md", render(reg))
    _append_audit(d, [], {"ts": stamp, "update_id": "bootstrap-2026-10-23", "type": "bootstrap", "actor": actor,
                          "reden": "eerste aanmaak uit het bewijsbestand van besluit 50 en de wachtrij; alle statussen nog 'onbekend' tot de eerste mutaties",
                          "mutatie": None, "mutatie_sha256": None, "register_sha256": sha(canon(reg)),
                          "bronnen": {"bewijsbestand_sha256": sha(Path(bewijs).read_text(encoding="utf-8")), "wachtrij_peil": peil}})
    # beginstatussen via de gewone, geauditeerde route
    reg, _ = load(d)
    muts = [m for m in _seed_mutations(reg, bw, jobs, peil)]
    apply(muts, actor, d, stamp, reden_batch="beginstatus bij aanmaak")
    apply(refresh_mutations(load(d)[0], queue), actor, d, stamp, reden_batch="afleiding uit de wachtrij bij aanmaak")


def _seed_mutations(reg, bw, jobs, peil):
    by = {b["nr"]: b for b in bw["besluiten"]}
    for b in reg["besluiten"]:
        nr = b["nr"]
        src = by[nr]["status"]
        bron = "bewijsbestand besluit 50"
        if nr in UITVOERING:
            st, note = UITVOERING[nr]
            bewijs = [{"soort": "job", "ref": UITVOERING_BEWIJS[nr]}] if nr in UITVOERING_BEWIJS else []
            if st == "gedaan" and nr in UITVOERING_BEWIJS:
                j = jobs.get(UITVOERING_BEWIJS[nr]) or {}
                bewijs = _job_bewijs(j) if j else bewijs
            yield {"id": f"seed:{nr}:uitvoering", "op": "status", "besluit": nr, "dimensie": "uitvoering", "status": st, "bewijs": bewijs,
                   "zekerheid": "bewezen" if bewijs else "afgeleid", "bron": bron, "peildatum": peil, "notitie": note}
        elif b["jobs"] and any(j["job"] in jobs and jobs[j["job"]]["status"] in ("queued", "held") for j in b["jobs"]):
            yield {"id": f"seed:{nr}:uitvoering", "op": "status", "besluit": nr, "dimensie": "uitvoering", "status": "wacht", "zekerheid": "bewezen",
                   "bron": "wachtrij (opdracht wacht)", "peildatum": peil, "notitie": "Opdracht staat in de wachtrij; niets uitgevoerd."}
        else:
            yield {"id": f"seed:{nr}:uitvoering", "op": "status", "besluit": nr, "dimensie": "uitvoering", "status": "onbekend", "zekerheid": "onbekend",
                   "bron": bron, "peildatum": peil, "notitie": ("Geen eigen opdracht of bewijs van uitvoering in de Runner 2-historie." if not b["jobs"] else
                              "Technische uitvoering niet afgeleid: een afgeronde besluitopdracht bewijst die niet.")}
        if nr in (1, 2, 3):
            yield {"id": f"seed:{nr}:documentatie", "op": "status", "besluit": nr, "dimensie": "documentatie", "status": "gedeeltelijk", "zekerheid": "afgeleid",
                   "bron": bron, "peildatum": peil, "notitie": src["documentatie"][:300]}
        if nr == 49:
            yield {"id": "seed:49:documentatie", "op": "status", "besluit": 49, "dimensie": "documentatie", "status": "bezig", "zekerheid": "bewezen",
                   "bron": "dit register", "peildatum": peil, "notitie": "Register en procedure staan in deze opdracht; 'gedaan' volgt pas na integratie."}
        if nr in (1, 2, 3, 4):
            yield {"id": f"seed:{nr}:nummer", "op": "opmerking", "besluit": nr,
                   "tekst": "Studio OBF labelt de vastlegging als D 23-1 t/m D 23-5 (tweede reeks, J-5b0bb823f3e1); de koppeling aan dit nummer is afgeleid."}


# --- CLI ---

def _read_mutations(path):
    data = json.loads(Path(path).read_text(encoding="utf-8")) if path != "-" else json.load(sys.stdin)
    return data["mutaties"] if isinstance(data, dict) else data


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="python3 -m runner2.beslissingen", description=__doc__.split("\n")[0])
    ap.add_argument("--dir", default=str(REGISTER_DIR))
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("validate", help="controleert register, auditketen en weergave (exitcode 1 bij een fout)")
    sub.add_parser("render", help="schrijft register.md opnieuw uit register.json")
    a = sub.add_parser("apply", help="past een mutatiebestand toe (idempotent)")
    a.add_argument("bestand")
    a.add_argument("--actor", required=True)
    a.add_argument("--reden", default="")
    r = sub.add_parser("refresh", help="leidt documentatie/controle af uit de wachtrij (alleen gelezen); zonder --toepassen een droge run")
    r.add_argument("--queue", default=str(DEFAULT_QUEUE))
    r.add_argument("--toepassen", action="store_true")
    r.add_argument("--actor", default="refresh")
    o = sub.add_parser("adopt", help="legt een wijziging buiten de procedure om vast in het auditspoor")
    o.add_argument("--reden", required=True)
    o.add_argument("--actor", required=True)
    sub.add_parser("bootstrap", help="eenmalig: maakt het register aan")
    args = ap.parse_args(argv)
    try:
        if args.cmd == "bootstrap":
            bootstrap(args.dir)
            print("register aangemaakt")
        elif args.cmd == "validate":
            reg, audit = load(args.dir)
            fouten = validate(reg) + check_audit(reg, audit)
            if (Path(args.dir) / "register.md").read_text(encoding="utf-8") != render(reg):
                fouten.append("register.md is niet gelijk aan de weergave van register.json (draai `render`)")
            for x in fouten:
                print("FOUT:", x)
            print("register gezond" if not fouten else f"{len(fouten)} fout(en)")
            return 1 if fouten else 0
        elif args.cmd == "render":
            reg, _ = load(args.dir)
            _write_atomic(Path(args.dir) / "register.md", render(reg))
        elif args.cmd == "apply":
            print(json.dumps(apply(_read_mutations(args.bestand), args.actor, args.dir, reden_batch=args.reden), indent=1))
        elif args.cmd == "refresh":
            reg, _ = load(args.dir)
            muts = refresh_mutations(reg, load_queue(args.queue))
            if args.toepassen:
                print(json.dumps(apply(muts, args.actor, args.dir, reden_batch="afleiding uit de wachtrij"), indent=1))
            else:
                print(json.dumps(muts, indent=1, ensure_ascii=False))
        elif args.cmd == "adopt":
            print(json.dumps(adopt(args.reden, args.actor, args.dir)))
    except RegisterError as e:
        print("FOUT:", e, file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
