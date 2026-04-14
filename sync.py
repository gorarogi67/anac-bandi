"""
sync.py — Scarica TUTTI i dati CIG e li importa nel database

NON filtra per keyword — importa tutto.
Il filtro avviene solo nell'interfaccia web al momento della query.

Uso:
  python sync.py              # sync incrementale (solo novità)
  python sync.py --force      # riscaricare tutto
  python sync.py --status     # mostra stato database
"""

import requests
import zipfile
import csv
import io
import os
import sys
import logging
from datetime import datetime
from typing import List, Dict

from config import CKAN_API, HEADERS, DATA_DIR, DB_PATH, DATASET_CIG_DELTA, DATASET_CIG_ANNUALE, ANNO_INIZIO
from database import (init_db, bulk_upsert, log_sync, is_already_synced, get_sync_log,
                       count_bandi, bulk_upsert_aggiudicatari, bulk_upsert_partecipanti,
                       count_aggiudicatari, count_partecipanti)

DATASET_SMARTCIG_DELTA   = "smartcig"
DATASET_SMARTCIG_ANNUALE = "smartcig-{anno}"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(DATA_DIR, "sync.log"), encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

# Session condivisa: mantiene cookie di sessione ottenuti visitando il sito
_session = None

def get_session() -> requests.Session:
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update(HEADERS)
        try:
            log.info("Inizializzazione sessione HTTP (warm-up cookie)...")
            r = _session.get("https://dati.anticorruzione.it/opendata/", timeout=30)
            log.info(f"  Warm-up: status={r.status_code}, cookie={list(r.cookies.keys())}")
        except Exception as e:
            log.warning(f"  Warm-up fallito (continuo comunque): {e}")
    return _session


def ckan_get(action, params=None):
    url = f"{CKAN_API}/{action}"
    try:
        r = get_session().get(url, params=params, timeout=60)
        ct = r.headers.get("Content-Type", "")
        if r.status_code == 200 and "json" in ct:
            body = r.json()
            if body.get("success"):
                return body["result"]
            log.warning(f"API {action}: success=false — {body.get('error')}")
        elif r.status_code == 403:
            log.warning(f"API {action}: 403 Forbidden (WAF/IP block) — la scoperta risorsa via CKAN non è disponibile")
        else:
            log.warning(f"API {action}: status={r.status_code} type={ct}")
        return None
    except Exception as e:
        log.warning(f"API {action}: {e}")
        return None


def scopri_risorse() -> List[dict]:
    risorse = []
    known_names = set()
    anno_corrente = datetime.now().year

    nomi = [DATASET_CIG_DELTA]
    for a in range(anno_corrente, ANNO_INIZIO - 1, -1):
        nomi.append(DATASET_CIG_ANNUALE.format(anno=a))

    for nome in nomi:
        ds = ckan_get("package_show", {"id": nome})
        if not ds:
            continue
        for r in ds.get("resources", []):
            fmt = (r.get("format") or "").upper()
            rname = r.get("name") or ""
            url = r.get("url") or ""
            if ("CSV" in fmt or "csv" in rname.lower()) and "logCsv" not in rname:
                risorse.append({"dataset": nome, "name": rname, "url": url})
                known_names.add(rname)
                log.info(f"  [{nome}] {rname}")

    # FALLBACK totale: se CKAN non risponde usa solo URL diretti
    if not risorse:
        log.info("API CKAN non disponibile — uso URL diretti (fallback)")
        return _url_diretti_fallback()

    # Integra sempre con URL diretti per file non trovati via CKAN
    # (es. delta mesi precedenti e dataset anno corrente non ancora su CKAN)
    aggiunti = 0
    for r in _url_diretti_fallback():
        if r["name"] not in known_names:
            risorse.append(r)
            known_names.add(r["name"])
            aggiunti += 1
            log.info(f"  [direct] {r['name']}")
    if aggiunti:
        log.info(f"  Aggiunti {aggiunti} file via URL diretto (non in CKAN)")

    log.info(f"Risorse CSV scoperte: {len(risorse)}")
    return risorse


def _url_diretti_fallback() -> List[dict]:
    """
    Costruisce URL diretti per i dataset CIG (pattern dal manuale ANAC).
    Testato: gli URL di download funzionano anche quando le API CKAN no.
    """
    BASE = "https://dati.anticorruzione.it/opendata/download/dataset"
    risorse = []
    anno = datetime.now().year
    mese = datetime.now().month

    # Delta mensili recenti (ultimi 6 mesi)
    for i in range(6):
        m = mese - i
        a = anno
        if m <= 0:
            m += 12
            a -= 1
        name = f"{a}{m:02d}01-cig_csv"
        url = f"{BASE}/cig/filesystem/{name}.zip"
        risorse.append({"dataset": "cig", "name": name, "url": url})

    # Dataset annuali (ogni mese, dal più recente)
    for a in range(anno, ANNO_INIZIO - 1, -1):
        max_m = mese if a == anno else 12  # non generare mesi futuri per l'anno corrente
        for m in range(1, max_m + 1):
            name = f"cig_csv_{a}_{m:02d}"
            url = f"{BASE}/cig-{a}/filesystem/{name}.zip"
            risorse.append({"dataset": f"cig-{a}", "name": name, "url": url})

    log.info(f"  URL diretti generati: {len(risorse)}")
    return risorse


def scarica(url: str) -> bytes | None:
    try:
        r = get_session().get(url, timeout=300, stream=True)
        if r.status_code == 404:
            log.info(f"  404 — non esiste: ...{url[-50:]}")
            return None
        if r.status_code == 403:
            log.warning(f"  403 Forbidden (WAF/IP block?) — ...{url[-60:]}")
            return None
        r.raise_for_status()
        chunks = []
        total = 0
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            chunks.append(chunk)
            total += len(chunk)
            if total % (10 * 1024 * 1024) < 1024 * 1024:
                log.info(f"    {total // (1024*1024)} MB...")
        log.info(f"  Scaricato: {total // 1024} KB")
        return b"".join(chunks)
    except Exception as e:
        log.error(f"  Download fallito: {e}")
        return None


def parse_csv(content: bytes) -> List[dict]:
    """Parsa TUTTI i record dal CSV/ZIP senza alcun filtro."""
    records = []

    def process(file_obj):
        pos = file_obj.tell()
        sample = file_obj.read(4000)
        file_obj.seek(pos)
        delim = ";" if sample.count(";") > sample.count(",") else ","
        reader = csv.DictReader(file_obj, delimiter=delim)
        if not reader.fieldnames:
            return
        for row in reader:
            records.append(dict(row))

    if content[:2] == b"PK":
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            for name in zf.namelist():
                if name.lower().endswith(".csv"):
                    with zf.open(name) as f:
                        process(io.TextIOWrapper(f, encoding="utf-8-sig"))
    else:
        process(io.StringIO(content.decode("utf-8-sig", errors="replace")))

    return records


def sync(force=False):
    log.info("=" * 60)
    log.info("SYNC ANAC — Import COMPLETO (tutti i record, nessun filtro)")
    log.info("=" * 60)

    init_db()

    log.info("Scoperta risorse via API CKAN...")
    risorse = scopri_risorse()
    if not risorse:
        log.error("Nessuna risorsa trovata!")
        return

    nuove = 0
    saltate = 0
    totale_importati = 0

    anno_corrente = str(datetime.now().year)

    for r in risorse:
        nome = r["name"]

        # Risorse di anni passati: skip permanente (non cambieranno più)
        # Risorse dell'anno corrente o delta: ricontrolla ogni 20h
        is_current = anno_corrente in nome or r["dataset"] == DATASET_CIG_DELTA
        max_age = 20 if is_current else 999999

        if not force and is_already_synced(nome, max_age_hours=max_age):
            log.info(f"  [{nome}] già sincronizzato, salto")
            saltate += 1
            continue

        log.info(f"\n{'─'*50}")
        log.info(f"[{nome}] ({r['dataset']})")

        content = scarica(r["url"])
        if not content:
            continue

        log.info("  Parsing CSV...")
        records = parse_csv(content)
        log.info(f"  {len(records):,} record trovati")

        # Filtra solo record dal 2025 in poi
        before = len(records)
        records = [rec for rec in records
                   if str(rec.get("anno_pubblicazione") or rec.get("ANNO_PUBBLICAZIONE") or "").strip() >= "2025"]
        if len(records) < before:
            log.info(f"  {before - len(records):,} record pre-2025 esclusi, {len(records):,} mantenuti")

        if records:
            log.info("  Import in database...")
            n = bulk_upsert(records, fonte=nome)
            totale_importati += n
            log.info(f"  {n:,} record importati/aggiornati")

        log_sync(nome, r["dataset"], r["url"], len(content), len(records))
        nuove += 1

    # Sync aggiudicatari e partecipanti
    sync_aggiudicatari_partecipanti(force=force)

    # Sync SmartCIG
    sync_smartcig(force=force)

    tot_db = count_bandi()
    db_size = os.path.getsize(DB_PATH) // (1024 * 1024) if os.path.exists(DB_PATH) else 0
    log.info(f"\n{'='*60}")
    log.info("SYNC COMPLETATO")
    log.info(f"  Risorse scaricate:        {nuove}")
    log.info(f"  Risorse saltate (già ok): {saltate}")
    log.info(f"  Record importati ora:     {totale_importati:,}")
    log.info(f"  Totale record in DB:      {tot_db:,}")
    log.info(f"  Dimensione DB:            {db_size} MB")
    log.info(f"{'='*60}")


def sync_aggiudicatari_partecipanti(force=False):
    """
    Scarica e importa aggiudicatari e partecipanti ANAC.
    - Prima esecuzione: scarica il file completo (storico)
    - Esecuzioni successive: solo delta mensili nuovi
    """
    BASE = "https://dati.anticorruzione.it/opendata/download/dataset"
    anno = datetime.now().year
    mese = datetime.now().month

    for tipo in ["aggiudicatari", "partecipanti"]:
        upsert_fn  = bulk_upsert_aggiudicatari if tipo == "aggiudicatari" else bulk_upsert_partecipanti
        count_fn   = count_aggiudicatari       if tipo == "aggiudicatari" else count_partecipanti
        log.info(f"\n{'─'*50}")
        log.info(f"Sync {tipo}...")

        # 1. Import iniziale: file completo se tabella vuota
        full_key = f"{tipo}_full_init"
        if force or not is_already_synced(full_key, max_age_hours=999999):
            if count_fn() == 0 or force:
                url = f"{BASE}/{tipo}/filesystem/{tipo}_csv.zip"
                log.info(f"  [{tipo}] Prima esecuzione — scarico file completo...")
                content = scarica(url)
                if content:
                    records = parse_csv(content)
                    log.info(f"  {len(records):,} record nel file completo")
                    n = upsert_fn(records)
                    log.info(f"  {n:,} importati (filtrati per CIG in DB)")
                    log_sync(full_key, tipo, url, len(content), n)

        # 2. Delta mensili recenti (ultimi 6 mesi)
        for i in range(6):
            m = mese - i
            a = anno
            if m <= 0:
                m += 12
                a -= 1
            name = f"{a}{m:02d}01-{tipo}_csv"
            is_current = (i == 0)
            max_age = 20 if is_current else 999999

            if not force and is_already_synced(name, max_age_hours=max_age):
                log.info(f"  [{name}] già sincronizzato, salto")
                continue

            url = f"{BASE}/{tipo}/filesystem/{name}.zip"
            content = scarica(url)
            if not content:
                continue

            records = parse_csv(content)
            log.info(f"  {len(records):,} record nel delta")
            n = upsert_fn(records)
            log.info(f"  {n:,} importati")
            log_sync(name, tipo, url, len(content), n)


def _normalize_smartcig_record(rec: dict) -> dict:
    """
    Rinomina i campi SmartCIG per compatibilità con lo schema bandi.
    SmartCIG ha 26 colonne (vs 62 dei CIG) con nomi leggermente diversi.
    """
    rk = {k.lower().strip(): v for k, v in rec.items()}
    # Mappa campi data
    if "data_comunicazione" in rk and "data_pubblicazione" not in rk:
        rk["data_pubblicazione"] = rk.pop("data_comunicazione")
    if "anno_comunicazione" in rk and "anno_pubblicazione" not in rk:
        rk["anno_pubblicazione"] = rk.pop("anno_comunicazione")
    if "mese_comunicazione" in rk and "mese_pubblicazione" not in rk:
        rk["mese_pubblicazione"] = rk.pop("mese_comunicazione")
    # Mappa luogo
    if "istat_comune" in rk and not rk.get("luogo_istat"):
        rk["luogo_istat"] = rk.get("istat_comune")
    # Usa città come provincia (SmartCIG non ha campo provincia)
    if "citta" in rk and not rk.get("provincia"):
        rk["provincia"] = rk.get("citta")
    return rk


def sync_smartcig(force=False):
    """
    Scarica e importa SmartCIG (affidamenti sotto soglia).
    Stessa logica di sync(): delta mensili + dataset annuali.
    I record vengono importati nella tabella bandi con tipo='smartcig'.
    """
    BASE = "https://dati.anticorruzione.it/opendata/download/dataset"
    anno = datetime.now().year
    mese = datetime.now().month
    anno_corrente = str(anno)

    log.info(f"\n{'─'*50}")
    log.info("Sync SmartCIG (affidamenti sotto soglia)...")

    risorse = []
    known = set()

    # Scoperta via CKAN
    for nome_ds in [DATASET_SMARTCIG_DELTA] + [DATASET_SMARTCIG_ANNUALE.format(anno=a)
                                                for a in range(anno, ANNO_INIZIO - 1, -1)]:
        ds = ckan_get("package_show", {"id": nome_ds})
        if not ds:
            continue
        for r in ds.get("resources", []):
            fmt = (r.get("format") or "").upper()
            rname = r.get("name") or ""
            url = r.get("url") or ""
            if ("CSV" in fmt or "csv" in rname.lower()) and "logCsv" not in rname:
                risorse.append({"dataset": nome_ds, "name": rname, "url": url})
                known.add(rname)
                log.info(f"  [CKAN {nome_ds}] {rname}")

    # URL diretti — delta mensili (ultimi 6 mesi)
    for i in range(6):
        m = mese - i
        a = anno
        if m <= 0:
            m += 12
            a -= 1
        name = f"{a}{m:02d}01-smartcig_csv"
        url = f"{BASE}/smartcig/filesystem/{name}.zip"
        if name not in known:
            risorse.append({"dataset": "smartcig", "name": name, "url": url})
            known.add(name)

    # URL diretti — dataset annuali mensili
    for a in range(anno, ANNO_INIZIO - 1, -1):
        max_m = mese if a == anno else 12
        for m in range(1, max_m + 1):
            name = f"smartcig_csv_{a}_{m:02d}"
            url = f"{BASE}/smartcig-{a}/filesystem/{name}.zip"
            if name not in known:
                risorse.append({"dataset": f"smartcig-{a}", "name": name, "url": url})
                known.add(name)

    log.info(f"SmartCIG — risorse candidate: {len(risorse)}")

    totale = 0
    for r in risorse:
        nome = r["name"]
        is_current = anno_corrente in nome or r["dataset"] == DATASET_SMARTCIG_DELTA
        max_age = 20 if is_current else 999999

        if not force and is_already_synced(nome, max_age_hours=max_age):
            log.info(f"  [{nome}] già sincronizzato, salto")
            continue

        log.info(f"  [{nome}] ({r['dataset']})")
        content = scarica(r["url"])
        if not content:
            continue

        records = parse_csv(content)
        log.info(f"  {len(records):,} record trovati")

        # Normalizza campi SmartCIG → schema bandi
        records = [_normalize_smartcig_record(rec) for rec in records]

        # Filtra pre-ANNO_INIZIO
        before = len(records)
        records = [rec for rec in records
                   if str(rec.get("anno_pubblicazione") or "").strip() >= str(ANNO_INIZIO)]
        if len(records) < before:
            log.info(f"  {before - len(records):,} record pre-{ANNO_INIZIO} esclusi, {len(records):,} mantenuti")

        if records:
            n = bulk_upsert(records, fonte=nome, tipo='smartcig')
            totale += n
            log.info(f"  {n:,} SmartCIG importati/aggiornati")

        log_sync(nome, r["dataset"], r["url"], len(content), len(records))

    log.info(f"Sync SmartCIG completato: {totale:,} record totali importati")


def show_status():
    init_db()
    tot = count_bandi()
    db_size = os.path.getsize(DB_PATH) // (1024 * 1024) if os.path.exists(DB_PATH) else 0
    sync_entries = get_sync_log()
    print(f"\nDatabase: {tot:,} bandi ({db_size} MB)")
    print(f"Risorse sincronizzate: {len(sync_entries)}")
    if sync_entries:
        print(f"\nUltimi sync:")
        for s in sync_entries[:10]:
            print(f"  {s['download_date'][:16]} | {s['resource_name']} | "
                  f"{s['records_imported']:,} record")


if __name__ == "__main__":
    if "--status" in sys.argv:
        show_status()
    elif "--force" in sys.argv:
        sync(force=True)
    else:
        sync()
