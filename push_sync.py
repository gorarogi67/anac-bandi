"""
push_sync.py — Sincronizza i nuovi record dal DB locale verso Railway

Uso:
  python push_sync.py                # invia i record nuovi dall'ultimo push
  python push_sync.py --sync-first   # esegue prima sync ANAC, poi push
  python push_sync.py --reset        # azzera last_push (rinvia tutto dall'inizio)

Configura RAILWAY_URL nel file .env oppure come variabile d'ambiente:
  set RAILWAY_URL=https://tuo-app.railway.app
"""

import sqlite3
import requests
import os
import sys
import logging
from datetime import datetime

from config import DB_PATH, SYNC_SECRET, DATA_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ── Configurazione ──
RAILWAY_URL = os.environ.get("RAILWAY_URL", "").rstrip("/")
BATCH_SIZE = 5000
LAST_PUSH_FILE = os.path.join(DATA_DIR, "last_push.txt")


def read_last_push() -> str:
    if os.path.exists(LAST_PUSH_FILE):
        with open(LAST_PUSH_FILE) as f:
            ts = f.read().strip()
            if ts:
                return ts
    return "1970-01-01T00:00:00"


def write_last_push(ts: str):
    with open(LAST_PUSH_FILE, "w") as f:
        f.write(ts)


def push():
    if not RAILWAY_URL:
        log.error("RAILWAY_URL non configurato!")
        log.error("Esegui: set RAILWAY_URL=https://tuo-app.railway.app")
        sys.exit(1)

    last_push = read_last_push()
    log.info(f"Ultimo push: {last_push}")
    log.info(f"Destinazione: {RAILWAY_URL}")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    count = conn.execute(
        "SELECT COUNT(*) FROM bandi WHERE data_import > ?", (last_push,)
    ).fetchone()[0]
    log.info(f"Record da inviare: {count:,}")

    if count == 0:
        log.info("Nessun record nuovo dal ultimo push. Fine.")
        conn.close()
        return

    now = datetime.now().isoformat()
    inviati_totale = 0
    offset = 0
    batch_num = 0

    while True:
        rows = conn.execute(
            "SELECT * FROM bandi WHERE data_import > ? ORDER BY data_import LIMIT ? OFFSET ?",
            (last_push, BATCH_SIZE, offset)
        ).fetchall()

        if not rows:
            break

        batch_num += 1
        records = [dict(r) for r in rows]
        log.info(f"  Batch {batch_num}: invio {len(records):,} record ({offset:,}–{offset+len(records):,} di {count:,})...")

        try:
            r = requests.post(
                f"{RAILWAY_URL}/api/import-records",
                params={"key": SYNC_SECRET},
                json={"records": records, "fonte": "push_locale"},
                timeout=180,
            )
        except requests.exceptions.RequestException as e:
            log.error(f"  Errore connessione: {e}")
            conn.close()
            sys.exit(1)

        if r.status_code != 200:
            log.error(f"  Errore HTTP {r.status_code}: {r.text[:300]}")
            conn.close()
            sys.exit(1)

        result = r.json()
        importati = result.get("importati", 0)
        inviati_totale += importati
        offset += BATCH_SIZE
        log.info(f"  OK: {importati:,} importati (totale finora: {inviati_totale:,})")

    conn.close()
    write_last_push(now)

    log.info("=" * 50)
    log.info(f"PUSH COMPLETATO")
    log.info(f"  Record inviati:  {inviati_totale:,}")
    log.info(f"  Prossimo push considererà record dal: {now}")
    log.info("=" * 50)


if __name__ == "__main__":
    args = sys.argv[1:]

    if "--reset" in args:
        if os.path.exists(LAST_PUSH_FILE):
            os.remove(LAST_PUSH_FILE)
            log.info("last_push.txt azzerato — il prossimo push invierà tutto")
        else:
            log.info("last_push.txt non esiste (già azzerato)")
        sys.exit(0)

    if "--sync-first" in args:
        log.info("Esecuzione sync ANAC prima del push...")
        from sync import sync as run_sync
        run_sync()

    push()
