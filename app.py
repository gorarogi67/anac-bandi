"""
app.py — Server web + sync giornaliero automatico

Funziona sia in locale che su Railway:
  - Serve l'interfaccia web su PORT (default 5000)
  - Esegue sync automatico ogni giorno alle 3:00 AM
  - Endpoint /api/sync?key=XXX per sync manuale

Locale:  python app.py
Railway: gunicorn app:app (avviato dal Procfile)
"""
import os
import sys
import tempfile
import logging
import threading
from datetime import datetime
from flask import Flask, render_template, request, jsonify, send_file
from database import (init_db, query_bandi, query_bandi_province_agg, query_bandi_charts,
                       query_albi_sa, upsert_albo_sa, get_filtri_disponibili, count_bandi,
                       get_sync_log, query_aggiudicatari_partecipanti, query_top_aggiudicatari,
                       query_top_aggiudicatari_province)
from config import PORT, KEYWORDS_DEFAULT, SYNC_SECRET
import pandas as pd

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger(__name__)

app = Flask(__name__)

# ── Stato finalizzazione DB upload (async) ──
_finalize_state: dict = {"status": "idle", "result": None, "error": None}
_finalize_lock = threading.Lock()

# ── Sync scheduler ──
def avvia_scheduler():
    """Avvia il sync giornaliero in background."""
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        from sync import sync as run_sync

        scheduler = BackgroundScheduler()
        scheduler.add_job(run_sync, "cron", hour=3, minute=0, id="daily_sync")
        scheduler.start()
        log.info("Scheduler attivato: sync giornaliero alle 03:00")
    except ImportError:
        log.warning("APScheduler non installato — sync giornaliero disabilitato")
        log.warning("Installa con: pip install apscheduler")
    except Exception as e:
        log.warning(f"Scheduler non avviato: {e}")


# ── Routes ──

def _parse_filters(args):
    """Estrae e normalizza i filtri comuni dalla query string."""
    kw_str = args.get("keywords", "").strip()
    anni_str = args.get("anni", "").strip()
    anni = []
    for a in anni_str.split(","):
        a = a.strip()
        if a.isdigit():
            anni.append(int(a))
    f = {
        "keywords": [k.strip() for k in kw_str.split(",") if k.strip()] if kw_str else [],
        "kw_mode": args.get("kw_mode", "or"),
        "q": args.get("q", "").strip(),
        "anni": anni,
        "con_scadenza": args.get("con_scadenza", "") == "1",
        "cf_sa": args.get("cf_sa", "").strip(),
        "solo_ad": args.get("solo_ad", "") == "1",
        "esito": args.get("esito", ""),
        "provincia": args.get("provincia", ""),
        "tipo": args.get("tipo", ""),
        "cf_aggiudicatario": args.get("cf_aggiudicatario", "").strip(),
    }
    return f


@app.route("/")
def index():
    init_db()
    return render_template("index.html",
        filtri=get_filtri_disponibili(), totale=count_bandi(),
        keywords_default=KEYWORDS_DEFAULT, sync_secret=SYNC_SECRET)


@app.route("/api/bandi")
def api_bandi():
    filters = _parse_filters(request.args)
    filters["sort"] = request.args.get("sort", "data_pubblicazione")
    filters["order"] = request.args.get("order", "desc")
    try:
        page = int(request.args.get("page", 1))
    except:
        page = 1
    try:
        per_page = min(int(request.args.get("per_page", 50)), 500)
    except:
        per_page = 50

    records, total = query_bandi(filters, limit=per_page, offset=(page - 1) * per_page)
    for r in records:
        for k in list(r.keys()):
            if r[k] is None:
                r[k] = ""
    return jsonify({
        "records": records, "total": total, "page": page,
        "per_page": per_page, "pages": max(1, (total + per_page - 1) // per_page),
    })


@app.route("/api/export")
def api_export():
    filters = _parse_filters(request.args)
    filters["sort"] = request.args.get("sort", "data_pubblicazione")
    filters["order"] = request.args.get("order", "desc")
    records, total = query_bandi(filters, limit=50000, offset=0)
    df = pd.DataFrame(records)
    if "fonte" in df.columns:
        df.drop(columns=["fonte"], inplace=True)
    tmp = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False)
    df.to_excel(tmp.name, index=False, sheet_name="Bandi ANAC")
    tmp.close()
    return send_file(tmp.name, as_attachment=True,
                     download_name=f"bandi_anac_{total}.xlsx",
                     mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


@app.route("/api/stats")
def api_stats():
    from database import get_conn
    conn = get_conn()
    stats = {
        "totale": conn.execute("SELECT COUNT(*) FROM bandi").fetchone()[0],
        "in_corso": conn.execute("SELECT COUNT(*) FROM bandi WHERE esito IS NULL OR esito=''").fetchone()[0],
        "aggiudicati": conn.execute("SELECT COUNT(*) FROM bandi WHERE esito='AGGIUDICATA'").fetchone()[0],
    }
    sl = get_sync_log()
    if sl:
        stats["ultimo_sync"] = sl[0]["download_date"]
        stats["risorse_sync"] = len(sl)
    stats["server_date"] = datetime.now().isoformat()
    conn.close()
    return jsonify(stats)


@app.route("/api/bando/<cig>")
def api_bando_dettaglio(cig):
    """Restituisce aggiudicatari e partecipanti per un CIG."""
    return jsonify(query_aggiudicatari_partecipanti(cig))


@app.route("/api/top-aggiudicatari")
def api_top_aggiudicatari():
    filters = _parse_filters(request.args)
    return jsonify(query_top_aggiudicatari(filters))


@app.route("/api/sync-log")
def api_sync_log():
    """Restituisce il dettaglio di tutti i file sincronizzati (sync_log)."""
    sl = get_sync_log()
    return jsonify(sl)


@app.route("/api/reindex")
def api_reindex():
    """Crea indici mancanti sul DB esistente (protetto da chiave)."""
    key = request.args.get("key", "")
    if key != SYNC_SECRET:
        return jsonify({"error": "Chiave non valida"}), 403
    from database import get_conn
    conn = get_conn()
    idxs = [
        "CREATE INDEX IF NOT EXISTS idx_anno_esito ON bandi(anno_pubblicazione, esito)",
        "CREATE INDEX IF NOT EXISTS idx_anno_scad ON bandi(anno_pubblicazione, data_scadenza_offerta)",
        "CREATE INDEX IF NOT EXISTS idx_scad ON bandi(data_scadenza_offerta)",
        "CREATE INDEX IF NOT EXISTS idx_com_esito ON bandi(data_comunicazione_esito)",
    ]
    for sql in idxs:
        conn.execute(sql)
    conn.commit()
    conn.close()
    return jsonify({"ok": True, "indici": len(idxs)})


# Coordinate centroidi province italiane (nome completo maiuscolo → lat, lng)
PROVINCE_COORDS = {
    "AGRIGENTO":(37.318,13.577),"ALESSANDRIA":(44.912,8.613),"ANCONA":(43.616,13.519),
    "AOSTA":(45.737,7.321),"VALLE D'AOSTA":(45.737,7.321),"ASCOLI PICENO":(42.847,13.575),
    "L'AQUILA":(42.350,13.400),"AQUILA":(42.350,13.400),"AREZZO":(43.464,11.880),
    "ASTI":(44.898,8.207),"AVELLINO":(40.914,15.053),"BARI":(41.117,16.872),
    "BERGAMO":(45.698,9.677),"BIELLA":(45.563,8.058),"BELLUNO":(46.140,12.217),
    "BENEVENTO":(41.130,14.778),"BOLOGNA":(44.494,11.343),"BRINDISI":(40.633,17.942),
    "BRESCIA":(45.542,10.212),"BARLETTA-ANDRIA-TRANI":(41.228,16.295),"BARLETTA ANDRIA TRANI":(41.228,16.295),
    "BOLZANO":(46.498,11.355),"CAGLIARI":(39.224,9.122),"CAMPOBASSO":(41.561,14.668),
    "CASERTA":(41.078,14.332),"CHIETI":(42.356,14.176),"CALTANISSETTA":(37.491,13.992),
    "CUNEO":(44.394,7.549),"COMO":(45.808,9.085),"CREMONA":(45.133,9.999),"COSENZA":(39.301,16.253),
    "CATANIA":(37.502,15.087),"CATANZARO":(38.909,16.588),"ENNA":(37.564,14.276),
    "FORLI'-CESENA":(44.223,12.041),"FORLI CESENA":(44.223,12.041),"FERRARA":(44.838,11.620),
    "FOGGIA":(41.462,15.545),"FIRENZE":(43.770,11.256),"FERMO":(43.156,13.723),
    "FROSINONE":(41.638,13.339),"GENOVA":(44.406,8.946),"GORIZIA":(45.940,13.622),
    "GROSSETO":(42.764,11.112),"IMPERIA":(43.892,8.018),"ISERNIA":(41.591,14.232),
    "CROTONE":(39.086,17.125),"LECCO":(45.857,9.397),"LECCE":(40.353,18.174),
    "LIVORNO":(43.551,10.311),"LODI":(45.310,9.504),"LATINA":(41.464,12.905),
    "LUCCA":(43.843,10.505),"MONZA E DELLA BRIANZA":(45.585,9.274),"MONZA E BRIANZA":(45.585,9.274),
    "MACERATA":(43.300,13.453),"MESSINA":(38.194,15.554),"MILANO":(45.465,9.186),
    "MANTOVA":(45.156,10.791),"MODENA":(44.647,10.925),"MASSA-CARRARA":(44.036,9.999),
    "MASSA CARRARA":(44.036,9.999),"MATERA":(40.665,16.604),"NAPOLI":(40.852,14.268),
    "NOVARA":(45.461,8.622),"NUORO":(40.320,9.327),"ORISTANO":(39.901,8.587),
    "PALERMO":(38.116,13.362),"PIACENZA":(44.994,9.695),"PADOVA":(45.406,11.877),
    "PESCARA":(42.462,14.216),"PERUGIA":(43.111,12.391),"PISA":(43.723,10.402),
    "PORDENONE":(46.063,12.664),"PRATO":(43.878,11.103),"PARMA":(44.801,10.328),
    "PISTOIA":(43.930,10.920),"PESARO E URBINO":(43.629,12.636),"PESARO-URBINO":(43.629,12.636),
    "PAVIA":(45.185,9.158),"POTENZA":(40.640,15.805),"RAVENNA":(44.418,12.204),
    "REGGIO CALABRIA":(38.111,15.647),"REGGIO DI CALABRIA":(38.111,15.647),
    "REGGIO EMILIA":(44.698,10.630),"REGGIO NELL'EMILIA":(44.698,10.630),
    "RAGUSA":(36.926,14.734),"RIETI":(42.404,12.856),"ROMA":(41.893,12.483),
    "RIMINI":(44.068,12.570),"ROVIGO":(45.071,11.790),"SALERNO":(40.682,14.768),
    "SIENA":(43.319,11.331),"SONDRIO":(46.170,9.871),"LA SPEZIA":(44.102,9.824),
    "SIRACUSA":(37.076,15.287),"SASSARI":(40.726,8.556),"SUD SARDEGNA":(39.357,9.017),
    "SAVONA":(44.307,8.482),"TARANTO":(40.476,17.230),"TERAMO":(42.659,13.704),
    "TRENTO":(46.075,11.122),"TORINO":(45.070,7.687),"TRAPANI":(37.769,12.537),
    "TERNI":(42.565,12.644),"TRIESTE":(45.650,13.777),"TREVISO":(45.667,12.244),
    "UDINE":(46.065,13.239),"VARESE":(45.821,8.826),"VERBANO-CUSIO-OSSOLA":(45.929,8.574),
    "VERBANO CUSIO OSSOLA":(45.929,8.574),"VERCELLI":(45.323,8.420),"VENEZIA":(45.441,12.316),
    "VICENZA":(45.546,11.535),"VERONA":(45.439,10.992),"VITERBO":(42.417,12.105),
    "VIBO VALENTIA":(38.675,16.103),
}


@app.route("/api/albi")
def api_albi():
    filters = _parse_filters(request.args)
    return jsonify(query_albi_sa(filters))


@app.route("/api/albi/<cf_sa>", methods=["POST"])
def api_albi_update(cf_sa):
    data = request.get_json(force=True)
    stato = data.get("stato", "DA_VERIFICARE")
    note = data.get("note", "")
    denominazione_sa = data.get("denominazione_sa", "")
    if stato not in ("PRESENTE", "ASSENTE", "DA_VERIFICARE"):
        return jsonify({"error": "stato non valido"}), 400
    upsert_albo_sa(cf_sa, denominazione_sa, stato, note)
    return jsonify({"ok": True})


@app.route("/api/chartsdata")
def api_chartsdata():
    filters = _parse_filters(request.args)
    return jsonify(query_bandi_charts(filters))


@app.route("/api/mapdata")
def api_mapdata():
    filters = _parse_filters(request.args)
    rows = query_bandi_province_agg(filters)
    result = []
    for row in rows:
        prov = (row["provincia"] or "").strip().upper()
        coords = PROVINCE_COORDS.get(prov)
        if not coords:
            continue
        result.append({
            "provincia": prov,
            "count": row["count"],
            "total_importo": row["total_importo"],
            "lat": coords[0],
            "lng": coords[1],
        })
    return jsonify(result)


@app.route("/api/aggiudicatari-mapdata")
def api_aggiudicatari_mapdata():
    filters = _parse_filters(request.args)
    data = query_top_aggiudicatari_province(filters, limit=5)
    for agg in data:
        enriched = []
        for p in agg["province"]:
            prov = (p["provincia"] or "").strip().upper()
            coords = PROVINCE_COORDS.get(prov)
            if coords:
                enriched.append({**p, "lat": coords[0], "lng": coords[1]})
        agg["province"] = enriched
    return jsonify(data)


@app.route("/api/reset-db")
def api_reset_db():
    """Azzera il DB e ripristina solo albi_fornitori (protetto da chiave)."""
    key = request.args.get("key", "")
    if key != SYNC_SECRET:
        return jsonify({"error": "Chiave non valida"}), 403

    from database import get_conn
    from config import DB_PATH
    import os

    try:
        # 1. Salva albi_fornitori in memoria
        conn = get_conn()
        albi = [dict(r) for r in conn.execute("SELECT * FROM albi_fornitori").fetchall()]
        conn.close()

        # 2. Cancella i file DB (inclusi WAL e SHM)
        for ext in ["", "-wal", "-shm"]:
            p = DB_PATH + ext
            if os.path.exists(p):
                os.remove(p)

        # 3. Ricrea il DB vuoto
        init_db()

        # 4. Ripristina albi_fornitori
        if albi:
            conn = get_conn()
            for r in albi:
                conn.execute("""
                    INSERT OR IGNORE INTO albi_fornitori
                    (cf_sa, denominazione_sa, stato, note, data_aggiornamento)
                    VALUES (?, ?, ?, ?, ?)
                """, (r["cf_sa"], r["denominazione_sa"], r["stato"], r["note"], r["data_aggiornamento"]))
            conn.commit()
            conn.close()

        log.info(f"DB azzerato. Albi fornitori ripristinati: {len(albi)}")
        return jsonify({"ok": True, "albi_ripristinati": len(albi)})

    except Exception as e:
        log.error(f"Reset DB fallito: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/import-records", methods=["POST"])
def api_import_records():
    """Riceve record dal PC locale e li importa nel DB (protetto da chiave)."""
    key = request.args.get("key", "")
    if key != SYNC_SECRET:
        return jsonify({"error": "Chiave non valida"}), 403

    import gzip as _gzip, json as _json
    raw = request.data
    if request.headers.get("Content-Encoding") == "gzip":
        raw = _gzip.decompress(raw)
    data = _json.loads(raw)
    records = data.get("records", [])
    fonte = data.get("fonte", "push_locale")

    if not records:
        return jsonify({"ok": True, "importati": 0})

    from database import bulk_upsert
    n = bulk_upsert(records, fonte=fonte)
    log.info(f"import-records: {n} record importati da {fonte}")
    return jsonify({"ok": True, "importati": n})


@app.route("/api/upload-db-chunk", methods=["POST"])
def api_upload_db_chunk():
    """Riceve un chunk del file DB (gzip a pezzi).
    Params: key, chunk_index (0-based), total_chunks, upload_id
    """
    key = request.args.get("key", "")
    if key != SYNC_SECRET:
        return jsonify({"error": "Chiave non valida"}), 403

    from config import DB_PATH, DATA_DIR
    import os

    chunk_index = int(request.args.get("chunk_index", 0))
    upload_id   = request.args.get("upload_id", "default")

    chunks_dir = os.path.join(DATA_DIR, f"upload_{upload_id}")
    os.makedirs(chunks_dir, exist_ok=True)
    chunk_path = os.path.join(chunks_dir, f"{chunk_index:05d}.bin")

    data = request.data
    with open(chunk_path, "wb") as f:
        f.write(data)

    log.info(f"upload-db-chunk: chunk {chunk_index} ricevuto ({len(data):,} B) → {chunk_path}")
    return jsonify({"ok": True, "chunk": chunk_index, "size": len(data)})


def _run_finalize_bg(upload_id: str, total_chunks: int):
    """Eseguito in background thread. Aggiorna _finalize_state al completamento."""
    global _finalize_state
    from database import get_conn
    from config import DB_PATH, DATA_DIR
    import zlib, sqlite3, shutil

    chunks_dir = os.path.join(DATA_DIR, f"upload_{upload_id}")
    new_path   = DB_PATH + ".new"

    try:
        # 1. Backup albi_fornitori dal DB corrente
        albi = []
        try:
            conn = get_conn()
            albi = [dict(r) for r in conn.execute("SELECT * FROM albi_fornitori").fetchall()]
            conn.close()
        except Exception as e:
            log.warning(f"finalize: impossibile leggere albi_fornitori: {e}")

        # 2. Verifica presenza di tutti i chunk
        present = sorted(int(f.split(".")[0]) for f in os.listdir(chunks_dir) if f.endswith(".bin"))
        missing = [i for i in range(total_chunks) if i not in present]
        if missing:
            raise RuntimeError(f"Chunk mancanti: {missing}")

        # 3. Elimina subito il DB corrente + WAL/SHM per liberare spazio
        log.info("finalize: elimino DB corrente per fare spazio al nuovo...")
        for p in [DB_PATH, DB_PATH + "-wal", DB_PATH + "-shm"]:
            if os.path.exists(p):
                try: os.remove(p)
                except Exception: pass

        # 4. Decomprimi i chunk in streaming direttamente nel nuovo DB
        #    wbits=31 → gzip; ogni chunk viene eliminato appena letto
        log.info(f"finalize: decomprimo {total_chunks} chunk in streaming → {new_path}...")
        dec = zlib.decompressobj(wbits=31)
        with open(new_path, "wb") as db_out:
            for i in range(total_chunks):
                chunk_path = os.path.join(chunks_dir, f"{i:05d}.bin")
                with open(chunk_path, "rb") as fin:
                    raw = fin.read()
                db_out.write(dec.decompress(raw))
                try: os.remove(chunk_path)   # libera ~80 MB progressivamente
                except Exception: pass
            tail = dec.flush()
            if tail:
                db_out.write(tail)

        bytes_out = os.path.getsize(new_path)
        log.info(f"finalize: scritti {bytes_out:,} B ({bytes_out//(1024**3):.1f} GB)")

        # 5. Valida il nuovo DB
        test = sqlite3.connect(new_path)
        n = test.execute("SELECT COUNT(*) FROM bandi").fetchone()[0]
        test.close()

        # 6. Swap atomico
        os.replace(new_path, DB_PATH)

        # 7. Assicura che tutte le tabelle e gli indici esistano
        init_db()

        # 8. Ripristina albi_fornitori
        ripristinati = 0
        if albi:
            conn = get_conn()
            for r in albi:
                conn.execute("""
                    INSERT OR REPLACE INTO albi_fornitori
                    (cf_sa, denominazione_sa, stato, note, data_aggiornamento)
                    VALUES (?, ?, ?, ?, ?)
                """, (r["cf_sa"], r["denominazione_sa"], r["stato"],
                      r["note"], r["data_aggiornamento"]))
            conn.commit()
            conn.close()
            ripristinati = len(albi)

        # 9. Pulizia chunks_dir residua
        try: shutil.rmtree(chunks_dir)
        except Exception: pass

        log.info(f"finalize: DB sostituito — {n:,} bandi, {ripristinati} albi ripristinati")
        with _finalize_lock:
            _finalize_state = {
                "status": "done",
                "result": {"bandi": n, "bytes_written": bytes_out, "albi_ripristinati": ripristinati},
                "error": None,
            }

    except Exception as e:
        if os.path.exists(new_path):
            try: os.remove(new_path)
            except Exception: pass
        log.error(f"finalize fallito: {e}")
        with _finalize_lock:
            _finalize_state = {"status": "error", "result": None, "error": str(e)}


@app.route("/api/upload-db-finalize", methods=["POST"])
def api_upload_db_finalize():
    """Avvia la finalizzazione in background e risponde subito (evita proxy timeout 60s).

    Il client deve fare polling su /api/upload-db-status finché status != 'running'.
    """
    global _finalize_state

    key = request.args.get("key", "")
    if key != SYNC_SECRET:
        return jsonify({"error": "Chiave non valida"}), 403

    upload_id    = request.args.get("upload_id", "default")
    total_chunks = int(request.args.get("total_chunks", 0))

    with _finalize_lock:
        if _finalize_state["status"] == "running":
            return jsonify({"ok": True, "status": "running", "message": "Finalizzazione già in corso"})
        _finalize_state = {"status": "running", "result": None, "error": None}

    t = threading.Thread(target=_run_finalize_bg, args=(upload_id, total_chunks), daemon=True)
    t.start()
    log.info(f"finalize: avviato in background (upload_id={upload_id}, chunks={total_chunks})")
    return jsonify({"ok": True, "status": "running", "message": "Finalizzazione avviata in background"})


@app.route("/api/upload-db-status")
def api_upload_db_status():
    """Polling endpoint per lo stato della finalizzazione DB."""
    key = request.args.get("key", "")
    if key != SYNC_SECRET:
        return jsonify({"error": "Chiave non valida"}), 403

    with _finalize_lock:
        state = dict(_finalize_state)

    if state["status"] == "done":
        return jsonify({"ok": True, "status": "done", **(state["result"] or {})})
    elif state["status"] == "error":
        return jsonify({"ok": False, "status": "error", "error": state["error"]}), 500
    else:
        return jsonify({"ok": True, "status": state["status"]})


@app.route("/api/upload-db-cleanup", methods=["POST"])
def api_upload_db_cleanup():
    """Elimina tutti i chunk di upload in sospeso per liberare spazio su disco."""
    key = request.args.get("key", "")
    if key != SYNC_SECRET:
        return jsonify({"error": "Chiave non valida"}), 403

    from config import DATA_DIR
    import shutil

    removed = []
    freed_bytes = 0
    try:
        for name in os.listdir(DATA_DIR):
            if name.startswith("upload_"):
                d = os.path.join(DATA_DIR, name)
                if os.path.isdir(d):
                    size = sum(
                        os.path.getsize(os.path.join(d, f))
                        for f in os.listdir(d)
                        if os.path.isfile(os.path.join(d, f))
                    )
                    shutil.rmtree(d)
                    removed.append(name)
                    freed_bytes += size
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    log.info(f"upload-db-cleanup: rimossi {len(removed)} upload dir, liberati {freed_bytes//(1024**2)} MB")
    return jsonify({"ok": True, "removed": removed, "freed_mb": freed_bytes // (1024 ** 2)})


@app.route("/api/sync")
def api_sync():
    """Endpoint per lanciare sync manuale (protetto da chiave)."""
    key = request.args.get("key", "")
    if key != SYNC_SECRET:
        return jsonify({"error": "Chiave non valida"}), 403

    def run_in_thread():
        from sync import sync as run_sync
        run_sync()

    t = threading.Thread(target=run_in_thread)
    t.start()
    return jsonify({"status": "Sync avviato in background"})


# ── Avvio ──

# Init DB all'import (per gunicorn)
init_db()

# Avvia scheduler — DISABILITATO (server in USA, ANAC blocca IP datacenter)
# avvia_scheduler()

if __name__ == "__main__":
    tot = count_bandi()
    print(f"\n  Bandi in database: {tot:,}")
    print(f"  Apri: http://localhost:{PORT}")
    print(f"  Sync manuale: http://localhost:{PORT}/api/sync?key={SYNC_SECRET}")
    print(f"  Ctrl+C per fermare\n")
    app.run(host="0.0.0.0", port=PORT, debug=False)
