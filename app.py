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
from flask import Flask, render_template, request, jsonify, send_file
from database import init_db, query_bandi, get_filtri_disponibili, count_bandi, get_sync_log
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

@app.route("/")
def index():
    init_db()
    return render_template("index.html",
        filtri=get_filtri_disponibili(), totale=count_bandi(),
        keywords_default=KEYWORDS_DEFAULT)


@app.route("/api/bandi")
def api_bandi():
    kw_str = request.args.get("keywords", "").strip()
    keywords = [k.strip() for k in kw_str.split(",") if k.strip()] if kw_str else []
    filters = {
        "keywords": keywords,
        "q": request.args.get("q", "").strip(),
        "anno": request.args.get("anno", ""),
        "esito": request.args.get("esito", ""),
        "provincia": request.args.get("provincia", ""),
        "sort": request.args.get("sort", "data_pubblicazione"),
        "order": request.args.get("order", "desc"),
    }
    try:
        page = int(request.args.get("page", 1))
    except:
        page = 1
    try:
        per_page = min(int(request.args.get("per_page", 50)), 500)
    except:
        per_page = 50
    if filters["anno"]:
        try:
            filters["anno"] = int(filters["anno"])
        except:
            filters["anno"] = ""

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
    kw_str = request.args.get("keywords", "").strip()
    keywords = [k.strip() for k in kw_str.split(",") if k.strip()] if kw_str else []
    filters = {
        "keywords": keywords, "q": request.args.get("q", "").strip(),
        "anno": request.args.get("anno", ""), "esito": request.args.get("esito", ""),
        "provincia": request.args.get("provincia", ""),
        "sort": request.args.get("sort", "data_pubblicazione"),
        "order": request.args.get("order", "desc"),
    }
    if filters["anno"]:
        try:
            filters["anno"] = int(filters["anno"])
        except:
            filters["anno"] = ""
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
    conn.close()
    return jsonify(stats)


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

# Avvia scheduler
avvia_scheduler()

if __name__ == "__main__":
    tot = count_bandi()
    print(f"\n  Bandi in database: {tot:,}")
    print(f"  Apri: http://localhost:{PORT}")
    print(f"  Sync manuale: http://localhost:{PORT}/api/sync?key={SYNC_SECRET}")
    print(f"  Ctrl+C per fermare\n")
    app.run(host="0.0.0.0", port=PORT, debug=False)
