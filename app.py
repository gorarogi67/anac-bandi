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
from database import init_db, query_bandi, query_bandi_province_agg, get_filtri_disponibili, count_bandi, get_sync_log
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
        "kw_mode": request.args.get("kw_mode", "or"),
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
        "keywords": keywords, "kw_mode": request.args.get("kw_mode", "or"),
        "q": request.args.get("q", "").strip(),
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
    stats["server_date"] = datetime.now().isoformat()
    conn.close()
    return jsonify(stats)


# Coordinate centroidi province italiane (codice 2 lettere → lat, lng)
PROVINCE_COORDS = {
    "AG":(37.318,13.577),"AL":(44.912,8.613),"AN":(43.616,13.519),"AO":(45.737,7.321),
    "AP":(42.847,13.575),"AQ":(42.350,13.400),"AR":(43.464,11.880),"AT":(44.898,8.207),
    "AV":(40.914,15.053),"BA":(41.117,16.872),"BG":(45.698,9.677),"BI":(45.563,8.058),
    "BL":(46.140,12.217),"BN":(41.130,14.778),"BO":(44.494,11.343),"BR":(40.633,17.942),
    "BS":(45.542,10.212),"BT":(41.228,16.295),"BZ":(46.498,11.355),"CA":(39.224,9.122),
    "CB":(41.561,14.668),"CE":(41.078,14.332),"CH":(42.356,14.176),"CL":(37.491,13.992),
    "CN":(44.394,7.549),"CO":(45.808,9.085),"CR":(45.133,9.999),"CS":(39.301,16.253),
    "CT":(37.502,15.087),"CZ":(38.909,16.588),"EN":(37.564,14.276),"FC":(44.223,12.041),
    "FE":(44.838,11.620),"FG":(41.462,15.545),"FI":(43.770,11.256),"FM":(43.156,13.723),
    "FR":(41.638,13.339),"GE":(44.406,8.946),"GO":(45.940,13.622),"GR":(42.764,11.112),
    "IM":(43.892,8.018),"IS":(41.591,14.232),"KR":(39.086,17.125),"LC":(45.857,9.397),
    "LE":(40.353,18.174),"LI":(43.551,10.311),"LO":(45.310,9.504),"LT":(41.464,12.905),
    "LU":(43.843,10.505),"MB":(45.585,9.274),"MC":(43.300,13.453),"ME":(38.194,15.554),
    "MI":(45.465,9.186),"MN":(45.156,10.791),"MO":(44.647,10.925),"MS":(44.036,9.999),
    "MT":(40.665,16.604),"NA":(40.852,14.268),"NO":(45.461,8.622),"NU":(40.320,9.327),
    "OR":(39.901,8.587),"PA":(38.116,13.362),"PC":(44.994,9.695),"PD":(45.406,11.877),
    "PE":(42.462,14.216),"PG":(43.111,12.391),"PI":(43.723,10.402),"PN":(46.063,12.664),
    "PO":(43.878,11.103),"PR":(44.801,10.328),"PT":(43.930,10.920),"PU":(43.629,12.636),
    "PV":(45.185,9.158),"PZ":(40.640,15.805),"RA":(44.418,12.204),"RC":(38.111,15.647),
    "RE":(44.698,10.630),"RG":(36.926,14.734),"RI":(42.404,12.856),"RM":(41.893,12.483),
    "RN":(44.068,12.570),"RO":(45.071,11.790),"SA":(40.682,14.768),"SI":(43.319,11.331),
    "SO":(46.170,9.871),"SP":(44.102,9.824),"SR":(37.076,15.287),"SS":(40.726,8.556),
    "SU":(39.357,9.017),"SV":(44.307,8.482),"TA":(40.476,17.230),"TE":(42.659,13.704),
    "TN":(46.075,11.122),"TO":(45.070,7.687),"TP":(37.769,12.537),"TR":(42.565,12.644),
    "TS":(45.650,13.777),"TV":(45.667,12.244),"UD":(46.065,13.239),"VA":(45.821,8.826),
    "VB":(45.929,8.574),"VC":(45.323,8.420),"VE":(45.441,12.316),"VI":(45.546,11.535),
    "VR":(45.439,10.992),"VT":(42.417,12.105),"VV":(38.675,16.103),
}


@app.route("/api/mapdata")
def api_mapdata():
    kw_str = request.args.get("keywords", "").strip()
    keywords = [k.strip() for k in kw_str.split(",") if k.strip()] if kw_str else []
    filters = {
        "keywords": keywords,
        "kw_mode": request.args.get("kw_mode", "or"),
        "q": request.args.get("q", "").strip(),
        "anno": request.args.get("anno", ""),
        "esito": request.args.get("esito", ""),
        "provincia": request.args.get("provincia", ""),
    }
    if filters["anno"]:
        try:
            filters["anno"] = int(filters["anno"])
        except:
            filters["anno"] = ""

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
