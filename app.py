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
from database import init_db, query_bandi, query_bandi_province_agg, query_bandi_charts, query_albi_sa, upsert_albo_sa, get_filtri_disponibili, count_bandi, get_sync_log
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
        "esito": args.get("esito", ""),
        "provincia": args.get("provincia", ""),
    }
    return f


@app.route("/")
def index():
    init_db()
    return render_template("index.html",
        filtri=get_filtri_disponibili(), totale=count_bandi(),
        keywords_default=KEYWORDS_DEFAULT)


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
