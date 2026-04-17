"""
database.py — Gestione database SQLite

Import TUTTO senza filtri. Filtro keyword solo al momento della query.
"""
import sqlite3
import logging
from datetime import datetime
from typing import List, Dict, Tuple
from config import DB_PATH

log = logging.getLogger(__name__)

# Colonne del CSV ANAC (tutte, in ordine)
DB_COLUMNS = [
    "cig", "cig_accordo_quadro", "numero_gara", "oggetto_gara",
    "importo_complessivo_gara", "n_lotti_componenti", "oggetto_lotto",
    "importo_lotto", "oggetto_principale_contratto", "stato", "settore",
    "luogo_istat", "provincia", "data_pubblicazione", "data_scadenza_offerta",
    "cod_tipo_scelta_contraente", "tipo_scelta_contraente",
    "cod_modalita_realizzazione", "modalita_realizzazione",
    "codice_ausa", "cf_amministrazione_appaltante",
    "denominazione_amministrazione_appaltante", "sezione_regionale",
    "id_centro_costo", "denominazione_centro_costo",
    "anno_pubblicazione", "mese_pubblicazione",
    "cod_cpv", "descrizione_cpv", "flag_prevalente",
    "cod_motivo_cancellazione", "motivo_cancellazione", "data_cancellazione",
    "data_ultimo_perfezionamento",
    "cod_modalita_indizione_speciali", "modalita_indizione_speciali",
    "cod_modalita_indizione_servizi", "modalita_indizione_servizi",
    "durata_prevista", "cod_strumento_svolgimento", "strumento_svolgimento",
    "flag_urgenza", "cod_motivo_urgenza", "motivo_urgenza",
    "flag_delega", "funzioni_delegate",
    "cf_sa_delegante", "denominazione_sa_delegante",
    "cf_sa_delegata", "denominazione_sa_delegata",
    "importo_sicurezza", "tipo_appalto_riservato", "cui_programma",
    "flag_prev_ripetizioni", "cod_ipotesi_collegamento", "ipotesi_collegamento",
    "cig_collegamento", "cod_esito", "esito", "data_comunicazione_esito",
    "flag_pnrr_pnc",
]


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-64000")  # 64MB cache
    return conn


def init_db():
    conn = get_conn()
    cols_sql = ",\n            ".join(f"{c} TEXT" for c in DB_COLUMNS)
    conn.executescript(f"""
        CREATE TABLE IF NOT EXISTS bandi (
            {cols_sql},
            fonte TEXT,
            data_import TEXT,
            tipo TEXT DEFAULT 'cig',
            PRIMARY KEY (cig)
        );

        CREATE TABLE IF NOT EXISTS sync_log (
            resource_name TEXT PRIMARY KEY,
            dataset TEXT,
            url TEXT,
            download_date TEXT,
            file_size INTEGER,
            records_imported INTEGER
        );

        CREATE INDEX IF NOT EXISTS idx_oggetto ON bandi(oggetto_lotto);
        CREATE INDEX IF NOT EXISTS idx_stato ON bandi(stato);
        CREATE INDEX IF NOT EXISTS idx_esito ON bandi(esito);
        CREATE INDEX IF NOT EXISTS idx_anno ON bandi(anno_pubblicazione);
        CREATE INDEX IF NOT EXISTS idx_provincia ON bandi(provincia);
        CREATE INDEX IF NOT EXISTS idx_sa ON bandi(denominazione_amministrazione_appaltante);
        CREATE INDEX IF NOT EXISTS idx_data_pub ON bandi(data_pubblicazione);
        CREATE INDEX IF NOT EXISTS idx_tipo_sc ON bandi(cod_tipo_scelta_contraente);
        CREATE INDEX IF NOT EXISTS idx_anno_esito ON bandi(anno_pubblicazione, esito);
        CREATE INDEX IF NOT EXISTS idx_anno_scad ON bandi(anno_pubblicazione, data_scadenza_offerta);
        CREATE INDEX IF NOT EXISTS idx_scad ON bandi(data_scadenza_offerta);
        CREATE INDEX IF NOT EXISTS idx_com_esito ON bandi(data_comunicazione_esito);

        CREATE TABLE IF NOT EXISTS albi_fornitori (
            cf_sa TEXT PRIMARY KEY,
            denominazione_sa TEXT,
            stato TEXT DEFAULT 'DA_VERIFICARE',
            note TEXT DEFAULT '',
            data_aggiornamento TEXT
        );

        CREATE TABLE IF NOT EXISTS aggiudicatari (
            cig TEXT NOT NULL,
            ruolo TEXT,
            codice_fiscale TEXT,
            denominazione TEXT,
            tipo_soggetto TEXT,
            id_aggiudicazione TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_agg_cig ON aggiudicatari(cig);
        CREATE INDEX IF NOT EXISTS idx_agg_cf  ON aggiudicatari(codice_fiscale);
        CREATE INDEX IF NOT EXISTS idx_agg_den ON aggiudicatari(denominazione);

        CREATE TABLE IF NOT EXISTS partecipanti (
            cig TEXT NOT NULL,
            ruolo TEXT,
            codice_fiscale TEXT,
            denominazione TEXT,
            tipo_soggetto TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_part_cig ON partecipanti(cig);
        CREATE INDEX IF NOT EXISTS idx_part_cf  ON partecipanti(codice_fiscale);
    """)
    conn.commit()

    # Migrazione: aggiunge colonna tipo se non esiste (DB esistenti)
    try:
        conn.execute("ALTER TABLE bandi ADD COLUMN tipo TEXT DEFAULT 'cig'")
        conn.commit()
        log.info("Colonna 'tipo' aggiunta alla tabella bandi")
    except Exception:
        pass  # colonna già esistente

    try:
        conn.execute("CREATE INDEX IF NOT EXISTS idx_tipo ON bandi(tipo)")
        conn.commit()
    except Exception:
        pass

    conn.close()
    log.info(f"Database pronto: {DB_PATH}")


def bulk_upsert(records: List[Dict], fonte: str, tipo: str = 'cig') -> int:
    """
    Import massivo con executemany. Molto più veloce di insert singoli.
    Gestisce colonne CSV con nomi diversi (case-insensitive).
    tipo: 'cig' (default) o 'smartcig'
    """
    if not records:
        return 0

    conn = get_conn()
    now = datetime.now().isoformat()

    all_cols = DB_COLUMNS + ["fonte", "data_import", "tipo"]
    placeholders = ", ".join(["?"] * len(all_cols))
    col_names = ", ".join(all_cols)
    updates = ", ".join(f"{c}=excluded.{c}" for c in all_cols if c != "cig")

    sql = f"""
        INSERT INTO bandi ({col_names}) VALUES ({placeholders})
        ON CONFLICT(cig) DO UPDATE SET {updates}
    """

    # Prepara batch
    batch = []
    skipped = 0
    for rec in records:
        rec_lower = {k.lower().strip(): v for k, v in rec.items()}
        cig = rec_lower.get("cig", "")
        if not cig:
            skipped += 1
            continue

        row = []
        for col in DB_COLUMNS:
            row.append(rec_lower.get(col, None))
        row.append(fonte)
        row.append(now)
        row.append(tipo)
        batch.append(row)

    # Inserisci in blocchi da 5000
    inserted = 0
    BATCH_SIZE = 5000
    for i in range(0, len(batch), BATCH_SIZE):
        chunk = batch[i:i + BATCH_SIZE]
        try:
            conn.executemany(sql, chunk)
            inserted += len(chunk)
        except Exception as e:
            log.warning(f"Errore batch {i}: {e}")
            # Fallback: inserisci uno a uno
            for row in chunk:
                try:
                    conn.execute(sql, row)
                    inserted += 1
                except Exception:
                    pass

    conn.commit()
    conn.close()

    if skipped:
        log.debug(f"  {skipped} righe senza CIG saltate")
    return inserted


def log_sync(resource_name, dataset, url, file_size, records_imported):
    conn = get_conn()
    conn.execute("""
        INSERT OR REPLACE INTO sync_log
        (resource_name, dataset, url, download_date, file_size, records_imported)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (resource_name, dataset, url, datetime.now().isoformat(),
          file_size, records_imported))
    conn.commit()
    conn.close()


def is_already_synced(resource_name: str, max_age_hours: int = 20) -> bool:
    """Considera già sincronizzata solo se scaricata nelle ultime max_age_hours ore."""
    conn = get_conn()
    row = conn.execute(
        "SELECT download_date FROM sync_log WHERE resource_name = ?", (resource_name,)
    ).fetchone()
    conn.close()
    if row is None:
        return False
    try:
        from datetime import timezone
        synced_at = datetime.fromisoformat(row[0])
        if synced_at.tzinfo is None:
            synced_at = synced_at.replace(tzinfo=timezone.utc)
        age_hours = (datetime.now(timezone.utc) - synced_at).total_seconds() / 3600
        return age_hours < max_age_hours
    except Exception:
        return False


def get_sync_log() -> List[Dict]:
    conn = get_conn()
    rows = conn.execute("SELECT * FROM sync_log ORDER BY download_date DESC").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def delete_old_records(anno_minimo: int = 2025) -> int:
    """Elimina dal DB tutti i record con anno_pubblicazione < anno_minimo."""
    conn = get_conn()
    cur = conn.execute(
        "DELETE FROM bandi WHERE CAST(anno_pubblicazione AS INTEGER) < ?", (anno_minimo,)
    )
    deleted = cur.rowcount
    conn.commit()
    conn.close()
    log.info(f"Eliminati {deleted:,} record precedenti al {anno_minimo}")
    return deleted


def count_bandi() -> int:
    conn = get_conn()
    n = conn.execute("SELECT COUNT(*) FROM bandi").fetchone()[0]
    conn.close()
    return n


def query_bandi(filters: dict = None, limit=50, offset=0) -> Tuple[List[dict], int]:
    """
    Query con filtri. I keyword vengono applicati QUI, non all'import.
    
    Filtri:
      q:        ricerca testo libero (oggetto, CIG, stazione appaltante)
      keywords: lista di keyword da cercare in OR nell'oggetto (es. CARRELLATI, CASSONETTI)
      anno:     anno pubblicazione
      esito:    esito gara (o "IN_CORSO" per quelli senza esito)
      provincia: filtro provincia
    """
    conn = get_conn()
    where = []
    params = []
    filters = filters or {}

    # Filtro keyword (OR o AND tra keyword, cerca in oggetto_lotto e oggetto_gara)
    if filters.get("keywords"):
        kw_list = filters["keywords"]
        kw_clauses = []
        for kw in kw_list:
            kw_clauses.append("(oggetto_lotto LIKE ? OR oggetto_gara LIKE ?)")
            params.extend([f"%{kw}%", f"%{kw}%"])
        join_op = " AND " if filters.get("kw_mode") == "and" else " OR "
        where.append(f"({join_op.join(kw_clauses)})")

    # Ricerca testo libero
    if filters.get("q"):
        q = f"%{filters['q']}%"
        where.append(
            "(oggetto_lotto LIKE ? OR cig LIKE ? OR "
            "denominazione_amministrazione_appaltante LIKE ? OR oggetto_gara LIKE ?)"
        )
        params.extend([q, q, q, q])

    if filters.get("anni"):
        placeholders = ",".join("?" * len(filters["anni"]))
        where.append(f"anno_pubblicazione IN ({placeholders})")
        params.extend([str(a) for a in filters["anni"]])
    if filters.get("con_scadenza"):
        where.append("data_scadenza_offerta IS NOT NULL AND data_scadenza_offerta != ''")
    if filters.get("cf_sa"):
        where.append("cf_amministrazione_appaltante = ?")
        params.append(filters["cf_sa"])
    if filters.get("solo_ad"):
        where.append("cod_tipo_scelta_contraente IN ('24','23')")

    if filters.get("esito"):
        if filters["esito"] == "IN_CORSO":
            where.append(
                "(esito IS NULL OR esito = '') AND "
                "(data_comunicazione_esito IS NULL OR data_comunicazione_esito = '') AND "
                "(data_scadenza_offerta IS NULL OR data_scadenza_offerta = '' "
                " OR data_scadenza_offerta >= date('now'))"
            )
        else:
            where.append("esito = ?")
            params.append(filters["esito"])

    if filters.get("provincia"):
        where.append("provincia = ?")
        params.append(filters["provincia"])
    if filters.get("tipo") in ("cig", "smartcig"):
        where.append("tipo = ?")
        params.append(filters["tipo"])
    if filters.get("cf_aggiudicatario"):
        where.append("cig IN (SELECT cig FROM aggiudicatari WHERE codice_fiscale = ?)")
        params.append(filters["cf_aggiudicatario"])

    where_sql = " AND ".join(where) if where else "1=1"

    total = conn.execute(f"SELECT COUNT(*) FROM bandi WHERE {where_sql}", params).fetchone()[0]

    # Ordinamento
    sort_col = filters.get("sort", "data_pubblicazione")
    sort_order = "DESC" if filters.get("order", "desc") == "desc" else "ASC"
    safe_cols = {
        "cig", "oggetto_lotto", "importo_lotto", "data_pubblicazione",
        "data_scadenza_offerta", "denominazione_amministrazione_appaltante",
        "provincia", "stato", "esito", "anno_pubblicazione", "importo_complessivo_gara",
        "data_import",
    }
    if sort_col not in safe_cols:
        sort_col = "data_pubblicazione"

    numeric_cols = {"importo_lotto", "importo_complessivo_gara"}
    order_expr = f"CAST(b.{sort_col} AS REAL)" if sort_col in numeric_cols else f"b.{sort_col}"
    rows = conn.execute(
        f"SELECT b.*, (SELECT denominazione FROM aggiudicatari WHERE cig=b.cig LIMIT 1) as aggiudicatario_nome "
        f"FROM bandi b WHERE {where_sql} ORDER BY {order_expr} {sort_order} LIMIT ? OFFSET ?",
        params + [limit, offset],
    ).fetchall()

    conn.close()
    return [dict(r) for r in rows], total


def _build_where(filters: dict):
    """Helper condiviso: costruisce WHERE clause e params dai filtri."""
    where = []
    params = []
    if filters.get("keywords"):
        kw_clauses = []
        for kw in filters["keywords"]:
            kw_clauses.append("(oggetto_lotto LIKE ? OR oggetto_gara LIKE ?)")
            params.extend([f"%{kw}%", f"%{kw}%"])
        join_op = " AND " if filters.get("kw_mode") == "and" else " OR "
        where.append(f"({join_op.join(kw_clauses)})")
    if filters.get("q"):
        q = f"%{filters['q']}%"
        where.append("(oggetto_lotto LIKE ? OR cig LIKE ? OR denominazione_amministrazione_appaltante LIKE ? OR oggetto_gara LIKE ?)")
        params.extend([q, q, q, q])
    if filters.get("anni"):
        placeholders = ",".join("?" * len(filters["anni"]))
        where.append(f"anno_pubblicazione IN ({placeholders})")
        params.extend([str(a) for a in filters["anni"]])
    if filters.get("con_scadenza"):
        where.append("data_scadenza_offerta IS NOT NULL AND data_scadenza_offerta != ''")
    if filters.get("cf_sa"):
        where.append("cf_amministrazione_appaltante = ?")
        params.append(filters["cf_sa"])
    if filters.get("solo_ad"):
        where.append("cod_tipo_scelta_contraente IN ('24','23')")
    if filters.get("esito"):
        if filters["esito"] == "IN_CORSO":
            where.append(
                "(esito IS NULL OR esito = '') AND "
                "(data_comunicazione_esito IS NULL OR data_comunicazione_esito = '') AND "
                "(data_scadenza_offerta IS NULL OR data_scadenza_offerta = '' "
                " OR data_scadenza_offerta >= date('now'))"
            )
        else:
            where.append("esito = ?")
            params.append(filters["esito"])
    if filters.get("provincia"):
        where.append("provincia = ?")
        params.append(filters["provincia"])
    if filters.get("tipo") in ("cig", "smartcig"):
        where.append("tipo = ?")
        params.append(filters["tipo"])
    if filters.get("cf_aggiudicatario"):
        where.append("cig IN (SELECT cig FROM aggiudicatari WHERE codice_fiscale = ?)")
        params.append(filters["cf_aggiudicatario"])
    return (" AND ".join(where) if where else "1=1"), params


def query_bandi_charts(filters: dict = None) -> Dict:
    """Restituisce dati aggregati per i grafici statistici."""
    filters = filters or {}
    conn = get_conn()
    where_sql, params = _build_where(filters)

    def q(sql, p=None):
        return conn.execute(sql, p if p is not None else params).fetchall()

    # Trend per anno
    anni = q(f"""
        SELECT anno_pubblicazione, COUNT(*) as n,
               SUM(CASE WHEN importo_lotto IS NOT NULL AND importo_lotto != ''
                   THEN CAST(importo_lotto AS REAL) ELSE 0 END) as tot
        FROM bandi WHERE {where_sql} AND anno_pubblicazione IS NOT NULL AND anno_pubblicazione != ''
        GROUP BY anno_pubblicazione ORDER BY anno_pubblicazione
    """)

    # Distribuzione esiti
    esiti = q(f"""
        SELECT CASE WHEN esito IS NULL OR esito='' THEN 'IN CORSO' ELSE esito END as esito,
               COUNT(*) as n
        FROM bandi WHERE {where_sql}
        GROUP BY esito ORDER BY n DESC LIMIT 10
    """)

    # Top 10 province per numero gare
    province = q(f"""
        SELECT provincia, COUNT(*) as n,
               SUM(CASE WHEN importo_lotto IS NOT NULL AND importo_lotto != ''
                   THEN CAST(importo_lotto AS REAL) ELSE 0 END) as tot
        FROM bandi WHERE {where_sql} AND provincia IS NOT NULL AND provincia != ''
        GROUP BY provincia ORDER BY n DESC LIMIT 10
    """)

    # Top 10 stazioni appaltanti
    sa = q(f"""
        SELECT denominazione_amministrazione_appaltante as sa, COUNT(*) as n,
               SUM(CASE WHEN importo_lotto IS NOT NULL AND importo_lotto != ''
                   THEN CAST(importo_lotto AS REAL) ELSE 0 END) as tot
        FROM bandi WHERE {where_sql}
          AND denominazione_amministrazione_appaltante IS NOT NULL
          AND denominazione_amministrazione_appaltante != ''
        GROUP BY denominazione_amministrazione_appaltante ORDER BY n DESC LIMIT 10
    """)

    # Trend mensile (anno-mese, ultimi 24 mesi)
    mensile = q(f"""
        SELECT anno_pubblicazione || '-' || printf('%02d', CAST(mese_pubblicazione AS INTEGER)) as ym,
               COUNT(*) as n,
               SUM(CASE WHEN importo_lotto IS NOT NULL AND importo_lotto != ''
                   THEN CAST(importo_lotto AS REAL) ELSE 0 END) as tot
        FROM bandi WHERE {where_sql}
          AND anno_pubblicazione IS NOT NULL AND anno_pubblicazione != ''
          AND mese_pubblicazione IS NOT NULL AND mese_pubblicazione != ''
        GROUP BY ym ORDER BY ym DESC LIMIT 24
    """)
    mensile = list(reversed(mensile))

    # Distribuzione per categoria merceologica (Lavori/Servizi/Forniture)
    categorie = q(f"""
        SELECT COALESCE(NULLIF(oggetto_principale_contratto,''), 'N/D') as cat,
               COUNT(*) as n,
               SUM(CASE WHEN importo_lotto IS NOT NULL AND importo_lotto != ''
                   THEN CAST(importo_lotto AS REAL) ELSE 0 END) as tot
        FROM bandi WHERE {where_sql}
        GROUP BY cat ORDER BY n DESC LIMIT 10
    """)

    # Distribuzione per tipo procedura
    procedure = q(f"""
        SELECT COALESCE(NULLIF(tipo_scelta_contraente,''), 'N/D') as proc,
               COUNT(*) as n
        FROM bandi WHERE {where_sql}
        GROUP BY proc ORDER BY n DESC LIMIT 10
    """)

    # Distribuzione importi per fascia
    fasce = q(f"""
        SELECT
          CASE
            WHEN CAST(importo_lotto AS REAL) < 40000       THEN '< 40K'
            WHEN CAST(importo_lotto AS REAL) < 150000      THEN '40K–150K'
            WHEN CAST(importo_lotto AS REAL) < 1000000     THEN '150K–1M'
            WHEN CAST(importo_lotto AS REAL) < 5000000     THEN '1M–5M'
            ELSE '> 5M'
          END as fascia,
          COUNT(*) as n,
          SUM(CAST(importo_lotto AS REAL)) as tot
        FROM bandi WHERE {where_sql}
          AND importo_lotto IS NOT NULL AND importo_lotto != ''
          AND CAST(importo_lotto AS REAL) > 0
        GROUP BY fascia
        ORDER BY MIN(CAST(importo_lotto AS REAL))
    """)

    # Top 10 CPV
    cpv = q(f"""
        SELECT COALESCE(NULLIF(descrizione_cpv,''), cod_cpv) as cpv,
               COUNT(*) as n
        FROM bandi WHERE {where_sql}
          AND (descrizione_cpv IS NOT NULL AND descrizione_cpv != ''
               OR cod_cpv IS NOT NULL AND cod_cpv != '')
        GROUP BY cpv ORDER BY n DESC LIMIT 10
    """)

    # Top 10 aggiudicatari
    top_agg = conn.execute(f"""
        SELECT a.denominazione, a.codice_fiscale,
               COUNT(*) as n_aggiudicazioni,
               COALESCE(SUM(CAST(fb.importo_lotto AS REAL)), 0) as tot_importo
        FROM aggiudicatari a
        JOIN (SELECT cig, importo_lotto FROM bandi WHERE {where_sql}) fb ON a.cig = fb.cig
        GROUP BY a.codice_fiscale, a.denominazione
        ORDER BY n_aggiudicazioni DESC
        LIMIT 10
    """, params).fetchall()

    # KPI sintetici
    kpi_row = conn.execute(f"""
        SELECT COUNT(*) as n,
               SUM(CASE WHEN importo_lotto IS NOT NULL AND importo_lotto != ''
                   THEN CAST(importo_lotto AS REAL) ELSE 0 END) as tot,
               AVG(CASE WHEN importo_lotto IS NOT NULL AND importo_lotto != '' AND CAST(importo_lotto AS REAL) > 0
                   THEN CAST(importo_lotto AS REAL) END) as media,
               COUNT(DISTINCT cf_amministrazione_appaltante) as sa_attive
        FROM bandi WHERE {where_sql}
    """, params).fetchone()

    conn.close()
    return {
        "anni":      [{"anno": r[0], "n": r[1], "tot": r[2]} for r in anni],
        "esiti":     [{"esito": r[0], "n": r[1]} for r in esiti],
        "province":  [{"provincia": r[0], "n": r[1], "tot": r[2]} for r in province],
        "sa":        [{"sa": r[0], "n": r[1], "tot": r[2]} for r in sa],
        "mensile":   [{"ym": r[0], "n": r[1], "tot": r[2]} for r in mensile],
        "categorie": [{"cat": r[0], "n": r[1], "tot": r[2]} for r in categorie],
        "procedure": [{"proc": r[0], "n": r[1]} for r in procedure],
        "fasce":     [{"fascia": r[0], "n": r[1], "tot": r[2]} for r in fasce],
        "cpv":       [{"cpv": r[0], "n": r[1]} for r in cpv],
        "kpi": {
            "n": kpi_row[0] or 0,
            "tot": kpi_row[1] or 0,
            "media": kpi_row[2] or 0,
            "sa_attive": kpi_row[3] or 0,
        },
        "top_aggiudicatari": [
            {"denominazione": r[0], "codice_fiscale": r[1], "n": r[2], "tot": r[3]}
            for r in top_agg
        ],
    }


def query_bandi_province_agg(filters: dict = None) -> List[Dict]:
    """Aggrega bandi per provincia: conteggio e somma importi."""
    filters = filters or {}
    conn = get_conn()
    where_sql, params = _build_where(filters)

    rows = conn.execute(f"""
        SELECT
            provincia,
            COUNT(*) AS count,
            SUM(CASE WHEN importo_lotto IS NOT NULL AND importo_lotto != ''
                THEN CAST(importo_lotto AS REAL) ELSE 0 END) AS total_importo
        FROM bandi
        WHERE {where_sql}
          AND provincia IS NOT NULL AND provincia != ''
        GROUP BY provincia
        ORDER BY total_importo DESC
    """, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def query_albi_sa(filters: dict = None) -> List[Dict]:
    """
    Restituisce le stazioni appaltanti con affidamenti diretti,
    raggruppate e arricchite con lo stato albo fornitore.
    Filtri keyword/q/anno/provincia applicati; esito ignorato (mostra sempre tutti gli AD).
    """
    filters = filters or {}
    # Forza solo affidamenti diretti, ignora filtro esito
    ad_filters = {k: v for k, v in filters.items() if k != "esito"}
    where_sql, params = _build_where(ad_filters)
    # Aggiungi filtro tipo affidamento diretto
    ad_where = f"({where_sql}) AND cod_tipo_scelta_contraente IN ('24','23')"

    conn = get_conn()
    rows = conn.execute(f"""
        SELECT
            b.cf_amministrazione_appaltante AS cf_sa,
            b.denominazione_amministrazione_appaltante AS denominazione_sa,
            COUNT(*) AS n_gare,
            SUM(CASE WHEN b.importo_lotto IS NOT NULL AND b.importo_lotto != ''
                THEN CAST(b.importo_lotto AS REAL) ELSE 0 END) AS total_importo,
            MAX(b.data_pubblicazione) AS ultima_gara,
            COALESCE(a.stato, 'DA_VERIFICARE') AS stato,
            COALESCE(a.note, '') AS note,
            a.data_aggiornamento
        FROM bandi b
        LEFT JOIN albi_fornitori a
            ON b.cf_amministrazione_appaltante = a.cf_sa
        WHERE {ad_where}
          AND b.cf_amministrazione_appaltante IS NOT NULL
          AND b.cf_amministrazione_appaltante != ''
        GROUP BY b.cf_amministrazione_appaltante
        ORDER BY n_gare DESC
        LIMIT 200
    """, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def upsert_albo_sa(cf_sa: str, denominazione_sa: str, stato: str, note: str):
    """Aggiorna o inserisce lo stato albo per una stazione appaltante."""
    conn = get_conn()
    conn.execute("""
        INSERT INTO albi_fornitori (cf_sa, denominazione_sa, stato, note, data_aggiornamento)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(cf_sa) DO UPDATE SET
            denominazione_sa = excluded.denominazione_sa,
            stato = excluded.stato,
            note = excluded.note,
            data_aggiornamento = excluded.data_aggiornamento
    """, (cf_sa, denominazione_sa, stato, note, datetime.now().isoformat()))
    conn.commit()
    conn.close()


def count_aggiudicatari() -> int:
    conn = get_conn()
    n = conn.execute("SELECT COUNT(*) FROM aggiudicatari").fetchone()[0]
    conn.close()
    return n


def count_partecipanti() -> int:
    conn = get_conn()
    n = conn.execute("SELECT COUNT(*) FROM partecipanti").fetchone()[0]
    conn.close()
    return n


def bulk_upsert_aggiudicatari(records: List[Dict]) -> int:
    """Importa aggiudicatari filtrando solo i CIG già presenti nel DB."""
    if not records:
        return 0
    conn = get_conn()
    db_cigs = {r[0] for r in conn.execute("SELECT cig FROM bandi").fetchall()}

    seen = set()
    batch = []
    for rec in records:
        rk = {k.lower().strip(): (v or "") for k, v in rec.items()}
        cig = rk.get("cig", "").strip()
        if not cig or cig not in db_cigs:
            continue
        cf  = rk.get("codice_fiscale", "").strip()
        agg = rk.get("id_aggiudicazione", "").strip()
        key = (cig, cf, agg)
        if key in seen:
            continue
        seen.add(key)
        batch.append((cig, rk.get("ruolo"), cf, rk.get("denominazione"), rk.get("tipo_soggetto"), agg))

    if not batch:
        conn.close()
        return 0

    sql = """INSERT OR IGNORE INTO aggiudicatari
             (cig, ruolo, codice_fiscale, denominazione, tipo_soggetto, id_aggiudicazione)
             VALUES (?,?,?,?,?,?)"""
    inserted = 0
    for i in range(0, len(batch), 5000):
        try:
            conn.executemany(sql, batch[i:i+5000])
            inserted += len(batch[i:i+5000])
        except Exception as e:
            log.warning(f"aggiudicatari batch {i}: {e}")
    conn.commit()
    conn.close()
    return inserted


def bulk_upsert_partecipanti(records: List[Dict]) -> int:
    """Importa partecipanti filtrando solo i CIG già presenti nel DB."""
    if not records:
        return 0
    conn = get_conn()
    db_cigs = {r[0] for r in conn.execute("SELECT cig FROM bandi").fetchall()}

    seen = set()
    batch = []
    for rec in records:
        rk = {k.lower().strip(): (v or "") for k, v in rec.items()}
        cig = rk.get("cig", "").strip()
        if not cig or cig not in db_cigs:
            continue
        cf   = rk.get("codice_fiscale", "").strip()
        ruolo = rk.get("ruolo", "").strip()
        key = (cig, cf, ruolo)
        if key in seen:
            continue
        seen.add(key)
        batch.append((cig, ruolo, cf, rk.get("denominazione"), rk.get("tipo_soggetto")))

    if not batch:
        conn.close()
        return 0

    sql = """INSERT OR IGNORE INTO partecipanti
             (cig, ruolo, codice_fiscale, denominazione, tipo_soggetto)
             VALUES (?,?,?,?,?)"""
    inserted = 0
    for i in range(0, len(batch), 5000):
        try:
            conn.executemany(sql, batch[i:i+5000])
            inserted += len(batch[i:i+5000])
        except Exception as e:
            log.warning(f"partecipanti batch {i}: {e}")
    conn.commit()
    conn.close()
    return inserted


def query_aggiudicatari_partecipanti(cig: str) -> Dict:
    """Restituisce aggiudicatari e partecipanti per un CIG."""
    conn = get_conn()
    agg = conn.execute(
        """SELECT ruolo, codice_fiscale, denominazione, tipo_soggetto, id_aggiudicazione
           FROM aggiudicatari WHERE cig=? ORDER BY id_aggiudicazione, ruolo""",
        (cig,)
    ).fetchall()
    part = conn.execute(
        """SELECT ruolo, codice_fiscale, denominazione, tipo_soggetto
           FROM partecipanti WHERE cig=? ORDER BY ruolo, denominazione""",
        (cig,)
    ).fetchall()
    conn.close()
    return {
        "aggiudicatari": [dict(r) for r in agg],
        "partecipanti":  [dict(r) for r in part],
    }


def query_top_aggiudicatari(filters: dict = None, limit: int = 50) -> List[Dict]:
    """Classifica aggiudicatari/fornitori per numero di gare vinte tra i bandi filtrati."""
    filters = filters or {}
    conn = get_conn()
    where_sql, params = _build_where(filters)

    rows = conn.execute(f"""
        SELECT a.denominazione, a.codice_fiscale,
               COUNT(*) as n_aggiudicazioni,
               COALESCE(SUM(CAST(fb.importo_lotto AS REAL)), 0) as tot_importo
        FROM aggiudicatari a
        JOIN (SELECT cig, importo_lotto FROM bandi WHERE {where_sql}) fb ON a.cig = fb.cig
        GROUP BY a.codice_fiscale, a.denominazione
        ORDER BY n_aggiudicazioni DESC
        LIMIT ?
    """, params + [limit]).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def query_top_aggiudicatari_province(filters: dict = None, limit: int = 10) -> List[Dict]:
    """Top N aggiudicatari con breakdown per provincia (per la mappa)."""
    filters = filters or {}
    conn = get_conn()
    where_sql, params = _build_where(filters)

    rows = conn.execute(f"""
        WITH top_cf AS (
            SELECT a.codice_fiscale, a.denominazione,
                   COUNT(*) AS n_tot,
                   COALESCE(SUM(CAST(b.importo_lotto AS REAL)),0) AS tot_importo
            FROM aggiudicatari a
            JOIN bandi b ON a.cig = b.cig
            WHERE {where_sql}
              AND a.codice_fiscale IS NOT NULL AND a.codice_fiscale != ''
            GROUP BY a.codice_fiscale, a.denominazione
            ORDER BY n_tot DESC
            LIMIT {limit}
        )
        SELECT tc.denominazione, tc.codice_fiscale, tc.n_tot, tc.tot_importo,
               b2.provincia, COUNT(*) AS n_prov,
               COALESCE(SUM(CAST(b2.importo_lotto AS REAL)),0) AS tot_prov
        FROM top_cf tc
        JOIN aggiudicatari a2 ON tc.codice_fiscale = a2.codice_fiscale
        JOIN bandi b2 ON a2.cig = b2.cig
        WHERE {where_sql}
          AND b2.provincia IS NOT NULL AND b2.provincia != ''
        GROUP BY tc.codice_fiscale, tc.denominazione, b2.provincia
        ORDER BY tc.n_tot DESC, n_prov DESC
    """, params + params).fetchall()
    conn.close()

    from collections import OrderedDict
    agg_map: dict = OrderedDict()
    for row in rows:
        cf = row[1]
        if cf not in agg_map:
            agg_map[cf] = {
                "denominazione": row[0],
                "codice_fiscale": cf,
                "n_aggiudicazioni": row[2],
                "tot_importo": row[3],
                "province": [],
            }
        agg_map[cf]["province"].append({"provincia": row[4], "n": row[5], "tot": row[6]})
    return list(agg_map.values())


def get_filtri_disponibili() -> dict:
    conn = get_conn()
    anni = [r[0] for r in conn.execute(
        "SELECT DISTINCT anno_pubblicazione FROM bandi WHERE anno_pubblicazione IS NOT NULL AND CAST(anno_pubblicazione AS INTEGER) >= 2022 ORDER BY anno_pubblicazione DESC"
    ).fetchall() if r[0]]
    province = [r[0] for r in conn.execute(
        "SELECT DISTINCT provincia FROM bandi WHERE provincia IS NOT NULL AND provincia != '' ORDER BY provincia"
    ).fetchall()]
    esiti = [r[0] for r in conn.execute(
        "SELECT DISTINCT esito FROM bandi WHERE esito IS NOT NULL AND esito != '' ORDER BY esito"
    ).fetchall()]
    conn.close()
    return {"anni": anni, "province": province, "esiti": esiti}
