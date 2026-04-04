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
    """)
    conn.commit()
    conn.close()
    log.info(f"Database pronto: {DB_PATH}")


def bulk_upsert(records: List[Dict], fonte: str) -> int:
    """
    Import massivo con executemany. Molto più veloce di insert singoli.
    Gestisce colonne CSV con nomi diversi (case-insensitive).
    """
    if not records:
        return 0

    conn = get_conn()
    now = datetime.now().isoformat()

    all_cols = DB_COLUMNS + ["fonte", "data_import"]
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


def is_already_synced(resource_name: str) -> bool:
    conn = get_conn()
    row = conn.execute(
        "SELECT 1 FROM sync_log WHERE resource_name = ?", (resource_name,)
    ).fetchone()
    conn.close()
    return row is not None


def get_sync_log() -> List[Dict]:
    conn = get_conn()
    rows = conn.execute("SELECT * FROM sync_log ORDER BY download_date DESC").fetchall()
    conn.close()
    return [dict(r) for r in rows]


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

    if filters.get("anno"):
        where.append("anno_pubblicazione = ?")
        params.append(str(filters["anno"]))

    if filters.get("esito"):
        if filters["esito"] == "IN_CORSO":
            where.append("(esito IS NULL OR esito = '')")
        else:
            where.append("esito = ?")
            params.append(filters["esito"])

    if filters.get("provincia"):
        where.append("provincia = ?")
        params.append(filters["provincia"])

    where_sql = " AND ".join(where) if where else "1=1"

    total = conn.execute(f"SELECT COUNT(*) FROM bandi WHERE {where_sql}", params).fetchone()[0]

    # Ordinamento
    sort_col = filters.get("sort", "data_pubblicazione")
    sort_order = "DESC" if filters.get("order", "desc") == "desc" else "ASC"
    safe_cols = {
        "cig", "oggetto_lotto", "importo_lotto", "data_pubblicazione",
        "data_scadenza_offerta", "denominazione_amministrazione_appaltante",
        "provincia", "stato", "esito", "anno_pubblicazione", "importo_complessivo_gara",
    }
    if sort_col not in safe_cols:
        sort_col = "data_pubblicazione"

    rows = conn.execute(
        f"SELECT * FROM bandi WHERE {where_sql} ORDER BY {sort_col} {sort_order} LIMIT ? OFFSET ?",
        params + [limit, offset],
    ).fetchall()

    conn.close()
    return [dict(r) for r in rows], total


def get_filtri_disponibili() -> dict:
    conn = get_conn()
    anni = [r[0] for r in conn.execute(
        "SELECT DISTINCT anno_pubblicazione FROM bandi WHERE anno_pubblicazione IS NOT NULL ORDER BY anno_pubblicazione DESC"
    ).fetchall() if r[0]]
    province = [r[0] for r in conn.execute(
        "SELECT DISTINCT provincia FROM bandi WHERE provincia IS NOT NULL AND provincia != '' ORDER BY provincia"
    ).fetchall()]
    esiti = [r[0] for r in conn.execute(
        "SELECT DISTINCT esito FROM bandi WHERE esito IS NOT NULL AND esito != '' ORDER BY esito"
    ).fetchall()]
    conn.close()
    return {"anni": anni, "province": province, "esiti": esiti}
