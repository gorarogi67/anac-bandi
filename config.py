"""
config.py — Configurazione (locale + Railway)

Su Railway le variabili si impostano nel pannello "Variables".
In locale funziona con i valori di default.
"""
import os

# Percorso dati: su Railway usa il volume montato, in locale usa ./data
DATA_DIR = os.environ.get("DATA_DIR", os.path.join(os.path.dirname(os.path.abspath(__file__)), "data"))
DB_PATH = os.path.join(DATA_DIR, "anac.db")
os.makedirs(DATA_DIR, exist_ok=True)

# API CKAN
CKAN_API = "https://dati.anticorruzione.it/opendata/api/3/action"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, */*",
}

# Dataset
DATASET_CIG_DELTA = "cig"
DATASET_CIG_ANNUALE = "cig-{anno}"
ANNO_INIZIO = int(os.environ.get("ANNO_INIZIO", "2025"))

# Keywords di default per l'interfaccia
KEYWORDS_DEFAULT = os.environ.get("KEYWORDS", "CARRELLATI,CASSONETTI,BIDONI").split(",")

# Chiave segreta per proteggere l'endpoint di sync manuale
SYNC_SECRET = os.environ.get("SYNC_SECRET", "cambiami123")

# Server
PORT = int(os.environ.get("PORT", "5000"))
