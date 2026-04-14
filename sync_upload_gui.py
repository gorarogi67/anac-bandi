"""
sync_upload_gui.py — Sync ANAC locale + Upload DB su Railway

Flusso completo:
  1. Sync ANAC: scarica CIG, SmartCIG, aggiudicatari, partecipanti
  2. Upload DB:  comprime anac.db e lo carica su Railway a blocchi

Avvio: python sync_upload_gui.py
"""

import tkinter as tk
from tkinter import ttk, messagebox
import threading
import logging
import os
import json
import gzip
import time
import uuid
from urllib.parse import urlencode

import requests

BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(BASE_DIR, "data", "push_config.json")
CHUNK_MB    = 80


# ── Helpers ──────────────────────────────────────────────────────────────────

def load_config():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_config(cfg):
    os.makedirs(os.path.dirname(CONFIG_FILE), exist_ok=True)
    with open(CONFIG_FILE, "w") as f:
        json.dump(cfg, f)


def default_db_path():
    try:
        from config import DB_PATH
        return DB_PATH
    except Exception:
        return os.path.join(BASE_DIR, "data", "anac.db")


def human_size(n: float) -> str:
    for unit in ["B", "KB", "MB", "GB"]:
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def db_info(path: str) -> str:
    if path and os.path.exists(path):
        return human_size(os.path.getsize(path))
    return "(non trovato)"


# ── Logging handler ───────────────────────────────────────────────────────────

class GuiLogHandler(logging.Handler):
    def __init__(self, text_widget):
        super().__init__()
        self.text_widget = text_widget

    def emit(self, record):
        msg  = self.format(record)
        level = record.levelno
        tag  = "info" if level == logging.INFO else "warning" if level == logging.WARNING else "error"

        def append():
            self.text_widget.configure(state="normal")
            self.text_widget.insert(tk.END, msg + "\n", tag)
            self.text_widget.configure(state="disabled")
            self.text_widget.see(tk.END)
        self.text_widget.after(0, append)


# ── App ───────────────────────────────────────────────────────────────────────

class SyncUploadApp:
    def __init__(self, root):
        self.root     = root
        self.root.title("ANAC Sync + Upload Railway")
        self.root.geometry("860x700")
        self.root.resizable(True, True)
        self.root.configure(bg="#f5f5f5")

        self.cfg      = load_config()
        self._running = False
        self._build_ui()
        self._setup_logging()
        self._refresh_status()

    # ── UI ────────────────────────────────────────────────────────────────────

    def _build_ui(self):
        root = self.root
        PAD  = 12

        # Titolo
        tf = tk.Frame(root, bg="#1a237e", pady=10)
        tf.pack(fill="x")
        tk.Label(tf, text="ANAC Sync + Upload", font=("Segoe UI", 16, "bold"),
                 bg="#1a237e", fg="white").pack()
        tk.Label(tf, text="Scarica da ANAC in locale  →  Carica DB su Railway",
                 font=("Segoe UI", 9), bg="#1a237e", fg="#c5cae9").pack()

        # Configurazione
        cf = ttk.LabelFrame(root, text="Configurazione", padding=PAD)
        cf.pack(fill="x", padx=PAD, pady=(PAD, 0))

        tk.Label(cf, text="Railway URL:", font=("Segoe UI", 9)).grid(
            row=0, column=0, sticky="w", padx=(0, 8))
        self.url_var = tk.StringVar(value=self.cfg.get("railway_url", ""))
        ttk.Entry(cf, textvariable=self.url_var, width=55, font=("Segoe UI", 9)
                  ).grid(row=0, column=1, sticky="ew", padx=(0, 8))

        tk.Label(cf, text="Chiave API:", font=("Segoe UI", 9)).grid(
            row=1, column=0, sticky="w", padx=(0, 8), pady=(6, 0))
        self.key_var = tk.StringVar(value=self.cfg.get("sync_secret", ""))
        ttk.Entry(cf, textvariable=self.key_var, width=22, font=("Segoe UI", 9), show="*"
                  ).grid(row=1, column=1, sticky="w", padx=(0, 8), pady=(6, 0))

        ttk.Button(cf, text="Salva", command=self._save_cfg, width=8
                   ).grid(row=0, column=2, rowspan=2, padx=(0, 0))
        cf.columnconfigure(1, weight=1)

        # Stato
        sf = ttk.LabelFrame(root, text="Stato DB locale", padding=PAD)
        sf.pack(fill="x", padx=PAD, pady=(8, 0))
        self.lbl_size   = tk.Label(sf, text="Dimensione: —", font=("Segoe UI", 9))
        self.lbl_size.grid(row=0, column=0, sticky="w", padx=(0, 30))
        self.lbl_path   = tk.Label(sf, text="", font=("Segoe UI", 8), fg="#666")
        self.lbl_path.grid(row=0, column=1, sticky="w", padx=(0, 30))
        self.lbl_chunk  = tk.Label(sf, text="", font=("Segoe UI", 9, "bold"), fg="#1a237e")
        self.lbl_chunk.grid(row=0, column=2, sticky="w", padx=(0, 20))
        self.lbl_speed  = tk.Label(sf, text="", font=("Segoe UI", 9), fg="#555")
        self.lbl_speed.grid(row=0, column=3, sticky="w")
        ttk.Button(sf, text="↻ Aggiorna", command=self._refresh_status, width=10
                   ).grid(row=0, column=4, sticky="e", padx=(20, 0))
        sf.columnconfigure(4, weight=1)

        # Pulsanti azione
        bf = tk.Frame(root, bg="#f5f5f5", pady=8)
        bf.pack(fill="x", padx=PAD)

        self.btn_tutto = tk.Button(
            bf, text="⬇  Sync ANAC  +  ⬆ Upload Railway",
            font=("Segoe UI", 11, "bold"), bg="#1a237e", fg="white",
            activebackground="#0d1b6e", activeforeground="white",
            relief="flat", padx=20, pady=10, cursor="hand2",
            command=self._run_tutto,
        )
        self.btn_tutto.pack(side="left", padx=(0, 8))

        self.btn_solo_sync = tk.Button(
            bf, text="⬇  Solo Sync ANAC",
            font=("Segoe UI", 10), bg="#1565c0", fg="white",
            activebackground="#0d47a1", activeforeground="white",
            relief="flat", padx=14, pady=10, cursor="hand2",
            command=self._run_solo_sync,
        )
        self.btn_solo_sync.pack(side="left", padx=(0, 8))

        self.btn_solo_upload = tk.Button(
            bf, text="⬆  Solo Upload Railway",
            font=("Segoe UI", 10), bg="#2e7d32", fg="white",
            activebackground="#1b5e20", activeforeground="white",
            relief="flat", padx=14, pady=10, cursor="hand2",
            command=self._run_solo_upload,
        )
        self.btn_solo_upload.pack(side="left", padx=(0, 8))

        self.btn_clear = tk.Button(
            bf, text="Pulisci log",
            font=("Segoe UI", 9), bg="#eeeeee", fg="#333",
            relief="flat", padx=10, pady=10, cursor="hand2",
            command=self._clear_log,
        )
        self.btn_clear.pack(side="right")

        # Progress bar
        self.progress = ttk.Progressbar(root, mode="determinate", maximum=100)
        self.progress.pack(fill="x", padx=PAD, pady=(0, 4))

        # Log
        lf = ttk.LabelFrame(root, text="Log", padding=4)
        lf.pack(fill="both", expand=True, padx=PAD, pady=(0, PAD))
        self.log_text = tk.Text(
            lf, state="disabled", wrap="word",
            font=("Consolas", 9), bg="#1e1e1e", fg="#e0e0e0",
            relief="flat", padx=6, pady=6,
        )
        self.log_text.tag_configure("info",    foreground="#e0e0e0")
        self.log_text.tag_configure("warning", foreground="#ffcc02")
        self.log_text.tag_configure("error",   foreground="#ef5350")
        sb = ttk.Scrollbar(lf, command=self.log_text.yview)
        self.log_text.configure(yscrollcommand=sb.set)
        sb.pack(side="right", fill="y")
        self.log_text.pack(fill="both", expand=True)

        # Status bar
        self.status_var = tk.StringVar(value="Pronto")
        tk.Label(root, textvariable=self.status_var, font=("Segoe UI", 8),
                 bg="#e0e0e0", anchor="w", padx=8).pack(fill="x", side="bottom")

    def _setup_logging(self):
        h = GuiLogHandler(self.log_text)
        h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s",
                                         datefmt="%H:%M:%S"))
        root_log = logging.getLogger()
        root_log.addHandler(h)
        root_log.setLevel(logging.INFO)

    # ── Helpers UI ────────────────────────────────────────────────────────────

    def _refresh_status(self):
        db = default_db_path()
        self.lbl_size.config(text=f"Dimensione: {db_info(db)}")
        self.lbl_path.config(text=db)
        self.lbl_chunk.config(text="")
        self.lbl_speed.config(text="")
        self.progress["value"] = 0

    def _save_cfg(self):
        self.cfg["railway_url"] = self.url_var.get().strip().rstrip("/")
        self.cfg["sync_secret"] = self.key_var.get().strip()
        save_config(self.cfg)
        self.status_var.set("Configurazione salvata")

    def _clear_log(self):
        self.log_text.configure(state="normal")
        self.log_text.delete("1.0", tk.END)
        self.log_text.configure(state="disabled")

    def _set_buttons(self, enabled: bool):
        state = "normal" if enabled else "disabled"
        for b in (self.btn_tutto, self.btn_solo_sync, self.btn_solo_upload):
            b.config(state=state)

    def _set_progress(self, pct, label=""):
        self.progress["value"] = pct
        if label:
            self.status_var.set(label)

    def _update_chunk(self, pct, chunk_label, speed_str):
        self.progress["value"] = pct
        self.lbl_chunk.config(text=chunk_label)
        self.lbl_speed.config(text=speed_str)

    # ── Avvio task ────────────────────────────────────────────────────────────

    def _run_tutto(self):
        self._start(do_sync=True, do_upload=True)

    def _run_solo_sync(self):
        self._start(do_sync=True, do_upload=False)

    def _run_solo_upload(self):
        url = self.url_var.get().strip().rstrip("/")
        key = self.key_var.get().strip()
        db  = default_db_path()
        if not url:
            messagebox.showerror("Errore", "Inserisci il Railway URL.")
            return
        if not key:
            messagebox.showerror("Errore", "Inserisci la chiave API.")
            return
        if not os.path.exists(db):
            messagebox.showerror("Errore", f"DB non trovato:\n{db}")
            return
        if not messagebox.askyesno(
                "Conferma upload",
                f"Carica su Railway:\n{db}\n"
                f"({db_info(db)})\n\n"
                "Il DB su Railway sarà SOSTITUITO\n"
                "(albi fornitori preservati).\n\nProcedere?"):
            return
        self._start(do_sync=False, do_upload=True)

    def _start(self, do_sync: bool, do_upload: bool):
        if self._running:
            return
        self._save_cfg()
        self._running = True
        self._set_buttons(False)
        self.progress["value"] = 0
        lbl = []
        if do_sync:   lbl.append("Sync ANAC")
        if do_upload: lbl.append("Upload Railway")
        self.status_var.set(" + ".join(lbl) + " in corso...")
        threading.Thread(
            target=self._worker,
            args=(do_sync, do_upload),
            daemon=True,
        ).start()

    # ── Worker ────────────────────────────────────────────────────────────────

    def _worker(self, do_sync: bool, do_upload: bool):
        try:
            url = self.url_var.get().strip().rstrip("/")
            key = self.key_var.get().strip()
            db  = default_db_path()

            # ── FASE 1: Sync ANAC ─────────────────────────────────────────────
            if do_sync:
                logging.info("=" * 60)
                logging.info("FASE 1 — Sync ANAC (locale)")
                logging.info("=" * 60)
                self.root.after(0, lambda: self._set_progress(2, "Sync ANAC in corso..."))
                from sync import sync as run_sync
                run_sync()
                self.root.after(0, lambda: self._set_progress(
                    50 if do_upload else 100, "Sync completato"))
                self.root.after(0, self._refresh_status)
                if not do_upload:
                    logging.info("Sync completato.")
                    return

            # ── FASE 2: Upload DB ─────────────────────────────────────────────
            if do_upload:
                logging.info("=" * 60)
                logging.info("FASE 2 — Upload DB su Railway")
                logging.info("=" * 60)

                if not url:
                    raise ValueError("Railway URL non configurato")
                if not key:
                    raise ValueError("Chiave API non configurata")
                if not os.path.exists(db):
                    raise FileNotFoundError(f"DB non trovato: {db}")

                self._upload(url, key, db, progress_offset=50 if do_sync else 0)

        except Exception as e:
            logging.error(f"✗ Errore: {e}")
            self.root.after(0, lambda: self.status_var.set(f"Errore: {e}"))
        finally:
            self._running = False
            self.root.after(0, lambda: self._set_buttons(True))
            self.root.after(0, self._refresh_status)

    def _upload(self, url: str, key: str, db: str, progress_offset: int = 0):
        """Carica il DB su Railway a blocchi gzip. progress_offset: 0 o 50."""
        gz_path = db + ".upload_tmp.gz"
        chunk_bytes = CHUNK_MB * 1024 * 1024
        upload_id   = str(uuid.uuid4())[:8]
        db_size     = os.path.getsize(db)
        scale       = (100 - progress_offset) / 100  # scala la barra nella seconda metà

        try:
            # Comprimi
            logging.info(f"DB locale: {db}  ({human_size(db_size)})")
            logging.info("Comprimo con gzip (livello 6)...")
            t0 = time.time()
            with open(db, "rb") as fin, gzip.open(gz_path, "wb", compresslevel=6) as fout:
                read_so_far = 0
                while True:
                    block = fin.read(4 * 1024 * 1024)
                    if not block:
                        break
                    fout.write(block)
                    read_so_far += len(block)
                    pct = progress_offset + read_so_far / db_size * 40 * scale
                    self.root.after(0, lambda p=pct: self._set_progress(p, "Compressione..."))

            gz_size = os.path.getsize(gz_path)
            ratio   = gz_size / db_size * 100
            logging.info(f"Compresso: {human_size(gz_size)} ({ratio:.0f}%) "
                         f"in {time.time()-t0:.1f}s")

            # Invia chunks
            total_chunks = (gz_size + chunk_bytes - 1) // chunk_bytes
            logging.info(f"Blocchi da inviare: {total_chunks} × {CHUNK_MB} MB  "
                         f"(upload_id={upload_id})")

            session = requests.Session()
            t_start = time.time()

            with open(gz_path, "rb") as gz_in:
                for i in range(total_chunks):
                    chunk_data = gz_in.read(chunk_bytes)
                    if not chunk_data:
                        break

                    params = urlencode({
                        "key":          key,
                        "chunk_index":  i,
                        "total_chunks": total_chunks,
                        "upload_id":    upload_id,
                    })
                    endpoint = f"{url}/api/upload-db-chunk?{params}"

                    ok = False
                    for attempt in range(3):
                        try:
                            r = session.post(
                                endpoint,
                                data=chunk_data,
                                headers={"Content-Type": "application/octet-stream"},
                                timeout=120,
                            )
                            if r.status_code == 200:
                                ok = True
                                break
                            logging.warning(f"  chunk {i}: HTTP {r.status_code} — riprovo ({attempt+1}/3)")
                        except Exception as e:
                            logging.warning(f"  chunk {i}: {e} — riprovo ({attempt+1}/3)")
                        time.sleep(2)

                    if not ok:
                        raise RuntimeError(f"Chunk {i} fallito dopo 3 tentativi")

                    bytes_sent = (i + 1) * chunk_bytes
                    elapsed    = max(time.time() - t_start, 0.001)
                    speed      = bytes_sent / elapsed
                    pct        = progress_offset + 40 * scale + (i + 1) / total_chunks * 50 * scale
                    chunk_lbl  = f"Blocco {i+1}/{total_chunks}"
                    spd_str    = human_size(speed) + "/s"
                    self.root.after(0, lambda p=pct, m=chunk_lbl, s=spd_str:
                                    self._update_chunk(p, m, s))
                    logging.info(f"  [{i+1}/{total_chunks}] {human_size(len(chunk_data))}  {spd_str}")

            # Finalizza
            logging.info("Finalizzazione in corso...")
            self.root.after(0, lambda: self._set_progress(97, "Finalizzazione..."))
            params = urlencode({"key": key, "upload_id": upload_id, "total_chunks": total_chunks})
            r = session.post(f"{url}/api/upload-db-finalize?{params}", timeout=600)
            if r.status_code != 200:
                raise RuntimeError(f"Finalizzazione fallita: HTTP {r.status_code} — {r.text[:300]}")

            res     = r.json()
            elapsed = time.time() - t0
            self.root.after(0, lambda: self._set_progress(100, "Completato"))
            logging.info(f"✓ Upload completato in {elapsed:.1f}s")
            logging.info(f"  Bandi nel DB Railway: {res.get('bandi', 0):,}")
            logging.info(f"  Albi ripristinati:    {res.get('albi_ripristinati', 0)}")
            self.root.after(0, lambda: messagebox.showinfo(
                "Completato",
                f"Sync + Upload completato in {elapsed:.1f}s\n\n"
                f"Bandi Railway: {res.get('bandi', 0):,}\n"
                f"Albi ripristinati: {res.get('albi_ripristinati', 0)}"))

        finally:
            if os.path.exists(gz_path):
                try:
                    os.remove(gz_path)
                except Exception:
                    pass


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    root = tk.Tk()
    try:
        root.iconbitmap(default="")
    except Exception:
        pass
    SyncUploadApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
