"""
upload_db_gui.py — Carica il DB SQLite locale su Railway a blocchi (chunks).

Flusso:
  1. Comprime anac.db → file .gz temporaneo
  2. Divide in blocchi da CHUNK_MB (default 80 MB)
  3. POST di ogni blocco a /api/upload-db-chunk
  4. POST a /api/upload-db-finalize → il server riassembla e sostituisce il DB

Avvio: python upload_db_gui.py
"""

import tkinter as tk
from tkinter import ttk, messagebox, filedialog
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
CHUNK_MB    = 80  # dimensione massima di ogni blocco in MB


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


class UploadDbApp:
    def __init__(self, root):
        self.root    = root
        self.root.title("ANAC Upload DB — Railway")
        self.root.geometry("820x660")
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
        tf = tk.Frame(root, bg="#1565c0", pady=10)
        tf.pack(fill="x")
        tk.Label(tf, text="ANAC Upload DB", font=("Segoe UI", 16, "bold"),
                 bg="#1565c0", fg="white").pack()
        tk.Label(tf, text="Carica il DB SQLite locale su Railway — upload a blocchi",
                 font=("Segoe UI", 9), bg="#1565c0", fg="#bbdefb").pack()

        # Config
        cf = ttk.LabelFrame(root, text="Configurazione", padding=PAD)
        cf.pack(fill="x", padx=PAD, pady=(PAD, 0))

        tk.Label(cf, text="Railway URL:", font=("Segoe UI", 9)).grid(row=0, column=0, sticky="w", padx=(0,8))
        self.url_var = tk.StringVar(value=self.cfg.get("railway_url", ""))
        ttk.Entry(cf, textvariable=self.url_var, width=52, font=("Segoe UI", 9)
                  ).grid(row=0, column=1, sticky="ew", padx=(0,8))

        tk.Label(cf, text="Chiave API:", font=("Segoe UI", 9)).grid(row=1, column=0, sticky="w", padx=(0,8), pady=(6,0))
        self.key_var = tk.StringVar(value=self.cfg.get("sync_secret", ""))
        ttk.Entry(cf, textvariable=self.key_var, width=20, font=("Segoe UI", 9), show="*"
                  ).grid(row=1, column=1, sticky="w", padx=(0,8), pady=(6,0))

        tk.Label(cf, text="File DB:", font=("Segoe UI", 9)).grid(row=2, column=0, sticky="w", padx=(0,8), pady=(6,0))
        self.db_var = tk.StringVar(value=self.cfg.get("db_path", default_db_path()))
        ttk.Entry(cf, textvariable=self.db_var, width=52, font=("Segoe UI", 9)
                  ).grid(row=2, column=1, sticky="ew", padx=(0,8), pady=(6,0))
        ttk.Button(cf, text="Sfoglia…", command=self._pick_db, width=10
                   ).grid(row=2, column=2, pady=(6,0))

        ttk.Button(cf, text="Salva", command=self._save_cfg, width=8
                   ).grid(row=0, column=2, rowspan=2)
        cf.columnconfigure(1, weight=1)

        # Stato
        sf = ttk.LabelFrame(root, text="Stato", padding=PAD)
        sf.pack(fill="x", padx=PAD, pady=(8, 0))
        self.lbl_size  = tk.Label(sf, text="Dimensione DB: —", font=("Segoe UI", 9))
        self.lbl_size.grid(row=0, column=0, sticky="w", padx=(0,30))
        self.lbl_chunk = tk.Label(sf, text="Blocco: —", font=("Segoe UI", 9, "bold"), fg="#1565c0")
        self.lbl_chunk.grid(row=0, column=1, sticky="w", padx=(0,30))
        self.lbl_speed = tk.Label(sf, text="Velocità: —", font=("Segoe UI", 9))
        self.lbl_speed.grid(row=0, column=2, sticky="w")
        ttk.Button(sf, text="↻ Aggiorna", command=self._refresh_status, width=10
                   ).grid(row=0, column=3, sticky="e", padx=(20,0))
        sf.columnconfigure(3, weight=1)

        # Pulsanti
        bf = tk.Frame(root, bg="#f5f5f5", pady=8)
        bf.pack(fill="x", padx=PAD)
        self.btn_upload = tk.Button(
            bf, text="⬆  Upload DB su Railway",
            font=("Segoe UI", 10, "bold"), bg="#1565c0", fg="white",
            activebackground="#0d47a1", activeforeground="white",
            relief="flat", padx=18, pady=8, cursor="hand2",
            command=self._run_upload,
        )
        self.btn_upload.pack(side="left", padx=(0, 8))
        self.btn_clear = tk.Button(
            bf, text="Pulisci log",
            font=("Segoe UI", 9), bg="#eeeeee", fg="#333",
            relief="flat", padx=10, pady=8, cursor="hand2",
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

        self.status_var = tk.StringVar(value="Pronto")
        tk.Label(root, textvariable=self.status_var, font=("Segoe UI", 8),
                 bg="#e0e0e0", anchor="w", padx=8).pack(fill="x", side="bottom")

    def _setup_logging(self):
        h = GuiLogHandler(self.log_text)
        h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s",
                                         datefmt="%H:%M:%S"))
        logging.getLogger().addHandler(h)
        logging.getLogger().setLevel(logging.INFO)

    # ── Azioni ────────────────────────────────────────────────────────────────

    def _pick_db(self):
        p = filedialog.askopenfilename(
            title="Seleziona file DB SQLite",
            filetypes=[("SQLite DB", "*.db *.sqlite *.sqlite3"), ("Tutti", "*.*")],
            initialdir=os.path.dirname(self.db_var.get() or BASE_DIR),
        )
        if p:
            self.db_var.set(p)
            self._refresh_status()

    def _save_cfg(self):
        self.cfg["railway_url"] = self.url_var.get().strip().rstrip("/")
        self.cfg["sync_secret"] = self.key_var.get().strip()
        self.cfg["db_path"]     = self.db_var.get().strip()
        save_config(self.cfg)
        self.status_var.set("Configurazione salvata")

    def _refresh_status(self):
        path = self.db_var.get().strip()
        if path and os.path.exists(path):
            self.lbl_size.config(text=f"Dimensione DB: {human_size(os.path.getsize(path))}")
        else:
            self.lbl_size.config(text="Dimensione DB: (file non trovato)")
        self.lbl_chunk.config(text="Blocco: —")
        self.lbl_speed.config(text="Velocità: —")
        self.progress["value"] = 0

    def _clear_log(self):
        self.log_text.configure(state="normal")
        self.log_text.delete("1.0", tk.END)
        self.log_text.configure(state="disabled")

    def _run_upload(self):
        if self._running:
            return
        url = self.url_var.get().strip().rstrip("/")
        key = self.key_var.get().strip()
        db  = self.db_var.get().strip()

        if not url:
            messagebox.showerror("Errore", "Inserisci il Railway URL.")
            return
        if not key:
            messagebox.showerror("Errore", "Inserisci la chiave API.")
            return
        if not db or not os.path.exists(db):
            messagebox.showerror("Errore", f"File DB non trovato:\n{db}")
            return
        sz = os.path.getsize(db)
        if not messagebox.askyesno(
                "Conferma upload",
                f"Stai per caricare:\n{db}\n"
                f"Dimensione: {human_size(sz)}\n\n"
                f"Il DB su Railway sarà SOSTITUITO\n"
                f"(albi fornitori preservati).\n\nProcedere?"):
            return

        self._save_cfg()
        self._running = True
        self.btn_upload.config(state="disabled")
        self.progress["value"] = 0
        self.status_var.set("Upload in corso...")
        threading.Thread(target=self._upload_worker,
                         args=(url, key, db), daemon=True).start()

    # ── Worker ────────────────────────────────────────────────────────────────

    def _upload_worker(self, url, key, db):
        gz_path = db + ".upload_tmp.gz"
        try:
            chunk_bytes = CHUNK_MB * 1024 * 1024
            upload_id   = str(uuid.uuid4())[:8]
            db_size     = os.path.getsize(db)

            # ── FASE 1: comprimi ──────────────────────────────────────────────
            logging.info(f"File: {db}  ({human_size(db_size)})")
            logging.info(f"Comprimo con gzip (livello 6)...")
            t0 = time.time()
            with open(db, "rb") as fin, gzip.open(gz_path, "wb", compresslevel=6) as fout:
                read_so_far = 0
                while True:
                    block = fin.read(4 * 1024 * 1024)
                    if not block:
                        break
                    fout.write(block)
                    read_so_far += len(block)
                    pct = read_so_far / db_size * 50  # prima metà della barra
                    self.root.after(0, lambda p=pct: self._set_progress(p, "Compressione..."))

            gz_size = os.path.getsize(gz_path)
            ratio   = gz_size / db_size * 100
            logging.info(f"Compresso: {human_size(gz_size)} ({ratio:.0f}% dell'originale) "
                         f"in {time.time()-t0:.1f}s")

            # ── FASE 2: calcola chunks ────────────────────────────────────────
            total_chunks = (gz_size + chunk_bytes - 1) // chunk_bytes
            logging.info(f"Blocchi da inviare: {total_chunks} × {CHUNK_MB} MB "
                         f"(upload_id={upload_id})")

            session = requests.Session()
            start   = time.time()

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

                    # Retry 3x per chunk
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

                    # Aggiorna UI
                    bytes_sent = (i + 1) * chunk_bytes
                    elapsed    = max(time.time() - start, 0.001)
                    speed      = bytes_sent / elapsed
                    pct        = 50 + (i + 1) / total_chunks * 45  # seconda metà
                    msg        = f"Blocco {i+1}/{total_chunks}"
                    spd_str    = human_size(speed) + "/s"
                    self.root.after(0, lambda p=pct, m=msg, s=spd_str:
                                    self._update_stats(p, m, s))
                    logging.info(f"  [{i+1}/{total_chunks}] {human_size(len(chunk_data))}  "
                                 f"{spd_str}")

            # ── FASE 3: finalizza ─────────────────────────────────────────────
            logging.info("Invio comando di finalizzazione...")
            self.root.after(0, lambda: self._set_progress(97, "Finalizzazione..."))
            params = urlencode({
                "key":          key,
                "upload_id":    upload_id,
                "total_chunks": total_chunks,
            })
            r = session.post(f"{url}/api/upload-db-finalize?{params}", timeout=600)
            if r.status_code != 200:
                raise RuntimeError(f"Finalizzazione fallita: HTTP {r.status_code} — {r.text[:300]}")

            res     = r.json()
            elapsed = time.time() - t0
            self.root.after(0, lambda: self._set_progress(100, "Completato"))
            logging.info(f"✓ Upload completato in {elapsed:.1f}s")
            logging.info(f"  Bandi nel DB: {res.get('bandi', 0):,}")
            logging.info(f"  Albi ripristinati: {res.get('albi_ripristinati', 0)}")
            self.root.after(0, lambda: self.status_var.set("Completato"))
            self.root.after(0, lambda: messagebox.showinfo(
                "Upload completato",
                f"DB caricato in {elapsed:.1f}s\n"
                f"Bandi: {res.get('bandi', 0):,}\n"
                f"Albi ripristinati: {res.get('albi_ripristinati', 0)}"))

        except Exception as e:
            logging.error(f"✗ Upload fallito: {e}")
            self.root.after(0, lambda: self.status_var.set(f"Errore: {e}"))
        finally:
            self._running = False
            self.root.after(0, lambda: self.btn_upload.config(state="normal"))
            if os.path.exists(gz_path):
                try: os.remove(gz_path)
                except Exception: pass

    def _set_progress(self, pct, label):
        self.progress["value"] = pct
        self.status_var.set(label)

    def _update_stats(self, pct, chunk_label, speed_str):
        self.progress["value"] = pct
        self.lbl_chunk.config(text=f"Blocco: {chunk_label}")
        self.lbl_speed.config(text=f"Velocità: {speed_str}")


def main():
    root = tk.Tk()
    UploadDbApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
