"""
upload_db_gui.py — Carica il DB SQLite locale su Railway in un unico upload.

Molto più veloce del push record-per-record:
  - Comprime il file .db con gzip (riduce ~75%)
  - Upload HTTP POST streaming a /api/upload-db
  - Il server sostituisce il DB preservando albi_fornitori

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
from urllib.parse import urlencode

import requests

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(BASE_DIR, "data", "push_config.json")


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


def human_size(n: int) -> str:
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
        msg = self.format(record)
        level = record.levelno
        tag = "info" if level == logging.INFO else "warning" if level == logging.WARNING else "error"

        def append():
            self.text_widget.configure(state="normal")
            self.text_widget.insert(tk.END, msg + "\n", tag)
            self.text_widget.configure(state="disabled")
            self.text_widget.see(tk.END)
        self.text_widget.after(0, append)


class StreamingGzipReader:
    """Comprime un file in gzip on-the-fly mentre viene letto da requests.

    Fornisce __iter__ per requests streaming upload.
    Aggiorna callback(bytes_read_source, bytes_produced) periodicamente.
    """
    def __init__(self, path, chunk_size=1024 * 1024, callback=None):
        self.path = path
        self.chunk_size = chunk_size
        self.callback = callback
        self.bytes_read = 0
        self.bytes_out = 0
        self.size = os.path.getsize(path)

    def __iter__(self):
        import io
        compressor_buf = io.BytesIO()
        gz = gzip.GzipFile(fileobj=compressor_buf, mode="wb", compresslevel=6)
        with open(self.path, "rb") as fin:
            while True:
                data = fin.read(self.chunk_size)
                if not data:
                    break
                gz.write(data)
                self.bytes_read += len(data)
                # Estrai ciò che è stato compresso finora
                compressor_buf.seek(0)
                out = compressor_buf.read()
                compressor_buf.seek(0)
                compressor_buf.truncate()
                if out:
                    self.bytes_out += len(out)
                    if self.callback:
                        self.callback(self.bytes_read, self.bytes_out, self.size)
                    yield out
            gz.close()
            compressor_buf.seek(0)
            tail = compressor_buf.read()
            if tail:
                self.bytes_out += len(tail)
                if self.callback:
                    self.callback(self.bytes_read, self.bytes_out, self.size)
                yield tail


class UploadDbApp:
    def __init__(self, root):
        self.root = root
        self.root.title("ANAC Upload DB — Railway")
        self.root.geometry("820x640")
        self.root.configure(bg="#f5f5f5")

        self.cfg = load_config()
        self._running = False
        self._build_ui()
        self._setup_logging()
        self._refresh_status()

    def _build_ui(self):
        root = self.root
        PAD = 12

        title_frame = tk.Frame(root, bg="#1565c0", pady=10)
        title_frame.pack(fill="x")
        tk.Label(title_frame, text="ANAC Upload DB", font=("Segoe UI", 16, "bold"),
                 bg="#1565c0", fg="white").pack()
        tk.Label(title_frame, text="Carica il DB SQLite completo su Railway (gzip streaming)",
                 font=("Segoe UI", 9), bg="#1565c0", fg="#bbdefb").pack()

        # Config
        cfg_frame = ttk.LabelFrame(root, text="Configurazione", padding=PAD)
        cfg_frame.pack(fill="x", padx=PAD, pady=(PAD, 0))

        tk.Label(cfg_frame, text="Railway URL:", font=("Segoe UI", 9)).grid(row=0, column=0, sticky="w", padx=(0, 8))
        self.url_var = tk.StringVar(value=self.cfg.get("railway_url", ""))
        ttk.Entry(cfg_frame, textvariable=self.url_var, width=50, font=("Segoe UI", 9)
                  ).grid(row=0, column=1, sticky="ew", padx=(0, 8))

        tk.Label(cfg_frame, text="Chiave API:", font=("Segoe UI", 9)).grid(row=1, column=0, sticky="w", padx=(0, 8), pady=(6, 0))
        self.key_var = tk.StringVar(value=self.cfg.get("sync_secret", ""))
        ttk.Entry(cfg_frame, textvariable=self.key_var, width=20, font=("Segoe UI", 9), show="*"
                  ).grid(row=1, column=1, sticky="w", padx=(0, 8), pady=(6, 0))

        tk.Label(cfg_frame, text="File DB:", font=("Segoe UI", 9)).grid(row=2, column=0, sticky="w", padx=(0, 8), pady=(6, 0))
        self.db_var = tk.StringVar(value=self.cfg.get("db_path", default_db_path()))
        ttk.Entry(cfg_frame, textvariable=self.db_var, width=50, font=("Segoe UI", 9)
                  ).grid(row=2, column=1, sticky="ew", padx=(0, 8), pady=(6, 0))
        ttk.Button(cfg_frame, text="Sfoglia…", command=self._pick_db, width=10
                   ).grid(row=2, column=2, pady=(6, 0))

        ttk.Button(cfg_frame, text="Salva", command=self._save_cfg, width=8
                   ).grid(row=0, column=2, rowspan=2)
        cfg_frame.columnconfigure(1, weight=1)

        # Stato
        status_frame = ttk.LabelFrame(root, text="Stato", padding=PAD)
        status_frame.pack(fill="x", padx=PAD, pady=(8, 0))
        self.lbl_size = tk.Label(status_frame, text="Dimensione DB: —", font=("Segoe UI", 9))
        self.lbl_size.grid(row=0, column=0, sticky="w", padx=(0, 30))
        self.lbl_sent = tk.Label(status_frame, text="Inviato: —", font=("Segoe UI", 9, "bold"), fg="#1565c0")
        self.lbl_sent.grid(row=0, column=1, sticky="w", padx=(0, 30))
        self.lbl_speed = tk.Label(status_frame, text="Velocità: —", font=("Segoe UI", 9))
        self.lbl_speed.grid(row=0, column=2, sticky="w")
        ttk.Button(status_frame, text="↻ Aggiorna", command=self._refresh_status, width=10
                   ).grid(row=0, column=3, sticky="e", padx=(20, 0))
        status_frame.columnconfigure(3, weight=1)

        # Pulsanti
        btn_frame = tk.Frame(root, bg="#f5f5f5", pady=8)
        btn_frame.pack(fill="x", padx=PAD)

        self.btn_upload = tk.Button(
            btn_frame, text="⬆  Upload DB su Railway",
            font=("Segoe UI", 10, "bold"), bg="#1565c0", fg="white",
            activebackground="#0d47a1", activeforeground="white",
            relief="flat", padx=18, pady=8, cursor="hand2",
            command=self._run_upload,
        )
        self.btn_upload.pack(side="left", padx=(0, 8))

        self.btn_clear = tk.Button(
            btn_frame, text="Pulisci log",
            font=("Segoe UI", 9), bg="#eeeeee", fg="#333",
            relief="flat", padx=10, pady=8, cursor="hand2",
            command=self._clear_log,
        )
        self.btn_clear.pack(side="right")

        # Progress bar
        self.progress = ttk.Progressbar(root, mode="determinate", maximum=100)
        self.progress.pack(fill="x", padx=PAD, pady=(0, 4))

        # Log
        log_frame = ttk.LabelFrame(root, text="Log", padding=4)
        log_frame.pack(fill="both", expand=True, padx=PAD, pady=(0, PAD))
        self.log_text = tk.Text(
            log_frame, state="disabled", wrap="word",
            font=("Consolas", 9), bg="#1e1e1e", fg="#e0e0e0",
            relief="flat", padx=6, pady=6,
        )
        self.log_text.tag_configure("info", foreground="#e0e0e0")
        self.log_text.tag_configure("warning", foreground="#ffcc02")
        self.log_text.tag_configure("error", foreground="#ef5350")
        scrollbar = ttk.Scrollbar(log_frame, command=self.log_text.yview)
        self.log_text.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side="right", fill="y")
        self.log_text.pack(fill="both", expand=True)

        self.status_var = tk.StringVar(value="Pronto")
        tk.Label(root, textvariable=self.status_var, font=("Segoe UI", 8),
                 bg="#e0e0e0", anchor="w", padx=8).pack(fill="x", side="bottom")

    def _setup_logging(self):
        handler = GuiLogHandler(self.log_text)
        handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s",
                                               datefmt="%H:%M:%S"))
        logging.getLogger().addHandler(handler)
        logging.getLogger().setLevel(logging.INFO)

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
        self.cfg["db_path"] = self.db_var.get().strip()
        save_config(self.cfg)
        self.status_var.set("Configurazione salvata")

    def _refresh_status(self):
        path = self.db_var.get().strip()
        if path and os.path.exists(path):
            self.lbl_size.config(text=f"Dimensione DB: {human_size(os.path.getsize(path))}")
        else:
            self.lbl_size.config(text="Dimensione DB: (file non trovato)")
        self.lbl_sent.config(text="Inviato: —")
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
        db = self.db_var.get().strip()

        if not url:
            messagebox.showerror("Errore", "Inserisci il Railway URL.")
            return
        if not key:
            messagebox.showerror("Errore", "Inserisci la chiave API.")
            return
        if not db or not os.path.exists(db):
            messagebox.showerror("Errore", f"File DB non trovato:\n{db}")
            return
        if not messagebox.askyesno(
            "Conferma upload",
            f"Stai per caricare il DB:\n{db}\n"
            f"Dimensione: {human_size(os.path.getsize(db))}\n\n"
            f"Il DB su Railway sarà SOSTITUITO (gli albi fornitori sono preservati).\n\n"
            f"Procedere?"):
            return

        self._save_cfg()
        self._running = True
        self.btn_upload.config(state="disabled")
        self.progress["value"] = 0
        self.status_var.set("Upload in corso...")
        threading.Thread(target=self._upload_worker,
                         args=(url, key, db), daemon=True).start()

    def _update_progress(self, bytes_read, bytes_out, total, start_time):
        pct = (bytes_read / total * 100) if total else 0
        elapsed = max(time.time() - start_time, 0.001)
        speed = bytes_out / elapsed

        def apply():
            self.progress["value"] = pct
            self.lbl_sent.config(
                text=f"Inviato: {human_size(bytes_out)} "
                     f"(letti {human_size(bytes_read)}/{human_size(total)})")
            self.lbl_speed.config(text=f"Velocità: {human_size(speed)}/s")
        self.root.after(0, apply)

    def _upload_worker(self, url, key, db):
        try:
            size = os.path.getsize(db)
            logging.info(f"File: {db}")
            logging.info(f"Dimensione: {human_size(size)}")
            logging.info(f"Endpoint: {url}/api/upload-db")
            logging.info("Compressione gzip in streaming + upload...")

            start = time.time()

            def cb(br, bo, tot):
                self._update_progress(br, bo, tot, start)

            reader = StreamingGzipReader(db, chunk_size=1024 * 1024, callback=cb)
            endpoint = f"{url}/api/upload-db?{urlencode({'key': key, 'gzip': '1'})}"
            headers = {
                "Content-Type": "application/octet-stream",
                "Content-Encoding": "gzip",
            }

            r = requests.post(endpoint, data=iter(reader), headers=headers, timeout=3600)
            elapsed = time.time() - start

            if r.status_code == 200:
                res = r.json()
                logging.info(f"✓ Upload completato in {elapsed:.1f}s")
                logging.info(f"  Ricevuti: {human_size(res.get('bytes_received', 0))}")
                logging.info(f"  Bandi nel DB: {res.get('bandi', 0):,}")
                logging.info(f"  Albi ripristinati: {res.get('albi_ripristinati', 0)}")
                logging.info(f"  Velocità media: {human_size(reader.bytes_out / max(elapsed, 0.001))}/s")
                self.root.after(0, lambda: self.status_var.set("Completato"))
                self.root.after(0, lambda: messagebox.showinfo(
                    "Upload completato",
                    f"DB caricato in {elapsed:.1f}s\n"
                    f"Bandi: {res.get('bandi', 0):,}\n"
                    f"Albi ripristinati: {res.get('albi_ripristinati', 0)}"))
            else:
                logging.error(f"✗ Errore HTTP {r.status_code}: {r.text[:400]}")
                self.root.after(0, lambda: self.status_var.set(f"Errore {r.status_code}"))
        except Exception as e:
            logging.error(f"✗ Upload fallito: {e}")
            self.root.after(0, lambda: self.status_var.set(f"Errore: {e}"))
        finally:
            self._running = False
            self.root.after(0, lambda: self.btn_upload.config(state="normal"))


def main():
    root = tk.Tk()
    UploadDbApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
