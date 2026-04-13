"""
push_sync_gui.py — Interfaccia grafica per sincronizzare ANAC → Railway
Avvio: python push_sync_gui.py
"""

import tkinter as tk
from tkinter import ttk, messagebox
import threading
import logging
import os
import json
import sqlite3
import sys
from datetime import datetime

# ── Percorsi ──
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(BASE_DIR, "data", "push_config.json")
LAST_PUSH_FILE = os.path.join(BASE_DIR, "data", "last_push.txt")

# ── Config ──
def load_config():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {"railway_url": ""}

def save_config(cfg):
    os.makedirs(os.path.dirname(CONFIG_FILE), exist_ok=True)
    with open(CONFIG_FILE, "w") as f:
        json.dump(cfg, f)

def read_last_push():
    if os.path.exists(LAST_PUSH_FILE):
        with open(LAST_PUSH_FILE) as f:
            ts = f.read().strip()
            if ts:
                try:
                    dt = datetime.fromisoformat(ts)
                    return dt.strftime("%d/%m/%Y %H:%M:%S")
                except Exception:
                    return ts
    return "Mai"

def count_local_records(since=None):
    try:
        from config import DB_PATH
        conn = sqlite3.connect(DB_PATH)
        if since:
            n = conn.execute("SELECT COUNT(*) FROM bandi WHERE data_import > ?", (since,)).fetchone()[0]
        else:
            n = conn.execute("SELECT COUNT(*) FROM bandi").fetchone()[0]
        conn.close()
        return n
    except Exception:
        return 0

def read_last_push_raw():
    if os.path.exists(LAST_PUSH_FILE):
        with open(LAST_PUSH_FILE) as f:
            ts = f.read().strip()
            if ts:
                return ts
    return "1970-01-01T00:00:00"


# ── Logging handler che scrive nella Text widget ──
class GuiLogHandler(logging.Handler):
    COLORS = {
        logging.INFO:    "#e8f5e9",
        logging.WARNING: "#fff8e1",
        logging.ERROR:   "#ffebee",
    }

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


# ── App principale ──
class PushSyncApp:
    def __init__(self, root):
        self.root = root
        self.root.title("ANAC Push Sync — Railway")
        self.root.geometry("800x620")
        self.root.resizable(True, True)
        self.root.configure(bg="#f5f5f5")

        self.cfg = load_config()
        self._running = False
        self._build_ui()
        self._setup_logging()
        self._refresh_status()

    def _build_ui(self):
        root = self.root
        PAD = 12

        # ── Titolo ──
        title_frame = tk.Frame(root, bg="#1565c0", pady=10)
        title_frame.pack(fill="x")
        tk.Label(title_frame, text="ANAC Push Sync", font=("Segoe UI", 16, "bold"),
                 bg="#1565c0", fg="white").pack()
        tk.Label(title_frame, text="Sincronizza il DB locale con Railway",
                 font=("Segoe UI", 9), bg="#1565c0", fg="#bbdefb").pack()

        # ── Configurazione ──
        cfg_frame = ttk.LabelFrame(root, text="Configurazione", padding=PAD)
        cfg_frame.pack(fill="x", padx=PAD, pady=(PAD, 0))

        tk.Label(cfg_frame, text="Railway URL:", font=("Segoe UI", 9)).grid(row=0, column=0, sticky="w", padx=(0, 8))
        self.url_var = tk.StringVar(value=self.cfg.get("railway_url", ""))
        url_entry = ttk.Entry(cfg_frame, textvariable=self.url_var, width=50, font=("Segoe UI", 9))
        url_entry.grid(row=0, column=1, sticky="ew", padx=(0, 8))

        tk.Label(cfg_frame, text="Chiave API:", font=("Segoe UI", 9)).grid(row=1, column=0, sticky="w", padx=(0, 8), pady=(6, 0))
        self.key_var = tk.StringVar(value=self.cfg.get("sync_secret", ""))
        key_entry = ttk.Entry(cfg_frame, textvariable=self.key_var, width=20, font=("Segoe UI", 9), show="*")
        key_entry.grid(row=1, column=1, sticky="w", padx=(0, 8), pady=(6, 0))

        ttk.Button(cfg_frame, text="Salva", command=self._save_url, width=8).grid(row=0, column=2, rowspan=2)
        cfg_frame.columnconfigure(1, weight=1)

        # ── Stato ──
        status_frame = ttk.LabelFrame(root, text="Stato", padding=PAD)
        status_frame.pack(fill="x", padx=PAD, pady=(8, 0))

        self.lbl_last_push = tk.Label(status_frame, text="Ultimo push: —", font=("Segoe UI", 9), anchor="w")
        self.lbl_last_push.grid(row=0, column=0, sticky="w", padx=(0, 30))

        self.lbl_totale = tk.Label(status_frame, text="Record locali: —", font=("Segoe UI", 9), anchor="w")
        self.lbl_totale.grid(row=0, column=1, sticky="w", padx=(0, 30))

        self.lbl_da_inviare = tk.Label(status_frame, text="Da inviare: —", font=("Segoe UI", 9, "bold"),
                                        fg="#1565c0", anchor="w")
        self.lbl_da_inviare.grid(row=0, column=2, sticky="w")

        ttk.Button(status_frame, text="↻ Aggiorna", command=self._refresh_status, width=10
                   ).grid(row=0, column=3, sticky="e", padx=(20, 0))
        status_frame.columnconfigure(3, weight=1)

        # ── Pulsanti ──
        btn_frame = tk.Frame(root, bg="#f5f5f5", pady=8)
        btn_frame.pack(fill="x", padx=PAD)

        self.btn_sync_push = tk.Button(
            btn_frame, text="⬇  Sync ANAC + Push Railway",
            font=("Segoe UI", 10, "bold"), bg="#1565c0", fg="white",
            activebackground="#0d47a1", activeforeground="white",
            relief="flat", padx=18, pady=8, cursor="hand2",
            command=self._run_sync_push
        )
        self.btn_sync_push.pack(side="left", padx=(0, 8))

        self.btn_push = tk.Button(
            btn_frame, text="📤  Solo Push Railway",
            font=("Segoe UI", 10), bg="#2e7d32", fg="white",
            activebackground="#1b5e20", activeforeground="white",
            relief="flat", padx=18, pady=8, cursor="hand2",
            command=self._run_push
        )
        self.btn_push.pack(side="left", padx=(0, 8))

        self.btn_reset = tk.Button(
            btn_frame, text="🗑  Reset tracciamento",
            font=("Segoe UI", 10), bg="#757575", fg="white",
            activebackground="#424242", activeforeground="white",
            relief="flat", padx=14, pady=8, cursor="hand2",
            command=self._reset_push
        )
        self.btn_reset.pack(side="left")

        self.btn_clear = tk.Button(
            btn_frame, text="Pulisci log",
            font=("Segoe UI", 9), bg="#eeeeee", fg="#333",
            relief="flat", padx=10, pady=8, cursor="hand2",
            command=self._clear_log
        )
        self.btn_clear.pack(side="right")

        # ── Progress ──
        self.progress = ttk.Progressbar(root, mode="indeterminate")
        self.progress.pack(fill="x", padx=PAD, pady=(0, 4))

        # ── Log ──
        log_frame = ttk.LabelFrame(root, text="Log", padding=4)
        log_frame.pack(fill="both", expand=True, padx=PAD, pady=(0, PAD))

        self.log_text = tk.Text(
            log_frame, state="disabled", wrap="word",
            font=("Consolas", 9), bg="#1e1e1e", fg="#e0e0e0",
            relief="flat", padx=6, pady=6
        )
        self.log_text.tag_configure("info",    foreground="#e0e0e0")
        self.log_text.tag_configure("warning", foreground="#ffcc02")
        self.log_text.tag_configure("error",   foreground="#ef5350")

        scrollbar = ttk.Scrollbar(log_frame, command=self.log_text.yview)
        self.log_text.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side="right", fill="y")
        self.log_text.pack(fill="both", expand=True)

        # ── Status bar ──
        self.status_var = tk.StringVar(value="Pronto")
        tk.Label(root, textvariable=self.status_var, font=("Segoe UI", 8),
                 bg="#e0e0e0", anchor="w", padx=8).pack(fill="x", side="bottom")

    def _setup_logging(self):
        handler = GuiLogHandler(self.log_text)
        handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s",
                                                datefmt="%H:%M:%S"))
        logging.getLogger().addHandler(handler)
        logging.getLogger().setLevel(logging.INFO)

    def _refresh_status(self):
        last = read_last_push()
        last_raw = read_last_push_raw()
        totale = count_local_records()
        da_inviare = count_local_records(since=last_raw)

        self.lbl_last_push.config(text=f"Ultimo push: {last}")
        self.lbl_totale.config(text=f"Record locali: {totale:,}")
        color = "#c62828" if da_inviare > 0 else "#2e7d32"
        self.lbl_da_inviare.config(text=f"Da inviare: {da_inviare:,}", fg=color)

    def _save_url(self):
        url = self.url_var.get().strip().rstrip("/")
        key = self.key_var.get().strip()
        self.cfg["railway_url"] = url
        self.cfg["sync_secret"] = key
        save_config(self.cfg)
        self.status_var.set(f"Configurazione salvata")

    def _set_buttons_state(self, enabled: bool):
        state = "normal" if enabled else "disabled"
        for btn in (self.btn_sync_push, self.btn_push, self.btn_reset):
            btn.config(state=state)

    def _run_sync_push(self):
        self._start_task(sync_first=True)

    def _run_push(self):
        self._start_task(sync_first=False)

    def _start_task(self, sync_first: bool):
        if self._running:
            return
        url = self.url_var.get().strip().rstrip("/")
        if not url:
            messagebox.showerror("Errore", "Inserisci il Railway URL prima di procedere.")
            return
        self._save_url()
        os.environ["RAILWAY_URL"] = url
        os.environ["SYNC_SECRET"] = self.key_var.get().strip()
        self._running = True
        self._set_buttons_state(False)
        self.progress.start(10)
        label = "Sync ANAC + Push Railway" if sync_first else "Push Railway"
        self.status_var.set(f"{label} in corso...")
        threading.Thread(target=self._task_worker, args=(sync_first,), daemon=True).start()

    def _task_worker(self, sync_first: bool):
        try:
            import importlib
            import push_sync
            importlib.reload(push_sync)

            if sync_first:
                logging.info("=" * 50)
                logging.info("FASE 1: Sync ANAC")
                logging.info("=" * 50)
                from sync import sync as run_sync
                run_sync()

            logging.info("=" * 50)
            logging.info("FASE 2: Push verso Railway" if sync_first else "PUSH verso Railway")
            logging.info("=" * 50)
            push_sync.push()
            self.root.after(0, lambda: self.status_var.set("Completato con successo"))
        except SystemExit:
            self.root.after(0, lambda: self.status_var.set("Errore — vedi log"))
        except Exception as e:
            logging.error(f"Errore: {e}")
            self.root.after(0, lambda: self.status_var.set(f"Errore: {e}"))
        finally:
            self._running = False
            self.root.after(0, self._task_done)

    def _task_done(self):
        self.progress.stop()
        self._set_buttons_state(True)
        self._refresh_status()

    def _reset_push(self):
        if not messagebox.askyesno("Reset tracciamento",
                                    "Azzera last_push.txt?\n\nIl prossimo push invierà TUTTI i record locali."):
            return
        if os.path.exists(LAST_PUSH_FILE):
            os.remove(LAST_PUSH_FILE)
        logging.info("Tracciamento azzerato — il prossimo push invierà tutti i record")
        self._refresh_status()

    def _clear_log(self):
        self.log_text.configure(state="normal")
        self.log_text.delete("1.0", tk.END)
        self.log_text.configure(state="disabled")


def main():
    root = tk.Tk()
    try:
        root.iconbitmap(default="")
    except Exception:
        pass
    app = PushSyncApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
