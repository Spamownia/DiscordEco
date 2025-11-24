#!/usr/bin/env python3
import os
import threading
import time
import ftplib
import json
import requests
from flask import Flask, jsonify, request
from datetime import datetime
import mysql.connector
from mysql.connector import errorcode

# ---------------- CONFIG ----------------
# --- MySQL ---
DB_HOST = "mysql-1f2c991-spamownia91-479a.h.aivencloud.com"
DB_PORT = 14365
DB_USER = "avnadmin"
DB_PASS = "AVNS_6gzpU-skelov685O3Gx"
DB_NAME = "defaultdb"

# --- FTP ---
FTP_HOST = "195.179.226.218"
FTP_PORT = 56421
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
REMOTE_DIR = "/SCUM/Saved/Config/WindowsServer/Loot"

# --- Discord webhook ---
DISCORD_WEBHOOK = "https://discord.com/api/webhooks/1442597158061736108/qw7YNghtxH0ol7nCaivCOe_iM-SAw62W27KeIUYo_a5yqr6oTPkETN_y8uEyC0CzC32x"

# --- Scheduler times (CET) ---
RUN_TIMES = [(3, 55), (9, 55), (15, 55), (21, 55)]

# --- Loot variants ---
VARIANTS = [f"GeneralZoneModifiers_{i}.json" for i in range(1, 92)]
TMP_REMOTE_NAME = "._tmp_upload.json"
TARGET_REMOTE_NAME = "GeneralZoneModifiers.json"
# ----------------------------------------

app = Flask(__name__)
_worker_thread = None
_worker_stop = threading.Event()
_last_chosen = None
_last_run_date = None
_lock = threading.Lock()


# ---------------- MySQL ----------------
def get_db_connection():
    try:
        return mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASS,
            database=DB_NAME,
            ssl_disabled=True
        )
    except mysql.connector.Error as err:
        print("Database connection error:", err)
        return None


def get_player(steam_id):
    conn = get_db_connection()
    if not conn:
        return None
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM players WHERE steam_id=%s", (steam_id,))
    player = cursor.fetchone()
    cursor.close()
    conn.close()
    return player


def create_player(steam_id, nickname):
    conn = get_db_connection()
    if not conn:
        return None
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO players (steam_id, nickname, balance) VALUES (%s, %s, 0)",
        (steam_id, nickname)
    )
    conn.commit()
    player_id = cursor.lastrowid
    cursor.close()
    conn.close()
    return player_id


def add_transaction(player_id, amount, tx_type, source):
    conn = get_db_connection()
    if not conn:
        return False
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO transactions (player_id, amount, type, source) VALUES (%s, %s, %s, %s)",
        (player_id, amount, tx_type, source)
    )
    cursor.execute(
        "UPDATE players SET balance = balance + %s WHERE id = %s",
        (amount if tx_type == "income" else -amount, player_id)
    )
    conn.commit()
    cursor.close()
    conn.close()
    return True


# ---------------- FTP ----------------
def upload_to_ftp(local_file: str) -> bool:
    try:
        print(f"[FTP] Connecting to {FTP_HOST}:{FTP_PORT} ...")
        with ftplib.FTP() as ftp:
            ftp.connect(FTP_HOST, FTP_PORT, timeout=20)
            ftp.login(FTP_USER, FTP_PASS)
            ftp.cwd(REMOTE_DIR)
            with open(local_file, "rb") as f:
                ftp.storbinary(f"STOR {TMP_REMOTE_NAME}", f)
            try:
                ftp.delete(TARGET_REMOTE_NAME)
            except Exception:
                pass
            ftp.rename(TMP_REMOTE_NAME, TARGET_REMOTE_NAME)
        print(f"[FTP] Upload of {local_file} successful.")
        return True
    except Exception as e:
        print("[FTP] Error:", e)
        return False


# ---------------- Discord ----------------
def send_discord_notification(chosen_file: str):
    try:
        with open(chosen_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        modifiers = data.get("Modifiers", [])
        zone_names = []
        for mod in modifiers:
            zones = mod.get("Zones", [])
            for z in zones:
                if "Name" in z:
                    zone_names.append(z["Name"])
        content = f"🎲 **Active Double Loot Zones:** {', '.join(zone_names)}" if zone_names else f"🎲 Variant: {chosen_file}"
    except Exception as e:
        content = f"🎲 Variant: {chosen_file} (error reading JSON: {e})"

    timestamp = int(time.time())
    content += f"\n⏱ Last draw: <t:{timestamp}:R>"

    try:
        r = requests.post(DISCORD_WEBHOOK, json={"content": content}, timeout=15)
        print(f"[Discord] Response: {r.status_code}")
    except Exception as e:
        print("[Discord] Error sending webhook:", e)


# ---------------- Scheduler ----------------
def choose_variant():
    global _last_chosen
    with _lock:
        chosen = random.choice(VARIANTS)
        if _last_chosen and len(VARIANTS) > 1:
            attempts = 0
            while chosen == _last_chosen and attempts < 5:
                chosen = random.choice(VARIANTS)
                attempts += 1
        _last_chosen = chosen
    return chosen


def run_cycle():
    chosen = choose_variant()
    if not os.path.isfile(chosen):
        print(f"[Cycle] ERROR: file not found {chosen}")
        return
    print(f"[Cycle] Running cycle with {chosen}")
    if upload_to_ftp(chosen):
        send_discord_notification(chosen)


def should_run_now():
    global _last_run_date
    now = datetime.now()
    current_hm = (now.hour, now.minute)
    if current_hm not in RUN_TIMES:
        return False
    if _last_run_date == now.date() and getattr(should_run_now, "last_minute", None) == now.minute:
        return False
    should_run_now.last_minute = now.minute
    _last_run_date = now.date()
    return True


def background_worker():
    print("[Worker] Scheduler started.")
    while not _worker_stop.is_set():
        try:
            if should_run_now():
                print("[Scheduler] Scheduled time hit — running cycle.")
                run_cycle()
        except Exception as e:
            print("[Worker] Exception:", e)
        time.sleep(15)
    print("[Worker] Scheduler stopped.")


def start_background_thread():
    global _worker_thread
    if not _worker_thread or not _worker_thread.is_alive():
        _worker_stop.clear()
        _worker_thread = threading.Thread(target=background_worker, daemon=True)
        _worker_thread.start()


# ---------------- Flask routes ----------------
@app.route("/")
def index():
    return "SCUM Economy + Loot Bot działa!"


@app.route("/login", methods=["POST"])
def login():
    data = request.get_json()
    if not data or "steam_id" not in data or "nickname" not in data:
        return jsonify({"ok": False, "error": "steam_id i nickname wymagane"}), 400

    steam_id = data["steam_id"]
    nickname = data["nickname"]

    player = get_player(steam_id)
    if player:
        return jsonify({"ok": True, "message": "Witaj ponownie!", "balance": float(player["balance"])})
    else:
        player_id = create_player(steam_id, nickname)
        add_transaction(player_id, 50, "income", "daily_bonus")
        return jsonify({"ok": True, "message": "Konto utworzone. 50 monet bonusu!", "balance": 50})


@app.route("/balance/<steam_id>")
def balance(steam_id):
    player = get_player(steam_id)
    if not player:
        return jsonify({"ok": False, "error": "Gracz nie istnieje"}), 404
    return jsonify({"ok": True, "balance": float(player["balance"])})


@app.route("/transactions/<steam_id>")
def transactions(steam_id):
    player = get_player(steam_id)
    if not player:
        return jsonify({"ok": False, "error": "Gracz nie istnieje"}), 404

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT amount, type, source, created_at FROM transactions WHERE player_id=%s ORDER BY created_at DESC", (player["id"],))
    txs = cursor.fetchall()
    cursor.close()
    conn.close()
    for tx in txs:
        tx["amount"] = float(tx["amount"])
        tx["created_at"] = tx["created_at"].strftime("%Y-%m-%d %H:%M:%S")
    return jsonify({"ok": True, "transactions": txs})


# ---------------- Start ----------------
start_background_thread()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
