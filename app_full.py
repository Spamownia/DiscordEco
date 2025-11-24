#!/usr/bin/env python3
import mysql.connector
from mysql.connector import errorcode
from flask import Flask, request, jsonify
from datetime import datetime

# ---------------- CONFIG ----------------
DB_HOST = "mysql-1f2c991-spamownia91-479a.h.aivencloud.com"
DB_PORT = 14365
DB_USER = "avnadmin"
DB_PASS = "AVNS_6gzpU-skelov685O3Gx"
DB_NAME = "defaultdb"

DISCORD_WEBHOOK = "https://discord.com/api/webhooks/..."  # opcjonalnie, jeśli będziemy wysyłać powiadomienia

# ---------------- FLASK ----------------
app = Flask(__name__)

# ---------------- DATABASE ----------------
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


# ---------------- HELPERS ----------------
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


# ---------------- ROUTES ----------------
@app.route("/")
def home():
    return "SCUM Economy Bot działa!"


@app.route("/login", methods=["POST"])
def login():
    """
    Logowanie gracza.
    Body JSON: { "steam_id": "...", "nickname": "..." }
    """
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
        # Przyznaj bonus startowy 50 monet
        add_transaction(player_id, 50, "income", "daily_bonus")
        return jsonify({"ok": True, "message": "Konto utworzone. 50 monet bonusu!", "balance": 50})


@app.route("/balance/<steam_id>", methods=["GET"])
def balance(steam_id):
    player = get_player(steam_id)
    if not player:
        return jsonify({"ok": False, "error": "Gracz nie istnieje"}), 404
    return jsonify({"ok": True, "balance": float(player["balance"])})


@app.route("/transactions/<steam_id>", methods=["GET"])
def transactions(steam_id):
    player = get_player(steam_id)
    if not player:
        return jsonify({"ok": False, "error": "Gracz nie istnieje"}), 404

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute(
        "SELECT amount, type, source, created_at FROM transactions WHERE player_id=%s ORDER BY created_at DESC",
        (player["id"],)
    )
    txs = cursor.fetchall()
    cursor.close()
    conn.close()

    for tx in txs:
        tx["amount"] = float(tx["amount"])
        tx["created_at"] = tx["created_at"].strftime("%Y-%m-%d %H:%M:%S")
    return jsonify({"ok": True, "transactions": txs})


# ---------------- START ----------------
if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
