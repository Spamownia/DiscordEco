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

# Lista SteamID adminów
ADMINS = ["76561197992396189"]  # przykładowy SteamID admina

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
def get_player_by_steam(steam_id):
    conn = get_db_connection()
    if not conn:
        return None
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM players WHERE steam_id=%s", (steam_id,))
    player = cursor.fetchone()
    cursor.close()
    conn.close()
    return player

def get_player_by_nick(nickname):
    conn = get_db_connection()
    if not conn:
        return None
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM players WHERE nickname=%s", (nickname,))
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
        (amount if tx_type=="income" else -amount, player_id)
    )
    conn.commit()
    cursor.close()
    conn.close()
    return True

def get_shop_items():
    conn = get_db_connection()
    if not conn:
        return []
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM shop_items ORDER BY id")
    items = cursor.fetchall()
    cursor.close()
    conn.close()
    return items

def add_shop_item(name, price):
    conn = get_db_connection()
    if not conn:
        return False
    cursor = conn.cursor()
    cursor.execute("INSERT INTO shop_items (name, price) VALUES (%s, %s)", (name, price))
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
    data = request.get_json()
    if not data or "steam_id" not in data or "nickname" not in data:
        return jsonify({"ok": False, "error": "steam_id i nickname wymagane"}), 400

    steam_id = data["steam_id"]
    nickname = data["nickname"]

    player = get_player_by_steam(steam_id)
    if player:
        return jsonify({"ok": True, "message": "Witaj ponownie!", "balance": float(player["balance"])})
    else:
        player_id = create_player(steam_id, nickname)
        add_transaction(player_id, 50, "income", "daily_bonus")
        return jsonify({"ok": True, "message": "Konto utworzone. 50 monet bonusu!", "balance": 50})

@app.route("/balance/<target_steam_id>", methods=["GET"])
def balance(target_steam_id):
    caller_steam_id = request.headers.get("X-STEAM-ID")
    if not caller_steam_id:
        return jsonify({"ok": False, "error": "Brak nagłówka X-STEAM-ID"}), 400

    # Gracz może tylko swoje konto, admin każde
    if caller_steam_id not in ADMINS and caller_steam_id != target_steam_id:
        return jsonify({"ok": False, "error": "Nie masz uprawnień do podglądu tego konta"}), 403

    player = get_player_by_steam(target_steam_id)
    if not player:
        return jsonify({"ok": False, "error": "Gracz nie istnieje"}), 404

    return jsonify({"ok": True, "balance": float(player["balance"])})

@app.route("/transactions", methods=["GET"])
def transactions():
    caller_steam_id = request.headers.get("X-STEAM-ID")
    nickname = request.args.get("nickname")
    if not caller_steam_id:
        return jsonify({"ok": False, "error": "Brak nagłówka X-STEAM-ID"}), 400
    if not nickname:
        return jsonify({"ok": False, "error": "Brak parametru nickname"}), 400

    player = get_player_by_nick(nickname)
    if not player:
        return jsonify({"ok": False, "error": "Gracz nie istnieje"}), 404

    # Gracz może tylko swoje konto, admin każde
    if caller_steam_id not in ADMINS and caller_steam_id != player["steam_id"]:
        return jsonify({"ok": False, "error": "Nie masz uprawnień do podglądu tego konta"}), 403

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

    return jsonify({"ok": True, "nickname": nickname, "transactions": txs})

@app.route("/shop", methods=["GET"])
def shop():
    items = get_shop_items()
    for item in items:
        item["price"] = float(item["price"])
    return jsonify({"ok": True, "items": items})

@app.route("/buy", methods=["POST"])
def buy():
    data = request.get_json()
    if not data or "steam_id" not in data or "item_id" not in data:
        return jsonify({"ok": False, "error": "steam_id i item_id wymagane"}), 400

    steam_id = data["steam_id"]
    item_id = data["item_id"]

    player = get_player_by_steam(steam_id)
    if not player:
        return jsonify({"ok": False, "error": "Gracz nie istnieje"}), 404

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM shop_items WHERE id=%s", (item_id,))
    item = cursor.fetchone()
    cursor.close()
    conn.close()
    if not item:
        return jsonify({"ok": False, "error": "Item nie istnieje"}), 404

    if player["balance"] < item["price"]:
        return jsonify({"ok": False, "error": "Brak wystarczających środków"}), 400

    add_transaction(player["id"], item["price"], "expense", f"buy_{item['name']}")
    return jsonify({"ok": True, "message": f"Kupiono {item['name']}", "balance": float(player['balance'] - item["price"])})

# ---------------- ADMIN ROUTES ----------------
@app.route("/admin/add_item", methods=["POST"])
def admin_add_item():
    data = request.get_json()
    if not data or "steam_id" not in data or "name" not in data or "price" not in data:
        return jsonify({"ok": False, "error": "steam_id, name, price wymagane"}), 400

    steam_id = data["steam_id"]
    if steam_id not in ADMINS:
        return jsonify({"ok": False, "error": "Brak uprawnień"}), 403

    name = data["name"]
    price = data["price"]

    add_shop_item(name, price)
    return jsonify({"ok": True, "message": f"Dodano item: {name} za {price} monet"})

@app.route("/admin/give_money", methods=["POST"])
def admin_give_money():
    data = request.get_json()
    if not data or "steam_id" not in data or "target_steam_id" not in data or "amount" not in data:
        return jsonify({"ok": False, "error": "steam_id, target_steam_id, amount wymagane"}), 400

    steam_id = data["steam_id"]
    if steam_id not in ADMINS:
        return jsonify({"ok": False, "error": "Brak uprawnień"}), 403

    target_steam_id = data["target_steam_id"]
    amount = float(data["amount"])
    player = get_player_by_steam(target_steam_id)
    if not player:
        return jsonify({"ok": False, "error": "Gracz nie istnieje"}), 404

    add_transaction(player["id"], amount, "income", "admin_grant")
    return jsonify({"ok": True, "message": f"Dodano {amount} monet graczowi {player['nickname']}", "balance": float(player["balance"] + amount)})

# ---------------- START ----------------
if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
