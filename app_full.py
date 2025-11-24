#!/usr/bin/env python3
import os
import mysql.connector
from flask import Flask, request, jsonify

# ---------------- CONFIG ----------------
MYSQL_HOST = "mysql-1f2c991-spamownia91-479a.h.aivencloud.com"
MYSQL_PORT = 14365
MYSQL_USER = "avnadmin"
MYSQL_PASS = "AVNS_6gzpU-skelov685O3Gx"
MYSQL_DB   = "defaultdb"

# Lista adminów po steam_id
ADMINS = ["76561197992396189"]  # np. AdminPlayer
# ----------------------------------------

app = Flask(__name__)

# ------------------- MySQL -------------------
def get_db_connection():
    return mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASS,
        database=MYSQL_DB
    )

# ------------------- Funkcje bota -------------------
def get_player_by_steam(steam_id):
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM players WHERE steam_id=%s", (steam_id,))
    player = cur.fetchone()
    cur.close()
    conn.close()
    return player

def get_shop_items():
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM shop_items")
    items = cur.fetchall()
    cur.close()
    conn.close()
    return items

def get_player_transactions(player_id):
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM transactions WHERE player_id=%s ORDER BY created_at DESC", (player_id,))
    transactions = cur.fetchall()
    cur.close()
    conn.close()
    return transactions

def buy_item(player_id, item_id):
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    # Pobierz item
    cur.execute("SELECT * FROM shop_items WHERE id=%s", (item_id,))
    item = cur.fetchone()
    if not item:
        cur.close()
        conn.close()
        return False, "Item nie istnieje"
    # Pobierz saldo gracza
    cur.execute("SELECT balance FROM players WHERE id=%s", (player_id,))
    balance = cur.fetchone()["balance"]
    if balance < item["price"]:
        cur.close()
        conn.close()
        return False, "Niewystarczające saldo"
    # Aktualizacja salda
    new_balance = balance - item["price"]
    cur.execute("UPDATE players SET balance=%s WHERE id=%s", (new_balance, player_id))
    # Dodanie transakcji
    cur.execute(
        "INSERT INTO transactions (player_id, amount, type, source) VALUES (%s, %s, %s, %s)",
        (player_id, item["price"], "expense", f"buy_{item['name']}")
    )
    conn.commit()
    cur.close()
    conn.close()
    return True, f"Kupiłeś: {item['name']} za {item['price']} monet"

# ------------------- Flask routes -------------------
@app.route("/balance", methods=["GET"])
def balance():
    steam_id = request.args.get("steam_id")
    if not steam_id:
        return jsonify({"error": "Podaj steam_id"}), 400
    player = get_player_by_steam(steam_id)
    if not player:
        return jsonify({"error": "Nie znaleziono gracza"}), 404
    return jsonify({
        "nickname": player["nickname"],
        "balance": float(player["balance"])
    })

@app.route("/transactions", methods=["GET"])
def transactions():
    steam_id = request.args.get("steam_id")
    if not steam_id:
        return jsonify({"error": "Podaj steam_id"}), 400
    player = get_player_by_steam(steam_id)
    if not player:
        return jsonify({"error": "Nie znaleziono gracza"}), 404

    # Admin może przeglądać wszystkich
    if steam_id in ADMINS:
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT t.*, p.nickname FROM transactions t JOIN players p ON t.player_id=p.id ORDER BY t.created_at DESC")
        tx = cur.fetchall()
        cur.close()
        conn.close()
    else:
        tx = get_player_transactions(player["id"])
    return jsonify(tx)

@app.route("/shop", methods=["GET"])
def shop():
    items = get_shop_items()
    return jsonify(items)

@app.route("/buy", methods=["POST"])
def buy():
    steam_id = request.json.get("steam_id")
    item_id = request.json.get("item_id")
    if not steam_id or not item_id:
        return jsonify({"error": "Podaj steam_id i item_id"}), 400
    player = get_player_by_steam(steam_id)
    if not player:
        return jsonify({"error": "Nie znaleziono gracza"}), 404
    ok, msg = buy_item(player["id"], item_id)
    if ok:
        return jsonify({"success": True, "message": msg})
    else:
        return jsonify({"success": False, "message": msg})

# ------------------- Start Flask -------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    print(f"Uruchamiam bota ekonomii na porcie {port}")
    app.run(host="0.0.0.0", port=port)
