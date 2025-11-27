#!/usr/bin/env python3
import os
import threading
import time
import hashlib
import ftplib
import mysql.connector
from mysql.connector import Error
import discord
from discord.ext import commands
from flask import Flask

# ---------------- CONFIG ----------------
DISCORD_TOKEN = os.environ.get("DISCORD_TOKEN")
FTP_HOST = "195.179.226.218"
FTP_PORT = 56421
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_LOG_DIR = "/SCUM/Saved/SaveFiles/Logs/"

MYSQL_HOST = "mysql-1f2c991-spamownia91-479a.h.aivencloud.com"
MYSQL_PORT = 14365
MYSQL_USER = "avnadmin"
MYSQL_PASSWORD = "AVNS_6gzpU-skelov685O3Gx"
MYSQL_DB = "defaultdb"

MONETY_ZA_LOG = 10
CHECK_INTERVAL = 60

SHOP_ITEMS = {
    "Miecz": 100,
    "Tarcza": 75,
    "Mikstura": 25,
    "Zbroja": 200,
    "Eliksir": 50
}

# ---------------- DATABASE ----------------
def get_connection():
    try:
        return mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DB
        )
    except Error as e:
        print(f"[DB] Błąd połączenia: {e}")
        return None

def init_db():
    conn = get_connection()
    if conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                discord_id BIGINT PRIMARY KEY,
                balance INT NOT NULL DEFAULT 0
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS processed_logs (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                filename VARCHAR(255) NOT NULL,
                line_hash VARCHAR(64) NOT NULL,
                UNIQUE KEY unique_log (filename, line_hash)
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                discord_id BIGINT,
                action VARCHAR(50),
                item VARCHAR(50),
                amount INT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        cursor.close()
        conn.close()

init_db()

# ---------------- DISCORD BOT ----------------
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

@bot.event
async def on_ready():
    print(f"[BOT] Zalogowano jako {bot.user}")

def hash_nick(nick):
    return int(hashlib.sha256(nick.encode()).hexdigest(), 16) % (10 ** 18)

@bot.command()
async def saldo(ctx, nick: str):
    conn = get_connection()
    if not conn:
        await ctx.send("Błąd połączenia z bazą danych.")
        return
    cursor = conn.cursor()
    cursor.execute("SELECT balance FROM users WHERE discord_id=%s", (hash_nick(nick),))
    row = cursor.fetchone()
    balance = row[0] if row else 0
    await ctx.send(f"{nick} ma {balance} monet.")
    cursor.close()
    conn.close()

@bot.command()
async def sklep(ctx):
    msg = "\n".join([f"{item}: {price} monet" for item, price in SHOP_ITEMS.items()])
    await ctx.send(f"**Sklep:**\n{msg}")

@bot.command()
async def kup(ctx, nick: str, item: str):
    if item not in SHOP_ITEMS:
        await ctx.send("Nie ma takiego przedmiotu w sklepie.")
        return
    conn = get_connection()
    if not conn:
        await ctx.send("Błąd połączenia z bazą danych.")
        return
    cursor = conn.cursor()
    discord_id_hashed = hash_nick(nick)
    cursor.execute("SELECT balance FROM users WHERE discord_id=%s", (discord_id_hashed,))
    row = cursor.fetchone()
    balance = row[0] if row else 0
    cena = SHOP_ITEMS[item]
    if balance < cena:
        await ctx.send("Nie masz wystarczająco monet.")
    else:
        nowy_saldo = balance - cena
        cursor.execute("""
            INSERT INTO users(discord_id, balance)
            VALUES(%s, %s)
            ON DUPLICATE KEY UPDATE balance=%s
        """, (discord_id_hashed, nowy_saldo, nowy_saldo))
        cursor.execute("""
            INSERT INTO transactions(discord_id, action, item, amount)
            VALUES(%s, %s, %s, %s)
        """, (discord_id_hashed, "BUY", item, cena))
        conn.commit()
        await ctx.send(f"{nick} kupił **{item}** za {cena} monet. Nowe saldo: {nowy_saldo}")
    cursor.close()
    conn.close()

# ---------------- FTP LOGIC ----------------
def get_ftp_file_list():
    try:
        ftp = ftplib.FTP()
        ftp.connect(FTP_HOST, FTP_PORT, timeout=10)
        ftp.login(FTP_USER, FTP_PASS)
        files = []
        ftp.retrlines(f'LIST {FTP_LOG_DIR}', lambda line: files.append(line.split()[-1]))
        ftp.quit()
        return [f for f in files if f.startswith("login_") and f.endswith(".log")]
    except Exception as e:
        print(f"[FTP] Błąd: {e}")
        return []

def read_file_lines(ftp, filename):
    lines = []
    try:
        ftp.retrlines(f'RETR {FTP_LOG_DIR}{filename}', lambda l: lines.append(l))
    except Exception as e:
        print(f"[FTP] Błąd przy otwieraniu {filename}: {e}")
    return lines

def line_hash(line):
    return hashlib.sha256(line.encode()).hexdigest()

def already_processed(cursor, filename, line_hash_val):
    cursor.execute("SELECT 1 FROM processed_logs WHERE filename=%s AND line_hash=%s",
                   (filename, line_hash_val))
    return cursor.fetchone() is not None

def process_line(cursor, line):
    if "logged in at" in line:
        try:
            start = line.index("'") + 1
            end = line.index("'", start)
            full = line[start:end]
            nick = full.split(":")[1].split("(")[0]
        except Exception:
            return
        discord_id_hashed = hash_nick(nick)
        cursor.execute("""
            INSERT INTO users(discord_id, balance)
            VALUES(%s, %s)
            ON DUPLICATE KEY UPDATE balance = balance + %s
        """, (discord_id_hashed, MONETY_ZA_LOG, MONETY_ZA_LOG))
        cursor.execute("""
            INSERT INTO transactions(discord_id, action, item, amount)
            VALUES(%s, %s, %s, %s)
        """, (discord_id_hashed, "LOGIN", None, MONETY_ZA_LOG))

def process_ftp_logs():
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT, timeout=10)
    ftp.login(FTP_USER, FTP_PASS)
    files = get_ftp_file_list()
    conn = get_connection()
    if not conn:
        print("[DB] Brak połączenia z bazą")
        return
    cursor = conn.cursor()
    for filename in files:
        print(f"[FTP] Otwieram {filename}")
        lines = read_file_lines(ftp, filename)
        for line in lines:
            h = line_hash(line)
            if not already_processed(cursor, filename, h):
                process_line(cursor, line)
                cursor.execute("INSERT INTO processed_logs(filename, line_hash) VALUES(%s,%s)", (filename, h))
    conn.commit()
    cursor.close()
    conn.close()
    ftp.quit()

def ftp_monitor_loop():
    while True:
        try:
            process_ftp_logs()
        except Exception as e:
            print(f"[FTP] Błąd w pętli monitorującej: {e}")
        time.sleep(CHECK_INTERVAL)

# ---------------- FLASK SERVER ----------------
app = Flask("")

@app.route("/")
def home():
    return "Bot działa!"

def run_flask():
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)

# ---------------- START ----------------
threading.Thread(target=run_flask).start()
threading.Thread(target=ftp_monitor_loop).start()
bot.run(DISCORD_TOKEN)
