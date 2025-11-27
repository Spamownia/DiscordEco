#!/usr/bin/env python3
import os
import threading
import time
import ftplib
import hashlib
import mysql.connector
import discord
from discord.ext import commands
from flask import Flask

# ---------------- CONFIG ----------------
FTP_HOST = "195.179.226.218"
FTP_PORT = 56421
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_PATH = "/SCUM/Saved/SaveFiles/Logs/"

DISCORD_TOKEN = os.environ.get("DISCORD_TOKEN")
if not DISCORD_TOKEN:
    raise ValueError("Brak tokena Discorda w zmiennych środowisk.")

CHECK_INTERVAL = 60  # sekundy

MYSQL_HOST = "mysql-1f2c991-spamownia91-479a.h.aivencloud.com"
MYSQL_PORT = 14365
MYSQL_USER = "avnadmin"
MYSQL_PASSWORD = "AVNS_6gzpU-skelov685O3Gx"
MYSQL_DB = "defaultdb"

# ----------------------------------------

intents = discord.Intents.default()
intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents)
app = Flask(__name__)

# ---------------- DATABASE ----------------
db = mysql.connector.connect(
    host=MYSQL_HOST,
    port=MYSQL_PORT,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    database=MYSQL_DB
)
cursor = db.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS processed_logs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    filename VARCHAR(255),
    line_hash VARCHAR(255)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    steam_id VARCHAR(64),
    nick VARCHAR(255),
    balance INT DEFAULT 0
)
""")
db.commit()

# ---------------- FTP ----------------
def get_log_list():
    try:
        with ftplib.FTP() as ftp:
            ftp.connect(FTP_HOST, FTP_PORT, timeout=10)
            ftp.login(FTP_USER, FTP_PASS)
            ftp.set_pasv(True)
            ftp.cwd(FTP_PATH)

            files = []
            ftp.retrlines("LIST", lambda line: files.append(line.split()[-1]))
            return files

    except Exception as e:
        print(f"[FTP] Błąd FTP: {e}")
        return []

def read_log_file(filename):
    try:
        lines = []
        with ftplib.FTP() as ftp:
            ftp.connect(FTP_HOST, FTP_PORT, timeout=10)
            ftp.login(FTP_USER, FTP_PASS)
            ftp.set_pasv(True)
            ftp.cwd(FTP_PATH)
            ftp.retrlines(f"RETR {filename}", lines.append)
        return lines

    except Exception as e:
        print(f"[FTP] Błąd pobierania {filename}: {e}")
        return []

# ---------------- HASHY ----------------
def line_already_processed(filename, line_hash):
    cursor.execute(
        "SELECT id FROM processed_logs WHERE filename=%s AND line_hash=%s",
        (filename, line_hash)
    )
    return cursor.fetchone() is not None

def mark_line_processed(filename, line_hash):
    try:
        cursor.execute(
            "INSERT IGNORE INTO processed_logs (filename, line_hash) VALUES (%s, %s)",
            (filename, line_hash)
        )
        db.commit()
    except Exception as e:
        print(f"[DB] Błąd INSERT: {e}")

# ---------------- PARSER ----------------
def handle_log_line(line):
    try:
        if "logged in" not in line and "logged out" not in line:
            return

        # Format SCUM logowania:
        # Player 'IP STEAMID:NICK(LEVEL)' logged in
        fragment = line.split("'")[1]
        parts = fragment.split(" ")

        # IP = parts[0]
        steam_segment = parts[1]  # STEAMID:NICK(LEVEL)

        steam_id = steam_segment.split(":")[0]
        nick = steam_segment.split(":")[1].split("(")[0].strip()

        is_login = "logged in" in line

        if is_login:
            cursor.execute("SELECT balance FROM users WHERE steam_id=%s", (steam_id,))
            result = cursor.fetchone()

            if result:
                cursor.execute("UPDATE users SET balance = balance + 10 WHERE steam_id=%s", (steam_id,))
            else:
                cursor.execute(
                    "INSERT INTO users (steam_id, nick, balance) VALUES (%s, %s, 10)",
                    (steam_id, nick)
                )
            db.commit()

            print(f"[LOG] +10 monet → {nick} ({steam_id})")

    except Exception as e:
        print(f"[LOG] Błąd parsowania: {e} | LINE: {line}")

# ---------------- PRZETWARZANIE LOGÓW ----------------
def process_logs():
    print("[FTP] Rozpoczynam skanowanie logów...")
    files = get_log_list()

    for filename in files:
        lines = read_log_file(filename)
        for line in lines:
            h = hashlib.sha256(line.encode()).hexdigest()
            if not line_already_processed(filename, h):
                handle_log_line(line)
                mark_line_processed(filename, h)

# ---------------- WĄTEK ----------------
def start_log_thread():
    def run():
        while True:
            process_logs()
            time.sleep(CHECK_INTERVAL)

    t = threading.Thread(target=run, daemon=True)
    t.start()

# ---------------- DISCORD ----------------
@bot.event
async def on_ready():
    print(f"Bot zalogowany jako {bot.user}")
    start_log_thread()

@bot.command()
async def saldo(ctx):
    steam_id = str(ctx.author.id)  # → jeśli chcesz powiązać DC=steam, zmień to później

    cursor.execute("SELECT balance, nick FROM users WHERE steam_id=%s", (steam_id,))
    result = cursor.fetchone()

    if result:
        bal, nick = result
        await ctx.send(f"{nick}, masz **{bal}** monet.")
    else:
        await ctx.send("Brak konta. Zaloguj się na serwer SCUM.")

# ---------------- FLASK ----------------
@app.route("/")
def index():
    return "Bot online"

# ---------------- START ----------------
if __name__ == "__main__":
    threading.Thread(
        target=lambda: app.run(host="0.0.0.0", port=10000),
        daemon=True
    ).start()

    bot.run(DISCORD_TOKEN)
