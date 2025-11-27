#!/usr/bin/env python3
import os
import threading
import time
import ftplib
import hashlib
import mysql.connector
import discord
from discord.ext import commands, tasks
from flask import Flask
from datetime import datetime

# ---------------- CONFIG ----------------
FTP_HOST = "195.179.226.218"
FTP_PORT = 56421
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_PATH = "/SCUM/Saved/SaveFiles/Logs/"

MYSQL_HOST = "localhost"
MYSQL_USER = "root"
MYSQL_PASS = "password"
MYSQL_DB = "discord_shop"

DISCORD_TOKEN = "TWÓJ_TOKEN_DISCORD"

# ----------------------------------------

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

# Flask dla Render healthcheck
app = Flask(__name__)

@app.route("/")
def index():
    return "Bot live 🎉"

# ---------------- DB --------------------
db = mysql.connector.connect(
    host=MYSQL_HOST,
    user=MYSQL_USER,
    password=MYSQL_PASS,
    database=MYSQL_DB
)
cursor = db.cursor()

# Utwórz tabele jeśli nie istnieją
cursor.execute("""
CREATE TABLE IF NOT EXISTS users (
    discord_id BIGINT PRIMARY KEY,
    nick VARCHAR(255) UNIQUE,
    balance INT NOT NULL DEFAULT 0
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS processed_logs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    filename VARCHAR(255),
    line_hash VARCHAR(64)
)
""")
db.commit()

# ---------------- FTP --------------------
def fetch_ftp_log_files():
    files = []
    try:
        ftp = ftplib.FTP()
        ftp.connect(FTP_HOST, FTP_PORT, timeout=10)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(FTP_PATH)

        def add_file(line):
            parts = line.split()
            filename = parts[-1]
            if filename.startswith("login_") and filename.endswith(".log"):
                files.append(filename)

        ftp.retrlines("LIST", add_file)
        ftp.quit()
    except Exception as e:
        print(f"[FTP] Błąd FTP: {e}")
    return sorted(files)

def process_log_line(filename, line):
    line_hash = hashlib.sha256(line.encode()).hexdigest()
    cursor.execute("SELECT 1 FROM processed_logs WHERE filename=%s AND line_hash=%s", (filename, line_hash))
    if cursor.fetchone():
        return False  # już przetworzona

    # Zapisz hash linii
    cursor.execute("INSERT INTO processed_logs (filename, line_hash) VALUES (%s, %s)", (filename, line_hash))
    db.commit()

    # Wyciągnięcie nicka z logu
    # Zakładam format: "Player <Nick> logged in"
    if "Player " in line and " logged in" in line:
        nick = line.split("Player ")[1].split(" logged in")[0].strip()
        cursor.execute("SELECT discord_id, balance FROM users WHERE nick=%s", (nick,))
        res = cursor.fetchone()
        if res:
            discord_id, balance = res
            cursor.execute("UPDATE users SET balance=%s+1 WHERE discord_id=%s", (balance, discord_id))
        else:
            cursor.execute("INSERT INTO users (nick, balance) VALUES (%s, 1)", (nick,))
        db.commit()
    return True

def scan_logs(full_scan=False):
    print("[FTP] Rozpoczynam skanowanie logów...")
    files = fetch_ftp_log_files()
    for filename in files:
        with ftplib.FTP() as ftp:
            try:
                ftp.connect(FTP_HOST, FTP_PORT, timeout=10)
                ftp.login(FTP_USER, FTP_PASS)
                ftp.cwd(FTP_PATH)
                lines = []
                ftp.retrlines(f"RETR {filename}", lines.append)
            except Exception as e:
                print(f"[FTP] Błąd połączenia FTP: {e}")
                continue

            for line in lines:
                if full_scan or process_log_line(filename, line):
                    pass  # linia przetworzona lub pełny scan

# ---------------- BACKGROUND ----------------
def ftp_thread():
    # pełne skanowanie przy starcie
    scan_logs(full_scan=True)
    # potem co 60s tylko nowe linie
    while True:
        scan_logs(full_scan=False)
        time.sleep(60)

# ---------------- DISCORD ----------------
@bot.event
async def on_ready():
    print(f"Bot zalogowany jako {bot.user}")
    threading.Thread(target=ftp_thread, daemon=True).start()

@bot.command()
async def saldo(ctx, nick: str = None):
    if nick is None:
        await ctx.send("Podaj nick: !saldo <nick>")
        return
    cursor.execute("SELECT balance FROM users WHERE nick=%s", (nick,))
    res = cursor.fetchone()
    if res:
        await ctx.send(f"{nick} ma {res[0]} monet.")
    else:
        await ctx.send(f"Nie znaleziono gracza {nick}.")

# ---------------- RUN ----------------
if __name__ == "__main__":
    # Flask w wątku, żeby nie blokował bota
    threading.Thread(target=lambda: app.run(host="0.0.0.0", port=10000), daemon=True).start()
    bot.run(DISCORD_TOKEN)
