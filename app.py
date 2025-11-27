#!/usr/bin/env python3
import os
import threading
import time
import hashlib
import ftplib
import re
import mysql.connector
from mysql.connector import Error
import discord
from discord.ext import commands
from flask import Flask

# ---------------- CONFIG ----------------
DISCORD_TOKEN = os.environ.get("DISCORD_TOKEN")  # Token bota

FTP_HOST = "195.179.226.218"
FTP_PORT = 56421
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_PATH = "/SCUM/Saved/SaveFiles/Logs/"

CHECK_INTERVAL = 60  # sekund

# ---------------- BOT SETUP ----------------
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

# ---------------- DATABASE ----------------
def get_connection():
    try:
        conn = mysql.connector.connect(
            host="mysql-1f2c991-spamownia91-479a.h.aivencloud.com",
            port=14365,
            user="avnadmin",
            password="AVNS_6gzpU-skelov685O3Gx",
            database="defaultdb"
        )
        return conn
    except Error as e:
        print(f"[DB] Błąd połączenia: {e}")
        return None

def init_db():
    conn = get_connection()
    if conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                discord_id BIGINT,
                steam_id BIGINT,
                nick VARCHAR(100),
                balance INT NOT NULL DEFAULT 0,
                PRIMARY KEY (steam_id)
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS processed_logs (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                filename VARCHAR(255),
                line_hash VARCHAR(64)
            )
        """)
        conn.commit()
        cursor.close()
        conn.close()

init_db()

# ---------------- FTP LOGIC ----------------
def get_ftp_log_files():
    try:
        ftp = ftplib.FTP()
        ftp.connect(FTP_HOST, FTP_PORT, timeout=10)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(FTP_PATH)
        files = []
        ftp.retrlines('LIST', lambda x: files.append(x.split()[-1]))
        # tylko logi login_*.log
        return [f for f in files if f.startswith("login_") and f.endswith(".log")]
    except Exception as e:
        print(f"[FTP] Błąd FTP: {e}")
        return []

def line_processed(filename, line_hash):
    conn = get_connection()
    if not conn:
        return False
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM processed_logs WHERE filename=%s AND line_hash=%s", (filename, line_hash))
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    return result is not None

def mark_line_processed(filename, line_hash):
    conn = get_connection()
    if not conn:
        return
    cursor = conn.cursor()
    cursor.execute("INSERT INTO processed_logs(filename, line_hash) VALUES(%s, %s)", (filename, line_hash))
    conn.commit()
    cursor.close()
    conn.close()

def process_log_line(filename, line):
    match = re.search(r":\s'[\d\.]+\s(\d+):([^\(]+)\(\d+\)'\slogged in", line)
    if match:
        steam_id = int(match.group(1))
        nick = match.group(2)
        conn = get_connection()
        if conn:
            cursor = conn.cursor()
            cursor.execute("SELECT balance FROM users WHERE steam_id=%s", (steam_id,))
            row = cursor.fetchone()
            balance = row[0] if row else 0
            balance += 10  # nagroda za logowanie
            if row:
                cursor.execute("UPDATE users SET balance=%s, nick=%s WHERE steam_id=%s", (balance, nick, steam_id))
            else:
                cursor.execute("INSERT INTO users(discord_id, steam_id, nick, balance) VALUES(%s, %s, %s, %s)",
                               (None, steam_id, nick, balance))
            conn.commit()
            cursor.close()
            conn.close()
            print(f"[LOG] Przyznano 10 monet dla {nick} ({steam_id}). Nowe saldo: {balance}")

def process_log_file(filename):
    try:
        ftp = ftplib.FTP()
        ftp.connect(FTP_HOST, FTP_PORT, timeout=10)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(FTP_PATH)
        lines = []
        ftp.retrlines(f'RETR {filename}', lambda l: lines.append(l))
        for line in lines:
            line_hash = hashlib.sha256(line.encode()).hexdigest()
            if not line_processed(filename, line_hash):
                process_log_line(filename, line)
                mark_line_processed(filename, line_hash)
        ftp.quit()
    except Exception as e:
        print(f"[FTP] Błąd przy przetwarzaniu {filename}: {e}")

def scan_logs():
    print("[FTP] Rozpoczynam skanowanie logów...")
    files = get_ftp_log_files()
    for f in files:
        process_log_file(f)

def periodic_scan():
    while True:
        scan_logs()
        time.sleep(CHECK_INTERVAL)

# ---------------- BOT COMMANDS ----------------
@bot.command(name="saldo")
async def saldo(ctx, steam_id: int = None):
    conn = get_connection()
    if not conn:
        await ctx.send("Błąd połączenia z bazą danych.")
        return
    cursor = conn.cursor()
    if steam_id is None:
        # Spróbuj znaleźć SteamID po DiscordID
        cursor.execute("SELECT balance, steam_id FROM users WHERE discord_id=%s", (ctx.author.id,))
    else:
        cursor.execute("SELECT balance FROM users WHERE steam_id=%s", (steam_id,))
    row = cursor.fetchone()
    if row:
        balance = row[0] if steam_id else row[0]
        await ctx.send(f"{ctx.author.name}, twoje saldo: {balance}$")
    else:
        await ctx.send("Nie znaleziono konta lub brak powiązania z Discord.")
    cursor.close()
    conn.close()

@bot.command(name="link")
async def link(ctx, steam_id: int):
    conn = get_connection()
    if not conn:
        await ctx.send("Błąd połączenia z bazą danych.")
        return
    cursor = conn.cursor()
    cursor.execute("SELECT discord_id FROM users WHERE steam_id=%s", (steam_id,))
    row = cursor.fetchone()
    if row:
        cursor.execute("UPDATE users SET discord_id=%s WHERE steam_id=%s", (ctx.author.id, steam_id))
    else:
        cursor.execute("INSERT INTO users(discord_id, steam_id, nick, balance) VALUES(%s, %s, %s, %s)",
                       (ctx.author.id, steam_id, f"Steam_{steam_id}", 0))
    conn.commit()
    cursor.close()
    conn.close()
    await ctx.send(f"Twój Discord został powiązany z SteamID {steam_id}.")

# ---------------- MINIMAL FLASK SERVER ----------------
app = Flask("")

@app.route("/")
def home():
    return "Bot działa!"

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)

# ---------------- START BOT & SCAN ----------------
if __name__ == "__main__":
    # Najpierw pełny skan logów
    scan_logs()
    # Wątek okresowego skanowania
    threading.Thread(target=periodic_scan, daemon=True).start()
    # Flask w tle
    threading.Thread(target=run_flask, daemon=True).start()
    # Uruchomienie bota
    bot.run(DISCORD_TOKEN)
