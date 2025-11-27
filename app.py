#!/usr/bin/env python3
import os
import threading
import time
import ftplib
import hashlib
import mysql.connector
from mysql.connector import Error
import discord
from discord.ext import commands
from flask import Flask

# ---------------- CONFIG ----------------
DISCORD_TOKEN = os.environ.get("DISCORD_TOKEN")

MYSQL_HOST = "mysql-1f2c991-spamownia91-479a.h.aivencloud.com"
MYSQL_PORT = 14365
MYSQL_USER = "avnadmin"
MYSQL_PASSWORD = "AVNS_6gzpU-skelov685O3Gx"
MYSQL_DB = "defaultdb"

FTP_HOST = "195.179.226.218"
FTP_PORT = 56421
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_LOG_PATH = "/SCUM/Saved/SaveFiles/Logs/"

MONETY_PER_LOGIN = 10
CHECK_INTERVAL = 60  # sekund

# ---------------- DATABASE ----------------
def get_connection():
    try:
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DB
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
        conn.commit()
        cursor.close()
        conn.close()

init_db()

# ---------------- FTP LOGIC ----------------
def compute_hash(line):
    return hashlib.sha256(line.encode("utf-8")).hexdigest()

def line_already_processed(cursor, filename, line_hash):
    cursor.execute("SELECT id FROM processed_logs WHERE filename=%s AND line_hash=%s", (filename, line_hash))
    return cursor.fetchone() is not None

def mark_line_processed(cursor, filename, line_hash):
    cursor.execute("INSERT INTO processed_logs(filename, line_hash) VALUES(%s,%s)", (filename, line_hash))

def process_log_line(cursor, line):
    # Szukamy logowania
    if "logged in at" in line:
        try:
            # Wyciągamy nick z formatu: IP SteamID:Nick(liczba)
            part = line.split(":")[1].strip()
            nick_part = part.split(" ")[1]  # Nick(liczba)
            nick = nick_part.split("(")[0]
            # Aktualizujemy saldo
            cursor.execute("""
                INSERT INTO users(nick, balance)
                VALUES(%s, %s)
                ON DUPLICATE KEY UPDATE balance = balance + %s
            """, (nick, MONETY_PER_LOGIN, MONETY_PER_LOGIN))
        except Exception as e:
            print(f"[LOG] Błąd parsowania linii: {line}\n{e}")

def scan_ftp_logs():
    try:
        ftp = ftplib.FTP()
        ftp.connect(FTP_HOST, FTP_PORT)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(FTP_LOG_PATH)
        files = ftp.nlst("login_*.log")
    except Exception as e:
        print(f"[FTP] Błąd FTP: {e}")
        return

    conn = get_connection()
    if not conn:
        ftp.quit()
        return

    cursor = conn.cursor()
    for filename in files:
        try:
            lines = []
            ftp.retrlines(f"RETR {filename}", lines.append)
            for line in lines:
                line_hash = compute_hash(line)
                if not line_already_processed(cursor, filename, line_hash):
                    process_log_line(cursor, line)
                    mark_line_processed(cursor, filename, line_hash)
            conn.commit()
        except Exception as e:
            print(f"[FTP] Błąd odczytu {filename}: {e}")

    cursor.close()
    conn.close()
    ftp.quit()

# ---------------- DISCORD BOT ----------------
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

@bot.event
async def on_ready():
    print(f"Bot zalogowany jako {bot.user}")

@bot.command()
async def saldo(ctx, nick: str):
    conn = get_connection()
    if not conn:
        await ctx.send("Błąd bazy danych.")
        return
    cursor = conn.cursor()
    cursor.execute("SELECT balance FROM users WHERE nick=%s", (nick,))
    row = cursor.fetchone()
    if row:
        await ctx.send(f"Saldo **{nick}**: {row[0]} monet")
    else:
        await ctx.send(f"Nie znaleziono użytkownika: {nick}")
    cursor.close()
    conn.close()

# ---------------- FLASK SERVER ----------------
app = Flask("")

@app.route("/")
def home():
    return "Bot działa!"

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)

threading.Thread(target=run_flask).start()

# ---------------- BACKGROUND LOG SCAN ----------------
def background_scan():
    while True:
        scan_ftp_logs()
        time.sleep(CHECK_INTERVAL)

threading.Thread(target=background_scan, daemon=True).start()

# ---------------- RUN BOT ----------------
bot.run(DISCORD_TOKEN)
