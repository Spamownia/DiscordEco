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

MYSQL_HOST = "mysql-1f2c991-spamownia91-479a.h.aivencloud.com"
MYSQL_PORT = 14365
MYSQL_USER = "avnadmin"
MYSQL_PASSWORD = "AVNS_6gzpU-skelov685O3Gx"
MYSQL_DB = "defaultdb"

FTP_HOST = "195.179.226.218"
FTP_PORT = 56421
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_PATH = "/SCUM/Saved/SaveFiles/Logs/"

MONETY_ZA_LOG = 10
SPRAWDZANIE_SEKUND = 60

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
        print(f"Błąd połączenia MySQL: {e}")
        return None

def init_db():
    conn = get_connection()
    if conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                discord_id BIGINT,
                steam_id BIGINT UNIQUE,
                nick VARCHAR(50),
                balance INT NOT NULL DEFAULT 0,
                PRIMARY KEY(discord_id)
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS processed_logs (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                filename VARCHAR(255),
                line_hash VARCHAR(64) UNIQUE
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

# ---------------- FTP LOG PARSER ----------------
def hash_line(line):
    return hashlib.sha256(line.encode("utf-8")).hexdigest()

def line_przetworzona(filename, line_hash):
    conn = get_connection()
    if not conn:
        return False
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM processed_logs WHERE filename=%s AND line_hash=%s", (filename, line_hash))
    exists = cursor.fetchone() is not None
    cursor.close()
    conn.close()
    return exists

def zapisz_przetworzona(filename, line_hash):
    conn = get_connection()
    if not conn:
        return
    cursor = conn.cursor()
    cursor.execute("INSERT IGNORE INTO processed_logs(filename, line_hash) VALUES(%s, %s)", (filename, line_hash))
    conn.commit()
    cursor.close()
    conn.close()

def przyznaj_monety(steam_id, nick, amount=MONETY_ZA_LOG):
    conn = get_connection()
    if not conn:
        return
    cursor = conn.cursor()
    # Sprawdź czy SteamID już istnieje
    cursor.execute("SELECT balance FROM users WHERE steam_id=%s", (steam_id,))
    row = cursor.fetchone()
    if row:
        new_balance = row[0] + amount
        cursor.execute("UPDATE users SET balance=%s, nick=%s WHERE steam_id=%s", (new_balance, nick, steam_id))
    else:
        # Wstaw nowy rekord
        cursor.execute("INSERT INTO users(discord_id, steam_id, nick, balance) VALUES(%s,%s,%s,%s)", (0, steam_id, nick, amount))
    conn.commit()
    cursor.close()
    conn.close()

def przetworz_linie(line, filename):
    # Szukamy logowania
    if "logged in" in line:
        try:
            part = line.split("'")[1]  # '83.28.140.182 76561197992396189:Anu(26)'
            steam, nick_full = part.split(":")
            steam_id = int(steam.strip().split()[1])
            nick = nick_full.split("(")[0]
            # Przyznaj monety
            przyznaj_monety(steam_id, nick)
            # Zapisz linię jako przetworzoną
            line_hash = hash_line(line)
            zapisz_przetworzona(filename, line_hash)
        except Exception as e:
            print(f"Błąd przy przetwarzaniu linii: {line}\n{e}")

def przetworz_logi():
    try:
        ftp = ftplib.FTP()
        ftp.connect(FTP_HOST, FTP_PORT, timeout=30)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(FTP_PATH)
        files = []
        ftp.retrlines('LIST', lambda x: files.append(x.split()[-1]))
        login_files = [f for f in files if f.startswith("login_") and f.endswith(".log")]
        print(f"[FTP] Rozpoczynam pełne skanowanie login_*.log ...")
        for filename in login_files:
            try:
                print(f"[FTP] Otwieram {filename}")
                lines = []
                ftp.retrlines(f"RETR {filename}", lambda x: lines.append(x))
                for line in lines:
                    line_hash = hash_line(line)
                    if not line_przetworzona(filename, line_hash):
                        przetworz_linie(line, filename)
            except Exception as e:
                print(f"[FTP] Błąd odczytu {filename}: {e}")
        ftp.quit()
    except Exception as e:
        print(f"[FTP] Błąd połączenia FTP: {e}")

def monitoruj_logi():
    while True:
        przetworz_logi()
        time.sleep(SPRAWDZANIE_SEKUND)

# ---------------- DISCORD KOMENDY ----------------
@bot.command(name="saldo")
async def saldo(ctx, steam_id: int = None):
    conn = get_connection()
    if not conn:
        await ctx.send("Błąd połączenia z bazą danych.")
        return
    cursor = conn.cursor()
    if steam_id:
        cursor.execute("SELECT nick, balance FROM users WHERE steam_id=%s", (steam_id,))
    else:
        cursor.execute("SELECT nick, balance FROM users WHERE discord_id=%s", (ctx.author.id,))
    row = cursor.fetchone()
    if row:
        await ctx.send(f"Saldo gracza **{row[0]}**: {row[1]} monet")
    else:
        await ctx.send("Nie znaleziono gracza w bazie.")
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
threading.Thread(target=monitoruj_logi).start()

# ---------------- RUN BOT ----------------
@bot.event
async def on_ready():
    print(f"Bot zalogowany jako {bot.user}")

bot.run(DISCORD_TOKEN)
