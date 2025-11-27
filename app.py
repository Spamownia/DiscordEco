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

# ---------------- CONFIG ----------------
FTP_HOST = "195.179.226.218"
FTP_PORT = 56421
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_PATH = "/SCUM/Saved/SaveFiles/Logs/"

DISCORD_TOKEN = "TWÓJ_TOKEN_DISCORD"
CHECK_INTERVAL = 60  # sekund

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
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASS,
    database=DB_NAME
)
cursor = db.cursor()

# ---------------- FTP ----------------
def get_log_list():
    try:
        with ftplib.FTP() as ftp:
            ftp.connect(FTP_HOST, FTP_PORT)
            ftp.login(FTP_USER, FTP_PASS)
            ftp.cwd(FTP_PATH)
            files = []
            ftp.retrlines('LIST', lambda line: files.append(line.split()[-1]))
            return files
    except Exception as e:
        print(f"[FTP] Błąd FTP: {e}")
        return []

def read_log_file(filename):
    try:
        lines = []
        with ftplib.FTP() as ftp:
            ftp.connect(FTP_HOST, FTP_PORT)
            ftp.login(FTP_USER, FTP_PASS)
            ftp.cwd(FTP_PATH)
            ftp.retrlines(f"RETR {filename}", lambda line: lines.append(line))
        return lines
    except Exception as e:
        print(f"[FTP] Błąd pobierania {filename}: {e}")
        return []

def line_already_processed(filename, line_hash):
    cursor.execute(
        "SELECT id FROM processed_logs WHERE filename=%s AND line_hash=%s",
        (filename, line_hash)
    )
    return cursor.fetchone() is not None

def mark_line_processed(filename, line_hash):
    cursor.execute(
        "INSERT INTO processed_logs (filename, line_hash) VALUES (%s, %s)",
        (filename, line_hash)
    )
    db.commit()

def process_logs():
    print("[FTP] Rozpoczynam skanowanie logów...")
    files = get_log_list()
    for filename in files:
        lines = read_log_file(filename)
        for line in lines:
            line_hash = hashlib.sha256(line.encode()).hexdigest()
            if not line_already_processed(filename, line_hash):
                handle_log_line(line)
                mark_line_processed(filename, line_hash)

def handle_log_line(line):
    # Przykład logu: "Player JohnDoe (SteamID: 76561198000000000) logged in"
    if "logged in" in line:
        try:
            nick = line.split("Player ")[1].split(" (SteamID")[0].strip()
            steam_id = line.split("SteamID: ")[1].split(")")[0].strip()
            cursor.execute("SELECT balance FROM users WHERE discord_id=%s", (steam_id,))
            result = cursor.fetchone()
            if result:
                cursor.execute("UPDATE users SET balance=balance+10 WHERE discord_id=%s", (steam_id,))
            else:
                cursor.execute(
                    "INSERT INTO users (discord_id, nick, balance) VALUES (%s, %s, %s)",
                    (steam_id, nick, 10)
                )
            db.commit()
        except Exception as e:
            print(f"[LOG] Błąd przetwarzania linii: {e}")

def start_log_thread():
    def run():
        while True:
            process_logs()
            time.sleep(CHECK_INTERVAL)
    thread = threading.Thread(target=run, daemon=True)
    thread.start()

# ---------------- DISCORD ----------------
@bot.event
async def on_ready():
    print(f"Bot zalogowany jako {bot.user}")
    start_log_thread()

@bot.command()
async def saldo(ctx):
    discord_id = str(ctx.author.id)
    cursor.execute("SELECT balance FROM users WHERE discord_id=%s", (discord_id,))
    result = cursor.fetchone()
    if result:
        await ctx.send(f"Masz {result[0]} monet.")
    else:
        await ctx.send("Nie znaleziono konta. Zaloguj się na serwer, aby otrzymać monety.")

# ---------------- FLASK ----------------
@app.route("/")
def index():
    return "Bot online"

if __name__ == "__main__":
    threading.Thread(target=lambda: app.run(host="0.0.0.0", port=10000), daemon=True).start()
    bot.run(DISCORD_TOKEN)

bot.run(DISCORD_TOKEN)
