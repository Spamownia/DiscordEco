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
    raise ValueError("Brak tokena Discorda w zmiennych środowiskowych!")

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
    host=MYSQL_HOST,
    port=MYSQL_PORT,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    database=MYSQL_DB
)
cursor = db.cursor()

# ---------------- FTP ----------------
def get_log_list():
    try:
        with ftplib.FTP() as ftp:
            ftp.connect(FTP_HOST, FTP_PORT)
            ftp.login(FTP_USER, FTP_PASS)
            ftp.set_pasv(True)  # tryb pasywny
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
            ftp.set_pasv(True)
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

# ---------------- PARSER LOGÓW ----------------
def handle_log_line(line):
    if "logged in" in line or "logged out" in line:
        try:
            # Wyciągamy fragment między apostrofami
            player_info = line.split("'")[1]
            # player_info = "83.28.140.182 76561197992396189:Anu(26)"
            steam_id, nick_with_level = player_info.split(" ")[1].split(":")
            nick = nick_with_level.split("(")[0].strip()
            status = "in" if "logged in" in line else "out"

            # Przyznajemy monety tylko przy logowaniu
            if status == "in":
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

            print(f"[LOG] Gracz {nick} ({steam_id}) {status}.")
        except Exception as e:
            print(f"[LOG] Błąd przetwarzania linii: {e}")

# ---------------- WĄTEK LOGÓW ----------------
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

# ---------------- URUCHOMIENIE ----------------
if __name__ == "__main__":
    threading.Thread(target=lambda: app.run(host="0.0.0.0", port=10000), daemon=True).start()
    bot.run(DISCORD_TOKEN)
