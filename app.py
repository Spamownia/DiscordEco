import os
import re
import ftplib
import threading
import discord
from discord.ext import commands
import mysql.connector
from mysql.connector import Error
from flask import Flask
from datetime import datetime

# -------------------------------- CONFIG --------------------------------

DISCORD_TOKEN = os.environ.get("DISCORD_TOKEN")

# FTP
FTP_HOST = "195.179.226.218"
FTP_PORT = 56421
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_PATH = "/SCUM/Saved/SaveFiles/Logs/"

# MySQL
MYSQL_HOST = "mysql-1f2c991-spamownia91-479a.h.aivencloud.com"
MYSQL_PORT = 14365
MYSQL_USER = "avnadmin"
MYSQL_PASSWORD = "AVNS_6gzpU-skelov685O3Gx"
MYSQL_DB = "defaultdb"

# Ekonomia
COINS_PER_LOGIN = 10

# -------------------------------- DISCORD SETUP --------------------------------

intents = discord.Intents.default()
intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents)

# -------------------------------- DATABASE --------------------------------

def get_connection():
    try:
        return mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DB,
        )
    except Error as e:
        print("MySQL error:", e)
        return None


def init_db():
    conn = get_connection()
    if not conn:
        print("DB error at init")
        return

    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            nick VARCHAR(100) PRIMARY KEY,
            balance INT NOT NULL DEFAULT 0
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS login_progress (
            logfile VARCHAR(255) PRIMARY KEY,
            last_line INT NOT NULL DEFAULT 0
        )
    """)

    conn.commit()
    cursor.close()
    conn.close()


init_db()

# -------------------------------- ECONOMY --------------------------------

def add_coins(nick, amount):
    """Dodaje monety dla nicku z loga."""
    conn = get_connection()
    if not conn:
        return

    cursor = conn.cursor()

    cursor.execute("SELECT balance FROM users WHERE nick=%s", (nick,))
    row = cursor.fetchone()

    if row:
        new_balance = row[0] + amount
        cursor.execute("UPDATE users SET balance=%s WHERE nick=%s", (new_balance, nick))
    else:
        new_balance = amount
        cursor.execute("INSERT INTO users(nick, balance) VALUES(%s, %s)", (nick, new_balance))

    conn.commit()
    cursor.close()
    conn.close()

    print(f"[ECON] +{amount} dla {nick}. Nowe saldo: {new_balance}")


# -------------------------------- FTP PARSING --------------------------------

def ftp_list_files():
    """Zwraca listę login_*.log z FTP."""
    files = []
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_PATH)

    ls = []
    ftp.retrlines("LIST", ls.append)

    for entry in ls:
        parts = entry.split()
        if len(parts) < 9:
            continue
        name = parts[-1]
        if name.startswith("login_") and name.endswith(".log"):
            files.append(name)

    ftp.quit()
    return files


def ftp_read_file(filename):
    """Pobiera plik z FTP jako listę linii."""
    lines = []
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_PATH)
    ftp.retrlines(f"RETR {filename}", lines.append)
    ftp.quit()
    return lines


def get_last_processed_line(logfile):
    conn = get_connection()
    if not conn:
        return 0

    cursor = conn.cursor()
    cursor.execute("SELECT last_line FROM login_progress WHERE logfile=%s", (logfile,))
    row = cursor.fetchone()
    cursor.close()
    conn.close()

    return row[0] if row else 0


def set_last_processed_line(logfile, last_line):
    conn = get_connection()
    if not conn:
        return

    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO login_progress(logfile, last_line)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE last_line=%s
    """, (logfile, last_line, last_line))

    conn.commit()
    cursor.close()
    conn.close()


def process_login_logs():
    print("[FTP] Skanowanie logów logowania...")

    files = ftp_list_files()
    for file in sorted(files):
        print(f"[FTP] Przetwarzanie: {file}")

        lines = ftp_read_file(file)
        last_done = get_last_processed_line(file)

        for idx in range(last_done, len(lines)):
            line = lines[idx]

            # Poprawny REGEX do wyciągania nicku
            match = re.search(r":([A-Za-z0-9_]+)\(\d+\)' logged in at:", line)

            if match:
                nick = match.group(1)
                print(f"[LOGIN] Wykryto logowanie: {nick}")
                add_coins(nick, COINS_PER_LOGIN)

        set_last_processed_line(file, len(lines))


# -------------------------------- BACKGROUND TASK --------------------------------

def background_worker():
    """Przetwarza logi co 30 sekund."""
    while True:
        try:
            process_login_logs()
        except Exception as e:
            print("[ERROR worker]:", e)

        threading.Event().wait(30)


threading.Thread(target=background_worker, daemon=True).start()

# -------------------------------- SLASH COMMANDS --------------------------------

class Economy(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @discord.slash_command(name="saldo", description="Sprawdź saldo (według nicku z gry)")
    async def saldo(self, ctx, nick: str):
        conn = get_connection()
        if not conn:
            await ctx.respond("Błąd bazy danych.")
            return

        cursor = conn.cursor()
        cursor.execute("SELECT balance FROM users WHERE nick=%s", (nick,))
        row = cursor.fetchone()

        balance = row[0] if row else 0

        await ctx.respond(f"Saldo gracza **{nick}**: {balance} monet")

        cursor.close()
        conn.close()


bot.add_cog(Economy(bot))

@bot.event
async def on_ready():
    await bot.sync_commands()
    print(f"Bot zalogowany jako {bot.user}. Slash commands zsynchronizowane.")


# -------------------------------- FLASK KEEP-ALIVE --------------------------------

app = Flask("")

@app.route("/")
def home():
    return "Bot działa!"

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)

threading.Thread(target=run_flask, daemon=True).start()

# -------------------------------- RUN BOT --------------------------------

bot.run(DISCORD_TOKEN)
