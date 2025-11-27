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
DISCORD_TOKEN = os.environ.get("DISCORD_TOKEN")  # token z env

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

MONETY_PER_LOGIN = 10
SCAN_INTERVAL = 60  # sekund

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
        print(f"[DB] Błąd połączenia MySQL: {e}")
        return None

def init_db():
    conn = get_connection()
    if conn:
        cursor = conn.cursor()
        # Users
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                discord_id BIGINT PRIMARY KEY,
                balance INT NOT NULL DEFAULT 0,
                nick VARCHAR(255) UNIQUE
            )
        """)
        # Transactions
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id BIGINT,
                action VARCHAR(50),
                item VARCHAR(50),
                amount INT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # Processed logs
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

# ---------------- BOT SETUP ----------------
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

# ---------------- SHOP ----------------
SHOP_ITEMS = {
    "Miecz": 100,
    "Tarcza": 75,
    "Mikstura": 25,
    "Zbroja": 200,
    "Eliksir": 50
}

# ---------------- FTP LOGIC ----------------
def hash_line(line: str):
    return hashlib.sha256(line.encode('utf-8')).hexdigest()

def get_processed_hashes():
    conn = get_connection()
    if not conn:
        return set()
    cursor = conn.cursor()
    cursor.execute("SELECT line_hash FROM processed_logs")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return set(r[0] for r in rows)

def mark_line_processed(filename, line_hash):
    conn = get_connection()
    if not conn:
        return
    cursor = conn.cursor()
    cursor.execute("INSERT INTO processed_logs(filename, line_hash) VALUES(%s, %s)", (filename, line_hash))
    conn.commit()
    cursor.close()
    conn.close()

def process_login_line(nick):
    conn = get_connection()
    if not conn:
        return
    cursor = conn.cursor()
    cursor.execute("SELECT discord_id, balance FROM users WHERE nick=%s", (nick,))
    row = cursor.fetchone()
    if row:
        discord_id, balance = row
        new_balance = balance + MONETY_PER_LOGIN
        cursor.execute("UPDATE users SET balance=%s WHERE discord_id=%s", (new_balance, discord_id))
    else:
        # nowy użytkownik, discord_id = NULL
        cursor.execute("INSERT INTO users(nick, balance) VALUES(%s, %s)", (nick, MONETY_PER_LOGIN))
    cursor.execute("INSERT INTO transactions(user_id, action, item, amount) VALUES(%s, %s, %s, %s)",
                   (None, "LOGIN", nick, MONETY_PER_LOGIN))
    conn.commit()
    cursor.close()
    conn.close()

def scan_logs():
    print("[FTP] Rozpoczynam skanowanie logów...")
    processed_hashes = get_processed_hashes()
    try:
        ftp = ftplib.FTP()
        ftp.connect(FTP_HOST, FTP_PORT)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(FTP_PATH)
        files = []
        ftp.retrlines('NLST', files.append)
        login_files = [f for f in files if f.startswith("login_")]
        for filename in login_files:
            try:
                lines = []
                ftp.retrlines(f"RETR {filename}", lines.append)
                for line in lines:
                    line_hash = hash_line(line)
                    if line_hash in processed_hashes:
                        continue
                    if "logged in" in line:
                        # pobranie nicka
                        parts = line.split("'")
                        if len(parts) > 1:
                            nick_part = parts[1].split(":")[-1].split("(")[0]
                            nick = nick_part.strip()
                            process_login_line(nick)
                    mark_line_processed(filename, line_hash)
            except Exception as e:
                print(f"[FTP] Błąd przetwarzania pliku {filename}: {e}")
        ftp.quit()
    except Exception as e:
        print(f"[FTP] Błąd połączenia FTP: {e}")

def periodic_log_scan():
    scan_logs()  # pełne przy starcie
    while True:
        time.sleep(SCAN_INTERVAL)
        scan_logs()

# ---------------- BOT COMMANDS ----------------
@bot.command()
async def saldo(ctx, nick: str = None):
    if not nick:
        await ctx.send("Podaj nick: !saldo <nick>")
        return
    conn = get_connection()
    if not conn:
        await ctx.send("Błąd DB")
        return
    cursor = conn.cursor()
    cursor.execute("SELECT balance FROM users WHERE nick=%s", (nick,))
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    if row:
        await ctx.send(f"Saldo {nick}: {row[0]}$")
    else:
        await ctx.send(f"Nie znaleziono użytkownika {nick}")

@bot.command()
async def shop(ctx):
    msg = "\n".join([f"{item}: {price}$" for item, price in SHOP_ITEMS.items()])
    await ctx.send(f"**Sklep:**\n{msg}")

@bot.command()
async def buy(ctx, item: str, nick: str):
    conn = get_connection()
    if not conn:
        await ctx.send("Błąd DB")
        return
    cursor = conn.cursor()
    cursor.execute("SELECT balance FROM users WHERE nick=%s", (nick,))
    row = cursor.fetchone()
    if not row:
        await ctx.send(f"Nie znaleziono użytkownika {nick}")
        cursor.close()
        conn.close()
        return
    balance = row[0]
    if item not in SHOP_ITEMS:
        await ctx.send(f"Brak przedmiotu {item} w sklepie")
    elif balance < SHOP_ITEMS[item]:
        await ctx.send(f"{nick} nie ma wystarczająco środków")
    else:
        new_balance = balance - SHOP_ITEMS[item]
        cursor.execute("UPDATE users SET balance=%s WHERE nick=%s", (new_balance, nick))
        cursor.execute("INSERT INTO transactions(user_id, action, item, amount) VALUES(%s,%s,%s,%s)",
                       (None, "BUY", item, SHOP_ITEMS[item]))
        conn.commit()
        await ctx.send(f"{nick} kupił {item} za {SHOP_ITEMS[item]}$. Nowe saldo: {new_balance}$")
    cursor.close()
    conn.close()

# ---------------- START BOT ----------------
@bot.event
async def on_ready():
    print(f"[BOT] Zalogowano jako {bot.user}")
    print("[BOT] Uruchamiam wątek logów FTP...")
    # uruchamiamy wątek tylko raz
    if not getattr(bot, "_scan_thread_started", False):
        threading.Thread(target=periodic_log_scan, daemon=True).start()
        bot._scan_thread_started = True

# ---------------- MINIMAL FLASK SERVER ----------------
app = Flask("")

@app.route("/")
def home():
    return "Bot działa!"

def run():
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)

threading.Thread(target=run, daemon=True).start()

# ---------------- RUN BOT ----------------
bot.run(DISCORD_TOKEN)
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
FTP_PATH = "/SCUM/Saved/SaveFiles/Logs/"

MONETY_ZA_LOG = 10
CHECK_INTERVAL = 60  # w sekundach

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
                discord_id BIGINT PRIMARY KEY,
                steam_id BIGINT UNIQUE,
                nick VARCHAR(50),
                balance INT NOT NULL DEFAULT 0
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id BIGINT,
                action VARCHAR(50),
                item VARCHAR(50),
                amount INT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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

# ---------------- DISCORD BOT ----------------
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

# ---------------- FTP LOGIC ----------------
def connect_ftp():
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_PATH)
    return ftp

def hash_line(line):
    return hashlib.sha256(line.encode('utf-8')).hexdigest()

def line_already_processed(filename, line_hash):
    conn = get_connection()
    if not conn:
        return False
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM processed_logs WHERE filename=%s AND line_hash=%s", (filename, line_hash))
    exists = cursor.fetchone() is not None
    cursor.close()
    conn.close()
    return exists

def mark_line_processed(filename, line_hash):
    conn = get_connection()
    if conn:
        cursor = conn.cursor()
        cursor.execute("INSERT INTO processed_logs(filename, line_hash) VALUES(%s, %s)", (filename, line_hash))
        conn.commit()
        cursor.close()
        conn.close()

def award_login(nick):
    conn = get_connection()
    if conn:
        cursor = conn.cursor()
        cursor.execute("SELECT discord_id, balance FROM users WHERE nick=%s", (nick,))
        row = cursor.fetchone()
        if row:
            discord_id, balance = row
            new_balance = balance + MONETY_ZA_LOG
            cursor.execute("UPDATE users SET balance=%s WHERE discord_id=%s", (new_balance, discord_id))
            cursor.execute("INSERT INTO transactions(user_id, action, item, amount) VALUES(%s,%s,%s,%s)",
                           (discord_id, "LOGIN_REWARD", None, MONETY_ZA_LOG))
            conn.commit()
        cursor.close()
        conn.close()

def process_log_file(ftp, filename):
    lines = []
    try:
        ftp.retrlines(f"RETR {filename}", lines.append)
    except Exception as e:
        print(f"[FTP] Błąd odczytu {filename}: {e}")
        return

    for line in lines:
        if "logged in" in line:
            line_hash = hash_line(line)
            if not line_already_processed(filename, line_hash):
                # wyciągnięcie nicku
                try:
                    nick_part = line.split("'")[1]  # 'IP SteamID:Nick(XX)'
                    nick = nick_part.split(":")[1].split("(")[0]
                    award_login(nick)
                except:
                    continue
                mark_line_processed(filename, line_hash)

def scan_logs():
    print("[FTP] Rozpoczynam skanowanie logów...")
    try:
        ftp = connect_ftp()
        files = []
        ftp.retrlines("LIST", files.append)
        log_files = [f.split()[-1] for f in files if f.startswith("login_")]
        for filename in log_files:
            process_log_file(ftp, filename)
        ftp.quit()
    except Exception as e:
        print(f"[FTP] Błąd FTP: {e}")

def periodic_log_scan():
    while True:
        scan_logs()
        time.sleep(CHECK_INTERVAL)

# ---------------- BOT COMMANDS ----------------
class Economy(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.shop_items = {
            "Miecz": 100,
            "Tarcza": 75,
            "Mikstura": 25,
            "Zbroja": 200,
            "Eliksir": 50
        }

    @commands.command(name="register_steam")
    async def register_steam(self, ctx, steam_id: int, nick: str):
        conn = get_connection()
        if conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO users(discord_id, steam_id, nick)
                VALUES(%s,%s,%s)
                ON DUPLICATE KEY UPDATE steam_id=%s, nick=%s
            """, (ctx.author.id, steam_id, nick, steam_id, nick))
            conn.commit()
            cursor.close()
            conn.close()
            await ctx.send(f"Zarejestrowano konto {nick} z Steam ID {steam_id}")
        else:
            await ctx.send("Błąd połączenia z bazą danych.")

    @commands.command(name="saldo")
    async def saldo(self, ctx):
        conn = get_connection()
        if conn:
            cursor = conn.cursor()
            cursor.execute("SELECT balance FROM users WHERE discord_id=%s", (ctx.author.id,))
            row = cursor.fetchone()
            balance = row[0] if row else 0
            await ctx.send(f"Twoje saldo: {balance} monet")
            cursor.close()
            conn.close()

bot.add_cog(Economy(bot))

@bot.event
async def on_ready():
    print(f"Bot zalogowany jako {bot.user}")

# ---------------- MINIMAL FLASK SERVER ----------------
app = Flask("")

@app.route("/")
def home():
    return "Bot działa!"

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)

threading.Thread(target=run_flask).start()
threading.Thread(target=periodic_log_scan, daemon=True).start()

# ---------------- RUN BOT ----------------
bot.run(DISCORD_TOKEN)
