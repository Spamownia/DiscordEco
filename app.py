import discord
from discord.ext import commands
import mysql.connector
from mysql.connector import Error
from flask import Flask
import threading
import os
import ftplib
import re
import hashlib
import time

# ---------------- CONFIG ----------------
DISCORD_TOKEN = os.environ.get("DISCORD_TOKEN")  # Token w Environment Variable

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
LOGIN_REWARD = 10  # monet za logowanie

# ---------------- BOT SETUP ----------------
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

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
        # Tabele ekonomii
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                discord_id BIGINT PRIMARY KEY,
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
        # Tabela do pamiętania przetworzonych linii logów
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS processed_log_lines (
                line_hash VARCHAR(64) PRIMARY KEY
            )
        """)
        # Opcjonalna tabela do mapowania nick -> discord_id
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS nick_mapping (
                nick VARCHAR(100) PRIMARY KEY,
                discord_id BIGINT
            )
        """)
        conn.commit()
        cursor.close()
        conn.close()

init_db()

# ---------------- ECONOMY COG ----------------
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

    # --------- PLAYER COMMANDS ---------
    @discord.slash_command(name="balance", description="Pokaż swoje saldo")
    async def balance(self, ctx: discord.ApplicationContext):
        user_id = ctx.author.id
        conn = get_connection()
        if conn:
            cursor = conn.cursor()
            cursor.execute("SELECT balance FROM users WHERE discord_id=%s", (user_id,))
            row = cursor.fetchone()
            balance = row[0] if row else 0
            await ctx.respond(f"Twoje saldo: {balance}$")
            cursor.close()
            conn.close()
        else:
            await ctx.respond("Błąd połączenia z bazą danych.")

    @discord.slash_command(name="shop", description="Pokaż dostępne przedmioty w sklepie")
    async def shop(self, ctx: discord.ApplicationContext):
        msg = "\n".join([f"{item}: {price}$" for item, price in self.shop_items.items()])
        await ctx.respond(f"**Sklep:**\n{msg}")

    @discord.slash_command(name="buy", description="Kup przedmiot ze sklepu")
    async def buy(self, ctx: discord.ApplicationContext, item: str):
        user_id = ctx.author.id
        conn = get_connection()
        if not conn:
            await ctx.respond("Błąd połączenia z bazą danych.")
            return
        cursor = conn.cursor()

        if item not in self.shop_items:
            await ctx.respond("Nie ma takiego przedmiotu w sklepie.")
            cursor.close()
            conn.close()
            return

        price = self.shop_items[item]
        cursor.execute("SELECT balance FROM users WHERE discord_id=%s", (user_id,))
        row = cursor.fetchone()
        balance = row[0] if row else 0

        if balance < price:
            await ctx.respond("Nie masz wystarczająco środków.")
        else:
            new_balance = balance - price
            cursor.execute("""
                INSERT INTO users(discord_id, balance)
                VALUES(%s, %s)
                ON DUPLICATE KEY UPDATE balance=%s
            """, (user_id, new_balance, new_balance))
            cursor.execute("""
                INSERT INTO transactions(user_id, action, item, amount)
                VALUES(%s, %s, %s, %s)
            """, (user_id, "BUY", item, price))
            conn.commit()
            await ctx.respond(f"Kupiłeś **{item}** za {price}$! Nowe saldo: {new_balance}$")
        cursor.close()
        conn.close()

    # --------- ADMIN COMMANDS ---------
    @discord.slash_command(name="set_balance", description="Ustaw saldo użytkownika (ADMIN)")
    @commands.has_permissions(administrator=True)
    async def set_balance(self, ctx: discord.ApplicationContext, user: discord.User, amount: int):
        conn = get_connection()
        if conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO users(discord_id, balance)
                VALUES(%s, %s)
                ON DUPLICATE KEY UPDATE balance=%s
            """, (user.id, amount, amount))
            cursor.execute("""
                INSERT INTO transactions(user_id, action, item, amount)
                VALUES(%s, %s, %s, %s)
            """, (user.id, "SET_BALANCE", None, amount))
            conn.commit()
            await ctx.respond(f"Ustawiono saldo **{user.name}** na {amount}$")
            cursor.close()
            conn.close()
        else:
            await ctx.respond("Błąd połączenia z bazą danych.")

    @discord.slash_command(name="all_transactions", description="Pokaż ostatnie transakcje (ADMIN)")
    @commands.has_permissions(administrator=True)
    async def all_transactions(self, ctx: discord.ApplicationContext):
        conn = get_connection()
        if conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT user_id, action, item, amount, timestamp
                FROM transactions
                ORDER BY timestamp DESC
                LIMIT 20
            """)
            rows = cursor.fetchall()
            if rows:
                msg = "\n".join([f"{r[0]}: {r[1]} {r[2] if r[2] else ''} {r[3]}$ ({r[4]})" for r in rows])
                await ctx.respond(f"**Ostatnie transakcje:**\n{msg}")
            else:
                await ctx.respond("Brak transakcji.")
            cursor.close()
            conn.close()
        else:
            await ctx.respond("Błąd połączenia z bazą danych.")

bot.add_cog(Economy(bot))

# ---------------- FTP LOGIN MONITOR ----------------
def add_coins(nick, amount):
    conn = get_connection()
    if not conn:
        return
    cursor = conn.cursor()
    cursor.execute("SELECT discord_id FROM nick_mapping WHERE nick=%s", (nick,))
    row = cursor.fetchone()
    discord_id = row[0] if row else 0
    cursor.execute("""
        INSERT INTO users(discord_id, balance)
        VALUES(%s, %s)
        ON DUPLICATE KEY UPDATE balance = balance + %s
    """, (discord_id, amount, amount))
    cursor.execute("""
        INSERT INTO transactions(user_id, action, item, amount)
        VALUES(%s, %s, %s, %s)
    """, (discord_id, "LOGIN_REWARD", nick, amount))
    conn.commit()
    cursor.close()
    conn.close()

def is_line_processed(line):
    conn = get_connection()
    if not conn:
        return False
    cursor = conn.cursor()
    line_hash = hashlib.sha256(line.encode('utf-8')).hexdigest()
    cursor.execute("SELECT 1 FROM processed_log_lines WHERE line_hash=%s", (line_hash,))
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    return result is not None

def mark_line_processed(line):
    conn = get_connection()
    if not conn:
        return
    cursor = conn.cursor()
    line_hash = hashlib.sha256(line.encode('utf-8')).hexdigest()
    cursor.execute("INSERT IGNORE INTO processed_log_lines(line_hash) VALUES(%s)", (line_hash,))
    conn.commit()
    cursor.close()
    conn.close()

def process_login_logs():
    try:
        ftp = ftplib.FTP()
        ftp.connect(FTP_HOST, FTP_PORT)
        ftp.login(FTP_USER, FTP_PASS)
        files = []
        ftp.retrlines(f'LIST {FTP_LOG_PATH}', files.append)
        login_logs = [f.split()[-1] for f in files if "login_" in f]

        for log_file in login_logs:
            lines = []
            try:
                ftp.retrlines(f'RETR {FTP_LOG_PATH}{log_file}', lines.append)
            except ftplib.error_perm:
                continue
            for line in lines:
                if "logged in at:" in line and not is_line_processed(line):
                    m = re.search(r"\d+:(.*?)\(\d+\)' logged in at:", line)
                    if m:
                        nick = m.group(1)
                        add_coins(nick, LOGIN_REWARD)
                        mark_line_processed(line)
        ftp.quit()
    except Exception as e:
        print("Błąd FTP/parsowania:", e)

def login_monitor_loop():
    while True:
        process_login_logs()
        time.sleep(60)

threading.Thread(target=login_monitor_loop, daemon=True).start()

# ---------------- MINIMAL FLASK SERVER ----------------
app = Flask("")

@app.route("/")
def home():
    return "Bot działa!"

def run_flask():
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)

threading.Thread(target=run_flask, daemon=True).start()

# ---------------- START BOT ----------------
@bot.event
async def on_ready():
    print(f"Bot zalogowany jako {bot.user}")

bot.run(DISCORD_TOKEN)
