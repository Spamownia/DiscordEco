#!/usr/bin/env python3
# app_full.py
# Discord bot + FTP SCUM login monitor + MySQL economy
# Powiązanie kont: /powiaz <steam_id>  (steam_id = liczba z logów SCUM)
# Przy starcie: przetwarza wszystkie login_*.log i przyznaje monety tylko powiązanym kontom
# Potem co 60s sprawdza nowe linie i dopisuje tylko nowe przetworzenia

import os
import re
import ftplib
import threading
import hashlib
import time
from datetime import datetime

import discord
from discord.ext import commands
import mysql.connector
from mysql.connector import Error
from flask import Flask

# ----------------------------- KONFIG -----------------------------

DISCORD_TOKEN = os.environ.get("DISCORD_TOKEN")  # musi być w ENV

# FTP (Twoje dane)
FTP_HOST = "195.179.226.218"
FTP_PORT = 56421
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_PATH = "/SCUM/Saved/SaveFiles/Logs/"

# MySQL (Twoje dane)
MYSQL_HOST = "mysql-1f2c991-spamownia91-479a.h.aivencloud.com"
MYSQL_PORT = 14365
MYSQL_USER = "avnadmin"
MYSQL_PASSWORD = "AVNS_6gzpU-skelov685O3Gx"
MYSQL_DB = "defaultdb"

# Ekonomia
COINS_PER_LOGIN = 10

# Interwał sprawdzania FTP (w sekundach)
FTP_CHECK_INTERVAL = 60

# ----------------------------- DISCORD SETUP -----------------------------

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

# ----------------------------- BAZA DANYCH -----------------------------

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
        print("[DB] Błąd połączenia:", e)
        return None

def init_db():
    conn = get_connection()
    if not conn:
        print("[DB] Nie można zainicjować bazy (połączenie nieudane).")
        return

    cursor = conn.cursor()
    # users: discord_id PK, steam_id UNIQUE (może być NULL), balance
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                discord_id BIGINT PRIMARY KEY,
                steam_id BIGINT UNIQUE,
                balance INT NOT NULL DEFAULT 0
            )
        """)
    except Exception as e:
        print("[DB] Błąd tworzenia tabeli users:", e)

    # transactions: zapis akcji
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                id INT AUTO_INCREMENT PRIMARY KEY,
                discord_id BIGINT,
                steam_id BIGINT,
                action VARCHAR(50),
                item VARCHAR(100),
                amount INT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
    except Exception as e:
        print("[DB] Błąd tworzenia transactions:", e)

    # processed_log_lines: aby nie dublować przy restarcie (hash linii)
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS processed_log_lines (
                line_hash VARCHAR(64) PRIMARY KEY,
                source_file VARCHAR(255),
                line_text TEXT,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
    except Exception as e:
        print("[DB] Błąd tworzenia processed_log_lines:", e)

    conn.commit()
    cursor.close()
    conn.close()

init_db()

# ----------------------------- POMOCNICZE FUNKCJE DB -----------------------------

def add_coins_for_discord(discord_id, steam_id, amount, reason="LOGIN"):
    """Dodaje monety do konta discord_id i zapisuje transakcję."""
    conn = get_connection()
    if not conn:
        print("[ECON] Brak połączenia z DB w add_coins_for_discord")
        return False
    cursor = conn.cursor()
    try:
        # upewnij się, że konto istnieje
        cursor.execute("SELECT balance FROM users WHERE discord_id=%s", (discord_id,))
        row = cursor.fetchone()
        if row:
            new_balance = row[0] + amount
            cursor.execute("UPDATE users SET balance=%s WHERE discord_id=%s", (new_balance, discord_id))
        else:
            new_balance = amount
            cursor.execute("INSERT INTO users(discord_id, steam_id, balance) VALUES(%s, %s, %s)", (discord_id, steam_id, new_balance))

        # transakcja
        cursor.execute("""
            INSERT INTO transactions(discord_id, steam_id, action, item, amount)
            VALUES(%s, %s, %s, %s, %s)
        """, (discord_id, steam_id, reason, None, amount))

        conn.commit()
        print(f"[ECON] +{amount} monet dla DiscordID={discord_id} (SteamID={steam_id}). Nowe saldo: {new_balance}")
        return True
    except Exception as e:
        print("[ECON] Błąd add_coins_for_discord:", e)
        return False
    finally:
        cursor.close()
        conn.close()

def get_discord_by_steam(steam_id):
    """Zwraca discord_id powiązany z podanym steam_id lub None."""
    conn = get_connection()
    if not conn:
        return None
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT discord_id FROM users WHERE steam_id=%s", (steam_id,))
        row = cursor.fetchone()
        return row[0] if row else None
    except Exception as e:
        print("[DB] Błąd get_discord_by_steam:", e)
        return None
    finally:
        cursor.close()
        conn.close()

def record_processed_line(line_hash, source_file, line_text):
    conn = get_connection()
    if not conn:
        return
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT IGNORE INTO processed_log_lines(line_hash, source_file, line_text) VALUES(%s, %s, %s)",
                       (line_hash, source_file, line_text))
        conn.commit()
    except Exception as e:
        print("[DB] Błąd record_processed_line:", e)
    finally:
        cursor.close()
        conn.close()

def is_line_already_processed(line_hash):
    conn = get_connection()
    if not conn:
        return False
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT 1 FROM processed_log_lines WHERE line_hash=%s", (line_hash,))
        r = cursor.fetchone()
        return bool(r)
    except Exception as e:
        print("[DB] Błąd is_line_already_processed:", e)
        return False
    finally:
        cursor.close()
        conn.close()

# ----------------------------- FTP HELPERS -----------------------------

def ftp_connect():
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT, timeout=30)
    ftp.login(FTP_USER, FTP_PASS)
    return ftp

def ftp_list_login_files():
    """Zwraca listę plików login_*.log w katalogu FTP."""
    try:
        ftp = ftp_connect()
        ftp.cwd(FTP_PATH)
        out = []
        lines = []
        ftp.retrlines("LIST", lines.append)
        for entry in lines:
            parts = entry.split()
            if not parts:
                continue
            name = parts[-1]
            if name.startswith("login_") and name.endswith(".log"):
                out.append(name)
        ftp.quit()
        return sorted(out)
    except Exception as e:
        print("[FTP] Błąd listowania plików:", e)
        return []

def ftp_read_file_lines(filename):
    try:
        ftp = ftp_connect()
        ftp.cwd(FTP_PATH)
        lines = []
        ftp.retrlines(f"RETR {filename}", lines.append)
        ftp.quit()
        return lines
    except Exception as e:
        print(f"[FTP] Błąd pobierania {filename}:", e)
        return []

# ----------------------------- PARSOWANIE LOGÓW -----------------------------

# Regex: szukamy fragmentu: ... 76561197992396189:Anu(26)' logged in at:
# grupy: 1=steam_id, 2=nick
LOGIN_REGEX = re.compile(r"'\S+\s+(\d+):([A-Za-z0-9_]+)\(\d+\)'\s+logged in at:")

def process_all_login_logs_once():
    """
    Przetwarza wszystkie pliki login_*.log na FTP.
    Dla każdej linii: jeśli pasuje regex i linia nie jest przetworzona (hash),
    to jeśli steam_id jest powiązany z discord_id -> przyznaj monety.
    Następnie oznacz linię jako przetworzoną (żeby nie dublować).
    """
    print("[FTP] Rozpoczynam pełne skanowanie login_*.log ...", datetime.utcnow().isoformat())
    files = ftp_list_login_files()
    if not files:
        print("[FTP] Brak plików login_*.log na FTP.")
        return

    for fname in files:
        print(f"[FTP] Otwieram {fname}")
        lines = ftp_read_file_lines(fname)
        for line in lines:
            match = LOGIN_REGEX.search(line)
            if not match:
                continue
            steam_id = match.group(1)
            nick = match.group(2)
            # hash linii aby nie dublować
            h = hashlib.sha256(line.encode("utf-8")).hexdigest()
            if is_line_already_processed(h):
                # już obrobione
                continue

            discord_id = get_discord_by_steam(steam_id)
            if discord_id:
                # przyznaj monety
                added = add_coins_for_discord(discord_id, steam_id, COINS_PER_LOGIN, reason="LOGIN")
                if added:
                    record_processed_line(h, fname, line)
                else:
                    print(f"[FTP] Nie udało się dodać monet dla SteamID={steam_id}")
            else:
                # nie powiązane konto - oznaczamy linię jako przetworzoną,
                # żeby przy restarcie nie przerabiać non-stop tej samej linii.
                # Robimy też wpis do transactions jako notatkę (opcjonalnie)
                print(f"[FTP] SteamID={steam_id} (nick={nick}) NIE jest powiązany z Discordem - oznaczam jako przetworzone.")
                record_processed_line(h, fname, line)

# ----------------------------- BACKGROUND MONITOR (nowe linie) -----------------------------

def ftp_monitor_loop():
    """
    Po pełnym wstępnym przetworzeniu, co FTP_CHECK_INTERVAL sekund
    sprawdzamy ponownie wszystkie pliki i dopisujemy nowe linie.
    (Każda linia sprawdzana po hash - więc tylko nowe będą przetwarzane)
    """
    print("[WORKER] Monitor FTP uruchomiony.")
    # Najpierw pełne przetworzenie (pierwszy raz)
    process_all_login_logs_once()
    # Dalej cyklicznie
    while True:
        try:
            process_all_login_logs_once()
        except Exception as e:
            print("[WORKER] Błąd podczas cyclic check:", e)
        time.sleep(FTP_CHECK_INTERVAL)

# Uruchom worker w tle (daemon)
threading.Thread(target=ftp_monitor_loop, daemon=True).start()

# ----------------------------- DISCORD COMMANDS -----------------------------

class EconomyCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @discord.slash_command(name="powiaz", description="Powiąż swoje konto Discord ze SteamID (np. 765611979xxxxxxxx)")
    async def powiaz(self, ctx, steam_id: str):
        """Powiąż konto - zapisuje steam_id w users dla wywołującego użytkownika."""
        # walidacja steam_id
        if not re.fullmatch(r"\d{15,20}", steam_id):
            await ctx.respond("Błędny SteamID. Podaj pełne SteamID (liczby).")
            return

        discord_id = ctx.author.id
        conn = get_connection()
        if not conn:
            await ctx.respond("Błąd połączenia z bazą danych.")
            return
        cursor = conn.cursor()
        try:
            # Jeśli konto istnieje - update steam_id
            cursor.execute("SELECT discord_id FROM users WHERE discord_id=%s", (discord_id,))
            if cursor.fetchone():
                cursor.execute("UPDATE users SET steam_id=%s WHERE discord_id=%s", (steam_id, discord_id))
            else:
                cursor.execute("INSERT INTO users(discord_id, steam_id, balance) VALUES(%s, %s, %s)", (discord_id, steam_id, 0))

            # Zapis transakcji informacyjnej
            cursor.execute("""
                INSERT INTO transactions(discord_id, steam_id, action, item, amount)
                VALUES(%s, %s, %s, %s, %s)
            """, (discord_id, steam_id, "BIND", None, 0))

            conn.commit()
            await ctx.respond(f"Powiązano SteamID `{steam_id}` z Twoim Discordem.")
            print(f"[BIND] Discord {discord_id} powiązał SteamID {steam_id}")
        except Exception as e:
            print("[BIND] Błąd podczas powiązywania:", e)
            await ctx.respond("Wystąpił błąd podczas powiązywania. Sprawdź logi.")
        finally:
            cursor.close()
            conn.close()

    @discord.slash_command(name="saldo", description="Pokaż swoje saldo lub saldo kogoś po SteamID")
    async def saldo(self, ctx, steam_id: str = None):
        conn = get_connection()
        if not conn:
            await ctx.respond("Błąd połączenia z bazą danych.")
            return
        cursor = conn.cursor()
        try:
            if steam_id:
                cursor.execute("SELECT balance, discord_id FROM users WHERE steam_id=%s", (steam_id,))
                row = cursor.fetchone()
                if row:
                    balance, discord_id = row
                    if discord_id:
                        await ctx.respond(f"Saldo gracza (SteamID {steam_id}, Discord <@{discord_id}>): {balance} monet")
                    else:
                        await ctx.respond(f"Saldo konta powiązanego z SteamID {steam_id}: {balance} monet")
                else:
                    await ctx.respond("Brak konta powiązanego z podanym SteamID.")
            else:
                discord_id = ctx.author.id
                cursor.execute("SELECT balance, steam_id FROM users WHERE discord_id=%s", (discord_id,))
                row = cursor.fetchone()
                if row:
                    balance, steam = row
                    await ctx.respond(f"Twoje saldo: {balance} monet (SteamID: {steam})")
                else:
                    await ctx.respond("Nie masz założonego konta. Użyj /powiaz <steam_id> aby się zarejestrować.")
        except Exception as e:
            print("[CMD saldo] Błąd:", e)
            await ctx.respond("Wystąpił błąd.")
        finally:
            cursor.close()
            conn.close()

    @discord.slash_command(name="leaderboard", description="Top 10 graczy wg salda")
    async def leaderboard(self, ctx):
        conn = get_connection()
        if not conn:
            await ctx.respond("Błąd połączenia z bazą danych.")
            return
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT steam_id, discord_id, balance FROM users ORDER BY balance DESC LIMIT 10")
            rows = cursor.fetchall()
            if not rows:
                await ctx.respond("Brak danych.")
                return
            lines = []
            rank = 1
            for steam_id, discord_id, balance in rows:
                who = f"<@{discord_id}>" if discord_id else f"SteamID {steam_id}"
                lines.append(f"{rank}. {who} — {balance} monet")
                rank += 1
            await ctx.respond("**Leaderboard:**\n" + "\n".join(lines))
        except Exception as e:
            print("[CMD leaderboard] Błąd:", e)
            await ctx.respond("Wystąpił błąd.")
        finally:
            cursor.close()
            conn.close()

    @discord.slash_command(name="shop", description="Pokaż sklep")
    async def shop(self, ctx):
        shop_items = {
            "Miecz": 100,
            "Tarcza": 75,
            "Mikstura": 25,
        }
        text = "\n".join([f"{k}: {v} monet" for k, v in shop_items.items()])
        await ctx.respond("**Sklep:**\n" + text)

    @discord.slash_command(name="buy", description="Kup przedmiot ze sklepu")
    async def buy(self, ctx, item: str):
        # prosty sklep w kodzie - rozbudujemy gdy zechcesz
        shop_items = {
            "Miecz": 100,
            "Tarcza": 75,
            "Mikstura": 25,
        }
        item_title = item.strip()
        if item_title not in shop_items:
            await ctx.respond("Brak takiego przedmiotu w sklepie.")
            return
        price = shop_items[item_title]
        discord_id = ctx.author.id
        conn = get_connection()
        if not conn:
            await ctx.respond("Błąd połączenia z bazą danych.")
            return
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT balance FROM users WHERE discord_id=%s", (discord_id,))
            row = cursor.fetchone()
            balance = row[0] if row else 0
            if balance < price:
                await ctx.respond("Nie masz wystarczająco monet.")
                return
            new_balance = balance - price
            cursor.execute("UPDATE users SET balance=%s WHERE discord_id=%s", (new_balance, discord_id))
            cursor.execute("INSERT INTO transactions(discord_id, steam_id, action, item, amount) VALUES(%s, %s, %s, %s, %s)",
                           (discord_id, None, "BUY", item_title, price))
            conn.commit()
            await ctx.respond(f"Kupiłeś **{item_title}** za {price} monet. Nowe saldo: {new_balance}")
        except Exception as e:
            print("[CMD buy] Błąd:", e)
            await ctx.respond("Wystąpił błąd podczas zakupu.")
        finally:
            cursor.close()
            conn.close()

# Dodaj Cog
bot.add_cog(EconomyCog(bot))

@bot.event
async def on_ready():
    try:
        await bot.sync_commands()
        print(f"[BOT] Zalogowano jako {bot.user} — komendy zsynchronizowane.")
    except Exception as e:
        print("[BOT] Błąd synchronizacji komend:", e)
    print("[BOT] Ready.")

# ----------------------------- FLASK KEEP-ALIVE -----------------------------
app = Flask("")

@app.route("/")
def index():
    return "Bot działa. " + datetime.utcnow().isoformat()

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)

threading.Thread(target=run_flask, daemon=True).start()

# ----------------------------- START BOTA -----------------------------
if not DISCORD_TOKEN:
    print("[ERROR] Brakuje DISCORD_TOKEN w environment variables. Ustaw DISCORD_TOKEN i restartuj.")
else:
    bot.run(DISCORD_TOKEN)
