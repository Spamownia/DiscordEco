import discord
from discord.ext import commands
import mysql.connector
from mysql.connector import Error
import os

# ---------------- CONFIG ----------------
DISCORD_TOKEN = os.environ.get("DISCORD_TOKEN")

MYSQL_HOST = "mysql-1f2c991-spamownia91-479a.h.aivencloud.com"
MYSQL_PORT = 14365
MYSQL_USER = "avnadmin"
MYSQL_PASSWORD = "AVNS_6gzpU-skelov685O3Gx"
MYSQL_DB = "defaultdb"

# ---------------- BOT SETUP ----------------
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(intents=intents)

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
        conn.commit()
        cursor.close()
        conn.close()

init_db()

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

    @discord.slash_command(name="balance", description="Pokaż swoje saldo")
    async def balance(self, ctx):
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
    async def shop(self, ctx):
        msg = "\n".join([f"{item}: {price}$" for item, price in self.shop_items.items()])
        await ctx.respond(f"**Sklep:**\n{msg}")

    @discord.slash_command(name="buy", description="Kup przedmiot ze sklepu")
    async def buy(self, ctx, item: str):
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

    @discord.slash_command(name="set_balance", description="Ustaw saldo użytkownika (ADMIN)")
    @commands.has_permissions(administrator=True)
    async def set_balance(self, ctx, user: discord.User, amount: int):
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
    async def all_transactions(self, ctx):
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

@bot.event
async def on_ready():
    print(f"Bot zalogowany jako {bot.user}")

bot.run(DISCORD_TOKEN)
