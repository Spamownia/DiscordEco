import ftplib
import re
import hashlib
import time

# ---------------- FTP CONFIG ----------------
FTP_HOST = "195.179.226.218"
FTP_PORT = 56421
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_LOG_PATH = "/SCUM/Saved/SaveFiles/Logs/"
LOGIN_REWARD = 10  # monet za logowanie

# ---------------- HELPERS ----------------
def add_coins(nick, amount):
    conn = get_connection()
    if not conn:
        return
    cursor = conn.cursor()

    # Sprawdzenie przypisania nick -> discord_id
    cursor.execute("SELECT discord_id FROM nick_mapping WHERE nick=%s", (nick,))
    row = cursor.fetchone()
    if not row:
        # Jeśli nie ma przypisanego Discorda, tylko dodajemy monety "do nicka"
        cursor.execute("INSERT INTO users(discord_id, balance) VALUES(%s, %s) ON DUPLICATE KEY UPDATE balance = balance + %s", (0, amount, amount))
    else:
        discord_id = row[0]
        cursor.execute("INSERT INTO users(discord_id, balance) VALUES(%s, %s) ON DUPLICATE KEY UPDATE balance = balance + %s", (discord_id, amount, amount))

    cursor.execute("""
        INSERT INTO transactions(user_id, action, item, amount)
        VALUES(%s, %s, %s, %s)
    """, (0, "LOGIN_REWARD", nick, amount))  # user_id = 0 dla nicków bez przypisanego Discorda
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

# ---------------- PROCESS LOGS ----------------
def process_login_logs():
    try:
        ftp = ftplib.FTP()
        ftp.connect(FTP_HOST, FTP_PORT)
        ftp.login(FTP_USER, FTP_PASS)
        logs = []
        ftp.retrlines(f'LIST {FTP_LOG_PATH}', logs.append)
        login_logs = [f.split()[-1] for f in logs if "login_" in f]

        for log_file in login_logs:
            lines = []
            try:
                ftp.retrlines(f'RETR {FTP_LOG_PATH}{log_file}', lines.append)
            except ftplib.error_perm:
                continue
            for line in lines:
                if "logged in at:" in line and not is_line_processed(line):
                    m = re.search(r"'[^\s]+ \d+:(.*?)\(\d+\)' logged in at:", line)
                    if m:
                        nick = m.group(1)
                        add_coins(nick, LOGIN_REWARD)
                        mark_line_processed(line)
        ftp.quit()
    except Exception as e:
        print("Błąd FTP/parsowania:", e)

# ---------------- BACKGROUND LOOP ----------------
def login_monitor_loop():
    while True:
        process_login_logs()
        time.sleep(60)  # sprawdzanie co 60 sekund

# ---------------- START W TLE ----------------
import threading
threading.Thread(target=login_monitor_loop, daemon=True).start()
