import sys
import logging
import random
import string
import json
import mysql.connector
import asyncio
import traceback
import os
from db_config import db_config
from datetime import datetime, timedelta, timezone  # Add timezone
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update, InputMediaPhoto, InputMediaVideo, ReplyParameters
from telegram.ext import Application, CommandHandler, CallbackContext, CallbackQueryHandler, MessageHandler
from telegram.ext import filters, ContextTypes
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telegram.error import BadRequest, TelegramError
import telegram.error
# Добавляем путь к директории бота
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from muhurta_test import register_handlers as register_muhurta_handlers
from muhurta_test import start_muhurta_test
from biorhythm_test import register_handlers as register_biorhythm_handlers
from utils import get_referrals
from sovmes import register_handlers as register_compatibility_handlers
from sovmes import start_compatibility_test
from lk import start_moon_calendar, register_handlers as register_moon_handlers

OFFSET_FILE = "/var/www/admin78/data/www/vdohnovenie.pro/bot/last_update_id.txt"

LOG_FILE = "/var/www/admin78/data/www/vdohnovenie.pro/bot/logs/bot.log"

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename=LOG_FILE,
    filemode="w",
    encoding='utf-8'
)

# Список ID администраторов
ADMIN_IDS = [49001683, 1346624809]  # Замените на реальные ID администраторов

# 1. Добавляем константы в начало файла после ADMIN_IDS
ALLOWED_GROUP_ID = -1002126524645  # ID основного канала
ALLOWED_DISCUSSION_ID = -1002140411209  # ID чата обсуждений
ALLOWED_GROUP_USERNAME = "vdohnovenie_pro"

TEST_CHANNEL_ID = -1002414600895  # ID тестового канала
TEST_DISCUSSION_ID = -1002432554211  # ID тестового чата обсуждений

def save_last_update_id(update_id):
    """Сохраняет последний update_id в файл"""
    try:
        with open(OFFSET_FILE, 'w') as f:
            f.write(str(update_id))
    except Exception as e:
        logging.error(f"Error saving last update ID: {e}")

def get_last_update_id():
    """Получает последний update_id из файла"""
    try:
        if os.path.exists(OFFSET_FILE):
            with open(OFFSET_FILE, 'r') as f:
                return int(f.read().strip())
    except Exception as e:
        logging.error(f"Error reading last update ID: {e}")
    return 0

# Добавляем в словарь MENU_CALLBACKS новый пункт
MENU_CALLBACKS = {
    # ...existing code...
    'muhurta': start_muhurta_test,
}

def get_level_requirements():
    """Возвращает требования к сообщениям для каждого уровня"""
    return {
        0: 0,     # 0-й уровень: старт
        1: 5,     # 1-й уровень: 5 сообщений
        2: 15,    # 2-й уровень: 15 сообщений
        3: 35,    # 3-й уровень: 35 сообщений 
        4: 65,    # 4-й уровень: 65 сообщений
        5: 105,   # 5-й уровень: 105 сообщений
        6: 155,   # 6-й уровень: 155 сообщений
        7: 215,   # 7-й уровень: 215 сообщений
        8: 285,   # 8-й уровень: 285 сообщений
        9: 355,   # 9-й уровень: 355 сообщений
        10: 445,  # 10-й уровень: 445 сообщений
        11: 545   # 11-й уровень: 545 сообщений (максимальный)
    }

def calculate_level_reward(level):
    """Возвращает награду за достижение уровня"""
    rewards = {
        0: 0,      # начальный уровень без награды
        1: 50,     # награда за 1-й уровень (5 сообщений)
        2: 100,    # награда за 2-й уровень (15 сообщений)
        3: 200,    # награда за 3-й уровень (35 сообщений)
        4: 300,    # награда за 4-й уровень (65 сообщений)
        5: 400,    # награда за 5-й уровень (105 сообщений)
        6: 500,    # награда за 6-й уровень (155 сообщений)
        7: 600,    # награда за 7-й уровень (215 сообщений)
        8: 700,    # награда за 8-й уровень (285 сообщений)
        9: 800,    # награда за 9-й уровень (355 сообщений)
        10: 900,   # награда за 10-й уровень (445 сообщений)
        11: 1000   # награда за 11-й уровень (545 сообщений)
    }
    return rewards.get(level, 0)

def format_level_info(level, progress, extra_cycles=0):
    """Форматирует информацию об уровне пользователя"""
    if level == 11:  # Максимальный уровень
        if extra_cycles > 0:
            return f"🏅 Уровень: {level} (Максимальный + {extra_cycles} наград за активность)\n"
        return f"🏅 Уровень: {level} (Максимальный)\n"
    else:
        next_level = level + 1
        return f"🏅 Уровень: {level}\n📊 Прогресс: {progress}%✅ до {next_level} уровня\n"

def calculate_initial_balance(comments):
    """Рассчитывает начальный баланс на основе комментариев"""
    try:
        initial_balance = 0
        current_level = 0
        
        # Определяем достигнутый уровень
        requirements = get_level_requirements()
        for level in range(11, -1, -1):
            if comments >= requirements[level]:
                current_level = level
                break
                
        # Суммируем награды за все достигнутые уровни
        for level in range(1, current_level + 1):
            initial_balance += calculate_level_reward(level)
            
        logging.info(f"calculate_initial_balance: comments={comments}, level={current_level}, balance={initial_balance}")
        return initial_balance, current_level
        
    except Exception as e:
        logging.error(f"Error in calculate_initial_balance: {e}")
        return 0, 0

def save_user_data(user_id, data):
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO user_data (user_id, data)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE data = %s
        """, (user_id, json.dumps(data), json.dumps(data)))
        conn.commit()
    except Exception as e:
        logging.error(f"Error saving user data: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def load_user_data(user_id):
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("SELECT data FROM user_data WHERE user_id = %s", (user_id,))
        row = cursor.fetchone()
        if row:
            return json.loads(row['data'])
        return None
    except Exception as e:
        logging.error(f"Error loading user data: {e}")
        return None
    finally:
        cursor.close()
        conn.close()

def get_referrals(user_id):
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        query = "SELECT username, first_name, last_name, join_date FROM users WHERE referred_by = %s"
        cursor.execute(query, (user_id,))
        referrals = cursor.fetchall()
        conn.close()
        return referrals
    except mysql.connector.Error as e:
        logging.error(f"Error getting referrals for user {user_id}: {e}")
        return []

def is_user_exists(user_id):
    """Проверяет, существует ли пользователь в базе данных."""
    conn = get_db_connection()
    if conn is None:
        return False
    cursor = conn.cursor()
    cursor.execute("SELECT user_id FROM users WHERE user_id = %s", (user_id,))
    result = cursor.fetchone()
    conn.close()
    return result is not None

# Функции для работы с базой данных
def get_db_connection():
    try:
        conn = mysql.connector.connect(**db_config, connection_timeout=10)
        conn.ping(reconnect=True)
        return conn
    except Exception as e:
        logging.error(f"DB connection error: {e}")
        return None

def get_user(user_id): # (старое название - load_user_data)
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM users WHERE user_id = %s", (user_id,)) # SQL запрос с WHERE
        user = cursor.fetchone() # Получаем ОДНОГО пользователя
        conn.close()
        return user
    except mysql.connector.Error as e:
        logging.error(f"Error getting user {user_id}: {e}")
        return None

def get_invited_users_from_db(user_id, offset=0, limit=5):
    logging.info(f"get_invited_users_from_db: user_id={user_id}")
    invited_users = []
    total = 0
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        # Получаем общее количество
        cursor.execute("SELECT COUNT(*) as total FROM users WHERE referred_by = %s", (user_id,))
        result = cursor.fetchone()
        total = result['total'] if result else 0
        
        # Получаем данные пользователей
        query = """
            SELECT user_id, username, first_name, last_name, join_date 
            FROM users 
            WHERE referred_by = %s 
            ORDER BY join_date DESC 
            LIMIT %s OFFSET %s
        """
        cursor.execute(query, (user_id, limit, offset))
        invited_users = cursor.fetchall() or []
        
    except mysql.connector.Error as e:
        logging.error(f"get_invited_users_from_db: Database error: {e}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()
            
    return invited_users, total

def get_user_coupons(user_id): # Новая функция
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT * FROM coupons 
            WHERE user_id = %s 
            ORDER BY date DESC
        """, (user_id,))  # Добавлен ORDER BY date DESC
        coupons = cursor.fetchall()
        conn.close()
        return coupons
    except mysql.connector.Error as e:
        logging.error(f"Error getting coupons for user {user_id}: {e}")
        return []

def get_all_coupons(offset=0, limit=10):
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        # Получаем общее количество
        cursor.execute("SELECT COUNT(*) as total FROM coupons")
        total = cursor.fetchone()['total']
        
        # Получаем страницу данных
        cursor.execute("""
            SELECT c.*, u.username 
            FROM coupons c 
            LEFT JOIN users u ON c.user_id = u.user_id 
            ORDER BY c.date DESC 
            LIMIT %s OFFSET %s
        """, (limit, offset))
        coupons = cursor.fetchall()
        conn.close()
        return coupons, total
    except mysql.connector.Error as e:
        logging.error(f"Error getting all coupons from db: {e}")
        return [], 0

def save_user(user):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        query = """
            INSERT INTO users (user_id, username, first_name, last_name, referrals, referral_link, balance, join_date, referred_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            username = VALUES(username), first_name = VALUES(first_name), last_name = VALUES(last_name), referrals = VALUES(referrals),
            referral_link = VALUES(referral_link), balance = VALUES(balance), join_date = VALUES(join_date), referred_by = VALUES(referred_by)
        """
        data = (user.get('user_id'), user.get('username'), user.get('first_name'), user.get('last_name'), user.get('referrals'), user.get('referral_link'), user.get('balance'), user.get('join_date'), user.get('referred_by'))
        logging.info(f"save_user: Выполнение запроса: {query} с данными: {data}")
        cursor.execute(query, data)
        conn.commit()
        return True
    except mysql.connector.Error as e:
        conn.rollback()
        logging.error(f"save_user: Ошибка записи в БД: {e}")
        return False
    finally:
        if conn:
            conn.close()
        
# Новые функции для работы с транзакциями
def add_transaction(user_id: int, amount: int, reason: str):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO transactions (user_id, amount, reason, date)
            VALUES (%s, %s, %s, %s)
        ''', (user_id, amount, reason, datetime.now()))
        conn.commit()
        conn.close()
        return True
    except mysql.connector.Error as e:
        logging.error(f"Error adding transaction: {e}")
        return False

def get_user_transactions(user_id: int, offset: int = 0, limit: int = 5):
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute('''
            SELECT * FROM transactions 
            WHERE user_id = %s 
            ORDER BY date DESC
            LIMIT %s OFFSET %s
        ''', (user_id, limit, offset))
        transactions = cursor.fetchall()
        
        # Получаем общее количество транзакций
        cursor.execute('SELECT COUNT(*) as total FROM transactions WHERE user_id = %s', (user_id,))
        total = cursor.fetchone()['total']
        
        conn.close()
        return transactions, total
    except mysql.connector.Error as e:
        logging.error(f"Error getting transactions: {e}")
        return [], 0

# Константы для статусов (для удобства)
STATUS_PENDING = "🔴На согласовании"
STATUS_APPROVED = "🟢Согласовано"
STATUS_UNKNOWN = "Неизвестно" # на всякий случай

# Функция для очистки устаревших купонов
def cleanup_coupons(context: CallbackContext):
    global coupons_data
    if not coupons_data: # Проверяем, пуст ли список
        logging.info("Список купонов пуст, очистка не требуется.")
        return # Выходим из функции, если список пуст

    now = datetime.now()
    coupons_data = [
        coupon for coupon in coupons_data if coupon['date'] + timedelta(days=30) > now
    ]
    save_coupons(coupons_data)
    logging.info("Очистка устаревших купонов выполнена.")

# Функция для загрузки купонов (с преобразованием даты)
def load_coupons():
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute('SELECT * FROM coupons')
        data = cursor.fetchall()
        conn.close()
        logging.debug(f"Loaded coupons data: {data}")
        return data
    except Exception as e:
        logging.error(f"Error loading coupons data: {e}")
        return []

def save_coupons(coupons):
    conn = get_db_connection()
    cursor = conn.cursor()
    for coupon in coupons:
        cursor.execute('''
            INSERT INTO coupons (coupon_id, amount, user_id, status, date)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                amount=VALUES(amount),
                user_id=VALUES(user_id),
                status=VALUES(status),
                date=VALUES(date)
        ''', (coupon['coupon_id'], coupon['amount'], coupon['user_id'], coupon['status'], coupon['date']))
    conn.commit()
    conn.close()

# Функция для генерации купона
def generate_coupon():
    return "VP" + "".join(random.choices(string.digits + string.ascii_uppercase, k=4))

# Функция для создания клавиатуры с кнопкой "Мой кабинет"
def create_profile_keyboard():
    keyboard = [[InlineKeyboardButton("📱Мой кабинет", callback_data="my_profile")]]
    return InlineKeyboardMarkup(keyboard)

# Функция для преобразования английского статуса в русский
def get_coupon_status_ru(status_en):
    return {
        "pending": STATUS_PENDING,
        "approved": STATUS_APPROVED,
    }.get(status_en, STATUS_UNKNOWN)

def get_user_activity(user_id):
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT comments FROM user_activity WHERE user_id = %s", (user_id,))
        activity = cursor.fetchone()
        conn.close()
        return activity if activity else {"comments": 0}
    except mysql.connector.Error as e:
        logging.error(f"Error getting user activity for user {user_id}: {e}")
        return {"comments": 0}

def update_user_activity(user_id, comments=0, likes=0):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO user_activity (user_id, comments, likes)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
                comments = comments + VALUES(comments),
                likes = likes + VALUES(likes)
        ''', (user_id, comments, likes))
        conn.commit()
        conn.close()
    except mysql.connector.Error as e:
        logging.error(f"Error updating user activity for user {user_id}: {e}")

def calculate_user_level(comments):
    """Рассчитывает уровень пользователя и прогресс до следующего уровня"""
    logging.info(f"calculate_user_level вызвана с comments={comments}")
    
    requirements = get_level_requirements()
    
    # Определяем текущий уровень
    current_level = 0
    for level in range(11, -1, -1):  # от 11 до 0
        if comments >= requirements[level]:
            current_level = level
            break
    
    # Проверяем максимальный уровень
    if current_level == 11:
        extra_cycles = (comments - requirements[11]) // requirements[11]
        logging.info(f"Возвращаем максимальный уровень=11, extra_cycles={extra_cycles}")
        return (11, 100, extra_cycles)
        
    # Расчет прогресса до следующего уровня
    if current_level < 11:
        comments_for_current = requirements[current_level]
        comments_for_next = requirements[current_level + 1]
        comments_after_current = comments - comments_for_current
        comments_needed_for_next = comments_for_next - comments_for_current
        progress = (comments_after_current * 100) // comments_needed_for_next
    else:
        progress = 100

    logging.info(f"Возвращаем уровень={current_level}, progress={progress}")
    return (current_level, progress, 0)

def get_leaderboard(offset=0, limit=10):
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        # Получаем общее количество
        cursor.execute("SELECT COUNT(*) as total FROM user_activity")
        total = cursor.fetchone()['total']
        
        # Получаем страницу данных (только комментарии)
        cursor.execute("""
            SELECT user_id, comments
            FROM user_activity 
            ORDER BY comments DESC 
            LIMIT %s OFFSET %s
        """, (limit, offset))
        leaderboard = cursor.fetchall()
        conn.close()
        return leaderboard, total
    except mysql.connector.Error as e:
        logging.error(f"Error getting leaderboard: {e}")
        return [], 0


# Добавление функции для получения общего количества пользователей
def get_total_users():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM users")
        total_users = cursor.fetchone()[0]
        conn.close()
        return total_users
    except mysql.connector.Error as e:
        logging.error(f"Error getting total users: {e}")
        return 0

def get_total_coupons():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM coupons")
        total_coupons = cursor.fetchone()[0]
        conn.close()
        return total_coupons
    except mysql.connector.Error as e:
        logging.error(f"Error getting total coupons: {e}")
        return 0

def get_total_referrals():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT SUM(referrals) FROM users")
        total_referrals = cursor.fetchone()[0]
        conn.close()
        return total_referrals
    except mysql.connector.Error as e:
        logging.error(f"Error getting total referrals: {e}")
        return 0

def get_approved_coupons():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM coupons WHERE status = 'approved'")
        approved_coupons = cursor.fetchone()[0]
        conn.close()
        return approved_coupons
    except mysql.connector.Error as e:
        logging.error(f"Error getting approved coupons: {e}")
        return 0

def get_pending_coupons():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM coupons WHERE status = 'pending'")
        pending_coupons = cursor.fetchone()[0]
        conn.close()
        return pending_coupons
    except mysql.connector.Error as e:
        logging.error(f"Error getting pending coupons: {e}")
        return 0

def get_pending_coupons_list(offset=0, limit=10):
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        # Get total count
        cursor.execute("SELECT COUNT(*) as total FROM coupons WHERE status = 'pending'")
        total = cursor.fetchone()['total']
        
        # Get page data
        cursor.execute("""
            SELECT c.*, u.username 
            FROM coupons c 
            LEFT JOIN users u ON c.user_id = u.user_id 
            WHERE c.status = 'pending'
            ORDER BY c.date DESC 
            LIMIT %s OFFSET %s
        """, (limit, offset))
        pending_coupons = cursor.fetchall()
        conn.close()
        return pending_coupons, total
    except mysql.connector.Error as e:
        logging.error(f"Error getting pending coupons: {e}")
        return [], 0

def get_active_coupons():
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        now = datetime.now()
        cursor.execute("SELECT COUNT(*) AS count FROM coupons WHERE date + INTERVAL 30 DAY > %s", (now,))
        result = cursor.fetchone()
        conn.close()
        if result:
            return result['count']
        else:
            return 0
    except mysql.connector.Error as e:
        logging.error(f"Error getting active coupons: {e}")
        return 0

def get_users_list(offset=0, limit=10, month_only=False):
    logging.info(f"get_users_list вызвана (offset={offset}, limit={limit}, month_only={month_only})")
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        # Получаем общее количество
        if month_only:
            one_month_ago = datetime.now() - timedelta(days=30)
            cursor.execute("SELECT COUNT(*) as total FROM users WHERE join_date >= %s", (one_month_ago,))
        else:
            cursor.execute("SELECT COUNT(*) as total FROM users")
        total = cursor.fetchone()['total']
        
        # Получаем страницу данных
        query = "SELECT user_id, username, first_name, last_name, join_date, balance FROM users"  # Добавлены first_name и last_name
        if month_only:
            query += " WHERE join_date >= %s"
            query += " ORDER BY join_date DESC LIMIT %s OFFSET %s"
            cursor.execute(query, (one_month_ago, limit, offset))
        else:
            query += " ORDER BY join_date DESC LIMIT %s OFFSET %s"
            cursor.execute(query, (limit, offset))
            
        users = cursor.fetchall()
        conn.close()
        return users, total
    except mysql.connector.Error as e:
        logging.error(f"get_users_list: Ошибка БД: {e}")
        return [], 0

def get_user_by_username(username):
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM users WHERE username = %s", (username,))
        user = cursor.fetchone()
        conn.close()
        return user
    except mysql.connector.Error as e:
        logging.error(f"Error getting user by username {username}: {e}")
        return None

def freeze_user_account(user_id: int) -> bool:
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("UPDATE users SET is_frozen = TRUE WHERE user_id = %s", (user_id,))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        logging.error(f"Error freezing user account {user_id}: {e}")
        return False

def unfreeze_user_account(user_id: int) -> bool:
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("UPDATE users SET is_frozen = FALSE WHERE user_id = %s", (user_id,))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        logging.error(f"Error unfreezing user account {user_id}: {e}")
        return False

def is_account_frozen(user_id: int) -> bool:
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT is_frozen FROM users WHERE user_id = %s", (user_id,))
        result = cursor.fetchone()
        conn.close()
        return result.get('is_frozen', False) if result else False
    except Exception as e:
        logging.error(f"Error checking account freeze status {user_id}: {e}")
        return False

def create_announcement(message: str) -> bool:
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("INSERT INTO announcements (message) VALUES (%s)", (message,))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        logging.error(f"Error creating announcement: {e}")
        return False

def get_active_announcements() -> list:
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM announcements WHERE is_active = TRUE ORDER BY created_at DESC")
        announcements = cursor.fetchall()
        conn.close()
        return announcements
    except Exception as e:
        logging.error(f"Error getting announcements: {e}")
        return []

def get_frozen_accounts_count():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM users WHERE is_frozen = TRUE")
        count = cursor.fetchone()[0]
        conn.close()
        return count
    except Exception as e:
        logging.error(f"Error getting frozen accounts count: {e}")
        return 0

def get_frozen_accounts(offset=0, limit=10):
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        # Get total count
        cursor.execute("SELECT COUNT(*) as total FROM users WHERE is_frozen = TRUE")
        total = cursor.fetchone()['total']
        
        # Get page data
        cursor.execute("""
            SELECT user_id, username, first_name, last_name, join_date, balance 
            FROM users 
            WHERE is_frozen = TRUE 
            ORDER BY join_date DESC 
            LIMIT %s OFFSET %s
        """, (limit, offset))
        frozen_users = cursor.fetchall()
        conn.close()
        return frozen_users, total
    except Exception as e:
        logging.error(f"Error getting frozen accounts: {e}")
        return [], 0

# Функция для начала работы с ботом
async def start(update: Update, context: CallbackContext):
    try:
        user_id = update.effective_user.id
        
        # Проверяем заморозку аккаунта
        if is_account_frozen(user_id):
            await update.message.reply_text(
                "❄️ Ваш аккаунт заморожен. "
                "Вы не можете использовать функции бота. "
                "Если у вас есть вопросы, напишите в чат - @vdohnovenie_pro_chat"
            )
            return
        user = get_user(user_id)

        if user:
            return await my_profile(update, context)
            
        conn = None
        cursor = None
        try:
            conn = get_db_connection()
            cursor = conn.cursor(dictionary=True)
            
            user_tg = update.effective_user
            username = user_tg.username
            first_name = user_tg.first_name
            last_name = user_tg.last_name
            join_date = datetime.now()
            referral_link = f"https://t.me/{context.bot.username}?start={user_id}"

            # Обработка реферального кода
            referred_by = None
            if context.args:  # Если есть аргументы в команде start
                try:
                    referred_by = int(context.args[0])
                    if referred_by == user_id:
                        referred_by = None
                    else:
                        # Отправляем уведомление новому пользователю
                        await context.bot.send_message(
                            chat_id=user_id,
                            text="🥳 Вы перешли по реферальной ссылке и ваш баланс пополнен на 2000 ₽\n"
                                 "Нажмите ниже кнопку \"Узнать о ✨ВселеннаяПомощи✨\" и далее перейдите в \"Мой кабинет\", "
                                 "чтобы увидеть ваш баланс"
                        )
                        logging.info(f"start: Отправлено уведомление новому пользователю {user_id} о реферальном бонусе")
                except ValueError:
                    logging.error(f"start: Некорректный реферальный код")
                    referred_by = None

            # Получаем данные из temp_activity
            cursor.execute("SELECT * FROM temp_activity WHERE user_id = %s", (user_id,))
            temp_activity = cursor.fetchone()
            
            # Определяем начальный баланс
            initial_balance = 0
            level = 0
            if temp_activity:
                comments = temp_activity['comments']
                initial_balance, level = calculate_initial_balance(comments)
                logging.info(f"Found temp_activity: comments={comments}, calculated balance={initial_balance}, level={level}")
            
            # Создаем нового пользователя
            new_user = {
                'user_id': user_id,
                'username': username,
                'first_name': first_name,
                'last_name': last_name,
                'join_date': join_date,
                'balance': initial_balance,
                'referrals': 0,
                'referral_link': referral_link,
                'referred_by': referred_by
            }

            # Сохраняем пользователя
            save_user(new_user)

            # Обработка реферера
            if referred_by:
                referrer = get_user(referred_by)
                if referrer:
                    referrer["referrals"] = (referrer.get("referrals", 0) or 0) + 1
                    referrer["balance"] = (referrer.get("balance", 0) or 0) + 2000
                    save_user(referrer)
                    add_transaction(referred_by, 2000, "Приглашение нового пользователя")
                    
                    # Отправляем сообщение рефереру
                    keyboard = [[InlineKeyboardButton("📱Мой кабинет", callback_data="my_profile")]]
                    reply_markup = InlineKeyboardMarkup(keyboard)
                    await context.bot.send_message(
                        chat_id=referred_by,
                        text="🥳 По вашей ссылке зарегистрировался новый пользователь!\n"
                            "Ваш баланс пополнен на 2000 ₽.",
                        reply_markup=reply_markup,
                        parse_mode="HTML"
                    )

                    # Начисляем бонус новому пользователю
                    new_user["balance"] = initial_balance + 2000
                    save_user(new_user)
                    add_transaction(user_id, 2000, "Бонус за регистрацию по реферальной ссылке")

            # Переносим временную активность
            if temp_activity:
                cursor.execute('''
                    INSERT INTO user_activity (user_id, comments, last_activity) 
                    VALUES (%s, %s, %s)
                ''', (user_id, temp_activity['comments'], temp_activity['last_activity']))
                
                # Добавляем транзакции за уровни
                for l in range(1, level + 1):
                    reward = calculate_level_reward(l)
                    add_transaction(user_id, reward, f"Награда за достижение {l} уровня")
                
                # Удаляем временную активность
                cursor.execute("DELETE FROM temp_activity WHERE user_id = %s", (user_id,))
                conn.commit()

            # Приветственное сообщение и кнопка
            message = (
                "😊 Добро пожаловать!\n\n"
                "Это официальный бот сайта vdohnovenie.pro и группы Telegram @vdohnovenie_pro\n\n"
                "Мы запустили уникальную программу '<b>ВселеннаяПомощи</b>', где вы можете:\n"
                "💫 бесплатно проходить различные тесты;\n"
                "🌟 делать расчёты по направлениям: ведические науки, астрология, биоритмы, нумерология, гороскопы, лунный календарь;\n"
                "💰 зарабатывать деньги за активность и приглашение своих друзей по ссылке из Личного кабинета, а затем тратить их на любые услуги с нашего сайта.\n\n"
                "Подробнее о программе можно узнать нажав кнопку ниже.👇"
            )
            
            keyboard = [
                [InlineKeyboardButton("👉Узнать о ✨ВселеннаяПомощи✨", callback_data=f"join_referral_program_{user_id}")],
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            photo_path = "/var/www/admin78/data/www/vdohnovenie.pro/bot/photo/Unt1.jpg"

            if update.callback_query:
                await update.callback_query.message.delete()
                await context.bot.send_photo(
                    chat_id=update.callback_query.message.chat_id,
                    photo=open(photo_path, 'rb'),
                    caption=message,
                    reply_markup=reply_markup,
                    parse_mode="HTML"
                )
            else:
                await context.bot.send_photo(
                    chat_id=update.message.chat_id,
                    photo=open(photo_path, 'rb'),
                    caption=message,
                    reply_markup=reply_markup,
                    parse_mode="HTML"
                )

        except mysql.connector.Error as db_err:
            if conn:
                conn.rollback()
            logging.error(f"start: Ошибка БД при создании пользователя: {db_err}")
            await update.message.reply_text("Произошла ошибка при регистрации (ошибка БД). Пожалуйста, попробуйте позже.")
            return
        except Exception as e:
            if conn:
                conn.rollback()
            logging.exception(f"start: Непредвиденная ошибка при создании пользователя: {e}")
            await update.message.reply_text("Произошла ошибка при регистрации (непредвиденная ошибка). Пожалуйста, попробуйте позже.")
            return
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    except Exception as e:
        logging.exception(f"start: Outer error: {e}")
        await update.message.reply_text("Произошла ошибка при регистрации. Пожалуйста, попробуйте позже.")

async def process_registered_user_message(update, context, user):
    """Обрабатывает сообщения от зарегистрированных пользователей"""
    try:
        user_id = user['user_id']
        
        # Получаем текущую активность и уровень
        old_activity = get_user_activity(user_id)
        old_level, _, _ = calculate_user_level(old_activity['comments'])
        logging.info(f"process_registered_user_message: old_level={old_level}, old_comments={old_activity['comments']}")
        
        # Обновляем активность
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('''
                INSERT INTO user_activity (user_id, comments, last_activity)
                VALUES (%s, 1, %s)
                ON DUPLICATE KEY UPDATE
                    comments = comments + 1,
                    last_activity = VALUES(last_activity)
            ''', (user_id, datetime.now()))
            conn.commit()
            logging.info(f"Обновлена активность пользователя {user_id}")
        finally:
            cursor.close()
            conn.close()

        # Получаем обновленную активность
        new_activity = get_user_activity(user_id)
        new_level, _, _ = calculate_user_level(new_activity['comments'])
        logging.info(f"process_registered_user_message: new_level={new_level}, new_comments={new_activity['comments']}")
        
        # Если уровень повысился
        if new_level > old_level:
            reward = calculate_level_reward(new_level)
            logging.info(f"Повышение уровня! Начисляем награду: {reward}")
            
            # Обновляем баланс пользователя
            if 'balance' in user:
                user['balance'] = user.get('balance', 0) + reward
                save_user(user)
                add_transaction(user_id, reward, f"Награда за достижение {new_level} уровня")
                
                # Отправляем уведомление
                await notify_level_up(context, update, user_id, new_level, reward)
                logging.info(f"Отправлено уведомление о повышении уровня для пользователя {user_id}")
            else:
                logging.error(f"Ключ 'balance' отсутствует в данных пользователя {user_id}")
            
    except Exception as e:
        logging.error(f"Ошибка в process_registered_user_message: {e}")
        logging.error(traceback.format_exc())

async def check_subscription(context: CallbackContext, user_id: int) -> bool:
    """Проверяет подписку пользователя на канал"""
    try:
        member = await context.bot.get_chat_member(chat_id=ALLOWED_GROUP_ID, user_id=user_id)
        return member.status in ['member', 'administrator', 'creator']
    except Exception as e:
        logging.error(f"Ошибка при проверке подписки: {e}")
        return False

async def join_referral_program(update: Update, context: CallbackContext):
    logging.info("join_referral_program вызвана")
    try:
        user_id = update.callback_query.from_user.id
        await update.callback_query.answer()

        message = (
            "О программе ✨'ВселеннаяПомощи'✨\n\n"
            "🌟 Наша программа - это уникальная система вознаграждений за активность и развитие сообщества.\n\n"
            "💰 Реферальная программа:\n"
            "🔹 2000₽ получаете вы за каждого приглашенного друга по вашей ссылке из личного кабинета\n"
            "🔹 2000₽ получает ваш друг при регистрации по вашей ссылке\n\n"
            "💰Бонусы за активность(❤️лайки и 💬комментарии) в нашей группе @vdohnovenie_pro. Бот будет повышать ваш уровень за активность, и за каждый уровень вы будете получать деньги на баланс.\n"
            "📈 Система уровней:\n"
            "🔹 50 ₽ на баланс - 1 уровень\n"
            "🔹 100 ₽ - 2 уровень\n"
            "🔹 1000 ₽ - 11 уровень\n\n"
            "🎁 Дополнительные возможности:\n"
            "🔹 Бесплатные расчеты и тесты.\n"
            "🔹 Купоны для оплаты услуг.\n"
            "Ваш баланс будет отображаться в разделе «Мой кабинет». Вы можете списать любую сумму со своего счёта для получения купона, который можно использовать для оплаты любой услуги на нашем сайте.\n\n"
            "❗Чтобы присоединиться к программе, вы должны подписаться на нашу группу — @vdohnovenie_pro\n\n"          
        )

        photo_path = "/var/www/admin78/data/www/vdohnovenie.pro/bot/photo/Unt2.jpg"
        
        referred_by_str = update.callback_query.data.split("_")[-1]
        if referred_by_str == "none":
            referred_by = None
        else:
            referred_by = int(referred_by_str)

        if referred_by is not None:
            keyboard = [
                [InlineKeyboardButton("✅ Присоединиться", callback_data=f"confirm_join_referral_program_{referred_by}")],
                [InlineKeyboardButton("↩️Назад", callback_data="event_return")]  # Изменено с "start" на "event_return"
            ]
        else:
            keyboard = [
                [InlineKeyboardButton("✅ Зарегистрироваться без реферала", callback_data="confirm_join_referral_program_none")],
                [InlineKeyboardButton("↩️Назад", callback_data="event_return")]  # Изменено с "start" на "event_return"
            ]

        reply_markup = InlineKeyboardMarkup(keyboard)

        # Создаем InputMediaPhoto объект
        from telegram import InputMediaPhoto
        media = InputMediaPhoto(
            media=open(photo_path, 'rb'),
            caption=message,
            parse_mode="HTML"
        )

        # Редактируем сообщение с медиа
        await update.callback_query.message.edit_media(
            media=media,
            reply_markup=reply_markup
        )

    except Exception as e:
        logging.exception(f"join_referral_program: Ошибка: {e}")
        await update.callback_query.message.reply_text("Произошла ошибка. Попробуйте позже.")

# Функция для присоединения к реферальной программе
async def confirm_join_referral_program(update: Update, context: CallbackContext):
    """Функция для подтверждения присоединения к реферальной программе"""
    logging.info("confirm_join_referral_program: Начало")
    user_id = update.callback_query.from_user.id
    logging.info(f"confirm_join_referral_program: user_id = {user_id}")
    referred_by = None

    try:
        await update.callback_query.answer()

        # Проверяем подписку на канал
        is_subscribed = await check_subscription(context, user_id)
        if not is_subscribed:
            message = (
                "❗ Для участия в программе необходимо быть подписанным "
                "на нашу группу @vdohnovenie_pro\n\n"
                "1️⃣ Перейдите по ссылке @vdohnovenie_pro\n"
                "2️⃣ Подпишитесь на группу\n"
                "3️⃣ Вернитесь в бот и нажмите кнопку \"Присоединиться\" еще раз"
            )
            keyboard = [
                [InlineKeyboardButton("📢 Перейти в группу", url="https://t.me/vdohnovenie_pro")],
                [InlineKeyboardButton("🔄 Проверить подписку", callback_data=update.callback_query.data)],
                [InlineKeyboardButton("↩️ Назад", callback_data="event_return")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            photo_path = "/var/www/admin78/data/www/vdohnovenie.pro/bot/photo/Unt2.jpg"
            media = InputMediaPhoto(media=open(photo_path, 'rb'), caption=message, parse_mode="HTML")
            await update.callback_query.message.edit_media(media=media, reply_markup=reply_markup)
            return

        referred_by_str = update.callback_query.data.split("_")[-1]
        try:
            referred_by = int(referred_by_str) if referred_by_str.isdigit() and referred_by_str != 'none' else None
            if referred_by == user_id:
                referred_by = None
        except ValueError:
            logging.error(f"Некорректный referred_by_str: {referred_by_str}")
            referred_by = None

        user = get_user(user_id)
        if user:  # Существующий пользователь
            if user.get("referred_by") is None:
                user["referred_by"] = referred_by
                save_user(user)
        elif referred_by is not None:  # Новый пользователь с реферером
            username = update.callback_query.from_user.username or "Аноним"
            first_name = update.callback_query.from_user.first_name
            last_name = update.callback_query.from_user.last_name
            balance = 2000  # Бонус новому пользователю
            
            # Обработка реферера
            referrer = get_user(referred_by)
            if referrer:
                referrer["referrals"] = (referrer.get("referrals", 0) or 0) + 1
                referrer["balance"] = (referrer.get("balance", 0) or 0) + 2000
                if not save_user(referrer):
                    logging.error("Ошибка сохранения реферера")
                    await context.bot.send_message(
                        chat_id="-1002382309656",
                        text=f"Ошибка при обновлении реферера {referred_by}"
                    )
                    return
                add_transaction(referred_by, 2000, "Бонус за приглашение нового пользователя")

            # Создание нового пользователя
            new_user = {
                "user_id": user_id,
                "username": username,
                "first_name": first_name,
                "last_name": last_name,
                "referrals": 0,
                "referral_link": f"https://t.me/vdohnoveniepro_bot?start={user_id}",
                "balance": balance,
                "referred_by": referred_by,
                "join_date": datetime.now().isoformat(),
            }
            if not save_user(new_user):
                await context.bot.send_message(
                    chat_id="-1002382309656",
                    text=f"Ошибка при создании пользователя {user_id}"
                )
                return
            add_transaction(user_id, 2000, "Бонус за регистрацию по реферальной ссылке")

            # Добавляем уведомление новому пользователю
            await context.bot.send_message(
                chat_id=user_id,
                text="🥳 Вы перешли по реферальной ссылке и ваш баланс пополнен на 2000 ₽\n"
                     "Нажмите ниже кнопку \"Узнать о ✨ВселеннаяПомощи✨\" и далее перейдите в \"Мой кабинет\", "
                     "чтобы увидеть ваш баланс"
            )

        # Отправка сообщения об успехе
        success_message = (
            f"🥳 Поздравляем!\n\n Вы успешно присоединились к программе ✨'ВселеннаяПомощи'✨.\n\n"
            f"Теперь вы можете приглашать друзей, используя вашу реферальную ссылку из личного кабинета "
            f"и зарабатывать деньги на свой баланс, перейдите в 'Мой кабинет' нажав кнопку ниже.👇\n"
        )
        keyboard = [[InlineKeyboardButton("📱Мой кабинет", callback_data="my_profile")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        photo_path = "/var/www/admin78/data/www/vdohnovenie.pro/bot/photo/Unt3.jpg"
        media = InputMediaPhoto(media=open(photo_path, 'rb'), caption=success_message, parse_mode="HTML")
        await update.callback_query.message.edit_media(media=media, reply_markup=reply_markup)

    except Exception as e:
        logging.exception(f"confirm_join_referral_program: Общая ошибка: {e}")
        await context.bot.send_message(
            chat_id="-1002382309656",
            text=f"Ошибка при регистрации пользователя {user_id}: {e}"
        )
        await update.callback_query.message.reply_text("Произошла ошибка. Попробуйте позже.")

# Функция для отображения профиля
async def my_profile(update: Update, context: CallbackContext):
    if await check_frozen_status(update, context):
        return
    try:
        # Проверяем подписку
        is_subscribed = await check_and_notify_subscription(update, context)
        if not is_subscribed:
            return

        user_id = update.effective_user.id
        
        # Проверяем заморозку аккаунта
        if is_account_frozen(user_id):
            message = (
                "❄️ Ваш аккаунт заморожен.\n\n"
                "Вы не можете использовать функции бота.\n"
                "Если у вас есть вопросы, напишите в чат - @vdohnovenie_pro_chat"
            )
            if update.callback_query:
                await update.callback_query.message.edit_text(message)
            else:
                await update.message.reply_text(message)
            return
        user = get_user(user_id)
        activity = get_user_activity(user_id)
        level = calculate_user_level(activity['comments'])
        
        # Получаем имя пользователя
        try:
            user_tg = await context.bot.get_chat(user_id)
            first_name = user_tg.first_name or ""
            last_name = user_tg.last_name or ""
            full_name = f"{first_name} {last_name}".strip()
        except Exception as e:
            logging.error(f"Ошибка при получении имени пользователя: {e}")
            full_name = "Неизвестно"
        
        # Получаем последние 3 транзакции
        recent_transactions, _ = get_user_transactions(user_id, 0, 3)
        
         # Расчет прогресса и уровня
        total_points = activity['comments']
        level, progress, extra_cycles = calculate_user_level(total_points)
        
        # Формируем информацию об уровне
        level_info = format_level_info(level, progress, extra_cycles)
        
        message = (
            f"➖➖➖➖➖➖➖➖➖➖\n"
            f"<b>💰 Баланс: {user.get('balance', 0)} ₽</b>\n"
            f"➖➖➖➖➖➖➖➖➖➖\n"
            f"👤 <b>Профиль:</b> @{user.get('username', 'Неизвестно')}\n"
            f"⭐️ Имя: {full_name}\n"
            f"📅 Дата регистрации: {user.get('join_date', 'Неизвестно')}\n"
            f"➖➖➖➖➖➖➖➖➖➖\n"         
            f"👥 <b>Реферальная программа:</b>\n"
            f"🤝 Приглашено: {user.get('referrals', 0)} человек\n"
            f"🔗 Нажмите на ссылку чтобы скопировать и отправьте ее друзьям👇\n"
            f"<pre>{user.get('referral_link', 'Не указана')}</pre>\n"
            f"💎 Бонус за приглашение: 2000 ₽\n"
            f"➖➖➖➖➖➖➖➖➖➖\n"
            f"🏆 <b>Достижения:</b>\n"
            f"{format_level_info(level, progress)}"
            f"💭 Комментарии: {activity['comments']}\n"
            f"➖➖➖➖➖➖➖➖➖➖\n"
            f"❓ <i>Есть вопросы? нашли ошибку? 👉 <a href='https://t.me/vdohnovenie_pro_chat'>Написать</a></i>\n"
            f"➖➖➖➖➖➖➖➖➖➖\n"
            f"💡 <i>Наш паблик @vdohnovenie_pro</i>\n"
            f"➖➖➖➖➖➖➖➖➖➖\n"
        )
        
        if recent_transactions:
            message += f"📋 <b>Последние операции:</b>\n"
            for trans in recent_transactions:
                date = trans['date'].strftime('%d.%m.%Y')
                amount = f"+{trans['amount']}" if trans['amount'] > 0 else f"{trans['amount']}"
                message += f"• {date}: {amount}₽\n"
            message += "➖➖➖➖➖➖➖➖➖➖\n"

        keyboard = [
            [
                InlineKeyboardButton("🔄Обновить", callback_data="refresh_balance"),
                InlineKeyboardButton("💰История", callback_data="transaction_history")
            ],
            [
                InlineKeyboardButton("👥Рефералы", callback_data="my_invited_users"),
                InlineKeyboardButton("📊Статистика", callback_data="my_stats")
            ],
            [
                InlineKeyboardButton("🎟️Создать купон", callback_data="create_coupon"),
                InlineKeyboardButton("🗃️Мои купоны", callback_data="my_coupons")
            ],
            [InlineKeyboardButton("📚 О ✨ВселеннаяПомощи✨", callback_data="about_program")],
            [InlineKeyboardButton("🗓️Тесты и расчеты", callback_data="show_tests")],
            [InlineKeyboardButton("✨Каталог услуг", url="https://t.me/vdohnoveniepro_bot/shop")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        photo_path = "/var/www/admin78/data/www/vdohnovenie.pro/bot/photo/Unt4.jpg"
        
        if update.callback_query:
            try:
                from telegram import InputMediaPhoto
                media = InputMediaPhoto(
                    media=open(photo_path, 'rb'),
                    caption=message,
                    parse_mode="HTML"
                )
                await update.callback_query.message.edit_media(
                    media=media,
                    reply_markup=reply_markup
                )
            except telegram.error.BadRequest as e:
                if "Message is not modified" in str(e):
                    await update.callback_query.answer("Профиль уже обновлен")
                else:
                    raise
        else:
            await context.bot.send_photo(
                chat_id=update.effective_chat.id,
                photo=open(photo_path, 'rb'),
                caption=message,
                reply_markup=reply_markup,
                parse_mode="HTML"
            )
        
    except Exception as e:
        logging.exception(f"my_profile: Ошибка: {e}")
        await update.effective_message.reply_text(f"Произошла ошибка: {e}")

async def show_about_program(update: Update, context: CallbackContext):
    """Показывает информацию о программе ВселеннаяПомощи"""
    try:
        message = (
            "✨ О программе 'ВселеннаяПомощи' ✨\n\n"
            "🌟 Наша программа - это уникальная система вознаграждений за активность и развитие сообщества\n\n"
            "💰 Реферальная программа:\n"
            "🔹 2000₽ на баланс получаете вы за каждого приглашенного друга по вашей ссылке из личного кабинета\n"
            "🔹 2000₽ на баланс получает ваш друг при регистрации по вашей ссылке\n\n"
            "💰Бонусы за активность(❤️лайки и 💬комментарии) в нашей группе @vdohnovenie_pro. Бот будет повышать ваш уровень за активность, и за каждый уровень вы будете получать деньги на баланс.\n"
            "📈 Система уровней:\n"
            "🔹 50 ₽ на баланс - 1 уровень\n"
            "🔹 100 ₽ - 2 уровень\n"
            "🔹 1000 ₽ - 11 уровень\n\n"
            "🎁 Дополнительные возможности:\n"
            "🔹 Бесплатные расчеты и тесты.\n"
            "🔹 Купоны для оплаты услуг.\n"
            "🔹 Накопление баланса.\n\n"
            "❗Вы можете списать любую сумму со своего счёта для получения купона, который можно использовать для оплаты любой услуги на нашем сайте (кнопка «Наш сайт» в нижней части этого бота)."
        )

        keyboard = [
            [InlineKeyboardButton("❓ Как использовать купоны?", callback_data="how_to_use_coupon")],
            [InlineKeyboardButton("↩️В профиль", callback_data="my_profile")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        photo_path = "/var/www/admin78/data/www/vdohnovenie.pro/bot/photo/Unt2.jpg"
        
        media = InputMediaPhoto(
            media=open(photo_path, 'rb'),
            caption=message,
            parse_mode="HTML"
        )
        
        await update.callback_query.message.edit_media(
            media=media,
            reply_markup=reply_markup
        )
        await update.callback_query.answer()

    except Exception as e:
        logging.error(f"Error in show_about_program: {e}")
        await update.effective_message.reply_text("Произошла ошибка при открытии информации о программе")

# Добавляем новый обработчик для обновления профиля
async def refresh_profile(update: Update, context: CallbackContext):
    """Обработчик кнопки 'Проверить подписку'"""
    await my_profile(update, context)

async def show_stats(update: Update, context: CallbackContext):
    try:
        user_id = update.callback_query.from_user.id
        user = get_user(user_id)
        activity = get_user_activity(user_id)
        level_data = calculate_user_level(activity['comments'])  # Получаем кортеж
        level = level_data[0]  # Берем только первый элемент (уровень)
        
        # Получаем сумму всех положительных транзакций
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT SUM(amount) as total_earned 
            FROM transactions 
            WHERE user_id = %s AND amount > 0
        """, (user_id,))
        result = cursor.fetchone()
        total_earned = result['total_earned'] or 0
        conn.close()
        
        message = (
            f"📊 <b>Ваша статистика:</b>\n\n"
            f"🏅 Уровень: {level}\n"  # Используем только значение уровня
            f"💭 Всего комментариев: {activity['comments']}\n"
            f"👥 Приглашено пользователей: {user.get('referrals', 0)}\n"
            f"💰 Заработано всего: {total_earned} ₽\n"
            f"💵 Текущий баланс: {user.get('balance', 0)} ₽\n"
        )
        
        keyboard = [[InlineKeyboardButton("↩️Назад", callback_data="my_profile")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        photo_path = "/var/www/admin78/data/www/vdohnovenie.pro/bot/photo/Unt10.jpg"
        
        media = InputMediaPhoto(
            media=open(photo_path, 'rb'),
            caption=message,
            parse_mode="HTML"
        )
        
        await update.callback_query.message.edit_media(
            media=media,
            reply_markup=reply_markup
        )
        await update.callback_query.answer()
        
    except Exception as e:
        logging.exception(f"show_stats: Ошибка: {e}")
        await update.callback_query.message.reply_text(f"Произошла ошибка: {e}")

# Функция для просмотра списка приглашенных
async def get_invited_users(update: Update, context: CallbackContext):
    if await check_frozen_status(update, context):
        return
    logging.info("get_invited_users вызвана")
    try:
        user_id = update.callback_query.from_user.id
        page = context.user_data.get('invited_page', 0)
        
        if update.callback_query.data == "invited_next":
            page += 1
            context.user_data['invited_page'] = page
        elif update.callback_query.data == "invited_prev":
            page = max(0, page - 1)
            context.user_data['invited_page'] = page

        invited_users, total = get_invited_users_from_db(user_id, page * 5, 5)
        total_pages = (total - 1) // 5 + 1

        if not invited_users:
            message = (
                "😔 У вас пока нет приглашённых пользователей.\n\n"
                "Вернитесь в ваш кабинет (кнопка ↩️Назад), скопируйте вашу "
                "пригласительную ссылку и отправьте её друзьям.\n"
                "За каждого приглашенного вам будет начислено 2000 ₽ на баланс 🤗"
            )
            keyboard = [[InlineKeyboardButton("↩️Назад", callback_data="my_profile")]]
        else:
            message = f"📋 Список ваших приглашенных (страница {page + 1} из {total_pages}):\n\n"
            for invited_user in invited_users:
                try:
                    user_tg = await context.bot.get_chat(invited_user['user_id'])
                    first_name = user_tg.first_name or ""
                    last_name = user_tg.last_name or ""
                    full_name = f"{first_name} {last_name}".strip()
                    username = invited_user.get('username', 'Информация недоступна')
                    join_date = invited_user.get('join_date')
                    join_date_formatted = join_date.strftime("%d.%m.%Y") if join_date else "Неизвестно"

                    message += (
                        f"👤 Пользователь: @{username}\n"
                        f"📝 Имя: {full_name}\n"
                        f"📅 Дата регистрации: {join_date_formatted}\n"
                        f"➖➖➖➖➖➖➖➖➖➖\n"
                    )
                except Exception as e:
                    logging.error(f"Ошибка при получении данных пользователя: {e}")
                    continue

            keyboard = []
            nav_buttons = []
            
            if page > 0:
                nav_buttons.append(InlineKeyboardButton("⬅️ Назад", callback_data="invited_prev"))
            if (page + 1) * 5 < total:
                nav_buttons.append(InlineKeyboardButton("Вперед ➡️", callback_data="invited_next"))
            
            if nav_buttons:
                keyboard.append(nav_buttons)
            keyboard.append([InlineKeyboardButton("↩️В профиль", callback_data="my_profile")])

        reply_markup = InlineKeyboardMarkup(keyboard)
        photo_path = "/var/www/admin78/data/www/vdohnovenie.pro/bot/photo/Unt5.jpg"
        
        from telegram import InputMediaPhoto
        media = InputMediaPhoto(
            media=open(photo_path, 'rb'),
            caption=message,
            parse_mode="HTML"
        )
        
        await update.callback_query.message.edit_media(
            media=media,
            reply_markup=reply_markup
        )
        
    except Exception as e:
        logging.exception(f"Ошибка в get_invited_users: {e}")
        await update.effective_message.reply_text(f"Произошла ошибка: {e}", parse_mode="HTML")

# Функция для создания купона (изменена)
async def create_coupon(update: Update, context: CallbackContext):
    if await check_frozen_status(update, context):
        return
    user_id = update.effective_user.id
    if is_account_frozen(user_id):
        await update.callback_query.answer(
            "❄️ Ваш аккаунт заморожен. "
            "Если у вас есть вопросы, напишите в чат - @vdohnovenie_pro_chat",
            show_alert=True
        )
        return
    """Функция для инициализации создания купона"""
    # Устанавливаем флаг создания купона
    context.user_data['creating_coupon'] = True
    
    message = (
        "<b>🎫 Создание купона</b>\n\n"
        "Или выберите из предложенных вариантов ниже, нажав на кнопку с необходимой суммой, либо напишите в чат и отправьте нужную сумму для списания на купон в числовом формате (например: 500). "
        "Это может быть как часть суммы, так и полная сумма. "
    )
    keyboard = [
        [InlineKeyboardButton("1000 ₽", callback_data="coupon_amount_1000"),
         InlineKeyboardButton("1500 ₽", callback_data="coupon_amount_1500"),
         InlineKeyboardButton("2000 ₽", callback_data="coupon_amount_2000")],
        [InlineKeyboardButton("2500 ₽", callback_data="coupon_amount_2500"),
         InlineKeyboardButton("3000 ₽", callback_data="coupon_amount_3000"),
         InlineKeyboardButton("3500 ₽", callback_data="coupon_amount_3500")],
        [InlineKeyboardButton("4000 ₽", callback_data="coupon_amount_4000"),
         InlineKeyboardButton("4500 ₽", callback_data="coupon_amount_4500"),
         InlineKeyboardButton("5000 ₽", callback_data="coupon_amount_5000")],
        [InlineKeyboardButton("↩️Назад", callback_data="my_profile")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    photo_path = "/var/www/admin78/data/www/vdohnovenie.pro/bot/photo/Unt6.jpg"
    
    # Сохраняем сообщение для последующего редактирования
    coupon_message = await update.callback_query.message.edit_media(
        media=InputMediaPhoto(
            media=open(photo_path, 'rb'),
            caption=message,
            parse_mode="HTML"
        ),
        reply_markup=reply_markup
    )
    
    # Сохраняем ID сообщения и чата
    context.user_data['coupon_message'] = coupon_message
    context.user_data['coupon_message_id'] = coupon_message.message_id
    context.user_data['coupon_chat_id'] = coupon_message.chat.id

async def handle_coupon_amount(update: Update, context: CallbackContext):
    if await check_frozen_status(update, context):
        return
    """Функция для обработки введенной суммы купона"""
    logging.info(f"handle_coupon_amount called with update: {update}")
    
    try:
        # Проверяем, активен ли режим создания купона
        if not context.user_data.get('creating_coupon'):
            if update.message:
                await update.message.delete()
            return

        # Определяем источник суммы (текст или кнопка)
        if update.callback_query:
            amount = int(update.callback_query.data.split("_")[-1])
            message = update.callback_query.message
        else:
            try:
                amount = int(update.message.text)
                # Получаем сохраненное сообщение с меню
                message = context.user_data.get('coupon_message')
                if not message:
                    if update.message:
                        await update.message.delete()
                    return
            except ValueError:
                if update.message:
                    await update.message.delete()
                return

        # Получаем данные пользователя
        user_id = update.effective_user.id
        user = get_user(user_id)

        # Проверки
        if not user:
            await message.reply_text("⛔ Пользователь не найден.")
            return

        if amount <= 0:
            await message.reply_text("❗Сумма должна быть больше 0.")
            return

        if amount > user.get('balance', 0):
            await message.reply_text("❌ Недостаточно средств на балансе.")
            return

        # Создаем купон
        coupon = generate_coupon()
        coupon_data = {
            "coupon_id": coupon,
            "amount": amount,
            "user_id": user_id,
            "status": "pending",
            "date": datetime.now()
        }

        # Сохраняем в БД
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            # Сохраняем купон
            cursor.execute('''
                INSERT INTO coupons (coupon_id, amount, user_id, status, date)
                VALUES (%s, %s, %s, %s, %s)
            ''', (coupon_data['coupon_id'], coupon_data['amount'], 
                  coupon_data['user_id'], coupon_data['status'], 
                  coupon_data['date']))
            conn.commit()

            # Отправляем уведомление админу
            admin_message = (
                f"<b>❗Запрос на создание купона (🔴На согласовании)</b>\n"
                f"Пользователь: @{user.get('username', 'Информация недоступна')} (ID: {user_id})\n"
                f"Сумма: {amount}р.\n"
                f"Номер купона: {coupon}\n"
                f"Дата запроса: {coupon_data['date'].strftime('%d.%m.%Y %H:%M')}"
            )

            admin_keyboard = [
                [InlineKeyboardButton("✅Создать купон", callback_data=f"approve_coupon_{coupon}")],
                [InlineKeyboardButton("❌Удалить купон", callback_data=f"delete_coupon_{coupon}")],
                [InlineKeyboardButton("👤Посмотреть Профиль", callback_data=f"view_profile_{user_id}")]
            ]
            admin_markup = InlineKeyboardMarkup(admin_keyboard)

            await context.bot.send_message(
                chat_id="-1002382309656", 
                text=admin_message, 
                reply_markup=admin_markup, 
                parse_mode="HTML"
            )

            # Показываем сообщение об успехе
            success_message = (
                "🎉 Отлично! Вы успешно создали купон!\n"
                "Ваш запрос отправлен администратору.\n"
                "После проверки вы получите уведомление в этот чат."
            )

            keyboard = [[InlineKeyboardButton("↩️Назад", callback_data="my_profile")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            photo_path = "/var/www/admin78/data/www/vdohnovenie.pro/bot/photo/Unt7.jpg"
            media = InputMediaPhoto(
                media=open(photo_path, 'rb'),
                caption=success_message,
                parse_mode="HTML"
            )

            await message.edit_media(
                media=media,
                reply_markup=reply_markup
            )

            # Удаляем сообщение пользователя с суммой
            if update.message:
                await update.message.delete()

            # Сбрасываем флаг создания купона
            context.user_data['creating_coupon'] = False
            
        except Exception as e:
            logging.error(f"Error creating coupon: {e}")
            if conn:
                conn.rollback()
            await message.reply_text("Произошла ошибка при создании купона. Попробуйте позже.")
        finally:
            if conn:
                conn.close()

    except Exception as e:
        logging.error(f"Outer error in handle_coupon_amount: {e}")
        if update.callback_query:
            await update.callback_query.message.reply_text("Произошла ошибка. Попробуйте позже.")
        elif update.message:
            await update.message.delete()

async def view_profile(update: Update, context: CallbackContext):
    logging.info("view_profile вызвана")
    try:
        user_id = int(update.callback_query.data.split("_")[-1])
        logging.info(f"view_profile: user_id = {user_id}")

        user = get_user(user_id)
        if not user:
            await update.callback_query.answer("⛔ Профиль пользователя не найден.")
            return

        # Получаем информацию о пользователе
        try:
            user_tg = await context.bot.get_chat(user_id)
            first_name = user_tg.first_name or ""
            last_name = user_tg.last_name or ""
            full_name = f"{first_name} {last_name}".strip()
        except Exception as e:
            logging.error(f"view_profile: Error getting user info: {e}")
            full_name = "Информация недоступна"

        # Получаем приглашенных пользователей
        invited_users, total = get_invited_users_from_db(user_id)
        invited_users_list = []

        for invited_user in invited_users:
            try:
                user_tg_invited = await context.bot.get_chat(invited_user['user_id'])
                full_name_invited = f"{user_tg_invited.first_name or ''} {user_tg_invited.last_name or ''}".strip()
                username = invited_user.get('username', 'Информация недоступна')
                join_date = invited_user.get('join_date')
                join_date_formatted = join_date.strftime("%d.%m.%Y") if join_date else "Неизвестно"
                
                invited_users_list.append(f"@{username} - {full_name_invited} - {join_date_formatted}")
            except Exception as e:
                logging.error(f"Error processing invited user {invited_user.get('user_id')}: {e}")
                continue

        invited_users_message = "\n".join(invited_users_list) if invited_users_list else "Нет приглашённых пользователей."

        # Получаем купоны пользователя
        user_coupons = get_user_coupons(user_id)
        coupons_message = ""
        if user_coupons:
            coupons_message = "\n".join([
                f"Купон: {coupon['coupon_id']} - Сумма: {coupon['amount']}р. - Статус: {get_coupon_status_ru(coupon['status'])}"
                for coupon in user_coupons
            ])
        else:
            coupons_message = "Нет созданных купонов."

        # Получаем активность и уровень
        activity = get_user_activity(user_id)
        level = calculate_user_level(activity['comments'])

        message = (
            f"👤 Профиль пользователя: @{user.get('username', 'Неизвестно')}\n\n"
            f"⭐ Имя пользователя: {full_name}\n"
            f"💵 Баланс: {user.get('balance', 0)} ₽\n"
            f"🏅 Уровень: {level}\n\n"
            f"📅 Дата регистрации: {user.get('join_date', 'Неизвестно')}\n"
            f"📈 Количество приглашенных: {total}\n"
            f"🔗 Ссылка для приглашения: {user.get('referral_link', 'Не указана')}\n\n"
            f"📋 Приглашенные пользователи:\n{invited_users_message}\n\n"
            f"️🎟️ Купоны:\n{coupons_message}"
        )

        keyboard = [[InlineKeyboardButton("↩️Назад", callback_data="pending_coupons")]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await update.callback_query.answer()
        await context.bot.send_message(
            chat_id=update.callback_query.message.chat_id,
            text=message,
            reply_markup=reply_markup,
            parse_mode="HTML"
        )

    except Exception as e:
        logging.exception(f"view_profile: Внешняя ошибка: {e}")
        await update.effective_message.reply_text(f"Произошла ошибка: {e}", parse_mode="HTML")

async def approve_coupon(update: Update, context: CallbackContext):
    logging.info("approve_coupon вызвана")
    try:
        await update.callback_query.answer()
        coupon_id = update.callback_query.data.split("_")[-1]

        # Получаем данные купона из БД
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM coupons WHERE coupon_id = %s", (coupon_id,))
        coupon = cursor.fetchone()
        conn.close()

        if not coupon:
            await update.callback_query.message.reply_text("❌ Купон не найден.")
            return

        if coupon['status'] != "pending":
            await update.callback_query.message.reply_text("❌ Купон уже обработан.")
            return

        user_db = get_user(coupon['user_id'])
        if user_db:
            if user_db.get('balance', 0) < coupon['amount']:
                await update.callback_query.message.reply_text("❌ Недостаточно средств на балансе пользователя для одобрения купона.")
                return

            try:
                if 'balance' in user_db:
                    user_db['balance'] -= coupon['amount']
                    save_user(user_db)
                    add_transaction(user_db['user_id'], -coupon['amount'], f"Списание средств на купон {coupon['coupon_id']}")
                    
                    # Обновляем статус купона в БД
                    conn = get_db_connection()
                    cursor = conn.cursor()
                    cursor.execute("UPDATE coupons SET status = 'approved' WHERE coupon_id = %s", (coupon_id,))
                    conn.commit()
                    conn.close()

                    logging.info(f"approve_coupon: Баланс пользователя {user_db.get('user_id')} успешно уменьшен на {coupon['amount']}")
                else:
                    logging.error(f"approve_coupon: у пользователя {coupon['user_id']} отсутствует ключ 'balance'")
                    await context.bot.send_message(chat_id="-1002382309656", text=f"Ошибка при списании баланса у пользователя {coupon['user_id']}. Отсутствует ключ 'balance'.")
                    return

            except Exception as save_user_error:
                logging.exception(f"approve_coupon: Ошибка при сохранении пользователя: {save_user_error}")
                await context.bot.send_message(chat_id="-1002382309656", text=f"Ошибка при сохранении пользователя {coupon['user_id']}: {save_user_error}")
                return

            coupon['status'] = "approved"
            logging.info(f"approve_coupon: Статус купона {coupon_id} успешно обновлен на 'approved'")

            # *** ВЫЗОВ notify_balance_update ЗДЕСЬ ***
            amount = -coupon['amount']  # Делаем amount отрицательным для списания
            user_id = coupon['user_id']
            reason = f"Списание средств на купон {coupon['coupon_id']}"
            if not await notify_balance_update(context, user_id, amount, reason):
                logging.error(f"approve_coupon: Не удалось отправить уведомление об изменении баланса пользователю {user_id}")
                await context.bot.send_message(chat_id="-1002382309656", text=f"Ошибка при отправке уведомления об изменении баланса пользователю {user_id}")
            else:
                logging.info(f"approve_coupon: Уведомление об изменении баланса успешно отправлено пользователю {user_id}")

            try:
                user_tg = await context.bot.get_chat(coupon.get('user_id'))
                formatted_date = coupon.get('date')
                formatted_date_str = formatted_date.strftime('%d.%m.%Y') if formatted_date else "Дата не указана"

                user_message = (
                    f"🥳 Ваш купон успешно одобрен!\n\n"
                    f"️🎟️ Номер(Нажмите, чтобы скопировать): <pre>{coupon.get('coupon_id')}</pre>\n"
                    f"💰 Сумма: {coupon.get('amount', 0)} ₽\n"
                    f"📅 Дата: {formatted_date_str}\n\n"
                    f"Теперь вы можете воспользоваться им при заказе услуги на нашем сайте. "
                    f"Просто нажмите на номер купона выше чтобы скопировать его и вставьте в поле \"Добавить купон\" при оформлении нужной вам услуги.\n\n"
                    f"Вы можете сделать это прямо сейчас, нажав на кнопку ниже \"Использовать Купон\" и перейдите в раздел \"Услуги\".\n\n"
                    f"❗ Купоны действительны для применения на сайте в течении 1 месяца с даты создания"
                )
                user_keyboard = [
                    [InlineKeyboardButton("❓Как использовать купон?", callback_data="how_to_use_coupon")],
                    [InlineKeyboardButton("✨Использовать Купон", url="https://t.me/vdohnoveniepro_bot/shop")],
                    [InlineKeyboardButton("📱Мой кабинет", callback_data="my_profile")]
                ]
                user_markup = InlineKeyboardMarkup(user_keyboard)

                photo_path = "/var/www/admin78/data/www/vdohnovenie.pro/bot/photo/Unt12.jpg"

                await context.bot.send_photo(
                    chat_id=user_tg.id,
                    photo=open(photo_path, 'rb'),
                    caption=user_message,
                    reply_markup=user_markup,
                    parse_mode="HTML"
                )

            except BadRequest as e:
                logging.error(f"approve_coupon: BadRequest при отправке сообщения пользователю {coupon.get('user_id')}: {e}")
                await context.bot.send_message(chat_id="-1002382309656", text=f"Ошибка BadRequest при отправке сообщения пользователю {coupon.get('user_id')}: {e}")
            except TelegramError as e:
                logging.error(f"approve_coupon: TelegramError при отправке сообщения пользователю {coupon.get('user_id')}: {e}")
                await context.bot.send_message(chat_id="-1002382309656", text=f"Ошибка TelegramError при отправке сообщения пользователю {coupon.get('user_id')}: {e}")
            except Exception as e:
                logging.exception(f"approve_coupon: Непредвиденная ошибка при отправке сообщения пользователю {coupon.get('user_id')}: {e}")
                await context.bot.send_message(chat_id="-1002382309656", text=f"Непредвиденная ошибка при отправке сообщения пользователю {coupon.get('user_id')}: {e}")

        # Изменяем сообщение админу
        admin_message_id = context.user_data.get(f"admin_message_{coupon_id}")
        if admin_message_id:
            status_ru = get_coupon_status_ru(coupon.get('status'))
            formatted_date = coupon.get('date')
            formatted_date_str = formatted_date.strftime('%d.%m.%Y %H:%M') if formatted_date else "Дата не указана"

            admin_message = (
                f"<b>Запрос на создание купона ({status_ru})</b>\n"
                f"Пользователь: @{user_db.get('username', 'Информация недоступна')} (ID: {coupon.get('user_id')})\n"
                f"Сумма: {coupon.get('amount', 0)}р.\n"
                f"Номер купона: {coupon.get('coupon_id')}\n"
                f"Дата запроса: {formatted_date_str}"
            )
            admin_keyboard = [
                [InlineKeyboardButton("Админ панель", callback_data="admin_panel")],
                [InlineKeyboardButton("Посмотреть Профиль", callback_data=f"view_profile_{coupon.get('user_id')}")]
            ]
            admin_markup = InlineKeyboardMarkup(admin_keyboard)
            try:
                await context.bot.edit_message_text(
                    chat_id=update.callback_query.message.chat_id,
                    message_id=update.callback_query.message.message_id,
                    text=admin_message,
                    reply_markup=admin_markup,
                    parse_mode="HTML"
                )
            except BadRequest as e:
                logging.error(f"approve_coupon: BadRequest при редактировании сообщения админа: {e}")
            except TelegramError as e:
                logging.error(f"approve_coupon: TelegramError при редактировании сообщения админа: {e}")
            except Exception as e:
                logging.exception(f"approve_coupon: Непредвиденная ошибка при редактировании сообщения админа: {e}")

    except mysql.connector.Error as e:
        logging.error(f"approve_coupon: Ошибка базы данных: {e}")
    finally:
        if conn:
            conn.close()
            logging.info("approve_coupon: Соединение с базой данных закрыто.")

async def all_coupons(update: Update, context: CallbackContext):
    logging.info("all_coupons вызвана")
    try:
        page = context.user_data.get('coupons_page', 0)
        
        if update.callback_query.data == "coupons_next":
            page += 1
            context.user_data['coupons_page'] = page
        elif update.callback_query.data == "coupons_prev":
            page = max(0, page - 1)
            context.user_data['coupons_page'] = page

        coupons, total = get_all_coupons(page * 10, 10)
        total_pages = (total - 1) // 10 + 1

        if not coupons:
            message = "Список купонов пуст."
            keyboard = [[InlineKeyboardButton("↩️Назад", callback_data="admin_panel")]]
        else:
            message = f"<b>📋 Список всех купонов (страница {page + 1} из {total_pages}):</b>\n\n"
            for coupon in coupons:
                status_ru = get_coupon_status_ru(coupon['status'])
                date_str = coupon.get('date')
                formatted_date = date_str.strftime('%d.%m.%Y %H:%M') if date_str else 'Дата не указана'
                message += (
                    f"🎟️ Номер: {coupon['coupon_id']}\n"
                    f"💰 Сумма: {coupon['amount']}р.\n"
                    f"👤 Пользователь: @{coupon.get('username', 'Неизвестно')}\n"
                    f"📅 Дата: {formatted_date}\n"
                    f"📌 Статус: {status_ru}\n"
                    f"➖➖➖➖➖➖➖➖➖➖\n"
                )

            keyboard = []
            nav_buttons = []
            
            if page > 0:
                nav_buttons.append(InlineKeyboardButton("⬅️ Назад", callback_data="coupons_prev"))
            if (page + 1) * 10 < total:
                nav_buttons.append(InlineKeyboardButton("Вперед ➡️", callback_data="coupons_next"))
            
            if nav_buttons:
                keyboard.append(nav_buttons)
            keyboard.append([InlineKeyboardButton("↩️В админ-панель", callback_data="admin_panel")])

        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.callback_query.message.edit_text(
            message,
            reply_markup=reply_markup,
            parse_mode="HTML"
        )
        await update.callback_query.answer()

    except Exception as e:
        logging.exception(f"all_coupons: Ошибка: {e}")
        await update.effective_message.reply_text(f"Произошла ошибка: {e}")


async def delete_coupon(update: Update, context: CallbackContext):
    logging.info("delete_coupon вызвана")
    conn = None
    try:
        await update.callback_query.answer()
        coupon_id = update.callback_query.data.split("_")[-1]

        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM coupons WHERE coupon_id = %s", (coupon_id,))
        coupon = cursor.fetchone()

        if not coupon:
            await update.callback_query.message.reply_text("❌ Купон не найден.")
            return

        cursor.execute("DELETE FROM coupons WHERE coupon_id = %s", (coupon_id,))
        conn.commit()

        user_db = get_user(coupon.get('user_id'))  # Безопасное получение user_id
        if user_db:
            try:
                user_tg = await context.bot.get_chat(coupon.get('user_id'))
                user_message = (
                    f"❌ Ваш запрос на создание купона был отклонен администратором.\n\n"
                    f"️🎟️ {coupon.get('coupon_id')} на сумму {coupon.get('amount', 0)} ₽ был удален.\n\n"
                    f"Пожалуйста, свяжитесь с поддержкой для получения дополнительной информации. - @vdohnovenie_pro_chat"
                )
                user_keyboard = [[InlineKeyboardButton("📱Мой кабинет", callback_data="my_profile")]]
                user_markup = InlineKeyboardMarkup(user_keyboard)
                await context.bot.send_message(chat_id=user_tg.id, text=user_message, reply_markup=user_markup, parse_mode="HTML")

            except BadRequest as e:
                logging.error(f"delete_coupon: BadRequest при отправке сообщения пользователю {coupon.get('user_id')}: {e}")
                await context.bot.send_message(chat_id="-1002382309656", text=f"Ошибка BadRequest при отправке сообщения пользователю {coupon.get('user_id')}: {e}")
            except TelegramError as e:
                logging.error(f"delete_coupon: TelegramError при отправке сообщения пользователю {coupon.get('user_id')}: {e}")
                await context.bot.send_message(chat_id="-1002382309656", text=f"Ошибка TelegramError при отправке сообщения пользователю {coupon.get('user_id')}: {e}")
            except Exception as e:
                logging.exception(f"delete_coupon: Непредвиденная ошибка при отправке сообщения пользователю {coupon.get('user_id')}: {e}")
                await context.bot.send_message(chat_id="-1002382309656", text=f"Непредвиденная ошибка при отправке сообщения пользователю {coupon.get('user_id')}: {e}")

        # Изменяем сообщение админу
        admin_message_id = context.user_data.get(f"admin_message_{coupon_id}")
        if admin_message_id:
            formatted_date = coupon.get('date')
            formatted_date_str = formatted_date.strftime('%d.%m.%Y %H:%M') if formatted_date else "Дата не указана"

            admin_message = (
                f"<b>Запрос на создание купона (❌Удален)</b>\n"
                f"Пользователь: @{user_db.get('username', 'Информация недоступна')} (ID: {coupon.get('user_id')})\n"
                f"Сумма: {coupon.get('amount', 0)}р.\n"
                f"Номер купона: {coupon.get('coupon_id')}\n"
                f"Дата запроса: {formatted_date_str}"
            )
            admin_keyboard = [
                [InlineKeyboardButton("Админ панель", callback_data="admin_panel")],
                [InlineKeyboardButton("Посмотреть Профиль", callback_data=f"view_profile_{coupon.get('user_id')}")]
            ]
            admin_markup = InlineKeyboardMarkup(admin_keyboard)
            await context.bot.edit_message_text(chat_id=update.callback_query.message.chat_id, message_id=update.callback_query.message.message_id, text=admin_message, reply_markup=admin_markup, parse_mode="HTML")

    except mysql.connector.Error as e:
        logging.error(f"delete_coupon: Ошибка базы данных: {e}")
    except Exception as e:
        logging.exception(f"delete_coupon: Внешняя ошибка: {e}")
        await update.effective_message.reply_text(f"Произошла ошибка: {e}", parse_mode="HTML")


# Функция для просмотра своих купонов (добавлена дата)
async def my_coupons(update: Update, context: CallbackContext):
    if await check_frozen_status(update, context):
        return
    logging.info("my_coupons вызвана")
    user_id = update.callback_query.from_user.id
    
    try:
        page = context.user_data.get('my_coupons_page', 0)
        
        if update.callback_query.data == "my_coupons_next":
            page += 1
            context.user_data['my_coupons_page'] = page
        elif update.callback_query.data == "my_coupons_prev":
            page = max(0, page - 1)
            context.user_data['my_coupons_page'] = page
            
        user_coupons = get_user_coupons(user_id)
        total_coupons = len(user_coupons)
        total_pages = (total_coupons - 1) // 5 + 1 if total_coupons > 0 else 1
        
        # Получаем купоны для текущей страницы
        start_idx = page * 5
        end_idx = start_idx + 5
        current_page_coupons = user_coupons[start_idx:end_idx]

        keyboard = []
        if not user_coupons:
            message = (
                "❌ У вас пока нет созданных купонов.\n\n"
                "Вернитесь назад и если ваш баланс позволяет создать купон, "
                "нажмите кнопку \"Создать купон\".\n\n"
                "Если средств на балансе недостаточно, пригласите друзей — "
                "за каждого приглашённого человека ваш баланс пополнится на 2000 ₽."
            )
            keyboard = [
                [InlineKeyboardButton("❓ Как использовать купоны?", callback_data="how_to_use_coupon")],
                [InlineKeyboardButton("↩️Назад", callback_data="my_profile")]
                ]
        else:
            message = f"🎟️ <b>Ваши купоны (страница {page + 1} из {total_pages}):</b>\n\n"
            for coupon in current_page_coupons:
                status_ru = get_coupon_status_ru(coupon['status'])
                message += (
                    f"➖➖➖➖➖➖➖➖➖➖\n"
                    f"📝 Номер: <pre>{coupon['coupon_id']}</pre> ⬅️Копировать\n"
                    f"💰 Сумма: {coupon['amount']}₽\n"
                    f"📅 Создан: {coupon['date'].strftime('%d.%m.%Y')}\n"
                    f"📌 Статус: {status_ru}\n"
                )
            
            message += (
                "➖➖➖➖➖➖➖➖➖➖\n\n"
                "☝️Нажмите на номер купона, чтобы скопировать его.\n\n"
                "❗ ВНИМАНИЕ ❗\n"
                "• Купоны '🔴На согласовании' - проходят проверку\n"
                "• Купоны '🟢Согласовано' - можно использовать\n"
                "• Срок действия - 1 месяц с даты создания"
            )

            # Навигационные кнопки
            nav_buttons = []
            if page > 0:
                nav_buttons.append(InlineKeyboardButton("⬅️ Назад", callback_data="my_coupons_prev"))
            if (page + 1) * 5 < total_coupons:
                nav_buttons.append(InlineKeyboardButton("Вперед ➡️", callback_data="my_coupons_next"))
            
            if nav_buttons:
                keyboard.append(nav_buttons)
                
            keyboard.extend([
                [InlineKeyboardButton("❓ Как использовать купон?", callback_data="how_to_use_coupon")],
                [InlineKeyboardButton("✨ Использовать Купон", url="https://t.me/vdohnoveniepro_bot/shop")],
                [InlineKeyboardButton("↩️Назад", callback_data="my_profile")]
            ])

        reply_markup = InlineKeyboardMarkup(keyboard)
        photo_path = "/var/www/admin78/data/www/vdohnovenie.pro/bot/photo/Unt9.jpg"

        with open(photo_path, 'rb') as photo:
            media = InputMediaPhoto(
                media=photo,
                caption=message,
                parse_mode="HTML"
            )
            await update.callback_query.message.edit_media(
                media=media,
                reply_markup=reply_markup
            )
        await update.callback_query.answer()

    except Exception as e:
        logging.error(f"Error in my_coupons: {e}")
        await update.callback_query.message.reply_text(f"Произошла ошибка при получении списка купонов: {e}")

async def copy_coupon(update: Update, context: CallbackContext):
    logging.info("copy_coupon вызвана")
    coupon_id = update.callback_query.data.split("_")[-1]  # Извлекаем ID купона
    await update.callback_query.message.reply_text(f"Нажмите на этот номер и он будет скопирован ➡️ <code>{coupon_id}</code>", parse_mode="HTML")  # Отправляем сообщение с кодом
    await update.callback_query.answer()
    await context.bot.send_message(chat_id=update.callback_query.from_user.id, text=coupon_id)  # Отправляем купон отдельным сообщением

# Добавляем новый обработчик для инструкции
async def show_coupon_instructions(update: Update, context: CallbackContext):
    try:
        message = (
            "<b>⬆️ ВИДЕО ИНСТРУКЦИЯ ☝️</b>\n\n"
            "📖 <b>Как использовать купон:</b>\n\n"
            "1️⃣ Нажмите на номер купона чтобы скопировать его\n\n"
            "2️⃣ Нажмите кнопку 'Использовать Купон' или 'Наш Сайт' внизу этого чата и выберите нужную услугу на сайте\n\n"
            "3️⃣ При оформлении заказа найдите поле 'Добавить купон' и вставьте скопированный номер\n\n"
            "4️⃣ Нажмите 'Применить купон' - сумма заказа автоматически уменьшится\n\n"
            "5️⃣ Завершите оформление заказа\n\n"
            "❗ Важно: купон действует 1 месяц с даты создания"
        )

        # Путь к видеофайлу
        video_path = "/var/www/admin78/data/www/vdohnovenie.pro/bot/video/instruction.mp4"
        
        keyboard = [[InlineKeyboardButton("↩️Назад к купонам", callback_data="my_coupons")]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        # Отправляем видео с указанием ширины и высоты
        with open(video_path, 'rb') as video:
            await update.callback_query.message.edit_media(
                media=InputMediaVideo(
                    media=video,
                    caption=message,
                    parse_mode="HTML",
                    width=592,     # Оригинальная ширина
                    height=1280,   # Оригинальная высота
                    supports_streaming=True  # Важно для стриминга
                ),
                reply_markup=reply_markup
            )
        await update.callback_query.answer()

    except Exception as e:
        logging.error(f"Error in show_coupon_instructions: {e}")
        await update.callback_query.message.reply_text("Произошла ошибка при показе инструкции")

# Функция для обновления баланса
async def refresh_balance(update: Update, context: CallbackContext):
    logging.info("refresh_balance вызвана")
    try:
        await my_profile(update, context)
    except BadRequest as e:
        logging.error(f"refresh_balance: BadRequest: {e}")
        await update.effective_message.reply_text(f"Произошла ошибка (BadRequest): {e}", parse_mode="HTML")
    except TelegramError as e:
        logging.error(f"refresh_balance: TelegramError: {e}")
        await update.effective_message.reply_text(f"Произошла ошибка (TelegramError): {e}", parse_mode="HTML")
    except Exception as e:
        logging.exception(f"refresh_balance: Непредвиденная ошибка: {e}")
        await update.effective_message.reply_text(f"Произошла непредвиденная ошибка: {e}", parse_mode="HTML")

async def leaderboard(update: Update, context: CallbackContext):
    logging.info("leaderboard вызвана")
    try:
        leaderboard_data = get_leaderboard()
        message = "<b>🏆 Таблица лидеров</b>\n\n"
        for i, entry in enumerate(leaderboard_data, start=1):
            user = get_user(entry['user_id'])
            username = user.get('username', 'Неизвестно') if user else 'Неизвестно'
            comments = entry['comments']
            likes = entry['likes']
            level = calculate_user_level(comments, likes)
            message += f"{i}. @{username} - Уровень: {level} - Комментарии: {comments} - Лайки: {likes}\n"

        keyboard = [[InlineKeyboardButton("↩️Назад", callback_data="admin_panel")]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        if update.callback_query:
            await update.callback_query.message.edit_text(message, reply_markup=reply_markup, parse_mode="HTML")
            await update.callback_query.answer()
        else:
            await update.message.reply_text(message, reply_markup=reply_markup, parse_mode="HTML")

    except Exception as e:
        logging.exception(f"leaderboard: Ошибка: {e}")
        await update.effective_message.reply_text(f"Произошла ошибка: {e}", parse_mode="HTML")

async def show_referrals(update: Update, context: CallbackContext):
    try:
        user_id = update.effective_user.id
        referrals = get_referrals(user_id)

        if referrals:
            message = "📑 Ваши приглашенные:\n"
            for referral in referrals:
                username = referral.get('username', 'Неизвестно')
                first_name = referral.get('first_name', '')
                last_name = referral.get('last_name', '')
                join_date = referral.get('join_date', 'Неизвестно')
                join_date_str = join_date.strftime('%d.%m.%Y') if isinstance(join_date, datetime) else 'Неизвестно'
                message += f"- {first_name} {last_name} (@{username}) - Дата присоединения: {join_date_str}\n"
        else:
            message = "😔 😔 У вас пока нет приглашённых пользователей.\nВернитесь ваш кабинет(Кнопка ↩️Назад), скопируйте вашу пригласительную ссылку и отправьте её друзьям. Как только они присоединятся к нам, вам будут начислены деньги на баланс в размере 2000 ₽ за каждого приглашенного 🤗\n"

        keyboard = [[InlineKeyboardButton("↩️Назад", callback_data="my_profile")]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        if update.callback_query:
            await update.callback_query.message.edit_text(message, reply_markup=reply_markup, parse_mode="HTML")
            await update.callback_query.answer()
        else:
            await update.message.reply_text(message, reply_markup=reply_markup, parse_mode="HTML")
    except Exception as e:
        logging.error(f"Ошибка при получении списка рефералов: {e}")
        await update.message.reply_text("Произошла ошибка при получении списка приглашенных. Пожалуйста, попробуйте позже.")

async def show_leaderboard(update: Update, context: CallbackContext):
    logging.info("show_leaderboard вызвана")
    try:
        page = context.user_data.get('leaderboard_page', 0)
        
        if update.callback_query.data == "leaderboard_next":
            page += 1
            context.user_data['leaderboard_page'] = page
        elif update.callback_query.data == "leaderboard_prev":
            page = max(0, page - 1)
            context.user_data['leaderboard_page'] = page

        leaderboard_data, total = get_leaderboard(page * 10, 10)
        total_pages = (total - 1) // 10 + 1

        message = f"<b>🏆 Таблица лидеров (страница {page + 1} из {total_pages})</b>\n\n"

        for i, entry in enumerate(leaderboard_data, start=1):
            user = get_user(entry['user_id'])
            username = user.get('username', 'Неизвестно') if user else 'Неизвестно'
            comments = entry.get('comments', 0) or 0  # Защита от None
            level = calculate_user_level(comments)  # Передаем только комментарии
            message += (
                f"🎯 Место #{page * 10 + i}\n"
                f"👤 @{username}\n"
                f"🏅 Уровень: {level}\n"
                f"💭 Комментарии: {comments}\n"
                f"➖➖➖➖➖➖➖➖➖➖\n"
            )

        keyboard = []
        nav_buttons = []
        
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("⬅️ Назад", callback_data="leaderboard_prev"))
        if (page + 1) * 10 < total:
            nav_buttons.append(InlineKeyboardButton("Вперед ➡️", callback_data="leaderboard_next"))
        
        if nav_buttons:
            keyboard.append(nav_buttons)
        keyboard.append([InlineKeyboardButton("↩️В админ-панель", callback_data="admin_panel")])

        reply_markup = InlineKeyboardMarkup(keyboard)

        if update.callback_query:
            await update.callback_query.message.edit_text(message, reply_markup=reply_markup, parse_mode="HTML")
            await update.callback_query.answer()
        else:
            await update.message.reply_text(message, reply_markup=reply_markup, parse_mode="HTML")

    except Exception as e:
        logging.exception(f"show_leaderboard: Ошибка: {e}")
        await update.effective_message.reply_text(f"Произошла ошибка: {e}", parse_mode="HTML")

async def show_pending_coupons(update: Update, context: CallbackContext):
    logging.info("show_pending_coupons вызвана")
    try:
        page = context.user_data.get('pending_page', 0)
        
        if update.callback_query.data == "pending_next":
            page += 1
            context.user_data['pending_page'] = page
        elif update.callback_query.data == "pending_prev":
            page = max(0, page - 1)
            context.user_data['pending_page'] = page

        pending_coupons, total = get_pending_coupons_list(page * 10, 10)
        total_pages = (total - 1) // 10 + 1

        if not pending_coupons:
            message = "🎉 Нет купонов, ожидающих одобрения."
            keyboard = [[InlineKeyboardButton("↩️В админ-панель", callback_data="admin_panel")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.callback_query.message.edit_text(message, reply_markup=reply_markup)
            return

        for coupon in pending_coupons:
            date_str = coupon['date']
            formatted_date = date_str.strftime('%d.%m.%Y %H:%M') if date_str else 'Дата не указана'
            message = (
                f"<b>🎟️ Купон (страница {page + 1} из {total_pages})</b>\n\n"
                f"📝 Номер: {coupon['coupon_id']}\n"
                f"💰 Сумма: {coupon['amount']}р.\n"
                f"👤 Пользователь: @{coupon.get('username', 'Неизвестно')}\n"
                f"📅 Дата запроса: {formatted_date}\n"
                f"📌 Статус: {get_coupon_status_ru(coupon['status'])}\n"
            )

            keyboard = [
                [InlineKeyboardButton("✅ Одобрить", callback_data=f"approve_coupon_{coupon['coupon_id']}")],
                [InlineKeyboardButton("❌ Удалить", callback_data=f"delete_coupon_{coupon['coupon_id']}")],
                [InlineKeyboardButton("👤 Посмотреть профиль", callback_data=f"view_profile_{coupon['user_id']}")]
            ]
            
            nav_buttons = []
            if page > 0:
                nav_buttons.append(InlineKeyboardButton("⬅️ Назад", callback_data="pending_prev"))
            if (page + 1) * 10 < total:
                nav_buttons.append(InlineKeyboardButton("Вперед ➡️", callback_data="pending_next"))
            
            if nav_buttons:
                keyboard.append(nav_buttons)
            keyboard.append([InlineKeyboardButton("↩️В админ-панель", callback_data="admin_panel")])
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.callback_query.message.edit_text(message, reply_markup=reply_markup, parse_mode="HTML")

        await update.callback_query.answer()

    except Exception as e:
        logging.exception(f"show_pending_coupons: Ошибка: {e}")
        await update.effective_message.reply_text(f"Произошла ошибка: {e}")

async def user_list(update: Update, context: CallbackContext):
    logging.info("user_list вызвана")
    try:
        # Сбрасываем флаг при возврате в меню списка
        context.user_data['viewing_users_list'] = False
        
        message = "Вам нужно выбрать пользователя из списка и написать его @nick в чат и тогда вы получите по данному пользователю всю информацию."
        keyboard = [
            [InlineKeyboardButton("Список за месяц", callback_data="show_users_list_month")],
            [InlineKeyboardButton("Весь список", callback_data="show_users_list_all")],
            [InlineKeyboardButton("↩️Назад", callback_data="admin_panel")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        if update.callback_query:
            await update.callback_query.message.edit_text(message, reply_markup=reply_markup)
            await update.callback_query.answer()
        elif update.message:
            await update.message.reply_text(message, reply_markup=reply_markup)
            
    except Exception as e:
        logging.exception(f"user_list: Внешняя ошибка: {e}")
        await update.effective_message.reply_text(f"Произошла ошибка: {e}", parse_mode="HTML")

async def show_users_list(update: Update, context: CallbackContext, month_only=False):
    logging.info(f"show_users_list вызвана (month_only={month_only})")
    try:
        # Устанавливаем флаг, что пользователь просматривает список
        context.user_data['viewing_users_list'] = True
        
        page = context.user_data.get('users_page', 0)
        
        if update.callback_query.data == "users_next":
            page += 1
            context.user_data['users_page'] = page
        elif update.callback_query.data == "users_prev":
            page = max(0, page - 1)
            context.user_data['users_page'] = page

        users, total = get_users_list(page * 10, 10, month_only)
        total_pages = (total - 1) // 10 + 1

        if not users:
            message = "Список пользователей пуст."
            keyboard = [[InlineKeyboardButton("↩️Назад", callback_data="user_list")]]
        else:
            message = f"📋 Список пользователей (страница {page + 1} из {total_pages}):\n\n"
            message += "Для просмотра профиля пользователя введите его @username\n\n"
            for user in users:
                if user is None:
                    continue
                username = user.get('username', 'Неизвестно')
                first_name = user.get('first_name', '')
                last_name = user.get('last_name', '')
                full_name = f"{first_name} {last_name}".strip() or "Не указано"
                join_date = user.get('join_date')
                join_date_str = join_date.strftime('%d.%m.%Y') if isinstance(join_date, datetime) else 'Дата не указана'
                balance = user.get('balance', 0)
                message += (
                    f"👤 @{username}\n"
                    f"📝 ФИО: {full_name}\n"
                    f"📅 Дата: {join_date_str}\n"
                    f"💰 Баланс: {balance} ₽\n"
                    f"➖➖➖➖➖➖➖➖➖➖\n"
                )

            keyboard = []
            nav_buttons = []
            
            if page > 0:
                nav_buttons.append(InlineKeyboardButton("⬅️ Назад", callback_data="users_prev"))
            if (page + 1) * 10 < total:
                nav_buttons.append(InlineKeyboardButton("Вперед ➡️", callback_data="users_next"))
            
            if nav_buttons:
                keyboard.append(nav_buttons)
            keyboard.append([InlineKeyboardButton("↩️В админ-панель", callback_data="admin_panel")])

        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.callback_query.message.edit_text(
            message, 
            reply_markup=reply_markup, 
            parse_mode="HTML"
        )
        await update.callback_query.answer()

    except Exception as e:
        logging.exception(f"show_users_list: Ошибка: {e}")
        await update.effective_message.reply_text(f"Произошла ошибка: {e}")

async def view_profile_by_user(update: Update, context: CallbackContext, user):
    logging.info(f"view_profile_by_user вызвана для пользователя {user.get('user_id')}")
    try:
        if not isinstance(user, dict):
            logging.error(f"view_profile_by_user: Неверный формат данных пользователя: {user}")
            await update.message.reply_text("Ошибка: неверный формат данных пользователя")
            return
            
        user_id = user.get('user_id')
        if not user_id:
            logging.error("view_profile_by_user: Отсутствует user_id")
            await update.message.reply_text("Ошибка: не удалось получить ID пользователя")
            return

        full_name = f"{user.get('first_name', '')} {user.get('last_name', '')}".strip()
        invited_users, _ = get_invited_users_from_db(user_id)
        invited_users_list = []
        
        for invited_user in invited_users:
            invited_user_id = invited_user.get('user_id')
            if not invited_user_id:
                continue
                
            try:
                user_tg_invited = await context.bot.get_chat(invited_user_id)
                full_name_invited = f"{user_tg_invited.first_name or ''} {user_tg_invited.last_name or ''}".strip()
                username = invited_user.get('username', 'Информация недоступна')
                join_date = invited_user.get('join_date')
                join_date_formatted = join_date.strftime("%d.%m.%Y") if join_date else "Неизвестно"
                
                invited_users_list.append(f"@{username} - {full_name_invited} - {join_date_formatted}")

            except Exception as e:
                logging.error(f"Ошибка получения данных приглашенного пользователя {invited_user_id}: {e}")
                invited_users_list.append(f"ID: {invited_user_id} - Информация недоступна")

        invited_users_message = "\n".join(invited_users_list) if invited_users_list else "Нет приглашённых пользователей."

        # Получаем купоны пользователя
        user_coupons = get_user_coupons(user_id)
        coupons_message = ""
        if user_coupons:
            coupons_message = "\n".join([
                f"Купон: {coupon['coupon_id']} - Сумма: {coupon['amount']}р. - Статус: {get_coupon_status_ru(coupon['status'])}"
                for coupon in user_coupons
            ])
        else:
            coupons_message = "Нет созданных купонов."

        # Получаем активность и уровень
        activity = get_user_activity(user_id)
        level = calculate_user_level(activity['comments'])

        message = (
            f"👤 Профиль пользователя: @{user.get('username', 'Неизвестно')}\n\n"
            f"⭐ Имя пользователя: {full_name}\n"
            f"💵 Баланс: {user.get('balance', 0)} ₽\n"
            f"🏅 Уровень: {level}\n\n"
            f"📅 Дата регистрации: {user.get('join_date', 'Неизвестно')}\n"
            f"📈 Количество приглашенных: {user.get('referrals', 0)}\n"
            f"🔗 Ссылка для приглашения: {user.get('referral_link', 'Не указана')}\n\n"
            f"📋 Приглашенные пользователи:\n{invited_users_message}\n\n"
            f"️🎟️ Купоны:\n{coupons_message}"
        )

        await update.message.reply_text(message, parse_mode="HTML")

    except Exception as e:
        logging.exception(f"view_profile_by_user: Внешняя ошибка: {e}")
        await update.message.reply_text(f"Произошла ошибка при получении профиля пользователя: {e}", parse_mode="HTML")

async def open_admin_panel(update: Update, context: CallbackContext):
    logging.info("open_admin_panel вызвана")
    try:
        admin_id = update.effective_user.id
        if admin_id not in ADMIN_IDS:
            await update.effective_message.reply_text("⛔ У вас нет доступа к этой панели.")
            return

        total_users = get_total_users()
        total_coupons = get_total_coupons()
        total_referrals = get_total_referrals()
        approved_coupons = get_approved_coupons()
        pending_coupons = get_pending_coupons()
        active_coupons = get_active_coupons()
        frozen_accounts = get_frozen_accounts_count()

        message = (
            f"👨‍💼 Админ панель\n\n"
            f"👥 Всего пользователей: {total_users}\n"
            f"❄️ Заморожено аккаунтов: {frozen_accounts}\n"
            f"🎫 Купонов за все время: {total_coupons}\n"
            f"🎟️ Активных купонов: {active_coupons}\n"
            f"📈 Всего рефералов: {total_referrals}\n\n"
            f"🟢 Одобрено купонов: {approved_coupons}\n"
            f"🔴 Ждут одобрения: {pending_coupons}\n"
            f"\n\nОбновлено: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}"
        )

        keyboard = [
            [InlineKeyboardButton("🔄Обновить", callback_data="admin_panel")],
            [InlineKeyboardButton("🏆 Таблица лидеров", callback_data="leaderboard")],
            [InlineKeyboardButton("👥Список пользователей", callback_data="user_list")],
            [InlineKeyboardButton("🎟️Все купоны", callback_data="all_coupons")],
            [InlineKeyboardButton("🔴 Ждут одобрения", callback_data="pending_coupons")],
            [InlineKeyboardButton("❄️ Заморозка аккаунтов", callback_data="manage_freezes")],
            [InlineKeyboardButton("📢 Объявления", callback_data="manage_announcements")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await context.bot.send_message(chat_id=update.effective_chat.id, text=message, reply_markup=reply_markup, parse_mode="HTML")

    except Exception as e:
        logging.exception(f"open_admin_panel: Внешняя ошибка: {e}")
        await update.effective_message.reply_text(f"Произошла ошибка: {e}", parse_mode="HTML")

async def notify_balance_update(context: CallbackContext, user_id: int, amount: int, reason: str) -> bool:
    try:
        user = get_user(user_id)
        if not user:
            logging.error(f"notify_balance_update: Пользователь с ID {user_id} не найден.")
            return False

        # Определяем тип операции и формируем сообщение
        if amount < 0:
            operation_type = "Списание: "
            amount_str = f"{amount}"  # Оставляем минус для отрицательных значений
        else:
            operation_type = "Пополнение: +"
            amount_str = f"{amount}"

        message = (
            f"💰 Ваш баланс был обновлен.\n"
            f"{operation_type}{abs(amount)} ₽\n"
            f"Причина: {reason}\n"
            f"Текущий баланс: {user.get('balance', 0)} ₽"
        )

        await context.bot.send_message(chat_id=user_id, text=message, parse_mode="HTML")
        logging.info(f"notify_balance_update: Уведомление отправлено пользователю {user_id}")
        return True
    except Exception as e:
        logging.exception(f"notify_balance_update: Ошибка при отправке уведомления пользователю {user_id}: {e}")
        return False

def update_user_activity_and_rewards(user_id, comments=0, likes=0):
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        # Обновление активности пользователя
        cursor.execute('''
            INSERT INTO user_activity (user_id, comments, likes, last_activity, activity_count)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                comments = comments + VALUES(comments),
                likes = likes + VALUES(likes),
                last_activity = VALUES(last_activity),
                activity_count = activity_count + 1
        ''', (user_id, comments, likes, datetime.now(), 1))
        
        # Получение текущей активности пользователя
        cursor.execute("SELECT comments FROM user_activity WHERE user_id = %s", (user_id,))
        activity = cursor.fetchone()
        
        # Расчет уровня и начислений
        if activity:
            level, rewards = calculate_user_level_and_rewards(activity['comments'])
            
            # Обновление уровня и баланса пользователя
            cursor.execute("UPDATE users SET balance = balance + %s WHERE user_id = %s", (rewards, user_id))
            add_transaction(user_id, rewards, f"Награда за достижение уровня {level}")
            conn.commit()
        
        conn.close()
    except mysql.connector.Error as e:
        logging.error(f"Error updating user activity and rewards for user {user_id}: {e}")

# Новый обработчик для истории транзакций
async def show_transaction_history(update: Update, context: CallbackContext):
    if await check_frozen_status(update, context):
        return
    logging.info("show_transaction_history вызвана")
    try:
        user_id = update.callback_query.from_user.id
        page = context.user_data.get('transaction_page', 0)
        
        if update.callback_query.data == "trans_next":
            page += 1
            context.user_data['transaction_page'] = page
        elif update.callback_query.data == "trans_prev":
            page = max(0, page - 1)
            context.user_data['transaction_page'] = page
        
        transactions, total = get_user_transactions(user_id, page * 5, 5)
        total_pages = (total - 1) // 5 + 1
        
        if not transactions:
            message = "У вас пока нет истории транзакций."
            keyboard = [[InlineKeyboardButton("↩️Назад", callback_data="my_profile")]]
        else:
            message = f"📊 История операций (страница {page + 1} из {total_pages}):\n\n"
            for trans in transactions:
                date = trans['date'].strftime('%d.%m.%Y %H:%M')
                if trans['amount'] > 0:
                    amount = f"+ {trans['amount']}"
                else:
                    amount = f"- {abs(trans['amount'])}"
                
                message += f"📅 {date}\n💰 {amount}₽\n📝 {trans['reason']}\n"
                message += "➖➖➖➖➖➖➖➖➖➖\n"
            
            # Создаем кнопки навигации
            keyboard = []
            nav_buttons = []
            
            if page > 0:
                nav_buttons.append(InlineKeyboardButton("⬅️ Назад", callback_data="trans_prev"))
            if (page + 1) * 5 < total:
                nav_buttons.append(InlineKeyboardButton("Вперед ➡️", callback_data="trans_next"))
            
            if nav_buttons:
                keyboard.append(nav_buttons)
            keyboard.append([InlineKeyboardButton("↩️В профиль", callback_data="my_profile")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        photo_path = "/var/www/admin78/data/www/vdohnovenie.pro/bot/photo/Unt8.jpg"
        
        from telegram import InputMediaPhoto
        media = InputMediaPhoto(
            media=open(photo_path, 'rb'),
            caption=message,
            parse_mode="HTML"
        )
        
        await update.callback_query.message.edit_media(
            media=media,
            reply_markup=reply_markup
        )
        
    except Exception as e:
        logging.error(f"Error in show_transaction_history: {e}")
        await update.callback_query.message.reply_text("Произошла ошибка при получении истории транзакций")

async def handle_group_message(update: Update, context: CallbackContext):
    try:
        message = update.message
        chat = message.chat
        user_id = message.from_user.id
        username = message.from_user.username
        first_name = message.from_user.first_name
        last_name = message.from_user.last_name
        
        # Расширенное логирование
        logging.info(f"""
        Получено групповое сообщение:
        chat_id: {chat.id}
        chat_type: {chat.type}
        ALLOWED_DISCUSSION_ID: {ALLOWED_DISCUSSION_ID}
        user_id: {user_id}
        username: {username}
        text: {message.text[:50] if message.text else 'No text'}
        """)

        # Проверка группы (теперь проверяем оба чата)
        if chat.id not in [ALLOWED_DISCUSSION_ID, TEST_DISCUSSION_ID]:
            logging.info(f"Сообщение из неразрешенной группы {chat.id}")
            return

        # Проверка регистрации
        registered_user = get_user(user_id)
        
        # Проверяем заморозку аккаунта только для обработки активности
        if registered_user and is_account_frozen(user_id):
            logging.info(f"Сообщение от замороженного пользователя {user_id} - пропускаем начисление активности")
            return
            
        if registered_user:
            logging.info(f"Обработка сообщения от зарегистрированного пользователя {user_id}")
            await process_registered_user_message(update, context, registered_user)
        else:
            logging.info(f"Обработка сообщения от незарегистрированного пользователя {user_id}")
            await process_unregistered_user_message(update, context, user_id, username, first_name, last_name)

    except Exception as e:
        logging.error(f"Ошибка в handle_group_message: {e}\n{traceback.format_exc()}")

async def process_unregistered_user_message(update, context, user_id, username, first_name, last_name):
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        now = datetime.now()

        # Получаем текущую активность
        cursor.execute("SELECT * FROM temp_activity WHERE user_id = %s", (user_id,))
        current_activity = cursor.fetchone()
        old_comments = current_activity['comments'] if current_activity else 0
        logging.info(f"Старое количество комментариев: {old_comments}")
        
        # Рассчитываем старый уровень
        old_level, _, _ = calculate_user_level(old_comments)
        logging.info(f"Старый уровень: {old_level}")
        
        # Обновляем или создаем запись
        if current_activity:
            cursor.execute("""
                UPDATE temp_activity 
                SET comments = comments + 1,
                    last_activity = %s
                WHERE user_id = %s
            """, (now, user_id))
        else:
            cursor.execute("""
                INSERT INTO temp_activity 
                (user_id, username, first_name, last_name, comments, first_activity, last_activity)
                VALUES (%s, %s, %s, %s, 1, %s, %s)
            """, (user_id, username, first_name, last_name, now, now))

        conn.commit()

        # Получаем обновленное количество комментариев
        cursor.execute("SELECT comments FROM temp_activity WHERE user_id = %s", (user_id,))
        new_activity = cursor.fetchone()
        new_comments = new_activity['comments']
        logging.info(f"Новое количество комментариев: {new_comments}")
        
        # Рассчитываем новый уровень
        new_level, _, _ = calculate_user_level(new_comments)
        logging.info(f"Новый уровень: {new_level}")
        
        # Проверяем повышение уровня
        if new_level > old_level:
            reward = calculate_level_reward(new_level)
            logging.info(f"Повышение уровня! new_level={new_level}, reward={reward}")
            await notify_temp_level_up(context, update, user_id, new_level, reward)
        else:
            logging.info("Уровень не повысился")

    except Exception as e:
        logging.error(f"Ошибка обработки незарегистрированного пользователя: {e}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

async def notify_temp_level_up(context, update, user_id, new_level, reward):
    """Отправляет уведомление о повышении уровня незарегистрированному пользователю"""
    try:
        username = update.message.from_user.username
        first_name = update.message.from_user.first_name
        display_name = f"@{username}" if username else first_name
        
        next_level_reward = calculate_level_reward(new_level + 1) if new_level < 11 else 1000
        
        message = (
            f"{display_name}, "
            f"🎉 Поздравляем! Ваш уровень повышен до {new_level}-го!\n\n"
            f"💰 Вы получили {reward}₽ на свой баланс.\n\n"
            f"Присоединитесь к нашей программе ✨ВселеннаяПомощи✨ и они отобразятся у вас в личном кабинете. Деньги вы можете тратить на любые услуги\n"
            f"❗Для этого просто нажмите кнопку ниже '📱Перейти в кабинет' и следуйте инструкциям.\n"
            f"Следующий уровень принесет вам {next_level_reward}₽!\n\n"
            f"Продолжайте активность в группе, учитываются комментарии💭 и лайки❤️"
        )
        
        keyboard = [[InlineKeyboardButton("📱Перейти в кабинет", url="https://t.me/vdohnoveniepro_bot")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        # Добавляем кнопку с ссылкой на бота

        photo_path = "/var/www/admin78/data/www/vdohnovenie.pro/bot/photo/Unt2.jpg"

        await context.bot.send_photo(
            chat_id=update.message.chat_id,  # Отправляем в тот же чат, где было сообщение
            photo=open(photo_path, 'rb'),
            caption=message,
            reply_markup=reply_markup,
            parse_mode="HTML",
            reply_to_message_id=update.message.message_id  # Отвечаем на сообщение пользователя
        )
        
        logging.info(f"Отправлено уведомление о повышении уровня: user_id={user_id}, new_level={new_level}")
        
    except Exception as e:
        logging.error(f"Error in notify_temp_level_up: {e}")

# Добавляем общую функцию проверки подписки и показа сообщения
async def check_and_notify_subscription(update: Update, context: CallbackContext):
    """Проверяет подписку и отправляет сообщение если не подписан"""
    user_id = update.effective_user.id
    is_subscribed = await check_subscription(context, user_id)
    
    if not is_subscribed:
        message = (
            "❗ Для доступа к боту необходимо быть подписанным "
            "на нашу группу @vdohnovenie_pro\n\n"
            "1️⃣ Перейдите по ссылке @vdohnovenie_pro\n"
            "2️⃣ Подпишитесь на группу\n"
            "3️⃣ Вернитесь в бот и нажмите кнопку 'Проверить подписку'"
        )

        keyboard = [
            [InlineKeyboardButton("📢 Перейти в группу", url="https://t.me/vdohnovenie_pro")],
            [InlineKeyboardButton("🔄 Проверить подписку", callback_data="refresh_profile")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        photo_path = "/var/www/admin78/data/www/vdohnovenie.pro/bot/photo/Unt2.jpg"

        # Отправляем всплывающее уведомление если это callback query
        if update.callback_query:
            await update.callback_query.answer(
                "Вы еще не подписались на нашу группу @vdohnovenie_pro\n"
                "Подпишитесь и попробуйте еще раз",
                show_alert=True
            )

        try:
            media = InputMediaPhoto(
                media=open(photo_path, 'rb'),
                caption=message,
                parse_mode="HTML"
            )
            
            # Если это callback query с сообщением
            if update.callback_query and update.callback_query.message:
                await update.callback_query.message.edit_media(
                    media=media,
                    reply_markup=reply_markup
                )
            # Если это обычное сообщение
            else:
                await context.bot.send_photo(
                    chat_id=update.effective_chat.id,
                    photo=open(photo_path, 'rb'),
                    caption=message,
                    reply_markup=reply_markup,
                    parse_mode="HTML"
                )
        except Exception as e:
            logging.error(f"Ошибка при отправке сообщения о подписке: {e}")
            if update.callback_query:
                await update.callback_query.answer(
                    "Пожалуйста, подпишитесь на группу @vdohnovenie_pro",
                    show_alert=True
                )
            else:
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text="Вы еще не подписались на нашу группу @vdohnovenie_pro\n"
                         "Подпишитесь и попробуйте еще раз"
                )
        return False
    return True

async def admin_panel(update: Update, context: CallbackContext):
    # При возврате в админ-панель сбрасываем флаг
    context.user_data['in_freeze_menu'] = False
    logging.info("admin_panel вызвана")
    try:
        admin_id = update.effective_user.id
        if admin_id not in ADMIN_IDS:
            await update.effective_message.reply_text("⛔ У вас нет доступа к этой панели.")
            return

        total_users = get_total_users()
        total_coupons = get_total_coupons()
        total_referrals = get_total_referrals()
        approved_coupons = get_approved_coupons()
        pending_coupons = get_pending_coupons()
        active_coupons = get_active_coupons()
        frozen_accounts = get_frozen_accounts_count()

        # *** Вот здесь формируется сообщение ***
        message = (
            f"👨‍💼 Админ панель\n\n"
            f"👥 Всего пользователей: {total_users}\n"
            f"❄️ Заморожено аккаунтов: {frozen_accounts}\n"
            f"🎫 Купонов за все время: {total_coupons}\n"
            f"🎟️ Активных купонов: {active_coupons}\n"
            f"📈 Всего рефералов: {total_referrals}\n\n"
            f"🟢 Одобрено купонов: {approved_coupons}\n"
            f"🔴 Ждут одобрения: {pending_coupons}\n"
            f"\n\nОбновлено: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}"  # Временная метка (рекомендуется)
        )
        # *** Заканчивается формирование сообщения ***

        keyboard = [
            [InlineKeyboardButton("🔄Обновить", callback_data="admin_panel")],
            [InlineKeyboardButton("🏆 Таблица лидеров", callback_data="leaderboard")],
            [InlineKeyboardButton("👥Список пользователей", callback_data="user_list")],
            [InlineKeyboardButton("🎟️Все купоны", callback_data="all_coupons")],
            [InlineKeyboardButton("🔴 Ждут одобрения", callback_data="pending_coupons")],
            [InlineKeyboardButton("❄️ Заморозка аккаунтов", callback_data="manage_freezes")],
            [InlineKeyboardButton("📢 Объявления", callback_data="manage_announcements")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        # *** ВАЖНО: Отладочный вывод ПЕРЕД отправкой сообщения ***
        print(f"update.callback_query: {update.callback_query}")
        print(f"Отправляемое сообщение: {message}")
        print(f"Отправляемая разметка: {reply_markup}")

        # Обрабатываем callback-запрос (нажатие кнопки)
        if update.callback_query and update.callback_query.message:
            try:
                await update.callback_query.message.delete()  # Удаляем старое сообщение
                sent_message = await context.bot.send_message(chat_id=update.effective_chat.id, text=message, reply_markup=reply_markup, parse_mode="HTML") # Отправляем новое сообщение
                await update.callback_query.answer()  # Подтверждаем получение callback-запроса
                context.chat_data['admin_panel_message_id'] = sent_message.message_id # Сохраняем ID нового сообщения
            except telegram.error.BadRequest as e:
                logging.error(f"BadRequest при удалении/отправке сообщения: {e}")
                await update.callback_query.answer(f"Произошла ошибка: {e}", show_alert=True)
            except telegram.error.TelegramError as e:
                logging.error(f"TelegramError при удалении/отправке сообщения: {e}")
                await update.callback_query.answer(f"Произошла ошибка: {e}", show_alert=True)
            except Exception as e:
                logging.exception(f"Непредвиденная ошибка при удалении/отправке сообщения: {e}")
                await update.callback_query.answer(f"Произошла ошибка: {e}", show_alert=True)
        # Обрабатываем первый запуск команды /admin_panel
        elif update.effective_message:
            sent_message = await update.effective_message.reply_text(message, reply_markup=reply_markup, parse_mode="HTML")
            context.chat_data['admin_panel_message_id'] = sent_message.message_id
        else:
            logging.warning("Ни update.callback_query.message, ни update.effective_message не определены")
            await context.bot.send_message(chat_id=update.effective_chat.id, text="Произошла ошибка. Попробуйте позже.")


    except Exception as e:
        logging.exception(f"admin_panel: Внешняя ошибка: {e}")
        await update.effective_message.reply_text(f"Произошла ошибка: {e}", parse_mode="HTML")

async def check_frozen_status(update: Update, context: CallbackContext) -> bool:
    """Проверяет статус заморозки и отправляет сообщение если аккаунт заморожен"""
    # Добавляем проверку на наличие effective_user
    if not update.effective_user:
        return False
        
    user_id = update.effective_user.id
    if is_account_frozen(user_id):
        message = (
            "❄️ Ваш аккаунт заморожен.\n\n"
            "Вы не можете использовать функции бота.\n"
            "Если у вас есть вопросы, напишите в чат - @vdohnovenie_pro_chat"
        )
        if update.callback_query:
            await update.callback_query.answer(message, show_alert=True)
        else:
            await update.message.reply_text(message)
        return True
    return False

async def manage_freezes(update: Update, context: CallbackContext):
    """Показывает меню управления заморозкой аккаунтов"""
    # Устанавливаем флаг, что пользователь находится в меню заморозки
    context.user_data['in_freeze_menu'] = True
    
    message = (
        "❄️ Управление заморозкой аккаунтов\n\n"
        "Для заморозки аккаунта введите команду:\n"
        "/freeze @username\n\n"
        "Для разморозки аккаунта:\n"
        "/unfreeze @username"
    )
    keyboard = [
        [InlineKeyboardButton("📋 Список замороженных", callback_data="show_frozen_list")],
        [InlineKeyboardButton("↩️Назад", callback_data="admin_panel")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.message.edit_text(message, reply_markup=reply_markup)

async def manage_announcements(update: Update, context: CallbackContext):
    """Показывает меню управления объявлениями"""
    announcements = get_active_announcements()
    message = "📢 Управление объявлениями\n\n"
    
    if announcements:
        message += "Активные объявления:\n\n"
        for ann in announcements:
            message += f"- {ann['message']}\n"
    else:
        message += "Нет активных объявлений\n\n"
    
    message += "\nДля создания объявления используйте команду:\n/announce <текст>"

    keyboard = [[InlineKeyboardButton("↩️Назад", callback_data="admin_panel")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.message.edit_text(message, reply_markup=reply_markup)

async def freeze_command(update: Update, context: CallbackContext):
    """Command to freeze a user account"""
    try:
        # Проверяем права админа
        if update.effective_user.id not in ADMIN_IDS:
            await update.message.reply_text("⛔ У вас нет прав для выполнения этой команды.")
            return

        # Проверяем, находится ли пользователь в меню заморозки
        if not context.user_data.get('in_freeze_menu', False):
            await update.message.reply_text(
                "❌ Эта команда доступна только в меню 'Заморозка аккаунтов'\n"
                "Перейдите в админ-панель и нажмите кнопку 'Заморозка аккаунтов'"
            )
            return

        # Get username from command
        args = context.args
        if not args:
            await update.message.reply_text("❌ Укажите username пользователя: /freeze @username")
            return

        username = args[0].replace("@", "")
        user = get_user_by_username(username)
        
        if not user:
            await update.message.reply_text(f"❌ Пользователь @{username} не найден.")
            return

        if freeze_user_account(user['user_id']):
            await update.message.reply_text(f"✅ Аккаунт @{username} заморожен.")
            # Notify user
            try:
                await context.bot.send_message(
                    chat_id=user['user_id'],
                    text="❄️ Ваш аккаунт был заморожен администратором.\n\n Вы не можете использовать функции бота.\n Если у вас есть вопросы, напишите в чат - @vdohnovenie_pro_chat"
                )
            except Exception as e:
                logging.error(f"Error notifying user about account freeze: {e}")
        else:
            await update.message.reply_text(f"❌ Ошибка при заморозке аккаунта @{username}.")

    except Exception as e:
        logging.error(f"Error in freeze_command: {e}")
        await update.message.reply_text("❌ Произошла ошибка при выполнении команды.")

async def unfreeze_command(update: Update, context: CallbackContext):
    """Command to unfreeze a user account"""
    try:
        # Проверяем права админа
        if update.effective_user.id not in ADMIN_IDS:
            await update.message.reply_text("⛔ У вас нет прав для выполнения этой команды.")
            return

        # Проверяем, находится ли пользователь в меню заморозки
        if not context.user_data.get('in_freeze_menu', False):
            await update.message.reply_text(
                "❌ Эта команда доступна только в меню 'Заморозка аккаунтов'\n"
                "Перейдите в админ-панель и нажмите кнопку 'Заморозка аккаунтов'"
            )
            return

        # Get username from command
        args = context.args
        if not args:
            await update.message.reply_text("❌ Укажите username пользователя: /unfreeze @username")
            return

        username = args[0].replace("@", "")
        user = get_user_by_username(username)

        if not user:
            await update.message.reply_text(f"❌ Пользователь @{username} не найден.")
            return

        if unfreeze_user_account(user['user_id']):
            # Сообщение администратору
            admin_message = (
                f"✅ Аккаунт разморожен успешно\n\n"
                f"👤 Пользователь: @{username}\n"
                f"🆔 ID: {user['user_id']}\n"
                f"📝 Имя: {user.get('first_name', '')} {user.get('last_name', '')}\n"
                f"📅 Дата: {datetime.now().strftime('%d.%m.%Y %H:%M')}"
            )
            await update.message.reply_text(admin_message)

            # Сообщение пользователю
            keyboard = [[InlineKeyboardButton("📱 Открыть бота", callback_data="my_profile")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            try:
                await context.bot.send_message(
                    chat_id=user['user_id'],
                    text="✨ Ваш аккаунт разблокирован администратором.\n"
                        "Теперь вы снова можете пользоваться всеми функциями бота.",
                    reply_markup=reply_markup
                )
            except Exception as e:
                logging.error(f"Error notifying user about account unfreeze: {e}")
                await update.message.reply_text(
                    f"⚠️ Аккаунт разморожен, но не удалось отправить уведомление пользователю: {e}"
                )
        else:
            await update.message.reply_text(f"❌ Ошибка при разморозке аккаунта @{username}.")

    except Exception as e:
        logging.error(f"Error in unfreeze_command: {e}")
        await update.message.reply_text("❌ Произошла ошибка при выполнении команды.")

async def show_frozen_accounts(update: Update, context: CallbackContext):
    try:
        page = context.user_data.get('frozen_page', 0)
        
        if update.callback_query.data == "frozen_next":
            page += 1
            context.user_data['frozen_page'] = page
        elif update.callback_query.data == "frozen_prev":
            page = max(0, page - 1)
            context.user_data['frozen_page'] = page
            
        frozen_users, total = get_frozen_accounts(page * 10, 10)
        total_pages = (total - 1) // 10 + 1

        if not frozen_users:
            message = "Нет замороженных аккаунтов."
            keyboard = [[InlineKeyboardButton("↩️Назад", callback_data="manage_freezes")]]
        else:
            message = f"❄️ Замороженные аккаунты (страница {page + 1} из {total_pages}):\n\n"
            for user in frozen_users:
                join_date = user['join_date'].strftime('%d.%m.%Y') if user['join_date'] else 'Дата не указана'
                message += (
                    f"👤 @{user['username']}\n"
                    f"📝 ФИО: {user['first_name']} {user['last_name']}\n"
                    f"📅 Дата регистрации: {join_date}\n"
                    f"➖➖➖➖➖➖➖➖➖➖\n"
                )

            keyboard = []
            nav_buttons = []
            if page > 0:
                nav_buttons.append(InlineKeyboardButton("⬅️ Назад", callback_data="frozen_prev"))
            if (page + 1) * 10 < total:
                nav_buttons.append(InlineKeyboardButton("Вперед ➡️", callback_data="frozen_next"))
            
            if nav_buttons:
                keyboard.append(nav_buttons)
            keyboard.append([InlineKeyboardButton("↩️В меню заморозки", callback_data="manage_freezes")])

        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.callback_query.message.edit_text(message, reply_markup=reply_markup, parse_mode="HTML")
        
    except Exception as e:
        logging.error(f"Error in show_frozen_accounts: {e}")
        await update.callback_query.message.reply_text("Произошла ошибка при получении списка замороженных аккаунтов")

async def announce_command(update: Update, context: CallbackContext):
    """Command to create a global announcement"""
    try:
        # Check admin permissions
        if update.effective_user.id not in ADMIN_IDS:
            await update.message.reply_text("⛔ У вас нет прав для выполнения этой команды.")
            return

        # Get announcement text
        announcement_text = " ".join(context.args)
        if not announcement_text:
            await update.message.reply_text("❌ Укажите текст объявления: /announce <текст>")
            return

        if create_announcement(announcement_text):
            await update.message.reply_text("✅ Объявление создано.")

            # Send to all users
            conn = get_db_connection()
            cursor = conn.cursor(dictionary=True)
            cursor.execute("SELECT user_id FROM users")
            users = cursor.fetchall()
            conn.close()

            for user in users:
                try:
                    await context.bot.send_message(
                        chat_id=user['user_id'],
                        text=f"📢 <b>Важное объявление:</b>\n\n{announcement_text}",
                        parse_mode="HTML"
                    )
                except Exception as e:
                    logging.error(f"Error sending announcement to user {user['user_id']}: {e}")
                    continue

        else:
            await update.message.reply_text("❌ Ошибка при создании объявления.")

    except Exception as e:
        logging.error(f"Error in announce_command: {e}")
        await update.message.reply_text("❌ Произошла ошибка при выполнении команды.")

async def show_tests_and_calculations(update: Update, context: CallbackContext):
    if await check_frozen_status(update, context):
        return
    """Показывает раздел с тестами и расчетами"""
    try:
        message = (
            "🌟 <b>Тесты и расчеты</b>\n\n"
            "В этом разделе собраны различные тесты и расчеты, "
            "которые помогут вам в познании себя и мира:\n\n"
            "🧘 Биоритмы - расчет ваших биоритмов(физического, эмоционального, инетллектуального и других) на основе даты рождения.\n\n"
            "❤️ Совместимость - нумерологический расчет совместимости\n\n"
        )

        keyboard = [
            [InlineKeyboardButton("🧘 Биоритмы", callback_data="biorhythm")],
            [InlineKeyboardButton("❤️ Совместимость", callback_data="compatibility")],
            [InlineKeyboardButton("↩️ 📱Мой кабинет", callback_data="my_profile")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        photo_path = "/var/www/admin78/data/www/vdohnovenie.pro/bot/photo/Unt11.jpg"

        if update.callback_query:
            media = InputMediaPhoto(
                media=open(photo_path, 'rb'),
                caption=message,
                parse_mode="HTML"
            )
            await update.callback_query.message.edit_media(
                media=media,
                reply_markup=reply_markup
            )
        else:
            await update.message.reply_photo(
                photo=open(photo_path, 'rb'),
                caption=message,
                reply_markup=reply_markup,
                parse_mode="HTML"
            )

    except Exception as e:
        logging.error(f"Error in show_tests_and_calculations: {e}")
        await update.effective_message.reply_text("Произошла ошибка при открытии раздела тестов")

# Добавляем новую функцию обработки команды /stat
async def show_my_stats(update: Update, context: CallbackContext, user_data=None, is_admin_view=False):
    if await check_frozen_status(update, context):
        return
    """Показывает статистику пользователя"""
    try:
        if user_data:
            user = user_data
            title = f"📊 <b>Статистика пользователя @{user.get('username', 'Неизвестно')}:</b>\n\n"
        else:
            user_id = update.effective_user.id
            user = get_user(user_id)
            title = "📊 <b>Ваша статистика:</b>\n\n"
        
        if not user:
            await update.message.reply_text("⛔ Пользователь не найден.")
            return

        # Получаем все транзакции
        transactions, _ = get_user_transactions(user['user_id'], 0, 1000)
        referral_earnings = sum(t['amount'] for t in transactions if "приглашение" in t['reason'].lower())
        activity_earnings = sum(t['amount'] for t in transactions if "награда за достижение" in t['reason'].lower())
        total_earned = sum(t['amount'] for t in transactions if t['amount'] > 0)
        total_spent = abs(sum(t['amount'] for t in transactions if t['amount'] < 0))

        # Получаем активность и уровень
        activity = get_user_activity(user['user_id'])
        level_data = calculate_user_level(activity['comments'])
        level = level_data[0]  # Уровень
        progress = level_data[1]  # Прогресс

        # Формируем строку с уровнем и прогрессом
        level_info = format_level_info(level, progress)

        referrals = get_referrals(user['user_id'])
        coupons = get_user_coupons(user['user_id'])
        active_coupons = len([c for c in coupons if c['status'] == 'approved'])
        pending_coupons = len([c for c in coupons if c['status'] == 'pending'])

        message = (
            f"{title}"
            f"💰 Текущий баланс: {user.get('balance', 0)} ₽\n"
            f"💵 Всего заработано: {total_earned} ₽\n"
            f"💸 Всего потрачено: {total_spent} ₽\n\n"
            f"👥 Реферальная программа:\n"
            f"• Приглашено пользователей: {len(referrals)}\n"
            f"• Заработано с рефералов: {referral_earnings} ₽\n\n"
            f"📈 Активность:\n"
            f"{format_level_info(level, progress)}"
            f"💭 Комментариев: {activity['comments']}\n"
            f"• Заработано с активности: {activity_earnings} ₽\n"
            f"• Дата регистрации: {user.get('join_date').strftime('%d.%m.%Y')}\n\n"
            f"🎫 Купоны:\n"
            f"🟢 Активные: {active_coupons}\n"
            f"🔴 Ожидают одобрения: {pending_coupons}\n"
        )

        keyboard = [[InlineKeyboardButton("📱 Мой кабинет", callback_data="my_profile")]]
        if is_admin_view:
            keyboard.append([InlineKeyboardButton("↩️ В админ-панель", callback_data="admin_panel")])
        reply_markup = InlineKeyboardMarkup(keyboard)

        photo_path = "/var/www/admin78/data/www/vdohnovenie.pro/bot/photo/Unt10.jpg"

        if update.callback_query:
            media = InputMediaPhoto(
                media=open(photo_path, 'rb'),
                caption=message,
                parse_mode="HTML"
            )
            await update.callback_query.message.edit_media(
                media=media,
                reply_markup=reply_markup
            )
        else:
            await update.message.reply_photo(
                photo=open(photo_path, 'rb'),
                caption=message,
                reply_markup=reply_markup,
                parse_mode="HTML"
            )

    except Exception as e:
        logging.exception(f"show_my_stats: Ошибка: {e}")
        await update.effective_message.reply_text("Произошла ошибка при получении статистики.")

async def show_compact_stats(update: Update, context: CallbackContext):
    """Показывает компактную статистику пользователя в чате"""
    try:
        user_id = update.effective_user.id
        activity = get_user_activity(user_id)
        level, progress, extra_cycles = calculate_user_level(activity['comments'])
        
        next_level_reward = calculate_level_reward(level + 1) if level < 11 else 1000
        
        # Компактное сообщение
        message = (
            f"📊 <b>Ваша статистика:</b>\n"
            f"🏅 Уровень: {level}\n"
            f"📈 Прогресс: {progress}%\n"
            f"💭 Комментарии: {activity['comments']}\n"
            f"💰 Награда за следующий уровень: {next_level_reward}₽\n"
        )
        
        # Добавляем кнопку с ссылкой на бота
        keyboard = [[InlineKeyboardButton("📱Перейти в кабинет", url="https://t.me/vdohnoveniepro_bot")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Путь к изображению
        photo_path = "/var/www/admin78/data/www/vdohnovenie.pro/bot/photo/Unt2.jpg"
        
        # Отправляем фото с текстом и кнопкой в ответ на сообщение пользователя
        await update.message.reply_photo(
            photo=open(photo_path, 'rb'),
            caption=message,
            reply_markup=reply_markup,
            parse_mode="HTML",
            reply_to_message_id=update.message.message_id
        )
            
    except Exception as e:
        logging.error(f"Error in show_compact_stats: {e}")
        await update.message.reply_text("Произошла ошибка при получении статистики")

async def handle_stat_command(update: Update, context: CallbackContext):
    logging.info(f"handle_stat_command вызвана с текстом: {update.message.text}")
    try:
        if not update.message:
            return
            
        text = update.message.text.strip()
        
        # Для простой команды /stat показываем компактную статистику
        if text == '/stat':
            logging.info("Вызов /stat без параметров")
            await show_compact_stats(update, context)
            return
            
        # Если это не /stat команда и мы не просматриваем список пользователей, 
        # просто игнорируем сообщение
        if not text.startswith('/stat') and not context.user_data.get('viewing_users_list'):
            return
            
        # Остальная логика для админов остается без изменений
        if text.startswith('/stat@') or text.startswith('@'):
            username = text.split('@')[1].strip()
        elif text.startswith('/stat @'):
            username = text.split('@')[1].strip()
        else:
            return
            
        if text.startswith('/stat') and update.effective_user.id not in ADMIN_IDS:
            await update.message.reply_text("⛔ У вас нет доступа к этой команде.")
            return
            
        user = get_user_by_username(username)
        if user:
            if text.startswith('/stat'):
                await show_my_stats(update, context, user_data=user, is_admin_view=True)
            else:
                await view_profile_by_user(update, context, user)
        else:
            await update.message.reply_text(f"Пользователь @{username} не найден.")
                
    except Exception as e:
        logging.exception(f"handle_stat_command: Ошибка: {e}")
        await update.message.reply_text(f"Произошла ошибка при обработке команды.")

async def notify_level_up(context, update, user_id, new_level, reward):
    """Отправляет уведомление о повышении уровня"""
    try:
        user = get_user(user_id)
        activity = get_user_activity(user_id)
        
        # Получаем прогресс для сообщения
        _, progress, _ = calculate_user_level(activity['comments'])
        
        level_message = ""
        if new_level == 11:  # Максимальный уровень
            level_message = (
                f"🎉 Поздравляем! Вы достигли максимального уровня!\n\n"
                f"💫 Теперь за каждые 545 новых сообщений вы будете "
                f"автоматически получать по 1000₽ на баланс!\n\n"
                f"Продолжайте общаться и зарабатывать!"
            )
        else:
            next_level_comments = get_level_requirements()[new_level + 1]
            current_comments = activity['comments']
            needed_comments = next_level_comments - current_comments
            level_message = f"До следующего уровня осталось {needed_comments} сообщений!"

        message = (
            f"@{user.get('username', 'Неизвестно')}, "
            f"🎉 Поздравляем! Ваш уровень повышен до {new_level}-го!\n"
            f"💰 Вам начислено {reward}₽\n\n"
            f"{level_message}\n\n"
            f"📈 Ваша статистика:\n"
            f"{format_level_info(new_level, progress)}\n"
            f"💭 Комментарии: {activity['comments']}\n"
        )
        
        keyboard = [[InlineKeyboardButton("📱Перейти в кабинет", url="https://t.me/vdohnoveniepro_bot")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        photo_path = "/var/www/admin78/data/www/vdohnovenie.pro/bot/photo/Unt2.jpg"
        
        # Отправляем в чат группы
        await context.bot.send_photo(
            chat_id=update.message.chat_id,  # Отправляем в тот же чат, где было сообщение
            photo=open(photo_path, 'rb'),
            caption=message,
            reply_markup=reply_markup,
            parse_mode="HTML",
            reply_to_message_id=update.message.message_id  # Отвечаем на сообщение пользователя
        )

    except Exception as e:
        logging.error(f"Error in notify_level_up: {e}")
        logging.error(traceback.format_exc())

async def handle_chat_message(update: Update, context: CallbackContext):
    try:
        user_id = update.message.from_user.id
        user = get_user(user_id)
        if not user:
            return

        # Получаем текущий уровень  
        old_activity = get_user_activity(user_id)
        old_level = calculate_user_level(old_activity['comments'])
        
        # Обновляем статистику
        update_user_activity(user_id, comments=1)
        
        # Получаем новый уровень
        new_activity = get_user_activity(user_id)
        new_level = calculate_user_level(new_activity['comments'])
        
        # Если уровень повысился
        if new_level > old_level:
            # Рассчитываем и начисляем награду
            reward = calculate_level_reward(new_level)
            
            # Обновляем баланс пользователя
            user['balance'] = user.get('balance', 0) + reward
            save_user(user)
            
            # Добавляем транзакцию
            add_transaction(user_id, reward, f"Награда за достижение {new_level} уровня")
            
            # Отправляем уведомление
            await notify_level_up(context, update, user_id, new_level, reward)
            
    except Exception as e:
        logging.error(f"Error in handle_chat_message: {e}")

async def handle_channel_post(update: Update, context: CallbackContext):
    """Handles posts from the channel and sends automated responses"""
    try:
        message = update.channel_post
        if not message:
            logging.info("Нет channel_post в update")
            return

        chat_id = message.chat_id
        message_id = message.message_id
        
        # Определяем chat_id для обсуждений
        discussion_chat_id = None
        if chat_id == ALLOWED_GROUP_ID:
            discussion_chat_id = ALLOWED_DISCUSSION_ID
        elif chat_id == TEST_CHANNEL_ID:
            discussion_chat_id = TEST_DISCUSSION_ID
        
        if not discussion_chat_id:
            logging.warning(f"Неизвестный канал: {chat_id}")
            return

        # Достаточная задержка для гарантированного форварда
        await asyncio.sleep(5)

        # Находим ID форварда в группе обсуждений
        messages = await context.bot.get_updates()
        forwarded_message_id = None
        
        for update in messages:
            if (update.message and 
                update.message.chat.id == discussion_chat_id and
                update.message.is_automatic_forward and
                update.message.forward_from_chat and
                update.message.forward_from_chat.id == chat_id and
                update.message.forward_from_message_id == message_id):
                forwarded_message_id = update.message.message_id
                break

        if not forwarded_message_id:
            logging.error("Не удалось найти ID форварда сообщения")
            return

        try:
            # Формируем сообщение
            comment_text = (
                "📌 Напоминаем, что активность в группе (❤️лайки и 💬комментарии) "
                "учитывается по программе ✨ВселеннаяПомощи✨.\n"
                "💰 Вас ждут:\n"
                "• Автоматические начисления за активность\n"
                "• Повышение уровня и увеличение наград\n"
                "• 11 уровней с призами до 5000₽ и больше\n"
                "👥 Реферальная программа - 2000₽ за каждого друга + 2000₽ другу\n\n"
                "🎁 Накопленные средства можно тратить на любые услуги с нашего сайта\n\n"
                "📲 Чтобы ознакомиться с программой, нажмите на кнопку ✨ВселеннаяПомощи✨"
            )

            keyboard = [
                [InlineKeyboardButton("✨ВселеннаяПомощи✨", url="https://t.me/vdohnoveniepro_bot")],
                [InlineKeyboardButton("🛍 Наш Сайт", url="https://t.me/vdohnoveniepro_bot/shop")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)

            # Отправляем сообщение как ответ на форвард
            sent_message = await context.bot.send_photo(
                chat_id=discussion_chat_id,
                photo=open("/var/www/admin78/data/www/vdohnovenie.pro/bot/photo/Unt2.jpg", 'rb'),
                caption=comment_text,
                reply_markup=reply_markup,
                parse_mode="HTML",
                reply_to_message_id=forwarded_message_id  # Добавляем reply_to_message_id
            )
            
            logging.info(f"Комментарий успешно отправлен в чат {discussion_chat_id} как ответ на сообщение {forwarded_message_id}")
            return True

        except telegram.error.TimedOut:
            logging.error("Timeout при отправке сообщения, повторная попытка...")
            await asyncio.sleep(2)
            return False
            
        except telegram.error.RetryAfter as e:
            logging.error(f"Need to wait {e.retry_after} seconds before retry")
            await asyncio.sleep(e.retry_after)
            return False

        except telegram.error.TelegramError as e:
            logging.error(f"Telegram error: {e}")
            return False

    except Exception as e:
        logging.exception(f"Ошибка в handle_channel_post: {e}")
        return False


def main():
    conn = None
    try:
        conn = get_db_connection()
        logging.info("Соединение с базой данных установлено.")

        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM users")
        result = cursor.fetchone()
        if result:
            logging.info(f"Таблица users доступна. Количество пользователей: {result[0]}")
        else:
            logging.warning("Таблица users пуста или запрос вернул неожиданный результат.")

    except mysql.connector.Error as e:
        logging.critical(f"Ошибка при работе с БД: {e}")
        return  # Важно выйти из main(), если ошибка БД
    except Exception as e:
        logging.critical(f"Непредвиденная ошибка: {e}")
        return
    finally:
        if conn:
            conn.close()
            logging.info("Соединение с базой данных закрыто.")

    logging.info("Начало функции main()")  # Теперь эта строка находится ПРАВИЛЬНОМ месте

    logging.info("Создание приложения...")
    application = Application.builder().token("7365896423:AAF9RJwe0SOD-Guh68ei7k_ccGYWusyHIs4").build()
    logging.info("Приложение создано.")

    logging.info("Настройка job_queue...")
    job_queue = application.job_queue
    job_queue.run_daily(cleanup_coupons, time=datetime.min.time())
    logging.info("job_queue настроена.")

    application.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND & ~filters.ChatType.GROUPS,
        handle_coupon_amount,
        block=False
    ), group=0)

    async def check_db_connection(context: CallbackContext):
        try:
            conn = get_db_connection()
            conn.ping(reconnect=True)
            conn.close()
            logging.info("DB connection check - OK")
        except Exception as e:
            logging.error(f"DB connection check failed: {e}")

    # Теперь можно использовать job_queue
    job_queue.run_repeating(check_db_connection, interval=300)
    job_queue.run_daily(cleanup_coupons, time=datetime.min.time())
    logging.info("job_queue настроена.")
    
    print(f"ALLOWED_GROUP_ID: {ALLOWED_GROUP_ID}")
    print(f"ALLOWED_DISCUSSION_ID: {ALLOWED_DISCUSSION_ID}") 
    print(f"TEST_CHANNEL_ID: {TEST_CHANNEL_ID}")
    print(f"TEST_DISCUSSION_ID: {TEST_DISCUSSION_ID}")

    register_moon_handlers(application)
    register_compatibility_handlers(application)
    register_muhurta_handlers(application)
    register_biorhythm_handlers(application)

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(start, pattern="event_return"))  # Добавлено
    application.add_handler(CommandHandler("stat", handle_stat_command))
    application.add_handler(CallbackQueryHandler(show_my_stats, pattern="my_stats"))
    application.add_handler(CommandHandler("admin_panel", admin_panel))  # Добавляем команду для вызова "Админ панели"
    application.add_handler(MessageHandler(
        filters.TEXT & filters.Regex(r"^(@|/stat\s*@)"),
        handle_stat_command
    ))
        
    group_handler = MessageHandler(
        filters.ChatType.GROUPS & filters.TEXT & ~filters.COMMAND,
        handle_group_message,
        block=False
    )
    application.add_handler(group_handler, group=1)

    application.add_handler(MessageHandler(
        (filters.ChatType.CHANNEL | filters.ChatType.SUPERGROUP) & filters.UpdateType.CHANNEL_POST,
        handle_channel_post,
        block=False
    ), group=1)

    application.add_handler(CommandHandler("freeze", freeze_command, filters=filters.User(ADMIN_IDS)))
    application.add_handler(CommandHandler("unfreeze", unfreeze_command, filters=filters.User(ADMIN_IDS)))
    application.add_handler(CommandHandler("announce", announce_command, filters=filters.User(ADMIN_IDS)))
    
    application.add_handler(CallbackQueryHandler(manage_freezes, pattern="manage_freezes"))
    application.add_handler(CallbackQueryHandler(show_frozen_accounts, pattern="show_frozen_list"))
    application.add_handler(CallbackQueryHandler(show_frozen_accounts, pattern="frozen_next"))
    application.add_handler(CallbackQueryHandler(show_frozen_accounts, pattern="frozen_prev"))
    application.add_handler(CallbackQueryHandler(manage_announcements, pattern="manage_announcements"))
    application.add_handler(CallbackQueryHandler(show_about_program, pattern="about_program"))
    application.add_handler(CallbackQueryHandler(refresh_profile, pattern="refresh_profile"))
    application.add_handler(CallbackQueryHandler(show_my_stats, pattern="my_stats"))
    application.add_handler(CallbackQueryHandler(check_subscription, pattern="check_subscription"))
    application.add_handler(CallbackQueryHandler(join_referral_program, pattern=r"^join_referral_program_(\d+|none)$"))
    application.add_handler(CallbackQueryHandler(confirm_join_referral_program, pattern=r"^confirm_join_referral_program_(none|\d+)$"))
    application.add_handler(CallbackQueryHandler(my_profile, pattern="my_profile"))
    application.add_handler(CallbackQueryHandler(refresh_balance, pattern="refresh_balance"))
    application.add_handler(CallbackQueryHandler(get_invited_users, pattern="my_invited_users"))
    application.add_handler(CallbackQueryHandler(all_coupons, pattern="all_coupons"))
    application.add_handler(CallbackQueryHandler(create_coupon, pattern="create_coupon"))
    application.add_handler(CallbackQueryHandler(handle_coupon_amount, pattern=r"coupon_amount_"))
    application.add_handler(CallbackQueryHandler(approve_coupon, pattern=r"approve_coupon_"))
    application.add_handler(CallbackQueryHandler(delete_coupon, pattern=r"delete_coupon_"))
    application.add_handler(CallbackQueryHandler(my_coupons, pattern="my_coupons"))
    application.add_handler(CallbackQueryHandler(copy_coupon, pattern=r"copy_coupon_"))
    application.add_handler(CallbackQueryHandler(view_profile, pattern=r"view_profile_"))
    application.add_handler(CallbackQueryHandler(admin_panel, pattern="admin_panel"))
    application.add_handler(CallbackQueryHandler(show_leaderboard, pattern="leaderboard"))
    application.add_handler(CallbackQueryHandler(show_leaderboard, pattern="leaderboard_next"))
    application.add_handler(CallbackQueryHandler(show_leaderboard, pattern="leaderboard_prev"))
    application.add_handler(CallbackQueryHandler(show_pending_coupons, pattern="pending_coupons"))
    application.add_handler(CallbackQueryHandler(user_list, pattern="user_list"))
    application.add_handler(CallbackQueryHandler(lambda update, context: show_users_list(update, context, month_only=True), pattern="show_users_list_month"))
    application.add_handler(CallbackQueryHandler(lambda update, context: show_users_list(update, context, month_only=False), pattern="show_users_list_all"))
    application.add_handler(CallbackQueryHandler(show_referrals, pattern="my_invited_users"))
    application.add_handler(CallbackQueryHandler(show_transaction_history, pattern="transaction_history"))
    application.add_handler(CallbackQueryHandler(show_transaction_history, pattern="trans_next"))
    application.add_handler(CallbackQueryHandler(show_transaction_history, pattern="trans_prev"))
    application.add_handler(CallbackQueryHandler(get_invited_users, pattern="invited_next"))
    application.add_handler(CallbackQueryHandler(get_invited_users, pattern="invited_prev"))
    application.add_handler(CallbackQueryHandler(show_users_list, pattern="users_next"))
    application.add_handler(CallbackQueryHandler(show_users_list, pattern="users_prev"))
    application.add_handler(CallbackQueryHandler(all_coupons, pattern="coupons_next"))
    application.add_handler(CallbackQueryHandler(all_coupons, pattern="coupons_prev"))
    application.add_handler(CallbackQueryHandler(show_pending_coupons, pattern="pending_next"))
    application.add_handler(CallbackQueryHandler(show_pending_coupons, pattern="pending_prev"))
    application.add_handler(CallbackQueryHandler(show_coupon_instructions, pattern="how_to_use_coupon"))
    application.add_handler(CallbackQueryHandler(show_tests_and_calculations, pattern="show_tests"))
    application.add_handler(CallbackQueryHandler(my_coupons, pattern="my_coupons_next"))
    application.add_handler(CallbackQueryHandler(my_coupons, pattern="my_coupons_prev"))

    logging.info("Запуск polling...")
    application.run_polling()
    logging.info("Polling запущен.")

if __name__ == "__main__":
    main()
