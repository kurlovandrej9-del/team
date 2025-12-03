

import logging
import os
import sys
import asyncio
import aiosqlite
from datetime import datetime
from dotenv import load_dotenv

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup, 
    InputMediaPhoto, ReplyKeyboardMarkup, ReplyKeyboardRemove
)
from telegram.ext import (
    ApplicationBuilder, ContextTypes, CommandHandler, 
    CallbackQueryHandler, MessageHandler, filters, ConversationHandler
)
from telegram.constants import ParseMode
from telegram.error import BadRequest

# --- ⚙️ CONFIGURATION ---
load_dotenv()

# LOAD TOKENS & SECRETS
BOT_TOKEN = os.getenv("BOT_TOKEN", "8053044453:AAGHu89oQfOKj_Q-nk7sr1XwTZhSXk1J9ZI")
ACCESS_PASSWORD = os.getenv("ACCESS_PASSWORD", "Zxcv1236")
admin_env = os.getenv("ADMIN_IDS", "844012884")
ADMIN_IDS = [int(x) for x in admin_env.split(",")] if admin_env else []
DB_NAME = "team_stats.db"

# DYNAMIC PATH RESOLUTION
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
IMG_PATHS = {
    "welcome": os.path.join(BASE_DIR, "logo.png"),
    "profile": os.path.join(BASE_DIR, "profile.png"),
    "pay": os.path.join(BASE_DIR, "pay.png")
}

# LOGGING
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

# --- 🗄️ DATABASE FUNCTIONS ---
async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        # Enable WAL mode for concurrency
        await db.execute("PRAGMA journal_mode=WAL;")
        await db.execute("PRAGMA synchronous=NORMAL;")
        
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                full_name TEXT,
                balance REAL DEFAULT 0,
                total_earned REAL DEFAULT 0,
                is_admin INTEGER DEFAULT 0,
                is_analyst INTEGER DEFAULT 0,
                is_manager INTEGER DEFAULT 0,
                analyst_balance REAL DEFAULT 0,
                analyst_total_earned REAL DEFAULT 0,
                manager_balance REAL DEFAULT 0,
                manager_total_earned REAL DEFAULT 0,
                date_joined TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # Migrate existing users
        try:
            await db.execute("ALTER TABLE users ADD COLUMN is_analyst INTEGER DEFAULT 0")
        except: pass
        try:
            await db.execute("ALTER TABLE users ADD COLUMN is_manager INTEGER DEFAULT 0")
        except: pass
        try:
            await db.execute("ALTER TABLE users ADD COLUMN analyst_balance REAL DEFAULT 0")
        except: pass
        try:
            await db.execute("ALTER TABLE users ADD COLUMN analyst_total_earned REAL DEFAULT 0")
        except: pass
        try:
            await db.execute("ALTER TABLE users ADD COLUMN manager_balance REAL DEFAULT 0")
        except: pass
        try:
            await db.execute("ALTER TABLE users ADD COLUMN manager_total_earned REAL DEFAULT 0")
        except: pass
        await db.execute("""
            CREATE TABLE IF NOT EXISTS clients (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                worker_id INTEGER,
                name TEXT,
                total_squeezed REAL DEFAULT 0
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS profits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                worker_id INTEGER,
                client_id INTEGER,
                amount REAL,
                worker_share REAL,
                analyst_id INTEGER,
                analyst_share REAL,
                analyst_percent REAL,
                manager_id INTEGER,
                manager_share REAL,
                manager_percent REAL,
                direction TEXT,
                stage TEXT,
                percent REAL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # Migrate existing profits
        try:
            await db.execute("ALTER TABLE profits ADD COLUMN analyst_id INTEGER")
        except: pass
        try:
            await db.execute("ALTER TABLE profits ADD COLUMN analyst_share REAL DEFAULT 0")
        except: pass
        try:
            await db.execute("ALTER TABLE profits ADD COLUMN analyst_percent REAL")
        except: pass
        try:
            await db.execute("ALTER TABLE profits ADD COLUMN manager_id INTEGER")
        except: pass
        try:
            await db.execute("ALTER TABLE profits ADD COLUMN manager_share REAL DEFAULT 0")
        except: pass
        try:
            await db.execute("ALTER TABLE profits ADD COLUMN manager_percent REAL")
        except: pass
        await db.execute("""
            CREATE TABLE IF NOT EXISTS payouts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                worker_id INTEGER,
                check_code TEXT,
                amount REAL,
                is_received INTEGER DEFAULT 0,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # Миграция: добавляем поле is_received если его нет
        try:
            await db.execute("ALTER TABLE payouts ADD COLUMN is_received INTEGER DEFAULT 0")
        except: pass
        await db.commit()

# --- 🛠️ HELPERS ---
async def get_db_user(user_id):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)) as cursor:
            return await cursor.fetchone()

async def is_admin(user_id):
    user = await get_db_user(user_id)
    if not user:
        return False
    # Find is_admin index dynamically or use known position
    # Schema: user_id, username, full_name, balance, total_earned, is_admin, is_analyst, is_manager, ...
    return bool(user[5] if len(user) > 5 else False)

async def send_screen(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, photo_key: str = None, markup=None):
    """
    Robust message sender for PTB. Handles edits vs new messages and image presence.
    """
    query = update.callback_query
    message = query.message if query else update.message
    
    file_path = IMG_PATHS.get(photo_key)
    has_photo = file_path and os.path.exists(file_path)
    
    # Debug logging
    logger.info(f"Photo key: {photo_key}")
    logger.info(f"File path: {file_path}")
    logger.info(f"Has photo: {has_photo}")
    
    is_edit = bool(query)

    try:
        if is_edit:
            if has_photo:
                # If message already has photo, edit media
                if message.photo:
                    with open(file_path, 'rb') as photo_file:
                        media = InputMediaPhoto(photo_file, caption=text, parse_mode=ParseMode.HTML)
                        await message.edit_media(media=media, reply_markup=markup)
                else:
                    # Message had no photo, delete and send new
                    await message.delete()
                    with open(file_path, 'rb') as photo_file:
                        await message.reply_photo(photo=photo_file, caption=text, reply_markup=markup, parse_mode=ParseMode.HTML)
            else:
                # No photo needed
                if message.photo:
                    await message.delete()
                    await message.reply_text(text, reply_markup=markup, parse_mode=ParseMode.HTML)
                else:
                    await message.edit_text(text, reply_markup=markup, parse_mode=ParseMode.HTML)
        else:
            # New message
            if has_photo:
                with open(file_path, 'rb') as photo_file:
                    await message.reply_photo(photo=photo_file, caption=text, reply_markup=markup, parse_mode=ParseMode.HTML)
            else:
                await message.reply_text(text, reply_markup=markup, parse_mode=ParseMode.HTML)
    except BadRequest as e:
        if "Message is not modified" in str(e):
            pass
        else:
            logger.error(f"Send Screen Error: {e}")
            if not is_edit:
                await message.reply_text(text, reply_markup=markup, parse_mode=ParseMode.HTML)

def get_main_menu_kb(is_admin_flag: bool):
    keyboard = [
        [InlineKeyboardButton("🦣 Мои Мамонты", callback_data="menu_clients_0")],
        [
            InlineKeyboardButton("💳 Выплаты и Профиты", callback_data="menu_finances")
        ],
        [
            InlineKeyboardButton("🏆 Топы и Аналитика", callback_data="menu_tops_analytics")
        ]
    ]
    if is_admin_flag:
        keyboard.append([InlineKeyboardButton("⚡️ ADMIN PANEL", callback_data="admin_dashboard")])
    
    keyboard.append([InlineKeyboardButton("🔄 Обновить данные", callback_data="menu_main")])
    return InlineKeyboardMarkup(keyboard)

def get_back_kb(target="menu_main"):
    return InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=target)]])

# --- 🚦 CONVERSATION STATES ---
AUTH_PWD = 1
PROF_WORKER, PROF_CLIENT, PROF_AMOUNT, PROF_DIR, PROF_STAGE, PROF_PERCENT, PROF_ANALYST, PROF_ANALYST_PERCENT, PROF_MANAGER, PROF_MANAGER_PERCENT, PROF_CONFIRM = range(2, 13)
PAY_CHECK, PAY_CONFIRM = range(13, 15)
USER_SEARCH = 15
USER_ROLE_EDIT = 16

# --- 🎮 HANDLERS ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    db_user = await get_db_user(user.id)
    
    # Auto-promote env admins
    if user.id in ADMIN_IDS:
        async with aiosqlite.connect(DB_NAME) as db:
            if not db_user:
                await db.execute("INSERT INTO users (user_id, username, full_name, is_admin) VALUES (?, ?, ?, 1)",
                                 (user.id, user.username or "Anon", user.full_name))
            else:
                await db.execute("UPDATE users SET is_admin = 1 WHERE user_id = ?", (user.id,))
            await db.commit()
        db_user = await get_db_user(user.id)

    if db_user:
        is_admin_flag = bool(db_user[5] if len(db_user) > 5 else False)
        roles = []
        if len(db_user) > 6 and db_user[6]: roles.append("Аналитик")
        if len(db_user) > 7 and db_user[7]: roles.append("Менеджер")
        role_str = f" ({', '.join(roles)})" if roles else ""
        
        # Объединяем приветствие со статистикой
        user_id = user.id
        async with aiosqlite.connect(DB_NAME) as db:
            async with db.execute("""
                SELECT full_name, total_earned, balance, is_analyst, is_manager,
                       analyst_balance, analyst_total_earned,
                       manager_balance, manager_total_earned
                FROM users WHERE user_id = ?
            """, (user_id,)) as cursor:
                user_data = await cursor.fetchone()
            
            now = datetime.now()
            month_start = now.replace(day=1, hour=0, minute=0, second=0)
            
            # Worker stats
            async with db.execute("SELECT SUM(worker_share) FROM profits WHERE worker_id = ? AND timestamp >= ?", (user_id, month_start)) as cursor:
                month_profit = (await cursor.fetchone())[0] or 0.0
            async with db.execute("SELECT COUNT(*) FROM clients WHERE worker_id = ?", (user_id,)) as cursor:
                clients_count = (await cursor.fetchone())[0]
            
            # Analyst stats
            month_analyst_profit = 0.0
            if user_data[3]:  # is_analyst
                async with db.execute("SELECT SUM(analyst_share) FROM profits WHERE analyst_id = ? AND timestamp >= ?", (user_id, month_start)) as cursor:
                    month_analyst_profit = (await cursor.fetchone())[0] or 0.0
            
            # Manager stats
            month_manager_profit = 0.0
            if user_data[4]:  # is_manager
                async with db.execute("SELECT SUM(manager_share) FROM profits WHERE manager_id = ? AND timestamp >= ?", (user_id, month_start)) as cursor:
                    month_manager_profit = (await cursor.fetchone())[0] or 0.0

        earned = user_data[1]
        if earned < 100: rank = "Новичок 🐣"
        elif earned < 1000: rank = "Бывалый 👊"
        elif earned < 5000: rank = "Хищник 🦈"
        elif earned < 10000: rank = "Машина 🤖"
        else: rank = "Легенда 👑"

        now = datetime.now()
        week_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        week_start = week_start.replace(day=week_start.day - week_start.weekday())
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        
        async with aiosqlite.connect(DB_NAME) as db:
            async with db.execute("SELECT SUM(worker_share) FROM profits WHERE worker_id = ? AND timestamp >= ?", (user_id, day_start)) as cursor:
                day_profit = (await cursor.fetchone())[0] or 0.0
            async with db.execute("SELECT SUM(worker_share) FROM profits WHERE worker_id = ? AND timestamp >= ?", (user_id, week_start)) as cursor:
                week_profit = (await cursor.fetchone())[0] or 0.0

        text = (
            f"<b>👋 Добро пожаловать, {user.first_name}!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>🖥 Рабочее пространство:</b> <code>Active</code>\n"
            f"<b>🛡 Статус:</b> {'👨‍💻 Администратор' if is_admin_flag else '👤 Воркер'}{role_str}\n"
            f"<b>📅 Дата:</b> <code>{datetime.now().strftime('%d.%m.%Y %H:%M')}</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>📊 ЛИЧНЫЙ КАБИНЕТ</b>\n"
            f"<b>👤 {user_data[0]}</b> | <b>🏆 {rank}</b>\n"
            f"<b>💰 ВОРКЕР:</b> <code>${user_data[2]:,.2f}</code> к выплате | <code>${user_data[1]:,.2f}</code> всего\n"
            f"<b>📊 Профит:</b> Месяц <code>${month_profit:,.2f}</code> | Неделя <code>${week_profit:,.2f}</code> | День <code>${day_profit:,.2f}</code>\n"
        )
        
        if user_data[3]:  # is_analyst
            async with aiosqlite.connect(DB_NAME) as db:
                async with db.execute("SELECT SUM(analyst_share) FROM profits WHERE analyst_id = ? AND timestamp >= ?", (user_id, day_start)) as cursor:
                    day_analyst = (await cursor.fetchone())[0] or 0.0
                async with db.execute("SELECT SUM(analyst_share) FROM profits WHERE analyst_id = ? AND timestamp >= ?", (user_id, week_start)) as cursor:
                    week_analyst = (await cursor.fetchone())[0] or 0.0
            
            text += (
                f"<b>🔬 АНАЛИТИК:</b> <code>${user_data[5]:,.2f}</code> к выплате | <code>${user_data[6]:,.2f}</code> всего\n"
            )
        
        if user_data[4]:  # is_manager
            async with aiosqlite.connect(DB_NAME) as db:
                async with db.execute("SELECT SUM(manager_share) FROM profits WHERE manager_id = ? AND timestamp >= ?", (user_id, day_start)) as cursor:
                    day_manager = (await cursor.fetchone())[0] or 0.0
                async with db.execute("SELECT SUM(manager_share) FROM profits WHERE manager_id = ? AND timestamp >= ?", (user_id, week_start)) as cursor:
                    week_manager = (await cursor.fetchone())[0] or 0.0
            
            text += (
                f"<b>👔 МЕНЕДЖЕР:</b> <code>${user_data[7]:,.2f}</code> к выплате | <code>${user_data[8]:,.2f}</code> всего\n"
            )
        
        text += (
            f"<b>🦣 Мамонтов:</b> <code>{clients_count}</code>\n"
        )
        
        # Прогресс до следующего ранга
        next_rank_threshold = 100 if earned < 100 else (1000 if earned < 1000 else (5000 if earned < 5000 else (10000 if earned < 10000 else float('inf'))))
        if next_rank_threshold != float('inf'):
            progress = (earned / next_rank_threshold) * 100
            progress_bar = "█" * int(progress / 5) + "░" * (20 - int(progress / 5))
            text += f"<b>📈 До ранга:</b> <code>{progress:.1f}%</code> <code>{progress_bar}</code>\n"
        
        await send_screen(update, context, text, "profile", get_main_menu_kb(is_admin_flag))
        return ConversationHandler.END
    else:
        text = (
            f"<b>⛔️ ACCESS DENIED</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"<b>🔒 Система закрытого доступа</b>\n\n"
            f"Ваша учетная запись не обнаружена в базе данных.\n\n"
            f"<i>⬇️ Введите ключ доступа для активации рабочего места:</i>"
        )
        await update.message.reply_text(text, parse_mode=ParseMode.HTML)
        return AUTH_PWD

async def auth_password(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    user = update.effective_user
    
    try: await msg.delete()
    except: pass
    
    if msg.text == ACCESS_PASSWORD:
        is_admin_flag = 1 if user.id in ADMIN_IDS else 0
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute(
                "INSERT OR IGNORE INTO users (user_id, username, full_name, is_admin) VALUES (?, ?, ?, ?)",
                (user.id, user.username or "Anon", user.full_name, is_admin_flag)
            )
            await db.commit()
        
        # Показываем приветствие со статистикой (как в start)
        await start(update, context)
        return ConversationHandler.END
    else:
        reply = await msg.reply_text(
            "<b>❌ Неверный пароль</b>\n\n<i>Попробуйте еще раз или обратитесь к администратору.</i>",
            parse_mode=ParseMode.HTML
        )
        await asyncio.sleep(3)
        try: await reply.delete()
        except: pass
        return AUTH_PWD

async def auth_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "<b>🚫 Авторизация отменена</b>\n\n<i>Для доступа к боту используйте команду /start</i>",
        parse_mode=ParseMode.HTML
    )
    return ConversationHandler.END

# --- 📊 MENU CALLBACKS ---
async def menu_main(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await start(update, context)

async def menu_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Теперь статистика показывается в start, эта функция просто возвращает в главное меню
    await start(update, context)

async def menu_clients(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    page = int(query.data.split("_")[-1])
    user_id = update.effective_user.id
    limit = 6
    offset = page * limit

    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT id, name, total_squeezed FROM clients WHERE worker_id = ? ORDER BY total_squeezed DESC LIMIT ? OFFSET ?", (user_id, limit, offset)) as cursor:
            clients = await cursor.fetchall()
        async with db.execute("SELECT COUNT(*) FROM clients WHERE worker_id = ?", (user_id,)) as cursor:
            total_count = (await cursor.fetchone())[0]

    keyboard = []
    for c in clients:
        keyboard.append([InlineKeyboardButton(f"{c[1]} | ${c[2]:.0f}", callback_data=f"client_view_{c[0]}")])
    
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("⬅️", callback_data=f"menu_clients_{page-1}"))
    if offset + limit < total_count:
        nav_row.append(InlineKeyboardButton("➡️", callback_data=f"menu_clients_{page+1}"))
    if nav_row:
        keyboard.append(nav_row)
    
    keyboard.append([InlineKeyboardButton("🔙 Меню", callback_data="menu_main")])

    text = (
        f"<b>🦣 ВАШИ КЛИЕНТЫ</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>📊 Всего мамонтов:</b> <code>{total_count}</code>\n\n"
        f"<i>👇 Нажмите на мамонта для просмотра детальной истории:</i>"
    )
    await send_screen(update, context, text, None, InlineKeyboardMarkup(keyboard))

async def client_view(update: Update, context: ContextTypes.DEFAULT_TYPE):
    client_id = int(update.callback_query.data.split("_")[-1])
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT name, total_squeezed FROM clients WHERE id = ?", (client_id,)) as cursor:
            client = await cursor.fetchone()
        async with db.execute("SELECT amount, stage, timestamp, direction FROM profits WHERE client_id = ? ORDER BY timestamp DESC LIMIT 5", (client_id,)) as cursor:
            history = await cursor.fetchall()

    text = (
        f"<b>👤 МАМОНТ</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>Имя:</b> <b>{client[0]}</b>\n"
        f"<b>💵 Общий профит:</b> <code>${client[1]:,.2f}</code>\n\n"
        f"<b>🕰 ИСТОРИЯ (Последние 5)</b>\n"
    )
    if not history:
        text += "<i>▫️ Транзакций нет</i>\n"
    else:
        for idx, h in enumerate(history, 1):
            date_str = datetime.strptime(h[2], "%Y-%m-%d %H:%M:%S").strftime("%d.%m %H:%M")
            dir_emoji = {"BTC": "₿", "USDT": "💵", "Card": "💳"}.get(h[3], "💰")
            stage_emoji = {"Депозит": "💸", "Комиссия": "💼", "Налог": "📋"}.get(h[1], "📊")
            text += (
                f"<b>{idx}.</b> <code>${h[0]:,.2f}</code>\n"
                f"   ├ <b>🏦 Направление:</b> {dir_emoji} <b>{h[3]}</b>\n"
                f"   ├ <b>📑 Стадия:</b> {stage_emoji} <b>{h[1]}</b>\n"
                f"   └ <b>📅 {date_str}</b>\n\n"
            )

    await send_screen(update, context, text, None, InlineKeyboardMarkup([[InlineKeyboardButton("🔙 К списку", callback_data="menu_clients_0")]]))

async def menu_profits(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    period = context.user_data.get('profit_period', 'all')
    
    if query and query.data.startswith("profit_period_"):
        period = query.data.split("_")[-1]
        context.user_data['profit_period'] = period
    
    user_id = update.effective_user.id
    now = datetime.now()
    
    # Определяем период
    if period == 'day':
        start_time = now.replace(hour=0, minute=0, second=0, microsecond=0)
        period_name = "🌅 ЗА ДЕНЬ"
    elif period == 'week':
        week_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        week_start = week_start.replace(day=week_start.day - week_start.weekday())
        start_time = week_start
        period_name = "📆 ЗА НЕДЕЛЮ"
    elif period == 'month':
        start_time = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        period_name = "📅 ЗА МЕСЯЦ"
    else:
        start_time = None
        period_name = "📈 ВСЕ ВРЕМЯ"
    
    async with aiosqlite.connect(DB_NAME) as db:
        if start_time:
            query_sql = """
                SELECT p.amount, p.worker_share, p.stage, c.name, p.direction, p.timestamp,
                       p.analyst_share, p.manager_share
                FROM profits p
                JOIN clients c ON p.client_id = c.id
                WHERE p.worker_id = ? AND p.timestamp >= ?
                ORDER BY p.timestamp DESC LIMIT 20
            """
            params = (user_id, start_time)
        else:
            query_sql = """
                SELECT p.amount, p.worker_share, p.stage, c.name, p.direction, p.timestamp,
                       p.analyst_share, p.manager_share
            FROM profits p
            JOIN clients c ON p.client_id = c.id
            WHERE p.worker_id = ?
                ORDER BY p.timestamp DESC LIMIT 20
            """
            params = (user_id,)
        
        async with db.execute(query_sql, params) as cursor:
            profits = await cursor.fetchall()

        # Статистика
        if start_time:
            stats_sql = "SELECT SUM(worker_share), COUNT(*) FROM profits WHERE worker_id = ? AND timestamp >= ?"
            stats_params = (user_id, start_time)
        else:
            stats_sql = "SELECT SUM(worker_share), COUNT(*) FROM profits WHERE worker_id = ?"
            stats_params = (user_id,)
        
        async with db.execute(stats_sql, stats_params) as cursor:
            stats = await cursor.fetchone()
            total_profit = stats[0] or 0.0
            count = stats[1] or 0

    # Дополнительная статистика
    avg_profit = total_profit / count if count > 0 else 0
    max_profit = 0
    if profits:
        max_profit = max(p[1] for p in profits)
    
    text = (
        f"<b>📈 ЛОГ ПРОФИТОВ</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>{period_name}</b>\n"
        f"<b>📊 Статистика:</b> <code>{count}</code> профитов | <code>${total_profit:,.2f}</code> всего | Средний <code>${avg_profit:,.2f}</code> | Макс <code>${max_profit:,.2f}</code>\n"
        f"<b>📋 ПОСЛЕДНИЕ ЗАПИСИ</b>\n"
    )
    
    if not profits:
        text += "<i>▫️ Записей нет</i>\n"
    else:
        for idx, p in enumerate(profits[:10], 1):
            date_str = datetime.strptime(p[5], "%Y-%m-%d %H:%M:%S").strftime("%d.%m %H:%M")
            # Эмодзи для типа направления
            dir_emoji = {"BTC": "₿", "USDT": "💵", "Card": "💳"}.get(p[4], "💰")
            # Эмодзи для стадии
            stage_emoji = {"Депозит": "💸", "Комиссия": "💼", "Налог": "📋"}.get(p[2], "📊")
            
            text += (
                f"<b>{idx}.</b> <code>+${p[1]:,.2f}</code> | {dir_emoji} <b>{p[4]}</b> | {stage_emoji} <b>{p[2]}</b>\n"
                f"   <b>🦣 {p[3]}</b> | <b>💵 ${p[0]:,.2f}</b>"
            )
            if p[6] and p[6] > 0:
                text += f" | <b>🔬 ${p[6]:,.2f}</b>"
            if p[7] and p[7] > 0:
                text += f" | <b>👔 ${p[7]:,.2f}</b>"
            text += f" | <b>📅 {date_str}</b>\n"
    
    keyboard = [
        [InlineKeyboardButton("💳 Перейти к Истории Выплат", callback_data="finances_payouts")],
        [
            InlineKeyboardButton("📅 Месяц", callback_data="profit_period_month"),
            InlineKeyboardButton("📈 Все время", callback_data="profit_period_all")
        ],
        [
            InlineKeyboardButton("🌅 День", callback_data="profit_period_day"),
            InlineKeyboardButton("📆 Неделя", callback_data="profit_period_week"),
            InlineKeyboardButton("📊 Детали", callback_data="profit_detailed")
        ],
        [InlineKeyboardButton("🔙 Назад", callback_data="menu_main")]
    ]
    
    await send_screen(update, context, text, None, InlineKeyboardMarkup(keyboard))

async def menu_salary(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    period = context.user_data.get('salary_period', 'all')
    
    if query and query.data.startswith("salary_period_"):
        period = query.data.split("_")[-1]
        context.user_data['salary_period'] = period
    
    user_id = update.effective_user.id
    now = datetime.now()
    
    # Определяем период
    if period == 'day':
        start_time = now.replace(hour=0, minute=0, second=0, microsecond=0)
        period_name = "🌅 ЗА ДЕНЬ"
    elif period == 'week':
        week_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        week_start = week_start.replace(day=week_start.day - week_start.weekday())
        start_time = week_start
        period_name = "📆 ЗА НЕДЕЛЮ"
    elif period == 'month':
        start_time = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        period_name = "📅 ЗА МЕСЯЦ"
    else:
        start_time = None
        period_name = "💰 ВСЕ ВРЕМЯ"
    
    async with aiosqlite.connect(DB_NAME) as db:
        # Получаем неполученные чеки
        async with db.execute("""
            SELECT id, check_code, amount, timestamp 
            FROM payouts 
            WHERE worker_id = ? AND is_received = 0
            ORDER BY timestamp DESC
        """, (user_id,)) as cursor:
            pending_payouts = await cursor.fetchall()
        
        # Получаем полученные чеки по периоду
        if start_time:
            async with db.execute("""
                SELECT id, check_code, amount, timestamp, is_received
                FROM payouts 
                WHERE worker_id = ? AND timestamp >= ? AND is_received = 1
                ORDER BY timestamp DESC LIMIT 20
            """, (user_id, start_time)) as cursor:
                payouts = await cursor.fetchall()
            async with db.execute("SELECT SUM(amount), COUNT(*) FROM payouts WHERE worker_id = ? AND timestamp >= ? AND is_received = 1", (user_id, start_time)) as cursor:
                stats = await cursor.fetchone()
                total_paid = stats[0] or 0.0
                count = stats[1] or 0
        else:
            async with db.execute("""
                SELECT id, check_code, amount, timestamp, is_received
                FROM payouts 
                WHERE worker_id = ? AND is_received = 1
                ORDER BY timestamp DESC LIMIT 20
            """, (user_id,)) as cursor:
                payouts = await cursor.fetchall()
            async with db.execute("SELECT SUM(amount), COUNT(*) FROM payouts WHERE worker_id = ? AND is_received = 1", (user_id,)) as cursor:
                stats = await cursor.fetchone()
                total_paid = stats[0] or 0.0
                count = stats[1] or 0

    # Дополнительная статистика
    avg_payout = total_paid / count if count > 0 else 0
    max_payout = 0
    if payouts:
        max_payout = max(p[2] for p in payouts)
    
    # Статистика по частоте выплат
    last_payout_date = None
    if payouts:
        last_payout_date = datetime.strptime(payouts[0][3], "%Y-%m-%d %H:%M:%S")
        days_since = (now - last_payout_date).days
    
    text = (
        f"<b>💰 ИСТОРИЯ ВЫПЛАТ</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>{period_name}</b>\n"
    )
    
    # Показываем неполученные чеки
    if pending_payouts:
        text += f"<b>⏳ НЕПОЛУЧЕННЫЕ:</b> "
        for p in pending_payouts:
            date_str = datetime.strptime(p[3], "%Y-%m-%d %H:%M:%S").strftime("%d.%m %H:%M")
            payout_emoji = "💎" if p[2] >= 5000 else ("💵" if p[2] >= 1000 else "💰")
            text += f"{payout_emoji} <code>${p[2]:,.2f}</code> ({date_str}) "
        text += "\n"
    
    text += (
        f"<b>📊 Статистика:</b> <code>{count}</code> выплат | <code>${total_paid:,.2f}</code> всего | Средняя <code>${avg_payout:,.2f}</code> | Макс <code>${max_payout:,.2f}</code>\n"
    )
    if last_payout_date:
        text += f"<b>📅 Последняя:</b> <code>{days_since} дн. назад</code>\n"
    
    text += f"<b>📋 ПОСЛЕДНИЕ ВЫПЛАТЫ</b>\n"
    
    if not payouts:
        text += "<i>▫️ Полученных выплат пока не было</i>\n"
    else:
        for idx, p in enumerate(payouts[:10], 1):
            date_str = datetime.strptime(p[3], "%Y-%m-%d %H:%M:%S").strftime("%d.%m %H:%M")
            payout_emoji = "💎" if p[2] >= 5000 else ("💵" if p[2] >= 1000 else "💰")
            text += (
                f"<b>{idx}.</b> {payout_emoji} <code>${p[2]:,.2f}</code> | <code>{date_str}</code> | <code>{p[1]}</code>\n"
            )
    
    keyboard = []
    
    # Добавляем кнопки для неполученных чеков
    if pending_payouts:
        for p in pending_payouts:
            date_str = datetime.strptime(p[3], "%Y-%m-%d %H:%M:%S").strftime("%d.%m %H:%M")
            keyboard.append([InlineKeyboardButton(
                f"✅ Получить ${p[2]:,.0f} | {date_str}", 
                callback_data=f"receive_payout_{p[0]}"
            )])
    
    keyboard.extend([
        [InlineKeyboardButton("📈 Перейти к Логу Профитов", callback_data="finances_profits")],
        [
            InlineKeyboardButton("📅 Месяц", callback_data="salary_period_month"),
            InlineKeyboardButton("💰 Все время", callback_data="salary_period_all")
        ],
        [
            InlineKeyboardButton("🌅 День", callback_data="salary_period_day"),
            InlineKeyboardButton("📆 Неделя", callback_data="salary_period_week"),
            InlineKeyboardButton("📊 График", callback_data="salary_chart")
        ],
        [InlineKeyboardButton("🔙 Назад", callback_data="menu_main")]
    ])
    
    await send_screen(update, context, text, "pay", InlineKeyboardMarkup(keyboard))

# --- 💳 ОБЪЕДИНЕННОЕ МЕНЮ ФИНАНСОВ ---
async def menu_finances(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    section = 'payouts'  # По умолчанию показываем выплаты
    
    if query and query.data.startswith("finances_"):
        section = query.data.split("_")[-1]
        context.user_data['finances_section'] = section
    else:
        section = context.user_data.get('finances_section', 'payouts')
    
    if section == 'profits':
        await menu_profits(update, context)
    else:
        await menu_salary(update, context)

# --- 🏆 TOPS ---
async def menu_tops(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    top_type = context.user_data.get('top_type', 'all')
    
    if query and query.data.startswith("top_"):
        top_type = query.data.split("_")[-1]
        context.user_data['top_type'] = top_type
    
    now = datetime.now()
    
    # Определяем период
    if top_type == 'day':
        start_time = now.replace(hour=0, minute=0, second=0, microsecond=0)
        period_name = "🌅 ТОП ЗА ДЕНЬ"
    elif top_type == 'week':
        week_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        week_start = week_start.replace(day=week_start.day - week_start.weekday())
        start_time = week_start
        period_name = "📆 ТОП ЗА НЕДЕЛЮ"
    elif top_type == 'month':
        start_time = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        period_name = "📅 ТОП ЗА МЕСЯЦ"
    else:
        start_time = None
        period_name = "🏆 ТОП ЗА ВСЕ ВРЕМЯ"
    
    async with aiosqlite.connect(DB_NAME) as db:
        if start_time:
            query_sql = """
                SELECT u.full_name, SUM(p.worker_share) as total
                FROM users u
                JOIN profits p ON u.user_id = p.worker_id
                WHERE p.timestamp >= ?
                GROUP BY u.user_id
                ORDER BY total DESC
                LIMIT 10
            """
            params = (start_time,)
        else:
            query_sql = """
                SELECT u.full_name, u.total_earned as total
                FROM users u
                ORDER BY u.total_earned DESC
                LIMIT 10
            """
            params = ()
        
        async with db.execute(query_sql, params) as cursor:
            tops = await cursor.fetchall()

    text = (
        f"<b>{period_name}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    )
    
    if not tops:
        text += "<i>▫️ Данных пока нет</i>\n"
    else:
        medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]
        user_id = update.effective_user.id
        
        # Находим позицию текущего пользователя
        user_position = None
        user_total = None
        if start_time:
            async with aiosqlite.connect(DB_NAME) as db:
                async with db.execute("""
                    SELECT SUM(p.worker_share) as total
                    FROM profits p
                    WHERE p.worker_id = ? AND p.timestamp >= ?
                """, (user_id, start_time)) as cursor:
                    result = await cursor.fetchone()
                    user_total = result[0] or 0
        else:
            async with aiosqlite.connect(DB_NAME) as db:
                async with db.execute("SELECT total_earned FROM users WHERE user_id = ?", (user_id,)) as cursor:
                    result = await cursor.fetchone()
                    user_total = result[0] if result else 0
        
        # Находим позицию
        for idx, (_, total) in enumerate(tops, 1):
            if user_total > 0 and total <= user_total:
                user_position = idx
                break
        
        for idx, (name, total) in enumerate(tops, 1):
            medal = medals[idx-1] if idx <= 10 else f"{idx}."
            is_current_user = False
            async with aiosqlite.connect(DB_NAME) as db:
                async with db.execute("SELECT user_id FROM users WHERE full_name = ?", (name,)) as cursor:
                    uid_result = await cursor.fetchone()
                    if uid_result and uid_result[0] == user_id:
                        is_current_user = True
            
            if is_current_user:
                text += (
                    f"{medal} <b><u>{name}</u></b> <i>← Вы</i> <code>${total:,.2f}</code>\n"
                )
            else:
                text += (
                    f"{medal} <b>{name}</b> <code>${total:,.2f}</code>\n"
                )
        
        # Показываем позицию пользователя, если он не в топе
        if user_position is None and user_total > 0:
            # Подсчитываем реальную позицию
            async with aiosqlite.connect(DB_NAME) as db:
                if start_time:
                    async with db.execute("""
                        SELECT COUNT(*) + 1
                        FROM (
                            SELECT u.user_id, SUM(p.worker_share) as total
                            FROM users u
                            JOIN profits p ON u.user_id = p.worker_id
                            WHERE p.timestamp >= ?
                            GROUP BY u.user_id
                            HAVING total > ?
                        )
                    """, (start_time, user_total)) as cursor:
                        real_position = (await cursor.fetchone())[0]
                else:
                    async with db.execute("""
                        SELECT COUNT(*) + 1
                        FROM users
                        WHERE total_earned > ?
                    """, (user_total,)) as cursor:
                        real_position = (await cursor.fetchone())[0]
            
            text += f"<b>📍 Ваша позиция:</b> <code>#{real_position}</code> | <code>${user_total:,.2f}</code>\n"
    
    keyboard = [
        [InlineKeyboardButton("📊 Перейти к Аналитике", callback_data="tops_analytics_analytics")],
        [
            InlineKeyboardButton("📅 Месяц", callback_data="top_month"),
            InlineKeyboardButton("🏆 Все время", callback_data="top_all")
        ],
        [
            InlineKeyboardButton("🌅 День", callback_data="top_day"),
            InlineKeyboardButton("📆 Неделя", callback_data="top_week"),
            InlineKeyboardButton("🔬 Аналитики", callback_data="top_analysts")
        ],
        [InlineKeyboardButton("🔙 Назад", callback_data="menu_main")]
    ]
    
    await send_screen(update, context, text, None, InlineKeyboardMarkup(keyboard))

# --- 📊 NEW MENU HANDLERS ---
async def menu_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        f"<b>ℹ️ ПОМОЩЬ</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"<b>📖 ОСНОВНЫЕ РАЗДЕЛЫ</b>\n\n"
        f"<b>📊 Моя Статистика</b>\n"
        f"Просмотр вашего баланса, профитов и ранга\n\n"
        f"<b>🦣 Мои Мамонты</b>\n"
        f"Список всех ваших клиентов и история работы\n\n"
        f"<b>💳 История Выплат</b>\n"
        f"Все полученные выплаты с фильтрацией по периодам\n\n"
        f"<b>📈 Лог Профитов</b>\n"
        f"Детальная история всех ваших профитов\n\n"
        f"<b>🏆 Топы</b>\n"
        f"Рейтинги воркеров за разные периоды\n\n"
        f"<b>📊 Аналитика</b>\n"
        f"Подробная статистика по вашей работе\n\n"
        f"<b>💡 СОВЕТЫ</b>\n"
        f"• Используйте фильтры по периодам для анализа\n"
        f"• Следите за своим прогрессом до следующего ранга\n"
        f"• Изучайте аналитику для улучшения результатов\n\n"
        f"<i>❓ Если возникли вопросы, обратитесь к администратору</i>"
    )
    keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="menu_main")]]
    await send_screen(update, context, text, None, InlineKeyboardMarkup(keyboard))

async def menu_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user = await get_db_user(user_id)
    
    text = (
        f"<b>⚙️ НАСТРОЙКИ</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"<b>🔔 УВЕДОМЛЕНИЯ</b>\n"
        f"├ <b>Новые профиты:</b> <code>Включены</code>\n"
        f"├ <b>Выплаты:</b> <code>Включены</code>\n"
        f"└ <b>Обновления:</b> <code>Включены</code>\n\n"
        f"<b>📊 ОТОБРАЖЕНИЕ</b>\n"
        f"├ <b>Формат даты:</b> <code>ДД.ММ.ГГГГ</code>\n"
        f"├ <b>Валюта:</b> <code>USD ($)</code>\n"
        f"└ <b>Язык:</b> <code>Русский</code>\n\n"
        f"<b>👤 ПРОФИЛЬ</b>\n"
        f"├ <b>ID:</b> <code>{user_id}</code>\n"
        f"└ <b>Имя:</b> <b>{user[2] if user else 'Неизвестно'}</b>\n"
    )
    
    keyboard = [
        [InlineKeyboardButton("🔔 Настройки уведомлений", callback_data="settings_notifications")],
        [InlineKeyboardButton("📊 Настройки отображения", callback_data="settings_display")],
        [InlineKeyboardButton("🔙 Назад", callback_data="menu_main")]
    ]
    await send_screen(update, context, text, None, InlineKeyboardMarkup(keyboard))

async def menu_stats_detailed(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("""
            SELECT full_name, total_earned, balance, is_analyst, is_manager,
                   analyst_balance, analyst_total_earned,
                   manager_balance, manager_total_earned, date_joined
            FROM users WHERE user_id = ?
        """, (user_id,)) as cursor:
            user_data = await cursor.fetchone()
        
        now = datetime.now()
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        week_start = day_start.replace(day=day_start.day - day_start.weekday())
        month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        
        # Детальная статистика по периодам
        periods = [
            ("День", day_start),
            ("Неделя", week_start),
            ("Месяц", month_start),
            ("Все время", None)
        ]
        
        text = (
            f"<b>📊 ДЕТАЛЬНАЯ СТАТИСТИКА</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>👤 {user_data[0]}</b>\n\n"
        )
        
        for period_name, start_time in periods:
            if start_time:
                async with db.execute("""
                    SELECT SUM(worker_share), COUNT(*), AVG(worker_share), MAX(worker_share)
                    FROM profits WHERE worker_id = ? AND timestamp >= ?
                """, (user_id, start_time)) as cursor:
                    stats = await cursor.fetchone()
            else:
                async with db.execute("""
                    SELECT SUM(worker_share), COUNT(*), AVG(worker_share), MAX(worker_share)
                    FROM profits WHERE worker_id = ?
                """, (user_id,)) as cursor:
                    stats = await cursor.fetchone()
            
            total = stats[0] or 0
            count = stats[1] or 0
            avg = stats[2] or 0
            max_profit = stats[3] or 0
            
            text += (
                f"<b>📅 {period_name}</b>\n"
                f"├ <b>Всего:</b> <code>${total:,.2f}</code>\n"
                f"├ <b>Количество:</b> <code>{count}</code>\n"
                f"├ <b>Средний:</b> <code>${avg:,.2f}</code>\n"
                f"└ <b>Максимальный:</b> <code>${max_profit:,.2f}</code>\n\n"
            )
        
        # Дата регистрации
        if user_data[9]:
            join_date = datetime.strptime(user_data[9], "%Y-%m-%d %H:%M:%S")
            days_active = (now - join_date).days
            text += f"<b>📅 В команде:</b> <code>{days_active} дней</code>\n"
    
    keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="menu_main")]]
    await send_screen(update, context, text, None, InlineKeyboardMarkup(keyboard))

async def profit_detailed(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    period = context.user_data.get('profit_period', 'all')
    now = datetime.now()
    
    if period == 'day':
        start_time = now.replace(hour=0, minute=0, second=0, microsecond=0)
    elif period == 'week':
        week_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        week_start = week_start.replace(day=week_start.day - week_start.weekday())
        start_time = week_start
    elif period == 'month':
        start_time = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    else:
        start_time = None
    
    async with aiosqlite.connect(DB_NAME) as db:
        # Статистика по направлениям
        if start_time:
            async with db.execute("""
                SELECT direction, SUM(worker_share), COUNT(*)
                FROM profits WHERE worker_id = ? AND timestamp >= ?
                GROUP BY direction
            """, (user_id, start_time)) as cursor:
                by_direction = await cursor.fetchall()
        else:
            async with db.execute("""
                SELECT direction, SUM(worker_share), COUNT(*)
                FROM profits WHERE worker_id = ?
                GROUP BY direction
            """, (user_id,)) as cursor:
                by_direction = await cursor.fetchall()
        
        text = (
            f"<b>📊 ДЕТАЛЬНАЯ СТАТИСТИКА ПРОФИТОВ</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"<b>🏦 ПО НАПРАВЛЕНИЯМ</b>\n"
        )
        
        for direction, total, count in by_direction:
            dir_emoji = {"BTC": "₿", "USDT": "💵", "Card": "💳"}.get(direction, "💰")
            text += f"├ {dir_emoji} <b>{direction}:</b> <code>${total:,.2f}</code> (<code>{count}</code> залетов)\n"
        
        text += "\n"
    
    keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="menu_profits")]]
    await send_screen(update, context, text, None, InlineKeyboardMarkup(keyboard))

async def profit_export(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer("📥 Функция экспорта в разработке", show_alert=True)

async def salary_chart(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    now = datetime.now()
    
    # Получаем выплаты за последние 30 дней
    month_ago = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("""
            SELECT DATE(timestamp) as date, SUM(amount) as total
            FROM payouts
            WHERE worker_id = ? AND timestamp >= ?
            GROUP BY DATE(timestamp)
            ORDER BY date DESC
            LIMIT 30
        """, (user_id, month_ago)) as cursor:
            daily_payouts = await cursor.fetchall()
    
    if not daily_payouts:
        text = "<b>📊 График выплат</b>\n\n<i>Нет данных за последний месяц</i>"
    else:
        text = (
            f"<b>📊 ГРАФИК ВЫПЛАТ (30 дней)</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        )
        max_amount = max(p[1] for p in daily_payouts) if daily_payouts else 1
        for date_str, amount in daily_payouts[:10]:  # Показываем последние 10
            bar_length = int((amount / max_amount) * 20)
            bar = "█" * bar_length + "░" * (20 - bar_length)
            date_formatted = datetime.strptime(date_str, "%Y-%m-%d").strftime("%d.%m")
            text += f"<code>{date_formatted}</code> <code>{bar}</code> <code>${amount:,.0f}</code>\n"
    
    keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="menu_salary")]]
    await send_screen(update, context, text, None, InlineKeyboardMarkup(keyboard))

async def salary_export(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer("📥 Функция экспорта в разработке", show_alert=True)

async def top_analysts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    now = datetime.now()
    month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("""
            SELECT u.full_name, SUM(p.analyst_share) as total
            FROM users u
            JOIN profits p ON u.user_id = p.analyst_id
            WHERE p.timestamp >= ? AND p.analyst_share > 0
            GROUP BY u.user_id
            ORDER BY total DESC
            LIMIT 10
        """, (month_start,)) as cursor:
            tops = await cursor.fetchall()
    
    text = (
        f"<b>🔬 ТОП АНАЛИТИКОВ (МЕСЯЦ)</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    )
    
    if not tops:
        text += "<i>▫️ Данных пока нет</i>\n"
    else:
        medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]
        for idx, (name, total) in enumerate(tops, 1):
            medal = medals[idx-1] if idx <= 10 else f"{idx}."
            text += f"{medal} <b>{name}</b> - <code>${total:,.2f}</code>\n"
    
    keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="menu_tops")]]
    await send_screen(update, context, text, None, InlineKeyboardMarkup(keyboard))

async def top_managers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    now = datetime.now()
    month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("""
            SELECT u.full_name, SUM(p.manager_share) as total
            FROM users u
            JOIN profits p ON u.user_id = p.manager_id
            WHERE p.timestamp >= ? AND p.manager_share > 0
            GROUP BY u.user_id
            ORDER BY total DESC
            LIMIT 10
        """, (month_start,)) as cursor:
            tops = await cursor.fetchall()
    
    text = (
        f"<b>👔 ТОП МЕНЕДЖЕРОВ (МЕСЯЦ)</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    )
    
    if not tops:
        text += "<i>▫️ Данных пока нет</i>\n"
    else:
        medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]
        for idx, (name, total) in enumerate(tops, 1):
            medal = medals[idx-1] if idx <= 10 else f"{idx}."
            text += f"{medal} <b>{name}</b> - <code>${total:,.2f}</code>\n"
    
    keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="menu_tops")]]
    await send_screen(update, context, text, None, InlineKeyboardMarkup(keyboard))

async def analytics_detailed(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer("📊 Детальная аналитика в разработке", show_alert=True)

# --- 🏆 ОБЪЕДИНЕННОЕ МЕНЮ ТОПОВ И АНАЛИТИКИ ---
async def menu_tops_analytics(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    section = 'tops'  # По умолчанию показываем топы
    
    if query and query.data.startswith("tops_analytics_"):
        section = query.data.split("_")[-1]
        context.user_data['tops_analytics_section'] = section
    else:
        section = context.user_data.get('tops_analytics_section', 'tops')
    
    if section == 'analytics':
        await menu_analytics(update, context)
    else:
        await menu_tops(update, context)

# --- 📊 ANALYTICS ---
async def menu_analytics(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    now = datetime.now()
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    week_start = day_start.replace(day=day_start.day - day_start.weekday())
    month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    
    async with aiosqlite.connect(DB_NAME) as db:
        # Статистика по периодам
        async with db.execute("SELECT SUM(worker_share), COUNT(*) FROM profits WHERE worker_id = ? AND timestamp >= ?", (user_id, day_start)) as cursor:
            day_stats = await cursor.fetchone()
        async with db.execute("SELECT SUM(worker_share), COUNT(*) FROM profits WHERE worker_id = ? AND timestamp >= ?", (user_id, week_start)) as cursor:
            week_stats = await cursor.fetchone()
        async with db.execute("SELECT SUM(worker_share), COUNT(*) FROM profits WHERE worker_id = ? AND timestamp >= ?", (user_id, month_start)) as cursor:
            month_stats = await cursor.fetchone()
        async with db.execute("SELECT SUM(worker_share), COUNT(*) FROM profits WHERE worker_id = ?", (user_id,)) as cursor:
            all_stats = await cursor.fetchone()
        
        # Топ мамонтов
        async with db.execute("""
            SELECT c.name, SUM(p.amount) as total
            FROM clients c
            JOIN profits p ON c.id = p.client_id
            WHERE c.worker_id = ?
            GROUP BY c.id
            ORDER BY total DESC
            LIMIT 5
        """, (user_id,)) as cursor:
            top_clients = await cursor.fetchall()
        
        # Статистика по направлениям
        async with db.execute("""
            SELECT p.direction, SUM(p.worker_share) as total, COUNT(*) as count
            FROM profits p
            WHERE p.worker_id = ?
            GROUP BY p.direction
            ORDER BY total DESC
        """, (user_id,)) as cursor:
            by_direction = await cursor.fetchall()
        
        # Статистика по стадиям
        async with db.execute("""
            SELECT p.stage, SUM(p.worker_share) as total, COUNT(*) as count
            FROM profits p
            WHERE p.worker_id = ?
            GROUP BY p.stage
            ORDER BY total DESC
        """, (user_id,)) as cursor:
            by_stage = await cursor.fetchall()

    # Расчет средних значений
    day_avg = (day_stats[0] or 0) / (day_stats[1] or 1) if day_stats[1] else 0
    week_avg = (week_stats[0] or 0) / (week_stats[1] or 1) if week_stats[1] else 0
    month_avg = (month_stats[0] or 0) / (month_stats[1] or 1) if month_stats[1] else 0
    
    text = (
        f"<b>📊 АНАЛИТИКА</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>📈 ПРОФИТЫ:</b> День <code>${day_stats[0] or 0:,.2f}</code> ({day_stats[1] or 0}) | Неделя <code>${week_stats[0] or 0:,.2f}</code> ({week_stats[1] or 0}) | Месяц <code>${month_stats[0] or 0:,.2f}</code> ({month_stats[1] or 0}) | Всего <code>${all_stats[0] or 0:,.2f}</code> ({all_stats[1] or 0})\n"
    )
    
    if top_clients:
        text += f"<b>🦣 ТОП МАМОНТОВ:</b> "
        medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"]
        for idx, (name, total) in enumerate(top_clients, 1):
            medal = medals[idx-1] if idx <= 5 else f"{idx}."
            text += f"{medal} <b>{name}</b> <code>${total:,.2f}</code> "
        text += "\n"
    
    if by_direction:
        text += f"<b>🏦 НАПРАВЛЕНИЯ:</b> "
        total_dir = sum(total for _, total, _ in by_direction)
        for direction, total, count in by_direction:
            percent = (total / total_dir * 100) if total_dir > 0 else 0
            dir_emoji = {"BTC": "₿", "USDT": "💵", "Card": "💳"}.get(direction, "💰")
            text += f"{dir_emoji} <b>{direction}</b> <code>${total:,.2f}</code> ({count}, {percent:.1f}%) "
        text += "\n"
    
    if by_stage:
        text += f"<b>📑 СТАДИИ:</b> "
        total_stage = sum(total for _, total, _ in by_stage)
        for stage, total, count in by_stage:
            percent = (total / total_stage * 100) if total_stage > 0 else 0
            stage_emoji = {"Депозит": "💸", "Комиссия": "💼", "Налог": "📋"}.get(stage, "📊")
            text += f"{stage_emoji} <b>{stage}</b> <code>${total:,.2f}</code> ({count}, {percent:.1f}%) "
        text += "\n"
    
    keyboard = [
        [InlineKeyboardButton("🏆 Перейти к Топам", callback_data="tops_analytics_tops")],
        [
            InlineKeyboardButton("📊 Детали", callback_data="analytics_detailed"),
            InlineKeyboardButton("🔙 Назад", callback_data="menu_main")
        ]
    ]
    await send_screen(update, context, text, None, InlineKeyboardMarkup(keyboard))

# --- 🔐 ADMIN PANEL ---

async def admin_dashboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id): return

    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT SUM(total_earned), SUM(balance) FROM users") as cursor:
            stats = await cursor.fetchone()
            total_turnover = stats[0] or 0
            total_debt = stats[1] or 0

    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT COUNT(*) FROM users") as cursor:
            users_count = (await cursor.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM clients") as cursor:
            clients_count = (await cursor.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM profits") as cursor:
            profits_count = (await cursor.fetchone())[0]
        now = datetime.now()
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        async with db.execute("SELECT SUM(worker_share + COALESCE(analyst_share, 0) + COALESCE(manager_share, 0)) FROM profits WHERE timestamp >= ?", (day_start,)) as cursor:
            day_turnover = (await cursor.fetchone())[0] or 0.0

    # Дополнительная статистика
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT COUNT(*) FROM users WHERE is_analyst = 1") as cursor:
            analysts_count = (await cursor.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM users WHERE is_manager = 1") as cursor:
            managers_count = (await cursor.fetchone())[0]
        async with db.execute("SELECT SUM(amount) FROM profits WHERE timestamp >= ?", (day_start,)) as cursor:
            day_profits_total = (await cursor.fetchone())[0] or 0.0
    
    text = (
        f"<b>🔐 АДМИН ПАНЕЛЬ</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"<b>💎 ОБОРОТ КОМАНДЫ</b>\n"
        f"├ <b>💰 Всего:</b> <code>${total_turnover:,.2f}</code>\n"
        f"├ <b>🌅 За день:</b> <code>${day_turnover:,.2f}</code>\n"
        f"└ <b>📊 Профитов за день:</b> <code>${day_profits_total:,.2f}</code>\n\n"
        f"<b>🩸 ДОЛГ ПО ЗП</b>\n"
        f"└ <code>${total_debt:,.2f}</code>\n\n"
        f"<b>📊 СТАТИСТИКА КОМАНДЫ</b>\n"
        f"├ <b>👤 Воркеров:</b> <code>{users_count}</code>\n"
        f"├ <b>🔬 Аналитиков:</b> <code>{analysts_count}</code>\n"
        f"├ <b>👔 Менеджеров:</b> <code>{managers_count}</code>\n"
        f"├ <b>🦣 Мамонтов:</b> <code>{clients_count}</code>\n"
        f"└ <b>📈 Профитов:</b> <code>{profits_count}</code>\n"
    )
    keyboard = [
        [InlineKeyboardButton("💵 Внести профит", callback_data="adm_start_profit")],
        [InlineKeyboardButton("💸 Выплатить ЗП", callback_data="adm_start_pay")],
        [InlineKeyboardButton("📋 Список воркеров", callback_data="adm_users_list")],
        [InlineKeyboardButton("🔙 В меню", callback_data="menu_main")]
    ]
    await send_screen(update, context, text, None, InlineKeyboardMarkup(keyboard))

async def adm_users_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    page = 0
    search_query = ""
    
    if query:
        data_parts = query.data.split("_")
        if len(data_parts) >= 3 and data_parts[1] == "page":
            page = int(data_parts[2])
        elif len(data_parts) >= 3 and data_parts[1] == "search":
            search_query = "_".join(data_parts[2:])
            context.user_data['user_search'] = search_query
            page = 0
        elif len(data_parts) >= 3 and data_parts[1] == "list" and len(data_parts) > 3:
            # adm_users_list_0 format
            page = int(data_parts[3]) if data_parts[3].isdigit() else 0
            search_query = context.user_data.get('user_search', "")
        else:
            # adm_users_list format - reset search
            context.user_data['user_search'] = ""
            search_query = ""
    else:
        context.user_data['user_search'] = ""
    
    limit = 10
    offset = page * limit
    
    async with aiosqlite.connect(DB_NAME) as db:
        if search_query:
            search_pattern = f"%{search_query}%"
            async with db.execute("""
                SELECT user_id, full_name, balance, total_earned, is_analyst, is_manager 
                FROM users 
                WHERE full_name LIKE ? OR username LIKE ?
                ORDER BY balance DESC 
                LIMIT ? OFFSET ?
            """, (search_pattern, search_pattern, limit, offset)) as cursor:
                users = await cursor.fetchall()
            async with db.execute("""
                SELECT COUNT(*) FROM users 
                WHERE full_name LIKE ? OR username LIKE ?
            """, (search_pattern, search_pattern)) as cursor:
                total_count = (await cursor.fetchone())[0]
        else:
            async with db.execute("""
                SELECT user_id, full_name, balance, total_earned, is_analyst, is_manager 
                FROM users 
                ORDER BY balance DESC 
                LIMIT ? OFFSET ?
            """, (limit, offset)) as cursor:
                users = await cursor.fetchall()
            async with db.execute("SELECT COUNT(*) FROM users") as cursor:
                total_count = (await cursor.fetchone())[0]
    
    keyboard = []
    for u in users:
        roles = []
        if u[4]: roles.append("Аналитик")
        if u[5]: roles.append("Менеджер")
        role_str = f" ({', '.join(roles)})" if roles else ""
        keyboard.append([InlineKeyboardButton(
            f"{u[1]}{role_str} | ${u[2]:.2f}", 
            callback_data=f"user_edit_{u[0]}"
        )])
    
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("⬅️", callback_data=f"adm_users_page_{page-1}"))
    if offset + limit < total_count:
        nav_row.append(InlineKeyboardButton("➡️", callback_data=f"adm_users_page_{page+1}"))
    if nav_row:
        keyboard.append(nav_row)
    
    keyboard.append([InlineKeyboardButton("🔍 Поиск", callback_data="adm_users_search")])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="admin_dashboard")])
    
    text = (
        f"<b>📋 СПИСОК ВОРКЕРОВ</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    )
    if search_query:
        text += f"<b>🔍 Поиск:</b> <code>{search_query}</code>\n"
    text += (
        f"<b>Всего:</b> <code>{total_count}</code> | <b>Страница:</b> <code>{page + 1}</code>\n\n"
        f"<i>Нажмите на воркера для изменения роли:</i>"
    )
    
    await send_screen(update, context, text, None, InlineKeyboardMarkup(keyboard))

async def adm_users_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.message.reply_text(
        "🔍 <b>Введите имя или username для поиска:</b>\n(Или отправьте '❌' для отмены)",
        parse_mode=ParseMode.HTML,
        reply_markup=ReplyKeyboardMarkup([['❌ Отмена']], resize_keyboard=True)
    )
    return USER_SEARCH

async def adm_users_search_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.text == "❌ Отмена":
        context.user_data['user_search'] = ""
        await update.message.reply_text("🚫 Поиск отменен.", reply_markup=ReplyKeyboardRemove())
        # Create a fake callback_query for adm_users_list
        class FakeQuery:
            def __init__(self):
                self.data = "adm_users_list"
                self.message = update.message
        update.callback_query = FakeQuery()
        await adm_users_list(update, context)
        return ConversationHandler.END
    
    context.user_data['user_search'] = update.message.text
    await update.message.reply_text("✅ Поиск выполнен.", reply_markup=ReplyKeyboardRemove())
    # Create a fake callback_query for adm_users_list
    class FakeQuery:
        def __init__(self):
            self.data = f"adm_users_search_{update.message.text}"
            self.message = update.message
    update.callback_query = FakeQuery()
    await adm_users_list(update, context)
    return ConversationHandler.END

async def user_edit_role(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = int(update.callback_query.data.split("_")[-1])
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("""
            SELECT full_name, is_analyst, is_manager, balance, total_earned, 
                   analyst_balance, analyst_total_earned, manager_balance, manager_total_earned
            FROM users WHERE user_id = ?
        """, (user_id,)) as cursor:
            user_data = await cursor.fetchone()
    
    roles = []
    if user_data[1]: roles.append("Аналитик")
    if user_data[2]: roles.append("Менеджер")
    role_str = ", ".join(roles) if roles else "Воркер"
    
    text = (
        f"<b>👤 РЕДАКТИРОВАНИЕ РОЛИ</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>Имя:</b> {user_data[0]}\n"
        f"<b>📊 Текущая роль:</b> {role_str}\n\n"
        f"<b>💰 БАЛАНСЫ</b>\n"
        f"├ <b>👤 Воркер:</b> <code>${user_data[3]:,.2f}</code> (Всего: <code>${user_data[4]:,.2f}</code>)\n"
    )
    if user_data[1]:
        text += f"├ <b>🔬 Аналитик:</b> <code>${user_data[5]:,.2f}</code> (Всего: <code>${user_data[6]:,.2f}</code>)\n"
    if user_data[2]:
        text += f"└ <b>👔 Менеджер:</b> <code>${user_data[7]:,.2f}</code> (Всего: <code>${user_data[8]:,.2f}</code>)\n"
    
    keyboard = [
        [
            InlineKeyboardButton(
                "✅ Аналитик" if user_data[1] else "❌ Аналитик",
                callback_data=f"role_toggle_analyst_{user_id}"
            ),
            InlineKeyboardButton(
                "✅ Менеджер" if user_data[2] else "❌ Менеджер",
                callback_data=f"role_toggle_manager_{user_id}"
            )
        ],
        [InlineKeyboardButton("🔙 К списку", callback_data="adm_users_list_0")]
    ]
    
    await send_screen(update, context, text, None, InlineKeyboardMarkup(keyboard))

async def role_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    parts = query.data.split("_")
    role_type = parts[2]  # analyst or manager
    user_id = int(parts[3])
    
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute(f"SELECT is_{role_type} FROM users WHERE user_id = ?", (user_id,)) as cursor:
            current = (await cursor.fetchone())[0]
        
        new_value = 0 if current else 1
        await db.execute(f"UPDATE users SET is_{role_type} = ? WHERE user_id = ?", (new_value, user_id))
        await db.commit()
    
    await query.answer(f"Роль {'включена' if new_value else 'отключена'}", show_alert=True)
    await user_edit_role(update, context)

# --- 🔄 ADMIN CONVERSATION: ADD PROFIT ---

async def prof_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT user_id, full_name FROM users ORDER BY full_name") as cursor:
            workers = await cursor.fetchall()
            
    keyboard = []
    for w in workers:
        keyboard.append([InlineKeyboardButton(w[1], callback_data=f"prof_sel_{w[0]}")])
    keyboard.append([InlineKeyboardButton("🔙 Отмена", callback_data="cancel_prof")])
    
    await send_screen(update, context, "👤 <b>Выберите воркера:</b>", None, InlineKeyboardMarkup(keyboard))
    return PROF_WORKER

async def prof_worker_sel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    worker_id = int(update.callback_query.data.split("_")[-1])
    context.user_data['worker_id'] = worker_id
    await update.callback_query.message.reply_text("✍️ <b>Введите имя Мамонта (или username):</b>", parse_mode=ParseMode.HTML, reply_markup=ReplyKeyboardMarkup([['❌ Отмена']], resize_keyboard=True))
    return PROF_CLIENT

async def prof_client(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['client_name'] = update.message.text
    await update.message.reply_text("💰 <b>Сумма залета (в $):</b>\nПример: 1500.50", parse_mode=ParseMode.HTML)
    return PROF_AMOUNT

async def prof_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        raw = update.message.text.replace(",", ".")
        amt = float(raw)
        if amt <= 0: raise ValueError
        context.user_data['amount'] = amt
        kb = ReplyKeyboardMarkup([["BTC", "USDT", "Card"], ["❌ Отмена"]], resize_keyboard=True)
        await update.message.reply_text("🏦 <b>Выберите направление:</b>", reply_markup=kb, parse_mode=ParseMode.HTML)
        return PROF_DIR
    except ValueError:
        await update.message.reply_text("⚠️ Введите корректное число (например 1500.50).")
        return PROF_AMOUNT

async def prof_dir(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['direction'] = update.message.text
    kb = ReplyKeyboardMarkup([["Депозит", "Комиссия", "Налог"], ["❌ Отмена"]], resize_keyboard=True)
    await update.message.reply_text("📑 <b>Стадия обработки:</b>", reply_markup=kb, parse_mode=ParseMode.HTML)
    return PROF_STAGE

async def prof_stage(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['stage'] = update.message.text
    await update.message.reply_text("📊 <b>Процент воркера?</b> (Только число, например 50):", reply_markup=ReplyKeyboardMarkup([['❌ Отмена']], resize_keyboard=True), parse_mode=ParseMode.HTML)
    return PROF_PERCENT

async def prof_percent(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        percent = float(update.message.text)
        context.user_data['percent'] = percent
        data = context.user_data
        worker_share = data['amount'] * (percent / 100)
        context.user_data['worker_share'] = worker_share
        
        # Show analyst selection
        async with aiosqlite.connect(DB_NAME) as db:
            async with db.execute("SELECT user_id, full_name FROM users WHERE is_analyst = 1 ORDER BY full_name") as cursor:
                analysts = await cursor.fetchall()
        
        keyboard = []
        if analysts:
            for a in analysts:
                keyboard.append([InlineKeyboardButton(a[1], callback_data=f"prof_analyst_{a[0]}")])
        keyboard.append([InlineKeyboardButton("⏭ Пропустить", callback_data="prof_analyst_skip")])
        keyboard.append([InlineKeyboardButton("🔙 Отмена", callback_data="cancel_prof")])
        
        text = "🔬 <b>Выберите аналитика (или пропустите):</b>"
        if not analysts:
            text += "\n<i>Аналитиков не найдено</i>"
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)
        return PROF_ANALYST
    except ValueError:
        await update.message.reply_text("⚠️ Введите число (например 50).")
        return PROF_PERCENT

async def prof_analyst_sel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query.data.endswith("_skip"):
        context.user_data['analyst_id'] = None
        context.user_data['analyst_percent'] = 0
        context.user_data['analyst_share'] = 0
    else:
        analyst_id = int(query.data.split("_")[-1])
        context.user_data['analyst_id'] = analyst_id
        await query.message.reply_text("📊 <b>Процент аналитика?</b> (Только число, например 10):", reply_markup=ReplyKeyboardMarkup([['❌ Отмена']], resize_keyboard=True), parse_mode=ParseMode.HTML)
        return PROF_ANALYST_PERCENT
    
    # Skip to manager selection
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT user_id, full_name FROM users WHERE is_manager = 1 ORDER BY full_name") as cursor:
            managers = await cursor.fetchall()
    
    keyboard = []
    if managers:
        for m in managers:
            keyboard.append([InlineKeyboardButton(m[1], callback_data=f"prof_manager_{m[0]}")])
    keyboard.append([InlineKeyboardButton("⏭ Пропустить", callback_data="prof_manager_skip")])
    keyboard.append([InlineKeyboardButton("🔙 Отмена", callback_data="cancel_prof")])
    
    text = "👔 <b>Выберите менеджера (или пропустите):</b>"
    if not managers:
        text += "\n<i>Менеджеров не найдено</i>"
    await query.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)
    return PROF_MANAGER

async def prof_analyst_percent(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        percent = float(update.message.text)
        data = context.user_data
        analyst_share = data['amount'] * (percent / 100)
        context.user_data['analyst_percent'] = percent
        context.user_data['analyst_share'] = analyst_share
        
        # Show manager selection
        async with aiosqlite.connect(DB_NAME) as db:
            async with db.execute("SELECT user_id, full_name FROM users WHERE is_manager = 1 ORDER BY full_name") as cursor:
                managers = await cursor.fetchall()
        
        keyboard = []
        if managers:
            for m in managers:
                keyboard.append([InlineKeyboardButton(m[1], callback_data=f"prof_manager_{m[0]}")])
        keyboard.append([InlineKeyboardButton("⏭ Пропустить", callback_data="prof_manager_skip")])
        keyboard.append([InlineKeyboardButton("🔙 Отмена", callback_data="cancel_prof")])
        
        text = "👔 <b>Выберите менеджера (или пропустите):</b>"
        if not managers:
            text += "\n<i>Менеджеров не найдено</i>"
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)
        return PROF_MANAGER
    except ValueError:
        await update.message.reply_text("⚠️ Введите число (например 10).")
        return PROF_ANALYST_PERCENT

async def prof_manager_sel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query.data.endswith("_skip"):
        context.user_data['manager_id'] = None
        context.user_data['manager_percent'] = 0
        context.user_data['manager_share'] = 0
    else:
        manager_id = int(query.data.split("_")[-1])
        context.user_data['manager_id'] = manager_id
        await query.message.reply_text("📊 <b>Процент менеджера?</b> (Только число, например 5):", reply_markup=ReplyKeyboardMarkup([['❌ Отмена']], resize_keyboard=True), parse_mode=ParseMode.HTML)
        return PROF_MANAGER_PERCENT
    
    # Show confirmation
    await prof_show_confirm(update, context)
    return PROF_CONFIRM

async def prof_manager_percent(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        percent = float(update.message.text)
        data = context.user_data
        manager_share = data['amount'] * (percent / 100)
        context.user_data['manager_percent'] = percent
        context.user_data['manager_share'] = manager_share
        
        # Show confirmation
        await prof_show_confirm(update, context)
        return PROF_CONFIRM
    except ValueError:
        await update.message.reply_text("⚠️ Введите число (например 5).")
        return PROF_MANAGER_PERCENT

async def prof_show_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data = context.user_data
    message = update.message if hasattr(update, 'message') and update.message else update.callback_query.message
    
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT full_name FROM users WHERE user_id = ?", (data['worker_id'],)) as cursor:
            worker_name = (await cursor.fetchone())[0]
        
        dir_emoji = {"BTC": "₿", "USDT": "💵", "Card": "💳"}.get(data['direction'], "💰")
        stage_emoji = {"Депозит": "💸", "Комиссия": "💼", "Налог": "📋"}.get(data['stage'], "📊")
        
        text = (
            f"<b>⚠️ ПРОВЕРКА ДАННЫХ</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>👤 Воркер:</b> <b>{worker_name}</b>\n"
            f"<b>🦣 Мамонт:</b> <b>{data['client_name']}</b>\n"
            f"<b>💰 Сумма залета:</b> <code>${data['amount']:,.2f}</code>\n"
            f"<b>🏦 Направление:</b> {dir_emoji} <b>{data['direction']}</b>\n"
            f"<b>📑 Стадия:</b> {stage_emoji} <b>{data['stage']}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>📊 Процент воркера:</b> <code>{data['percent']:.1f}%</code>\n"
            f"<b>💵 Доля воркера:</b> <code>${data['worker_share']:,.2f}</code>\n"
        )
        
        if data.get('analyst_id'):
            async with db.execute("SELECT full_name FROM users WHERE user_id = ?", (data['analyst_id'],)) as cursor:
                analyst_name = (await cursor.fetchone())[0]
            text += (
                f"\n<b>🔬 Аналитик:</b> <b>{analyst_name}</b>\n"
                f"   ├ <b>Процент:</b> <code>{data.get('analyst_percent', 0):.1f}%</code>\n"
                f"   └ <b>Доля:</b> <code>${data.get('analyst_share', 0):,.2f}</code>\n"
            )
        
        if data.get('manager_id'):
            async with db.execute("SELECT full_name FROM users WHERE user_id = ?", (data['manager_id'],)) as cursor:
                manager_name = (await cursor.fetchone())[0]
            text += (
                f"\n<b>👔 Менеджер:</b> <b>{manager_name}</b>\n"
                f"   ├ <b>Процент:</b> <code>{data.get('manager_percent', 0):.1f}%</code>\n"
                f"   └ <b>Доля:</b> <code>${data.get('manager_share', 0):,.2f}</code>\n"
            )
        
        text += "\n<i>Все верно?</i>"
    
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Подтвердить", callback_data="prof_commit")],
            [InlineKeyboardButton("❌ Отмена", callback_data="cancel_prof")]
        ])
    await message.reply_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)

async def prof_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data = context.user_data
    async with aiosqlite.connect(DB_NAME) as db:
        # Client logic
        async with db.execute("SELECT id FROM clients WHERE worker_id = ? AND name = ?", (data['worker_id'], data['client_name'])) as cursor:
            client = await cursor.fetchone()
        
        if client:
            client_id = client[0]
            await db.execute("UPDATE clients SET total_squeezed = total_squeezed + ? WHERE id = ?", (data['amount'], client_id))
        else:
            cur = await db.execute("INSERT INTO clients (worker_id, name, total_squeezed) VALUES (?, ?, ?)", 
                                   (data['worker_id'], data['client_name'], data['amount']))
            client_id = cur.lastrowid
        
        # Log Profit
        await db.execute("""
            INSERT INTO profits (worker_id, client_id, amount, worker_share, direction, stage, percent,
                                analyst_id, analyst_share, analyst_percent,
                                manager_id, manager_share, manager_percent)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data['worker_id'], client_id, data['amount'], data['worker_share'], 
            data['direction'], data['stage'], data.get('percent', 0),
            data.get('analyst_id'), data.get('analyst_share', 0), data.get('analyst_percent', 0),
            data.get('manager_id'), data.get('manager_share', 0), data.get('manager_percent', 0)
        ))
        
        # Update Worker Balance
        await db.execute("UPDATE users SET balance = balance + ?, total_earned = total_earned + ? WHERE user_id = ?",
                         (data['worker_share'], data['worker_share'], data['worker_id']))
        
        # Update Analyst Balance
        if data.get('analyst_id') and data.get('analyst_share', 0) > 0:
            await db.execute("""
                UPDATE users 
                SET analyst_balance = analyst_balance + ?, 
                    analyst_total_earned = analyst_total_earned + ? 
                WHERE user_id = ?
            """, (data['analyst_share'], data['analyst_share'], data['analyst_id']))
        
        # Update Manager Balance
        if data.get('manager_id') and data.get('manager_share', 0) > 0:
            await db.execute("""
                UPDATE users 
                SET manager_balance = manager_balance + ?, 
                    manager_total_earned = manager_total_earned + ? 
                WHERE user_id = ?
            """, (data['manager_share'], data['manager_share'], data['manager_id']))
        
        await db.commit()

    await update.callback_query.message.edit_text(
        f"✅ <b>Профит успешно добавлен!</b>\n\n"
        f"<b>🦣 Мамонт:</b> <b>{data['client_name']}</b>\n"
        f"<b>💰 Сумма:</b> <code>${data['amount']:,.2f}</code>\n"
        f"<b>💵 Доля воркера:</b> <code>${data['worker_share']:,.2f}</code>\n\n"
        f"<i>📨 Уведомления отправлены всем участникам.</i>",
        parse_mode=ParseMode.HTML
    )
    
    # Notify Worker
    try:
        dir_emoji = {"BTC": "₿", "USDT": "💵", "Card": "💳"}.get(data['direction'], "💰")
        stage_emoji = {"Депозит": "💸", "Комиссия": "💼", "Налог": "📋"}.get(data['stage'], "📊")
        
        await context.bot.send_message(data['worker_id'], 
            f"<b>🚨 НОВЫЙ ЗАЛЕТ!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>🦣 Мамонт:</b> <b>{data['client_name']}</b>\n"
            f"<b>💵 Сумма залета:</b> <code>${data['amount']:,.2f}</code>\n"
            f"<b>🏦 Направление:</b> {dir_emoji} <b>{data['direction']}</b>\n"
            f"<b>📑 Стадия:</b> {stage_emoji} <b>{data['stage']}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>💰 ТВОЯ ДОЛЯ:</b> <code>${data['worker_share']:,.2f}</code>\n"
            f"<b>📊 Процент:</b> <code>{data.get('percent', 0):.1f}%</code>\n\n"
            f"<i>🚀 Keep pushing!</i>", 
            parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.warning(f"Notify worker failed: {e}")
    
    # Notify Analyst
    if data.get('analyst_id') and data.get('analyst_share', 0) > 0:
        try:
            dir_emoji = {"BTC": "₿", "USDT": "💵", "Card": "💳"}.get(data['direction'], "💰")
            await context.bot.send_message(data['analyst_id'],
                f"<b>🔬 НОВЫЙ ПРОФИТ КАК АНАЛИТИК!</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"<b>🦣 Мамонт:</b> <b>{data['client_name']}</b>\n"
                f"<b>💵 Сумма залета:</b> <code>${data['amount']:,.2f}</code>\n"
                f"<b>🏦 Направление:</b> {dir_emoji} <b>{data['direction']}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"<b>💰 ВАША ДОЛЯ:</b> <code>${data['analyst_share']:,.2f}</code>\n"
                f"<b>📊 Процент:</b> <code>{data.get('analyst_percent', 0):.1f}%</code>\n\n"
                f"<i>🎯 Отличная работа!</i>",
                parse_mode=ParseMode.HTML)
        except Exception as e:
            logger.warning(f"Notify analyst failed: {e}")
    
    # Notify Manager
    if data.get('manager_id') and data.get('manager_share', 0) > 0:
        try:
            dir_emoji = {"BTC": "₿", "USDT": "💵", "Card": "💳"}.get(data['direction'], "💰")
            await context.bot.send_message(data['manager_id'],
                f"<b>👔 НОВЫЙ ПРОФИТ КАК МЕНЕДЖЕР!</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"<b>🦣 Мамонт:</b> <b>{data['client_name']}</b>\n"
                f"<b>💵 Сумма залета:</b> <code>${data['amount']:,.2f}</code>\n"
                f"<b>🏦 Направление:</b> {dir_emoji} <b>{data['direction']}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"<b>💰 ВАША ДОЛЯ:</b> <code>${data['manager_share']:,.2f}</code>\n"
                f"<b>📊 Процент:</b> <code>{data.get('manager_percent', 0):.1f}%</code>\n\n"
                f"<i>💼 Продолжайте в том же духе!</i>",
                parse_mode=ParseMode.HTML)
        except Exception as e:
            logger.warning(f"Notify manager failed: {e}")
        
    await admin_dashboard(update, context)
    return ConversationHandler.END

# --- 🔄 ADMIN CONVERSATION: PAYOUT ---

async def pay_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("""
            SELECT user_id, full_name, 
                   (balance + analyst_balance + manager_balance) as total_balance,
                   balance, analyst_balance, manager_balance
            FROM users 
            WHERE (balance + analyst_balance + manager_balance) > 0 
            ORDER BY (balance + analyst_balance + manager_balance) DESC
        """) as cursor:
            users = await cursor.fetchall()
            
    if not users:
        await update.callback_query.answer("🤷‍♂️ Все выплачено!", show_alert=True)
        return ConversationHandler.END
        
    keyboard = []
    for u in users:
        roles_info = []
        if u[3] > 0: roles_info.append(f"В:${u[3]:.0f}")
        if u[4] > 0: roles_info.append(f"А:${u[4]:.0f}")
        if u[5] > 0: roles_info.append(f"М:${u[5]:.0f}")
        roles_str = f" ({', '.join(roles_info)})" if roles_info else ""
        keyboard.append([InlineKeyboardButton(
            f"{u[1]} | ${u[2]:.2f}{roles_str}", 
            callback_data=f"pay_sel_{u[0]}"
        )])
    keyboard.append([InlineKeyboardButton("🔙 Отмена", callback_data="cancel_pay")])
    
    await send_screen(update, context, "💸 <b>Кому выплачиваем?</b>", None, InlineKeyboardMarkup(keyboard))
    return PAY_CHECK

async def pay_user_sel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = int(update.callback_query.data.split("_")[-1])
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("""
            SELECT full_name, balance, analyst_balance, manager_balance,
                   (balance + analyst_balance + manager_balance) as total_balance
            FROM users WHERE user_id = ?
        """, (user_id,)) as cursor:
            u_data = await cursor.fetchone()
            
    context.user_data['pay_id'] = user_id
    context.user_data['pay_amount'] = u_data[4]  # total_balance
    context.user_data['pay_name'] = u_data[0]
    context.user_data['pay_worker'] = u_data[1]
    context.user_data['pay_analyst'] = u_data[2]
    context.user_data['pay_manager'] = u_data[3]
    
    text = (
        f"<b>💳 ВЫПЛАТА</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>👤 Воркер:</b> <b>{u_data[0]}</b>\n\n"
        f"<b>💰 БАЛАНСЫ</b>\n"
    )
    if u_data[1] > 0:
        text += f"├ <b>👤 Воркер:</b> <code>${u_data[1]:,.2f}</code>\n"
    if u_data[2] > 0:
        text += f"├ <b>🔬 Аналитик:</b> <code>${u_data[2]:,.2f}</code>\n"
    if u_data[3] > 0:
        text += f"├ <b>👔 Менеджер:</b> <code>${u_data[3]:,.2f}</code>\n"
    text += (
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>💳 Всего к выплате:</b> <code>${u_data[4]:,.2f}</code>\n\n"
        f"<i>⬇️ Вставьте чек CryptoBot или код транзакции:</i>"
    )
    
    await update.callback_query.message.reply_text(
        text, 
        parse_mode=ParseMode.HTML, reply_markup=ReplyKeyboardMarkup([['❌ Отмена']], resize_keyboard=True)
    )
    return PAY_CONFIRM

async def pay_confirm_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['check_code'] = update.message.text
    data = context.user_data
    text = (
        f"<b>⚠️ ПОДТВЕРЖДЕНИЕ ВЫПЛАТЫ</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>👤 Воркер:</b> <b>{data['pay_name']}</b>\n"
        f"<b>💰 Сумма:</b> <code>${data['pay_amount']:,.2f}</code>\n"
        f"<b>🧾 Чек:</b> <code>{data['check_code']}</code>\n\n"
        f"<i>Чек будет добавлен в очередь выплат. Воркер получит его в меню зарплаты.</i>\n\n"
        f"<b>⚠️ Баланс воркера будет обнулен сразу после добавления чека.</b>"
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ ДОБАВИТЬ ЧЕК", callback_data="pay_commit")],
        [InlineKeyboardButton("❌ Отмена", callback_data="cancel_pay")]
    ])
    await update.message.reply_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)
    return PAY_CONFIRM 

async def pay_execute(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data = context.user_data
    async with aiosqlite.connect(DB_NAME) as db:
        # Создаем запись о выплате с is_received=0
        await db.execute("INSERT INTO payouts (worker_id, check_code, amount, is_received) VALUES (?, ?, ?, 0)", 
                         (data['pay_id'], data['check_code'], data['pay_amount']))
        
        # Обнуляем баланс сразу при создании чека, чтобы пользователь исчез из списка
        await db.execute("""
            UPDATE users 
            SET balance = 0, analyst_balance = 0, manager_balance = 0 
            WHERE user_id = ?
        """, (data['pay_id'],))
        
        await db.commit()
        
    await update.callback_query.message.edit_text(
        f"✅ <b>Чек добавлен в очередь выплат!</b>\n\n"
        f"<b>👤 Воркер:</b> <b>{data['pay_name']}</b>\n"
        f"<b>💰 Сумма:</b> <code>${data['pay_amount']:,.2f}</code>\n"
        f"<b>🧾 Чек:</b> <code>{data['check_code']}</code>\n\n"
        f"<i>📨 Воркер получит уведомление о новой выплате в меню зарплаты.</i>\n"
        f"<i>💰 Баланс воркера обнулен.</i>",
        parse_mode=ParseMode.HTML
    )
    
    # Уведомляем воркера о новой выплате
    try:
        text = (
            f"<b>💸 НОВАЯ ВЫПЛАТА ДОСТУПНА!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"<b>💰 Сумма:</b> <code>${data['pay_amount']:,.2f}</code>\n\n"
            f"<i>⬇️ Перейдите в меню 💳 История Выплат для получения чека</i>"
        )
        await context.bot.send_message(data['pay_id'], text, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.warning(f"Notify payout failed: {e}")
    
    await admin_dashboard(update, context)
    return ConversationHandler.END

async def cancel_op(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🚫 Операция отменена.", reply_markup=ReplyKeyboardRemove())
    await start(update, context)
    return ConversationHandler.END

async def cancel_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.message.edit_text("🚫 Операция отменена.")
    await admin_dashboard(update, context)
    return ConversationHandler.END

# --- 💰 RECEIVE PAYOUT ---
async def receive_payout(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    payout_id = int(query.data.split("_")[-1])
    user_id = update.effective_user.id
    
    async with aiosqlite.connect(DB_NAME) as db:
        # Получаем информацию о выплате
        async with db.execute("""
            SELECT check_code, amount, worker_id
            FROM payouts 
            WHERE id = ? AND worker_id = ? AND is_received = 0
        """, (payout_id, user_id)) as cursor:
            payout = await cursor.fetchone()
        
        if not payout:
            await query.answer("❌ Чек не найден или уже получен", show_alert=True)
            return
        
        check_code = payout[0]
        amount = payout[1]
        
        # Баланс уже обнулен при создании чека админом
        # Просто помечаем чек как полученный
        await db.execute("""
            UPDATE payouts 
            SET is_received = 1 
            WHERE id = ?
        """, (payout_id,))
        
        await db.commit()
    
    # Отправляем чек пользователю
    try:
        await context.bot.send_message(
            user_id,
            check_code,
            parse_mode=ParseMode.HTML
        )
        
        await query.answer("✅ Чек отправлен!", show_alert=False)
        
        # Обновляем меню зарплаты
        await menu_salary(update, context)
    except Exception as e:
        logger.error(f"Error sending check: {e}")
        await query.answer("❌ Ошибка при отправке чека", show_alert=True)

# --- 🚀 BOOTSTRAP ---
if __name__ == "__main__":
    if not BOT_TOKEN:
        sys.exit("❌ Error: BOT_TOKEN missing in .env file")

    # DB Init
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(init_db())

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # Handlers Registration
    auth_handler = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={AUTH_PWD: [MessageHandler(filters.TEXT & ~filters.COMMAND, auth_password)]},
        fallbacks=[CommandHandler("cancel", auth_cancel)]
    )
    
    prof_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(prof_start, pattern="^adm_start_profit$")],
        states={
            PROF_WORKER: [CallbackQueryHandler(prof_worker_sel, pattern="^prof_sel_"), CallbackQueryHandler(cancel_cb, pattern="^cancel_prof$")],
            PROF_CLIENT: [MessageHandler(filters.TEXT & ~filters.Regex("^❌ Отмена$"), prof_client)],
            PROF_AMOUNT: [MessageHandler(filters.TEXT & ~filters.Regex("^❌ Отмена$"), prof_amount)],
            PROF_DIR:    [MessageHandler(filters.TEXT & ~filters.Regex("^❌ Отмена$"), prof_dir)],
            PROF_STAGE:  [MessageHandler(filters.TEXT & ~filters.Regex("^❌ Отмена$"), prof_stage)],
            PROF_PERCENT:[MessageHandler(filters.TEXT & ~filters.Regex("^❌ Отмена$"), prof_percent)],
            PROF_ANALYST: [CallbackQueryHandler(prof_analyst_sel, pattern="^prof_analyst_"), CallbackQueryHandler(cancel_cb, pattern="^cancel_prof$")],
            PROF_ANALYST_PERCENT: [MessageHandler(filters.TEXT & ~filters.Regex("^❌ Отмена$"), prof_analyst_percent)],
            PROF_MANAGER: [CallbackQueryHandler(prof_manager_sel, pattern="^prof_manager_"), CallbackQueryHandler(cancel_cb, pattern="^cancel_prof$")],
            PROF_MANAGER_PERCENT: [MessageHandler(filters.TEXT & ~filters.Regex("^❌ Отмена$"), prof_manager_percent)],
            PROF_CONFIRM:[CallbackQueryHandler(prof_confirm, pattern="^prof_commit$"), CallbackQueryHandler(cancel_cb, pattern="^cancel_prof$")]
        },
        fallbacks=[MessageHandler(filters.Regex("^❌ Отмена$"), cancel_op), CallbackQueryHandler(cancel_cb, pattern="^cancel_prof$")]
    )
    
    user_search_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(adm_users_search, pattern="^adm_users_search$")],
        states={
            USER_SEARCH: [MessageHandler(filters.TEXT & ~filters.COMMAND, adm_users_search_input)]
        },
        fallbacks=[MessageHandler(filters.Regex("^❌ Отмена$"), cancel_op)]
    )

    pay_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(pay_start, pattern="^adm_start_pay$")],
        states={
            PAY_CHECK: [CallbackQueryHandler(pay_user_sel, pattern="^pay_sel_"), CallbackQueryHandler(cancel_cb, pattern="^cancel_pay$")],
            PAY_CONFIRM: [
                MessageHandler(filters.TEXT & ~filters.Regex("^❌ Отмена$"), pay_confirm_input),
                CallbackQueryHandler(pay_execute, pattern="^pay_commit$"),
                CallbackQueryHandler(cancel_cb, pattern="^cancel_pay$")
            ]
        },
        fallbacks=[MessageHandler(filters.Regex("^❌ Отмена$"), cancel_op), CallbackQueryHandler(cancel_cb, pattern="^cancel_pay$")]
    )

    app.add_handler(auth_handler)
    app.add_handler(prof_handler)
    app.add_handler(pay_handler)
    app.add_handler(user_search_handler)
    
    app.add_handler(CallbackQueryHandler(menu_stats, pattern="^menu_stats$"))
    app.add_handler(CallbackQueryHandler(menu_stats_detailed, pattern="^menu_stats_detailed$"))
    app.add_handler(CallbackQueryHandler(menu_clients, pattern="^menu_clients"))
    app.add_handler(CallbackQueryHandler(client_view, pattern="^client_view_"))
    app.add_handler(CallbackQueryHandler(menu_finances, pattern="^menu_finances$|^finances_(payouts|profits)$"))
    app.add_handler(CallbackQueryHandler(menu_profits, pattern="^menu_profits$|^profit_period_"))
    app.add_handler(CallbackQueryHandler(profit_detailed, pattern="^profit_detailed$"))
    app.add_handler(CallbackQueryHandler(profit_export, pattern="^profit_export$"))
    app.add_handler(CallbackQueryHandler(menu_salary, pattern="^menu_salary$|^salary_period_"))
    app.add_handler(CallbackQueryHandler(receive_payout, pattern="^receive_payout_"))
    app.add_handler(CallbackQueryHandler(salary_chart, pattern="^salary_chart$"))
    app.add_handler(CallbackQueryHandler(salary_export, pattern="^salary_export$"))
    app.add_handler(CallbackQueryHandler(menu_tops_analytics, pattern="^menu_tops_analytics$|^tops_analytics_(tops|analytics)$"))
    app.add_handler(CallbackQueryHandler(menu_tops, pattern="^menu_tops$|^top_(day|week|month|all)$"))
    app.add_handler(CallbackQueryHandler(top_analysts, pattern="^top_analysts$"))
    app.add_handler(CallbackQueryHandler(top_managers, pattern="^top_managers$"))
    app.add_handler(CallbackQueryHandler(menu_analytics, pattern="^menu_analytics$"))
    app.add_handler(CallbackQueryHandler(analytics_detailed, pattern="^analytics_detailed$"))
    app.add_handler(CallbackQueryHandler(menu_main, pattern="^menu_main$"))
    app.add_handler(CallbackQueryHandler(admin_dashboard, pattern="^admin_dashboard$"))
    app.add_handler(CallbackQueryHandler(adm_users_list, pattern="^adm_users_list"))
    app.add_handler(CallbackQueryHandler(user_edit_role, pattern="^user_edit_"))
    app.add_handler(CallbackQueryHandler(role_toggle, pattern="^role_toggle_"))

    print("✅ Bot is running (Python-Telegram-Bot v20+)...")
    app.run_polling()
