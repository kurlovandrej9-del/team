

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
                date_joined TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
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
                direction TEXT,
                stage TEXT,
                percent REAL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS payouts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                worker_id INTEGER,
                check_code TEXT,
                amount REAL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.commit()

# --- 🛠️ HELPERS ---
async def get_db_user(user_id):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)) as cursor:
            return await cursor.fetchone()

async def is_admin(user_id):
    user = await get_db_user(user_id)
    return bool(user and user[5]) # 5 is is_admin index

async def send_screen(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, photo_key: str = None, markup=None):
    """
    Robust message sender for PTB. Handles edits vs new messages and image presence.
    """
    query = update.callback_query
    message = query.message if query else update.message
    
    file_path = IMG_PATHS.get(photo_key)
    has_photo = file_path and os.path.exists(file_path)
    
    is_edit = bool(query)

    try:
        if is_edit:
            if has_photo:
                # If message already has photo, edit media
                if message.photo:
                    media = InputMediaPhoto(open(file_path, 'rb'), caption=text, parse_mode=ParseMode.HTML)
                    await message.edit_media(media=media, reply_markup=markup)
                else:
                    # Message had no photo, delete and send new
                    await message.delete()
                    await message.reply_photo(photo=open(file_path, 'rb'), caption=text, reply_markup=markup, parse_mode=ParseMode.HTML)
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
                await message.reply_photo(photo=open(file_path, 'rb'), caption=text, reply_markup=markup, parse_mode=ParseMode.HTML)
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
        [
            InlineKeyboardButton("📊 Моя Статистика", callback_data="menu_stats"),
            InlineKeyboardButton("🦣 Мои Мамонты", callback_data="menu_clients_0")
        ],
        [
            InlineKeyboardButton("💳 История Выплат", callback_data="menu_salary"),
            InlineKeyboardButton("📈 Лог Профитов", callback_data="menu_profits")
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
PROF_WORKER, PROF_CLIENT, PROF_AMOUNT, PROF_DIR, PROF_STAGE, PROF_PERCENT, PROF_CONFIRM = range(2, 9)
PAY_CHECK, PAY_CONFIRM = range(9, 11)

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
        is_admin_flag = bool(db_user[5])
        text = (
            f"👋 <b>Добро пожаловать, {user.first_name}.</b>\n\n"
            f"🖥 <b>Рабочее пространство:</b> <code>Active</code>\n"
            f"🛡 <b>Статус:</b> {'👨‍💻 Администратор' if is_admin_flag else '👤 Воркер'}\n\n"
            f"👇 <i>Используйте навигацию ниже:</i>"
        )
        await send_screen(update, context, text, "welcome", get_main_menu_kb(is_admin_flag))
        return ConversationHandler.END
    else:
        text = (
            f"⛔️ <b>ACCESS DENIED</b>\n\n"
            f"Система закрытого доступа. Ваша учетная запись не обнаружена.\n"
            f"<i>Введите ключ доступа для активации рабочего места:</i>"
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
        
        text = f"✅ <b>Доступ разрешен.</b>\nДобро пожаловать в команду."
        await send_screen(update, context, text, "welcome", get_main_menu_kb(bool(is_admin_flag)))
        return ConversationHandler.END
    else:
        reply = await msg.reply_text("❌ Неверный пароль.")
        await asyncio.sleep(2)
        try: await reply.delete()
        except: pass
        return AUTH_PWD

async def auth_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🚫 Авторизация отменена.")
    return ConversationHandler.END

# --- 📊 MENU CALLBACKS ---
async def menu_main(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await start(update, context)

async def menu_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT full_name, total_earned, balance FROM users WHERE user_id = ?", (user_id,)) as cursor:
            user_data = await cursor.fetchone()
        
        now = datetime.now()
        month_start = now.replace(day=1, hour=0, minute=0, second=0)
        async with db.execute("SELECT SUM(worker_share) FROM profits WHERE worker_id = ? AND timestamp >= ?", (user_id, month_start)) as cursor:
            month_profit = (await cursor.fetchone())[0] or 0.0
        async with db.execute("SELECT COUNT(*) FROM clients WHERE worker_id = ?", (user_id,)) as cursor:
            clients_count = (await cursor.fetchone())[0]

    earned = user_data[1]
    if earned < 100: rank = "Новичок 🐣"
    elif earned < 1000: rank = "Бывалый 👊"
    elif earned < 5000: rank = "Хищник 🦈"
    elif earned < 10000: rank = "Машина 🤖"
    else: rank = "Легенда 👑"

    text = (
        f"📊 <b>ЛИЧНЫЙ КАБИНЕТ</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🏆 <b>Ранг:</b> {rank}\n"
        f"🆔 <b>ID:</b> <code>{user_id}</code>\n\n"
        f"💰 <b>ФИНАНСЫ:</b>\n"
        f"├ 💳 <b>К выплате:</b> <code>${user_data[2]:.2f}</code>\n"
        f"├ 💵 <b>Всего заработано:</b> ${user_data[1]:.2f}\n"
        f"└ 📅 <b>Профит за месяц:</b> ${month_profit:.2f}\n\n"
        f"📂 <b>АКТИВНОСТЬ:</b>\n"
        f"└ 🦣 <b>Активных мамонтов:</b> {clients_count}\n"
    )
    await send_screen(update, context, text, "profile", get_back_kb())

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

    text = f"🦣 <b>ВАШИ КЛИЕНТЫ ({total_count})</b>\nНажмите для просмотра истории:"
    await send_screen(update, context, text, None, InlineKeyboardMarkup(keyboard))

async def client_view(update: Update, context: ContextTypes.DEFAULT_TYPE):
    client_id = int(update.callback_query.data.split("_")[-1])
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT name, total_squeezed FROM clients WHERE id = ?", (client_id,)) as cursor:
            client = await cursor.fetchone()
        async with db.execute("SELECT amount, stage, timestamp, direction FROM profits WHERE client_id = ? ORDER BY timestamp DESC LIMIT 5", (client_id,)) as cursor:
            history = await cursor.fetchall()

    text = (
        f"👤 <b>Мамонт:</b> {client[0]}\n"
        f"💵 <b>Общий профит:</b> ${client[1]:.2f}\n\n"
        f"🕰 <b>История (Последние 5):</b>\n"
    )
    if not history: text += "▫️ Транзакций нет."
    for h in history:
        text += f"▫️ {h[3]} | ${h[0]:.0f} | {h[1]}\n"

    await send_screen(update, context, text, None, InlineKeyboardMarkup([[InlineKeyboardButton("🔙 К списку", callback_data="menu_clients_0")]]))

async def menu_profits(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("""
            SELECT p.amount, p.worker_share, p.stage, c.name, p.direction 
            FROM profits p
            JOIN clients c ON p.client_id = c.id
            WHERE p.worker_id = ?
            ORDER BY p.timestamp DESC LIMIT 10
        """, (user_id,)) as cursor:
            profits = await cursor.fetchall()

    text = "📈 <b>ПОСЛЕДНИЕ ПРОФИТЫ</b>\n\n"
    if not profits: text += "В базе нет записей."
    for p in profits:
        text += f"🟢 <b>+${p[1]:.2f}</b> (Вход: ${p[0]})\n└ {p[3]} | {p[4]} | {p[2]}\n\n"
    await send_screen(update, context, text, None, get_back_kb())

async def menu_salary(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT check_code, amount, timestamp FROM payouts WHERE worker_id = ? ORDER BY timestamp DESC LIMIT 10", (user_id,)) as cursor:
            payouts = await cursor.fetchall()
        async with db.execute("SELECT SUM(amount) FROM payouts WHERE worker_id = ?", (user_id,)) as cursor:
            total_paid = (await cursor.fetchone())[0] or 0.0

    text = f"💰 <b>ИСТОРИЯ ВЫПЛАТ</b>\nВсего получено: <b>${total_paid:.2f}</b>\n━━━━━━━━━━━━━━━━━━\n\n"
    if not payouts: text += "Выплат пока не было."
    for p in payouts:
        date_str = datetime.strptime(p[2], "%Y-%m-%d %H:%M:%S").strftime("%d.%m %H:%M")
        text += f"🧾 <b>${p[1]:.2f}</b> | {date_str}\n<code>{p[0]}</code>\n\n"
    await send_screen(update, context, text, "pay", get_back_kb())

# --- 🔐 ADMIN PANEL ---

async def admin_dashboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id): return

    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT SUM(total_earned), SUM(balance) FROM users") as cursor:
            stats = await cursor.fetchone()
            total_turnover = stats[0] or 0
            total_debt = stats[1] or 0

    text = (
        f"🔐 <b>АДМИН ПАНЕЛЬ</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"💎 <b>Оборот команды:</b> ${total_turnover:.2f}\n"
        f"🩸 <b>Долг по ЗП:</b> ${total_debt:.2f}\n"
    )
    keyboard = [
        [InlineKeyboardButton("💵 Внести профит", callback_data="adm_start_profit")],
        [InlineKeyboardButton("💸 Выплатить ЗП", callback_data="adm_start_pay")],
        [InlineKeyboardButton("📋 Список воркеров", callback_data="adm_users_list")],
        [InlineKeyboardButton("🔙 В меню", callback_data="menu_main")]
    ]
    await send_screen(update, context, text, None, InlineKeyboardMarkup(keyboard))

async def adm_users_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT full_name, balance, total_earned FROM users ORDER BY balance DESC") as cursor:
            users = await cursor.fetchall()
    text = "📋 <b>ТОП ВОРКЕРОВ</b>\n\n"
    for u in users:
        text += f"👤 <b>{u[0]}</b>\n💵 Баланс: ${u[1]:.2f} | Всего: ${u[2]:.2f}\n\n"
    await send_screen(update, context, text, None, InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="admin_dashboard")]]))

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
        
        text = (
            f"⚠️ <b>ПРОВЕРКА ДАННЫХ</b>\n"
            f"👤 Воркер ID: {data['worker_id']}\n"
            f"🦣 Мамонт: {data['client_name']}\n"
            f"💰 Сумма: ${data['amount']}\n"
            f"📊 Процент: {percent}%\n"
            f"💵 <b>Доля воркера: ${worker_share:.2f}</b>\n\n"
            f"Все верно?"
        )
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Подтвердить", callback_data="prof_commit")],
            [InlineKeyboardButton("❌ Отмена", callback_data="cancel_prof")]
        ])
        await update.message.reply_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)
        return PROF_CONFIRM
    except ValueError:
        await update.message.reply_text("⚠️ Введите число (например 50).")
        return PROF_PERCENT

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
            INSERT INTO profits (worker_id, client_id, amount, worker_share, direction, stage, percent)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (data['worker_id'], client_id, data['amount'], data['worker_share'], data['direction'], data['stage'], data.get('percent', 0)))
        
        # Update Balance
        await db.execute("UPDATE users SET balance = balance + ?, total_earned = total_earned + ? WHERE user_id = ?",
                         (data['worker_share'], data['worker_share'], data['worker_id']))
        await db.commit()

    await update.callback_query.message.edit_text(f"✅ <b>Профит добавлен!</b>", parse_mode=ParseMode.HTML)
    
    # Notify Worker
    try:
        await context.bot.send_message(data['worker_id'], 
            f"🚨 <b>НОВЫЙ ЗАЛЕТ!</b>\n━━━━━━━━━━━━━━━━━━\n"
            f"🦣 <b>Мамонт:</b> {data['client_name']}\n"
            f"💵 <b>Сумма:</b> <code>${data['amount']}</code>\n"
            f"⚙️ <b>Тип:</b> {data['direction']} ({data['stage']})\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"💰 <b>ТВОЯ ДОЛЯ:</b> <b>${data['worker_share']:.2f}</b>\n"
            f"🚀 <i>Keep pushing!</i>", 
            parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.warning(f"Notify failed: {e}")
        
    await admin_dashboard(update, context)
    return ConversationHandler.END

# --- 🔄 ADMIN CONVERSATION: PAYOUT ---

async def pay_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT user_id, full_name, balance FROM users WHERE balance > 0 ORDER BY balance DESC") as cursor:
            users = await cursor.fetchall()
            
    if not users:
        await update.callback_query.answer("🤷‍♂️ Все выплачено!", show_alert=True)
        return ConversationHandler.END
        
    keyboard = []
    for u in users:
        keyboard.append([InlineKeyboardButton(f"{u[1]} (${u[2]:.2f})", callback_data=f"pay_sel_{u[0]}")])
    keyboard.append([InlineKeyboardButton("🔙 Отмена", callback_data="cancel_pay")])
    
    await send_screen(update, context, "💸 <b>Кому выплачиваем?</b>", None, InlineKeyboardMarkup(keyboard))
    return PAY_CHECK

async def pay_user_sel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = int(update.callback_query.data.split("_")[-1])
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT full_name, balance FROM users WHERE user_id = ?", (user_id,)) as cursor:
            u_data = await cursor.fetchone()
            
    context.user_data['pay_id'] = user_id
    context.user_data['pay_amount'] = u_data[1]
    context.user_data['pay_name'] = u_data[0]
    
    await update.callback_query.message.reply_text(
        f"💳 Выплата для <b>{u_data[0]}</b>\nСумма: <b>${u_data[1]:.2f}</b>\n\n⬇️ Вставьте чек CryptoBot или код транзакции:", 
        parse_mode=ParseMode.HTML, reply_markup=ReplyKeyboardMarkup([['❌ Отмена']], resize_keyboard=True)
    )
    return PAY_CONFIRM

async def pay_confirm_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['check_code'] = update.message.text
    data = context.user_data
    text = (
        f"⚠️ <b>ПОДТВЕРЖДЕНИЕ ВЫПЛАТЫ</b>\n"
        f"👤 Воркер: {data['pay_name']}\n"
        f"💰 Сумма: ${data['pay_amount']:.2f}\n"
        f"🧾 Чек: {data['check_code']}\n\n"
        f"Обнуляем баланс и отправляем?"
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ ВЫПЛАТИТЬ", callback_data="pay_commit")],
        [InlineKeyboardButton("❌ Отмена", callback_data="cancel_pay")]
    ])
    await update.message.reply_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)
    return PAY_CONFIRM 

async def pay_execute(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data = context.user_data
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE users SET balance = 0 WHERE user_id = ?", (data['pay_id'],))
        await db.execute("INSERT INTO payouts (worker_id, check_code, amount) VALUES (?, ?, ?)", 
                         (data['pay_id'], data['check_code'], data['pay_amount']))
        await db.commit()
        
    await update.callback_query.message.edit_text("✅ <b>Выплата проведена успешно!</b>", parse_mode=ParseMode.HTML)
    
    try:
        await context.bot.send_message(data['pay_id'], 
            f"💸 <b>ВЫПЛАТА ПОЛУЧЕНА</b>\n━━━━━━━━━━━━━━━━━━\n"
            f"💳 <b>Сумма:</b> <code>${data['pay_amount']:.2f}</code>\n"
            f"🧾 <b>Чек:</b> <code>{data['check_code']}</code>\n"
            f"📅 <b>Дата:</b> {datetime.now().strftime('%d.%m.%Y')}\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🫡 <i>Спасибо за отличную работу.</i>",
            parse_mode=ParseMode.HTML)
    except: pass
    
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
            PROF_CONFIRM:[CallbackQueryHandler(prof_confirm, pattern="^prof_commit$"), CallbackQueryHandler(cancel_cb, pattern="^cancel_prof$")]
        },
        fallbacks=[MessageHandler(filters.Regex("^❌ Отмена$"), cancel_op), CallbackQueryHandler(cancel_cb, pattern="^cancel_prof$")]
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
    
    app.add_handler(CallbackQueryHandler(menu_stats, pattern="^menu_stats$"))
    app.add_handler(CallbackQueryHandler(menu_clients, pattern="^menu_clients"))
    app.add_handler(CallbackQueryHandler(client_view, pattern="^client_view_"))
    app.add_handler(CallbackQueryHandler(menu_profits, pattern="^menu_profits$"))
    app.add_handler(CallbackQueryHandler(menu_salary, pattern="^menu_salary$"))
    app.add_handler(CallbackQueryHandler(menu_main, pattern="^menu_main$"))
    app.add_handler(CallbackQueryHandler(admin_dashboard, pattern="^admin_dashboard$"))
    app.add_handler(CallbackQueryHandler(adm_users_list, pattern="^adm_users_list$"))

    print("✅ Bot is running (Python-Telegram-Bot v20+)...")
    app.run_polling()
