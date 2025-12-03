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

BOT_TOKEN = os.getenv("BOT_TOKEN", "8053044453:AAGHu89oQfOKj_Q-nk7sr1XwTZhSXk1J9ZI")
ACCESS_PASSWORD = os.getenv("ACCESS_PASSWORD", "Zxcv1236")
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "844012884").split(",")] if os.getenv("ADMIN_IDS") else []
DB_NAME = "team_stats.db"

# Пути к изображениям
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
IMG_PATHS = {
    "welcome": os.path.join(BASE_DIR, "logo.png"),
    "profile": os.path.join(BASE_DIR, "profile.png"),
    "pay": os.path.join(BASE_DIR, "pay.png")
}

# Логирование
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# --- 🗄️ DATABASE & MIGRATIONS ---
async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("PRAGMA journal_mode=WAL;")
        await db.execute("PRAGMA synchronous=NORMAL;")
        
        # Основные таблицы
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT, full_name TEXT,
                balance REAL DEFAULT 0, total_earned REAL DEFAULT 0,
                is_admin INTEGER DEFAULT 0, is_analyst INTEGER DEFAULT 0, is_manager INTEGER DEFAULT 0,
                analyst_balance REAL DEFAULT 0, analyst_total_earned REAL DEFAULT 0,
                manager_balance REAL DEFAULT 0, manager_total_earned REAL DEFAULT 0,
                date_joined TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.execute("CREATE TABLE IF NOT EXISTS clients (id INTEGER PRIMARY KEY AUTOINCREMENT, worker_id INTEGER, name TEXT, total_squeezed REAL DEFAULT 0)")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS profits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                worker_id INTEGER, client_id INTEGER, amount REAL, worker_share REAL,
                analyst_id INTEGER, analyst_share REAL DEFAULT 0, analyst_percent REAL,
                manager_id INTEGER, manager_share REAL DEFAULT 0, manager_percent REAL,
                direction TEXT, stage TEXT, percent REAL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS payouts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                worker_id INTEGER, check_code TEXT, amount REAL,
                is_received INTEGER DEFAULT 0,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Миграции (безопасное добавление колонок)
        columns = [
            ("users", "is_analyst INTEGER DEFAULT 0"), ("users", "is_manager INTEGER DEFAULT 0"),
            ("users", "analyst_balance REAL DEFAULT 0"), ("users", "analyst_total_earned REAL DEFAULT 0"),
            ("users", "manager_balance REAL DEFAULT 0"), ("users", "manager_total_earned REAL DEFAULT 0"),
            ("profits", "analyst_id INTEGER"), ("profits", "analyst_share REAL DEFAULT 0"), ("profits", "analyst_percent REAL"),
            ("profits", "manager_id INTEGER"), ("profits", "manager_share REAL DEFAULT 0"), ("profits", "manager_percent REAL"),
            ("payouts", "is_received INTEGER DEFAULT 0")
        ]
        for table, col_def in columns:
            try: await db.execute(f"ALTER TABLE {table} ADD COLUMN {col_def}")
            except: pass
        await db.commit()

# --- 🛠️ HELPERS ---
SEPARATOR = "━━━━━━━━━━━━━━━━━━"

async def get_db_user(user_id):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)) as cursor:
            return await cursor.fetchone()

async def is_admin(user_id):
    user = await get_db_user(user_id)
    return bool(user[5]) if user and len(user) > 5 else False

def format_money(amount):
    return f"${amount:,.2f}"

def get_rank(earned):
    if earned < 100: return "Новичок 🐣", 100
    if earned < 1000: return "Бывалый 👊", 1000
    if earned < 5000: return "Хищник 🦈", 5000
    if earned < 10000: return "Машина 🤖", 10000
    return "Легенда 👑", 0

async def send_screen(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, photo_key: str = None, markup=None):
    """Умная отправка сообщений: редактирует, если можно, переотправляет, если нужно фото."""
    query = update.callback_query
    message = query.message if query else update.message
    file_path = IMG_PATHS.get(photo_key)
    has_photo = file_path and os.path.exists(file_path)
    
    try:
        if query: # Это Callback (нажатие кнопки)
            if has_photo:
                if message.photo: # Фото уже есть, меняем медиа
                    with open(file_path, 'rb') as f:
                        await message.edit_media(InputMediaPhoto(f, caption=text, parse_mode=ParseMode.HTML), reply_markup=markup)
                else: # Фото нет, удаляем текст и шлем фото
                    await message.delete()
                    with open(file_path, 'rb') as f:
                        await message.reply_photo(f, caption=text, reply_markup=markup, parse_mode=ParseMode.HTML)
            else:
                if message.photo: # Было фото, но теперь нужен текст
                    await message.delete()
                    await message.reply_text(text, reply_markup=markup, parse_mode=ParseMode.HTML)
                else: # Просто редактируем текст
                    await message.edit_text(text, reply_markup=markup, parse_mode=ParseMode.HTML)
        else: # Это обычное сообщение (команда)
            if has_photo:
                with open(file_path, 'rb') as f:
                    await message.reply_photo(f, caption=text, reply_markup=markup, parse_mode=ParseMode.HTML)
            else:
                await message.reply_text(text, reply_markup=markup, parse_mode=ParseMode.HTML)
    except BadRequest as e:
        if "Message is not modified" not in str(e):
            logger.error(f"Send Screen Error: {e}")
            if not query: await message.reply_text(text, reply_markup=markup, parse_mode=ParseMode.HTML)

# --- 🚦 STATES ---
AUTH_PWD = 1
PROF_WORKER, PROF_CLIENT, PROF_AMOUNT, PROF_DIR, PROF_STAGE, PROF_PERCENT, \
PROF_ANALYST, PROF_ANALYST_PERCENT, PROF_MANAGER, PROF_MANAGER_PERCENT, PROF_CONFIRM = range(2, 13)
PAY_CHECK, PAY_CONFIRM = range(13, 15)
USER_SEARCH = 15

# --- 🎮 HANDLERS ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    db_user = await get_db_user(user.id)
    
    # Auto-admin env check
    if user.id in ADMIN_IDS:
        async with aiosqlite.connect(DB_NAME) as db:
            if not db_user:
                await db.execute("INSERT INTO users (user_id, username, full_name, is_admin) VALUES (?, ?, ?, 1)", 
                               (user.id, user.username or "Anon", user.full_name))
            else:
                await db.execute("UPDATE users SET is_admin = 1 WHERE user_id = ?", (user.id,))
            await db.commit()
        db_user = await get_db_user(user.id)

    if not db_user:
        await update.message.reply_text(
            f"<b>🔒 ДОСТУП ЗАПРЕЩЕН</b>\n{SEPARATOR}\n"
            f"Система не обнаружила вас в базе данных.\n"
            f"<i>Введите пароль доступа:</i>", 
            parse_mode=ParseMode.HTML
        )
        return AUTH_PWD

    # Prepare Dashboard Data
    user_id = user.id
    is_admin_flag = bool(db_user[5])
    async with aiosqlite.connect(DB_NAME) as db:
        # User Stats
        user_row = await db.execute_fetchall("SELECT * FROM users WHERE user_id = ?", (user_id,))
        ud = user_row[0] # user data tuple
        
        # Calc Profits
        now = datetime.now()
        month_start = now.replace(day=1, hour=0, minute=0, second=0)
        week_start = now.replace(hour=0, minute=0, second=0)
        week_start = week_start.replace(day=week_start.day - week_start.weekday())
        day_start = now.replace(hour=0, minute=0, second=0)

        async def get_profit(uid, role_col, ts):
            q = f"SELECT SUM({role_col}) FROM profits WHERE {'worker_id' if role_col == 'worker_share' else role_col.replace('_share','_id')} = ? AND timestamp >= ?"
            async with db.execute(q, (uid, ts)) as c: return (await c.fetchone())[0] or 0.0

        p_day = await get_profit(user_id, 'worker_share', day_start)
        p_week = await get_profit(user_id, 'worker_share', week_start)
        p_month = await get_profit(user_id, 'worker_share', month_start)
        
        # Count Clients
        async with db.execute("SELECT COUNT(*) FROM clients WHERE worker_id = ?", (user_id,)) as c:
            clients_cnt = (await c.fetchone())[0]

    rank_name, rank_next = get_rank(ud[4]) # total_earned index
    progress_bar = ""
    if rank_next > 0:
        pct = min((ud[4] / rank_next) * 100, 100)
        filled = int(pct / 10)
        progress_bar = f"\n<b>Lvl:</b> <code>{'▰'*filled}{'▱'*(10-filled)}</code> {pct:.0f}%"

    roles_txt = "Воркер"
    if ud[6]: roles_txt += ", Аналитик"
    if ud[7]: roles_txt += ", Менеджер"

    text = (
        f"<b>👋 Привет, {user.first_name}!</b>\n"
        f"{SEPARATOR}\n"
        f"<b>Статус:</b> {roles_txt}\n"
        f"<b>Ранг:</b> {rank_name}{progress_bar}\n\n"
        f"<b>💳 БАЛАНС:</b> <code>{format_money(ud[3])}</code>\n"
        f"<b>💰 ВСЕГО:</b> <code>{format_money(ud[4])}</code>\n"
        f"{SEPARATOR}\n"
        f"<b>📊 СТАТИСТИКА ПРОФИТОВ</b>\n"
        f"🔹 День:   <code>{format_money(p_day)}</code>\n"
        f"🔹 Неделя: <code>{format_money(p_week)}</code>\n"
        f"🔹 Месяц:  <code>{format_money(p_month)}</code>\n\n"
        f"<b>🦣 Мамонтов:</b> <code>{clients_cnt}</code>"
    )

    # Additional Roles
    if ud[6]: # Analyst
        text += f"\n\n<b>🔬 АНАЛИТИК</b>\nБаланс: <code>{format_money(ud[8])}</code> | Всего: <code>{format_money(ud[9])}</code>"
    if ud[7]: # Manager
        text += f"\n\n<b>👔 МЕНЕДЖЕР</b>\nБаланс: <code>{format_money(ud[10])}</code> | Всего: <code>{format_money(ud[11])}</code>"

    # Keyboard
    kb = [
        [InlineKeyboardButton("🦣 Мои Мамонты", callback_data="menu_clients_0"), 
         InlineKeyboardButton("💳 Финансы", callback_data="menu_finances")],
        [InlineKeyboardButton("🏆 Топы и Статистика", callback_data="menu_tops_analytics")]
    ]
    if is_admin_flag:
        kb.append([InlineKeyboardButton("⚡️ ADMIN PANEL", callback_data="admin_dashboard")])
    kb.append([InlineKeyboardButton("🔄 Обновить", callback_data="menu_main")])

    await send_screen(update, context, text, "profile", InlineKeyboardMarkup(kb))
    return ConversationHandler.END

async def auth_password(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    try: await msg.delete()
    except: pass
    
    if msg.text == ACCESS_PASSWORD:
        user = update.effective_user
        is_adm = 1 if user.id in ADMIN_IDS else 0
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute("INSERT OR IGNORE INTO users (user_id, username, full_name, is_admin) VALUES (?, ?, ?, ?)",
                             (user.id, user.username or "Anon", user.full_name, is_adm))
            await db.commit()
        await start(update, context)
        return ConversationHandler.END
    else:
        info = await msg.reply_text("<b>❌ Неверный пароль</b>", parse_mode=ParseMode.HTML)
        await asyncio.sleep(2)
        try: await info.delete()
        except: pass
        return AUTH_PWD

async def auth_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Авторизация отменена. Нажмите /start")
    return ConversationHandler.END

# --- 🗂️ CLIENTS MENU ---
async def menu_clients(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    page = int(query.data.split("_")[-1])
    user_id = update.effective_user.id
    limit = 6
    offset = page * limit

    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT id, name, total_squeezed FROM clients WHERE worker_id = ? ORDER BY total_squeezed DESC LIMIT ? OFFSET ?", (user_id, limit, offset)) as c:
            clients = await c.fetchall()
        async with db.execute("SELECT COUNT(*) FROM clients WHERE worker_id = ?", (user_id,)) as c:
            total = (await c.fetchone())[0]

    kb = []
    # Grid layout for clients (2 columns)
    row = []
    for c in clients:
        row.append(InlineKeyboardButton(f"{c[1]} | ${c[2]:.0f}", callback_data=f"client_view_{c[0]}"))
        if len(row) == 2:
            kb.append(row)
            row = []
    if row: kb.append(row)

    nav = []
    if page > 0: nav.append(InlineKeyboardButton("⬅️", callback_data=f"menu_clients_{page-1}"))
    if offset + limit < total: nav.append(InlineKeyboardButton("➡️", callback_data=f"menu_clients_{page+1}"))
    if nav: kb.append(nav)
    kb.append([InlineKeyboardButton("🔙 Главное меню", callback_data="menu_main")])

    text = f"<b>🦣 ВАШИ МАМОНТЫ</b>\n{SEPARATOR}\nВсего: <code>{total}</code>\n\n<i>Выберите мамонта для деталей:</i>"
    await send_screen(update, context, text, None, InlineKeyboardMarkup(kb))

async def client_view(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cid = int(update.callback_query.data.split("_")[-1])
    async with aiosqlite.connect(DB_NAME) as db:
        client = await (await db.execute("SELECT name, total_squeezed FROM clients WHERE id = ?", (cid,))).fetchone()
        history = await (await db.execute("SELECT amount, stage, timestamp, direction FROM profits WHERE client_id = ? ORDER BY timestamp DESC LIMIT 5", (cid,))).fetchall()

    text = (f"<b>👤 {client[0]}</b>\n{SEPARATOR}\n<b>💵 Профит:</b> <code>{format_money(client[1])}</code>\n\n<b>📜 Последние действия:</b>\n")
    if not history: text += "▫️ Пусто"
    for i, h in enumerate(history, 1):
        dt = datetime.strptime(h[2], "%Y-%m-%d %H:%M:%S").strftime("%d.%m")
        icon = {"BTC": "₿", "USDT": "₮", "Card": "💳"}.get(h[3], "💰")
        text += f"<b>{i}.</b> {icon} <code>{format_money(h[0])}</code> ({h[1]}) - {dt}\n"

    kb = [[InlineKeyboardButton("🔙 К списку", callback_data="menu_clients_0")]]
    await send_screen(update, context, text, None, InlineKeyboardMarkup(kb))

# --- 💳 FINANCES MENU ---
async def menu_finances(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Unified finance menu
    kb = [
        [InlineKeyboardButton("📜 История профитов", callback_data="menu_profits"),
         InlineKeyboardButton("💸 Зарплата и выплаты", callback_data="menu_salary")],
        [InlineKeyboardButton("🔙 Назад", callback_data="menu_main")]
    ]
    await send_screen(update, context, f"<b>💳 ФИНАНСЫ</b>\n{SEPARATOR}\nВыберите раздел:", "pay", InlineKeyboardMarkup(kb))

async def menu_profits(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    period = query.data.split("_")[-1] if "period" in query.data else "all"
    uid = update.effective_user.id
    
    # Period logic
    now = datetime.now()
    if period == 'day': start_dt = now.replace(hour=0, minute=0, second=0)
    elif period == 'week': start_dt = now.replace(day=now.day - now.weekday(), hour=0, minute=0, second=0)
    elif period == 'month': start_dt = now.replace(day=1, hour=0, minute=0, second=0)
    else: start_dt = None

    where = "WHERE worker_id = ?"
    params = [uid]
    if start_dt:
        where += " AND timestamp >= ?"
        params.append(start_dt)

    async with aiosqlite.connect(DB_NAME) as db:
        stats = await (await db.execute(f"SELECT SUM(worker_share), COUNT(*) FROM profits {where}", tuple(params))).fetchone()
        rows = await (await db.execute(f"""
            SELECT p.amount, p.worker_share, p.stage, c.name, p.direction, p.timestamp 
            FROM profits p JOIN clients c ON p.client_id = c.id {where} 
            ORDER BY p.timestamp DESC LIMIT 15
        """, tuple(params))).fetchall()

    total, count = stats[0] or 0, stats[1] or 0
    
    text = f"<b>📈 МОИ ПРОФИТЫ</b>\n{SEPARATOR}\n" \
           f"Период: <b>{period.upper()}</b>\n" \
           f"Залетов: <code>{count}</code> | Сумма: <code>{format_money(total)}</code>\n\n"
    
    for r in rows:
        dt = datetime.strptime(r[5], "%Y-%m-%d %H:%M:%S").strftime("%d.%m")
        emo = {"BTC": "₿", "USDT": "₮"}.get(r[4], "💳")
        text += f"▪️ {emo} <code>{format_money(r[1])}</code> | {r[3]} | {dt}\n"

    kb = [
        [InlineKeyboardButton("День", callback_data="profit_period_day"),
         InlineKeyboardButton("Неделя", callback_data="profit_period_week"),
         InlineKeyboardButton("Месяц", callback_data="profit_period_month")],
        [InlineKeyboardButton("Все время", callback_data="profit_period_all")],
        [InlineKeyboardButton("🔙 Финансы", callback_data="menu_finances")]
    ]
    await send_screen(update, context, text, None, InlineKeyboardMarkup(kb))

async def menu_salary(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    async with aiosqlite.connect(DB_NAME) as db:
        pending = await (await db.execute("SELECT id, amount, timestamp FROM payouts WHERE worker_id = ? AND is_received = 0", (uid,))).fetchall()
        paid = await (await db.execute("SELECT amount, timestamp, check_code FROM payouts WHERE worker_id = ? AND is_received = 1 ORDER BY timestamp DESC LIMIT 10", (uid,))).fetchall()
        total_paid = await (await db.execute("SELECT SUM(amount) FROM payouts WHERE worker_id = ? AND is_received = 1", (uid,))).fetchone()

    text = f"<b>💸 ЗАРПЛАТА</b>\n{SEPARATOR}\n" \
           f"<b>Всего выплачено:</b> <code>{format_money(total_paid[0] or 0)}</code>\n\n"

    kb = []
    if pending:
        text += "<b>⏳ ОЖИДАЮТ ПОЛУЧЕНИЯ:</b>\n"
        for p in pending:
            dt = datetime.strptime(p[2], "%Y-%m-%d %H:%M:%S").strftime("%d.%m")
            text += f"❗️ <code>{format_money(p[1])}</code> от {dt}\n"
            kb.append([InlineKeyboardButton(f"✅ Получить {format_money(p[1])}", callback_data=f"receive_payout_{p[0]}")])
        text += "\n"
    
    text += "<b>📜 ПОСЛЕДНИЕ ВЫПЛАТЫ:</b>\n"
    if not paid: text += "<i>История пуста</i>"
    for p in paid:
        dt = datetime.strptime(p[1], "%Y-%m-%d %H:%M:%S").strftime("%d.%m")
        text += f"✅ <code>{format_money(p[0])}</code> | {dt}\n"

    kb.append([InlineKeyboardButton("🔙 Финансы", callback_data="menu_finances")])
    await send_screen(update, context, text, "pay", InlineKeyboardMarkup(kb))

async def receive_payout(update: Update, context: ContextTypes.DEFAULT_TYPE):
    pid = int(update.callback_query.data.split("_")[-1])
    async with aiosqlite.connect(DB_NAME) as db:
        row = await (await db.execute("SELECT check_code FROM payouts WHERE id = ? AND is_received = 0", (pid,))).fetchone()
        if row:
            await db.execute("UPDATE payouts SET is_received = 1 WHERE id = ?", (pid,))
            await db.commit()
            await context.bot.send_message(update.effective_user.id, f"<b>Ваш чек:</b>\n<code>{row[0]}</code>", parse_mode=ParseMode.HTML)
            await update.callback_query.answer("Чек отправлен в ЛС!")
        else:
            await update.callback_query.answer("Ошибка или чек уже получен", show_alert=True)
    await menu_salary(update, context)

# --- 🏆 TOPS & ANALYTICS ---
async def menu_tops_analytics(update: Update, context: ContextTypes.DEFAULT_TYPE):
    kb = [
        [InlineKeyboardButton("🏆 Топ Воркеров", callback_data="top_month"),
         InlineKeyboardButton("📊 Аналитика", callback_data="menu_analytics")],
        [InlineKeyboardButton("🔬 Топ Аналитиков", callback_data="top_analysts"),
         InlineKeyboardButton("👔 Топ Менеджеров", callback_data="top_managers")],
        [InlineKeyboardButton("🔙 Главное меню", callback_data="menu_main")]
    ]
    await send_screen(update, context, f"<b>🏆 ЗАЛ СЛАВЫ</b>\n{SEPARATOR}\nВыберите категорию:", None, InlineKeyboardMarkup(kb))

async def menu_tops(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    period = query.data.split("_")[-1]
    now = datetime.now()
    
    if period == 'month': 
        start_ts = now.replace(day=1, hour=0, minute=0, second=0)
        title = "МЕСЯЦ"
    elif period == 'week':
        start_ts = now.replace(hour=0, minute=0, second=0)
        start_ts = start_ts.replace(day=start_ts.day - start_ts.weekday())
        title = "НЕДЕЛЮ"
    else: 
        start_ts = None
        title = "ВСЕ ВРЕМЯ"

    sql = """
        SELECT u.full_name, SUM(p.worker_share) as total 
        FROM users u JOIN profits p ON u.user_id = p.worker_id 
    """
    if start_ts: sql += f" WHERE p.timestamp >= '{start_ts}'"
    sql += " GROUP BY u.user_id ORDER BY total DESC LIMIT 10"

    async with aiosqlite.connect(DB_NAME) as db:
        rows = await (await db.execute(sql)).fetchall()

    text = f"<b>🏆 ТОП ВОРКЕРОВ ({title})</b>\n{SEPARATOR}\n"
    medals = ["🥇", "🥈", "🥉"]
    for i, (name, total) in enumerate(rows):
        icon = medals[i] if i < 3 else f"{i+1}."
        text += f"{icon} <b>{name}</b> — <code>{format_money(total)}</code>\n"

    kb = [
        [InlineKeyboardButton("Неделя", callback_data="top_week"), InlineKeyboardButton("Месяц", callback_data="top_month"), InlineKeyboardButton("Все время", callback_data="top_all")],
        [InlineKeyboardButton("🔙 Назад", callback_data="menu_tops_analytics")]
    ]
    await send_screen(update, context, text, None, InlineKeyboardMarkup(kb))

async def menu_analytics(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    async with aiosqlite.connect(DB_NAME) as db:
        # Simple stats
        stats = await (await db.execute("SELECT SUM(worker_share), COUNT(*) FROM profits WHERE worker_id = ?", (uid,))).fetchone()
        top_dir = await (await db.execute("SELECT direction, COUNT(*) as c FROM profits WHERE worker_id = ? GROUP BY direction ORDER BY c DESC LIMIT 1", (uid,))).fetchone()
    
    text = (
        f"<b>📊 ЛИЧНАЯ АНАЛИТИКА</b>\n{SEPARATOR}\n"
        f"<b>Всего профитов:</b> <code>{stats[1] or 0}</code>\n"
        f"<b>Общая сумма:</b> <code>{format_money(stats[0] or 0)}</code>\n"
        f"<b>Ср. чек:</b> <code>{format_money((stats[0] or 0)/(stats[1] or 1))}</code>\n"
    )
    if top_dir: text += f"<b>Любимое направление:</b> {top_dir[0]}"
    
    await send_screen(update, context, text, None, InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="menu_tops_analytics")]]))

async def top_special(update: Update, context: ContextTypes.DEFAULT_TYPE, role_col):
    now = datetime.now()
    start_ts = now.replace(day=1, hour=0, minute=0, second=0)
    role_name = "АНАЛИТИКОВ" if "analyst" in role_col else "МЕНЕДЖЕРОВ"
    
    sql = f"""
        SELECT u.full_name, SUM(p.{role_col}) as total 
        FROM users u JOIN profits p ON u.user_id = p.{role_col.replace('_share','_id')}
        WHERE p.timestamp >= ? AND p.{role_col} > 0
        GROUP BY u.user_id ORDER BY total DESC LIMIT 10
    """
    async with aiosqlite.connect(DB_NAME) as db:
        rows = await (await db.execute(sql, (start_ts,))).fetchall()

    text = f"<b>🏆 ТОП {role_name} (Месяц)</b>\n{SEPARATOR}\n"
    for i, (name, total) in enumerate(rows, 1):
        text += f"<b>{i}. {name}</b> — <code>{format_money(total)}</code>\n"
    
    await send_screen(update, context, text, None, InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="menu_tops_analytics")]]))

async def top_analysts(update: Update, context: ContextTypes.DEFAULT_TYPE): await top_special(update, context, 'analyst_share')
async def top_managers(update: Update, context: ContextTypes.DEFAULT_TYPE): await top_special(update, context, 'manager_share')

# --- 🔐 ADMIN PANEL ---
async def admin_dashboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id): return
    
    async with aiosqlite.connect(DB_NAME) as db:
        total_team = (await (await db.execute("SELECT SUM(total_earned) FROM users")).fetchone())[0] or 0
        debt = (await (await db.execute("SELECT SUM(balance + analyst_balance + manager_balance) FROM users")).fetchone())[0] or 0
        
        day_start = datetime.now().replace(hour=0, minute=0, second=0)
        day_prof = (await (await db.execute("SELECT SUM(amount) FROM profits WHERE timestamp >= ?", (day_start,))).fetchone())[0] or 0
        
    text = (
        f"<b>🔐 ADMIN PANEL</b>\n{SEPARATOR}\n"
        f"<b>💰 Оборот команды:</b> <code>{format_money(total_team)}</code>\n"
        f"<b>📉 Долг по ЗП:</b> <code>{format_money(debt)}</code>\n"
        f"<b>🌅 Профиты сегодня:</b> <code>{format_money(day_prof)}</code>"
    )
    
    kb = [
        [InlineKeyboardButton("➕ Внести профит", callback_data="adm_start_profit"),
         InlineKeyboardButton("💸 Выплатить ЗП", callback_data="adm_start_pay")],
        [InlineKeyboardButton("👥 Список воркеров", callback_data="adm_users_list_0")],
        [InlineKeyboardButton("🔙 Выход", callback_data="menu_main")]
    ]
    await send_screen(update, context, text, None, InlineKeyboardMarkup(kb))

async def adm_users_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    page = int(update.callback_query.data.split("_")[-1])
    limit = 8; offset = page * limit
    
    async with aiosqlite.connect(DB_NAME) as db:
        users = await (await db.execute(f"SELECT user_id, full_name, balance FROM users ORDER BY balance DESC LIMIT {limit} OFFSET {offset}")).fetchall()
        total = (await (await db.execute("SELECT COUNT(*) FROM users")).fetchone())[0]

    kb = []
    # Grid 2 cols
    row = []
    for u in users:
        row.append(InlineKeyboardButton(f"{u[1][:10]}.. ${u[2]:.0f}", callback_data=f"user_edit_{u[0]}"))
        if len(row) == 2: kb.append(row); row = []
    if row: kb.append(row)

    nav = []
    if page > 0: nav.append(InlineKeyboardButton("⬅️", callback_data=f"adm_users_list_{page-1}"))
    if offset + limit < total: nav.append(InlineKeyboardButton("➡️", callback_data=f"adm_users_list_{page+1}"))
    kb.append(nav)
    kb.append([InlineKeyboardButton("🔙 Админка", callback_data="admin_dashboard")])

    await send_screen(update, context, f"<b>👥 ВОРКЕРЫ ({total})</b>\nСтр. {page+1}", None, InlineKeyboardMarkup(kb))

async def user_edit_role(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = int(update.callback_query.data.split("_")[-1])
    async with aiosqlite.connect(DB_NAME) as db:
        u = await (await db.execute("SELECT full_name, is_analyst, is_manager FROM users WHERE user_id = ?", (uid,))).fetchone()
    
    kb = [
        [InlineKeyboardButton(f"{'✅' if u[1] else '❌'} Аналитик", callback_data=f"role_toggle_analyst_{uid}"),
         InlineKeyboardButton(f"{'✅' if u[2] else '❌'} Менеджер", callback_data=f"role_toggle_manager_{uid}")],
        [InlineKeyboardButton("🔙 Назад", callback_data="adm_users_list_0")]
    ]
    await send_screen(update, context, f"<b>⚙️ Права: {u[0]}</b>", None, InlineKeyboardMarkup(kb))

async def role_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    parts = update.callback_query.data.split("_")
    role, uid = parts[2], int(parts[3])
    async with aiosqlite.connect(DB_NAME) as db:
        curr = (await (await db.execute(f"SELECT is_{role} FROM users WHERE user_id = ?", (uid,))).fetchone())[0]
        await db.execute(f"UPDATE users SET is_{role} = ? WHERE user_id = ?", (0 if curr else 1, uid))
        await db.commit()
    await user_edit_role(update, context)

# --- ➕ ADD PROFIT CONVERSATION ---
async def prof_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    async with aiosqlite.connect(DB_NAME) as db:
        workers = await (await db.execute("SELECT user_id, full_name FROM users ORDER BY full_name")).fetchall()
    
    kb = []
    row = []
    for w in workers:
        row.append(InlineKeyboardButton(w[1], callback_data=f"prof_sel_{w[0]}"))
        if len(row) == 2: kb.append(row); row = []
    if row: kb.append(row)
    kb.append([InlineKeyboardButton("🔙 Отмена", callback_data="cancel_prof")])
    
    await send_screen(update, context, "👤 <b>Выберите воркера:</b>", None, InlineKeyboardMarkup(kb))
    return PROF_WORKER

async def prof_worker_sel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['worker_id'] = int(update.callback_query.data.split("_")[-1])
    await update.callback_query.message.reply_text("✍️ <b>Имя Мамонта:</b>", reply_markup=ReplyKeyboardMarkup([['❌ Отмена']], resize_keyboard=True))
    return PROF_CLIENT

async def prof_client(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['client_name'] = update.message.text
    await update.message.reply_text("💰 <b>Сумма ($):</b>")
    return PROF_AMOUNT

async def prof_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        val = float(update.message.text.replace(",", "."))
        context.user_data['amount'] = val
        await update.message.reply_text("🏦 <b>Направление:</b>", reply_markup=ReplyKeyboardMarkup([["BTC", "USDT", "Card"], ["❌ Отмена"]], resize_keyboard=True))
        return PROF_DIR
    except:
        await update.message.reply_text("❌ Введите число!")
        return PROF_AMOUNT

async def prof_dir(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['direction'] = update.message.text
    await update.message.reply_text("📑 <b>Стадия:</b>", reply_markup=ReplyKeyboardMarkup([["Депозит", "Комиссия", "Налог"], ["❌ Отмена"]], resize_keyboard=True))
    return PROF_STAGE

async def prof_stage(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['stage'] = update.message.text
    await update.message.reply_text("📊 <b>Процент воркера (число):</b>")
    return PROF_PERCENT

async def prof_percent(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        pct = float(update.message.text)
        context.user_data['percent'] = pct
        context.user_data['worker_share'] = context.user_data['amount'] * (pct / 100)
        
        # Check Analysts
        async with aiosqlite.connect(DB_NAME) as db:
            analysts = await (await db.execute("SELECT user_id, full_name FROM users WHERE is_analyst=1")).fetchall()
        
        if analysts:
            kb = [[InlineKeyboardButton(a[1], callback_data=f"prof_analyst_{a[0]}")] for a in analysts]
            kb.append([InlineKeyboardButton("Пропустить", callback_data="prof_analyst_skip")])
            await update.message.reply_text("🔬 <b>Аналитик:</b>", reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.HTML)
            return PROF_ANALYST
        else:
            return await prof_skip_analyst(update, context)
    except: return PROF_PERCENT

async def prof_skip_analyst(update, context):
    context.user_data.update({'analyst_id': None, 'analyst_share': 0})
    return await prof_check_manager(update, context)

async def prof_analyst_sel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data = update.callback_query.data
    if "skip" in data: return await prof_skip_analyst(update, context)
    context.user_data['analyst_id'] = int(data.split("_")[-1])
    await update.callback_query.message.reply_text("📊 <b>Процент аналитика:</b>")
    return PROF_ANALYST_PERCENT

async def prof_analyst_pct(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        pct = float(update.message.text)
        context.user_data['analyst_percent'] = pct
        context.user_data['analyst_share'] = context.user_data['amount'] * (pct / 100)
        return await prof_check_manager(update, context)
    except: return PROF_ANALYST_PERCENT

async def prof_check_manager(update, context):
    msg = update.message if update.message else update.callback_query.message
    async with aiosqlite.connect(DB_NAME) as db:
        managers = await (await db.execute("SELECT user_id, full_name FROM users WHERE is_manager=1")).fetchall()
    
    if managers:
        kb = [[InlineKeyboardButton(m[1], callback_data=f"prof_manager_{m[0]}")] for m in managers]
        kb.append([InlineKeyboardButton("Пропустить", callback_data="prof_manager_skip")])
        await msg.reply_text("👔 <b>Менеджер:</b>", reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.HTML)
        return PROF_MANAGER
    else:
        return await prof_skip_manager(update, context)

async def prof_skip_manager(update, context):
    context.user_data.update({'manager_id': None, 'manager_share': 0})
    return await prof_confirm_screen(update, context)

async def prof_manager_sel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if "skip" in update.callback_query.data: return await prof_skip_manager(update, context)
    context.user_data['manager_id'] = int(update.callback_query.data.split("_")[-1])
    await update.callback_query.message.reply_text("📊 <b>Процент менеджера:</b>")
    return PROF_MANAGER_PERCENT

async def prof_manager_pct(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        pct = float(update.message.text)
        context.user_data['manager_percent'] = pct
        context.user_data['manager_share'] = context.user_data['amount'] * (pct / 100)
        return await prof_confirm_screen(update, context)
    except: return PROF_MANAGER_PERCENT

async def prof_confirm_screen(update, context):
    d = context.user_data
    msg = update.message if update.message else update.callback_query.message
    
    text = (
        f"<b>⚠️ ПРОВЕРКА</b>\n{SEPARATOR}\n"
        f"Мамонт: <b>{d['client_name']}</b>\n"
        f"Сумма: <code>{format_money(d['amount'])}</code>\n"
        f"Доля воркера: <code>{format_money(d['worker_share'])}</code> ({d['percent']}%)"
    )
    kb = [[InlineKeyboardButton("✅ Подтвердить", callback_data="prof_commit"), InlineKeyboardButton("❌ Отмена", callback_data="cancel_prof")]]
    await msg.reply_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.HTML)
    return PROF_CONFIRM

async def prof_commit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    d = context.user_data
    async with aiosqlite.connect(DB_NAME) as db:
        # Client
        c_row = await (await db.execute("SELECT id FROM clients WHERE worker_id=? AND name=?", (d['worker_id'], d['client_name']))).fetchone()
        if c_row: cid = c_row[0]; await db.execute("UPDATE clients SET total_squeezed = total_squeezed + ? WHERE id=?", (d['amount'], cid))
        else: cursor = await db.execute("INSERT INTO clients (worker_id, name, total_squeezed) VALUES (?, ?, ?)", (d['worker_id'], d['client_name'], d['amount'])); cid = cursor.lastrowid
        
        # Profit
        await db.execute("""
            INSERT INTO profits (worker_id, client_id, amount, worker_share, direction, stage, percent, analyst_id, analyst_share, analyst_percent, manager_id, manager_share, manager_percent)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (d['worker_id'], cid, d['amount'], d['worker_share'], d['direction'], d['stage'], d['percent'], d.get('analyst_id'), d.get('analyst_share',0), d.get('analyst_percent'), d.get('manager_id'), d.get('manager_share',0), d.get('manager_percent')))
        
        # Balances
        await db.execute("UPDATE users SET balance=balance+?, total_earned=total_earned+? WHERE user_id=?", (d['worker_share'], d['worker_share'], d['worker_id']))
        if d.get('analyst_id'): await db.execute("UPDATE users SET analyst_balance=analyst_balance+?, analyst_total_earned=analyst_total_earned+? WHERE user_id=?", (d['analyst_share'], d['analyst_share'], d['analyst_id']))
        if d.get('manager_id'): await db.execute("UPDATE users SET manager_balance=manager_balance+?, manager_total_earned=manager_total_earned+? WHERE user_id=?", (d['manager_share'], d['manager_share'], d['manager_id']))
        await db.commit()

    await update.callback_query.message.edit_text("✅ <b>Успешно добавлено!</b>", parse_mode=ParseMode.HTML)
    
    # Notify Worker
    try: await context.bot.send_message(d['worker_id'], f"🚀 <b>НОВЫЙ ПРОФИТ!</b>\nДоля: <code>{format_money(d['worker_share'])}</code>", parse_mode=ParseMode.HTML)
    except: pass
    
    await admin_dashboard(update, context)
    return ConversationHandler.END

# --- 💸 PAYOUT CONVERSATION ---
async def pay_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    async with aiosqlite.connect(DB_NAME) as db:
        users = await (await db.execute("SELECT user_id, full_name, (balance + analyst_balance + manager_balance) as total FROM users WHERE (balance + analyst_balance + manager_balance) > 0")).fetchall()
    
    if not users:
        await update.callback_query.answer("🤷‍♂️ Платить некому!", show_alert=True)
        return ConversationHandler.END

    kb = []
    row = []
    for u in users:
        row.append(InlineKeyboardButton(f"{u[1]} (${u[2]:.0f})", callback_data=f"pay_sel_{u[0]}"))
        if len(row) == 2: kb.append(row); row = []
    if row: kb.append(row)
    kb.append([InlineKeyboardButton("🔙 Отмена", callback_data="cancel_pay")])
    
    await send_screen(update, context, "💸 <b>Выберите кому платить:</b>", None, InlineKeyboardMarkup(kb))
    return PAY_CHECK

async def pay_sel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = int(update.callback_query.data.split("_")[-1])
    async with aiosqlite.connect(DB_NAME) as db:
        u = await (await db.execute("SELECT full_name, (balance+analyst_balance+manager_balance) FROM users WHERE user_id=?", (uid,))).fetchone()
    
    context.user_data.update({'pay_id': uid, 'pay_amount': u[1], 'pay_name': u[0]})
    await update.callback_query.message.reply_text(f"💳 <b>К выплате:</b> <code>{format_money(u[1])}</code>\n\n👇 Отправьте ЧЕК или код транзакции:", reply_markup=ReplyKeyboardMarkup([['❌ Отмена']], resize_keyboard=True), parse_mode=ParseMode.HTML)
    return PAY_CONFIRM

async def pay_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    check = update.message.text
    d = context.user_data
    
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("INSERT INTO payouts (worker_id, check_code, amount, is_received) VALUES (?, ?, ?, 0)", (d['pay_id'], check, d['pay_amount']))
        await db.execute("UPDATE users SET balance=0, analyst_balance=0, manager_balance=0 WHERE user_id=?", (d['pay_id'],))
        await db.commit()
    
    await update.message.reply_text("✅ <b>Выплата оформлена!</b>", reply_markup=ReplyKeyboardRemove(), parse_mode=ParseMode.HTML)
    try: await context.bot.send_message(d['pay_id'], f"💸 <b>ВАМ ПРИШЛА ВЫПЛАТА!</b>\nСумма: <code>{format_money(d['pay_amount'])}</code>\nЗаберите чек в меню Зарплата.", parse_mode=ParseMode.HTML)
    except: pass
    
    await admin_dashboard(update, context)
    return ConversationHandler.END

async def cancel_op(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🚫 Отмена", reply_markup=ReplyKeyboardRemove())
    return ConversationHandler.END

async def cancel_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.message.delete()
    await admin_dashboard(update, context)
    return ConversationHandler.END

# --- 🚀 RUN ---
if __name__ == "__main__":
    if not BOT_TOKEN: sys.exit("❌ TOKEN NOT FOUND")

    app = ApplicationBuilder().token(BOT_TOKEN).build()
    
    # Init DB
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(init_db())

    # Handlers
    app.add_handler(ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={AUTH_PWD: [MessageHandler(filters.TEXT & ~filters.COMMAND, auth_password)]},
        fallbacks=[CommandHandler("cancel", auth_cancel)]
    ))
    
    app.add_handler(ConversationHandler(
        entry_points=[CallbackQueryHandler(prof_start, pattern="^adm_start_profit$")],
        states={
            PROF_WORKER: [CallbackQueryHandler(prof_worker_sel, pattern="^prof_sel_"), CallbackQueryHandler(cancel_cb, pattern="^cancel_prof$")],
            PROF_CLIENT: [MessageHandler(filters.TEXT & ~filters.Regex("^❌ Отмена$"), prof_client)],
            PROF_AMOUNT: [MessageHandler(filters.TEXT & ~filters.Regex("^❌ Отмена$"), prof_amount)],
            PROF_DIR:    [MessageHandler(filters.TEXT & ~filters.Regex("^❌ Отмена$"), prof_dir)],
            PROF_STAGE:  [MessageHandler(filters.TEXT & ~filters.Regex("^❌ Отмена$"), prof_stage)],
            PROF_PERCENT:[MessageHandler(filters.TEXT & ~filters.Regex("^❌ Отмена$"), prof_percent)],
            PROF_ANALYST:[CallbackQueryHandler(prof_analyst_sel, pattern="^prof_analyst_"), CallbackQueryHandler(cancel_cb, pattern="^cancel_prof$")],
            PROF_ANALYST_PERCENT: [MessageHandler(filters.TEXT & ~filters.Regex("^❌ Отмена$"), prof_analyst_pct)],
            PROF_MANAGER:[CallbackQueryHandler(prof_manager_sel, pattern="^prof_manager_"), CallbackQueryHandler(cancel_cb, pattern="^cancel_prof$")],
            PROF_MANAGER_PERCENT: [MessageHandler(filters.TEXT & ~filters.Regex("^❌ Отмена$"), prof_manager_pct)],
            PROF_CONFIRM:[CallbackQueryHandler(prof_commit, pattern="^prof_commit$"), CallbackQueryHandler(cancel_cb, pattern="^cancel_prof$")]
        },
        fallbacks=[MessageHandler(filters.ALL, cancel_op)]
    ))

    app.add_handler(ConversationHandler(
        entry_points=[CallbackQueryHandler(pay_start, pattern="^adm_start_pay$")],
        states={
            PAY_CHECK: [CallbackQueryHandler(pay_sel, pattern="^pay_sel_"), CallbackQueryHandler(cancel_cb, pattern="^cancel_pay$")],
            PAY_CONFIRM: [MessageHandler(filters.TEXT & ~filters.Regex("^❌ Отмена$"), pay_confirm)]
        },
        fallbacks=[MessageHandler(filters.ALL, cancel_op)]
    ))

    # Menu Callbacks
    async def menu_main(u, c): await start(u, c)
    app.add_handler(CallbackQueryHandler(menu_main, pattern="^menu_main$"))
    app.add_handler(CallbackQueryHandler(menu_clients, pattern="^menu_clients"))
    app.add_handler(CallbackQueryHandler(client_view, pattern="^client_view_"))
    app.add_handler(CallbackQueryHandler(menu_finances, pattern="^menu_finances$"))
    app.add_handler(CallbackQueryHandler(menu_profits, pattern="^menu_profits|profit_period_"))
    app.add_handler(CallbackQueryHandler(menu_salary, pattern="^menu_salary$"))
    app.add_handler(CallbackQueryHandler(receive_payout, pattern="^receive_payout_"))
    app.add_handler(CallbackQueryHandler(menu_tops_analytics, pattern="^menu_tops_analytics$"))
    app.add_handler(CallbackQueryHandler(menu_tops, pattern="^top_(week|month|all)$"))
    app.add_handler(CallbackQueryHandler(menu_analytics, pattern="^menu_analytics$"))
    app.add_handler(CallbackQueryHandler(top_analysts, pattern="^top_analysts$"))
    app.add_handler(CallbackQueryHandler(top_managers, pattern="^top_managers$"))
    app.add_handler(CallbackQueryHandler(admin_dashboard, pattern="^admin_dashboard$"))
    app.add_handler(CallbackQueryHandler(adm_users_list, pattern="^adm_users_list"))
    app.add_handler(CallbackQueryHandler(user_edit_role, pattern="^user_edit_"))
    app.add_handler(CallbackQueryHandler(role_toggle, pattern="^role_toggle_"))

    # Alias for menu_main to link back
    async def menu_main_wrapper(u, c): await start(u, c)
    app.add_handler(CallbackQueryHandler(menu_main_wrapper, pattern="^menu_main$"))

    print("🤖 Bot Started!")
    app.run_polling()
