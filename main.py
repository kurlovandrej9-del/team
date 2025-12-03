import logging
import os
import sys
import asyncio
import aiosqlite
from datetime import datetime
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, F, Router
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup,
    KeyboardButton, ReplyKeyboardMarkup, ReplyKeyboardRemove,
    InputMediaPhoto, FSInputFile
)
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.exceptions import TelegramBadRequest

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
    "welcome": "/Users/nikitakurlov/tima/logo.png",
    "profile": "/Users/nikitakurlov/tima/profile.png",
    "pay": "/Users/nikitakurlov/tima/pay.png"
}

# LOGGING
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

# --- 🚦 STATES (FSM) ---
class AuthStates(StatesGroup):
    pwd = State()

class ProfitStates(StatesGroup):
    worker = State()
    client = State()
    amount = State()
    direction = State()
    stage = State()
    percent = State()
    confirm = State()

class PayStates(StatesGroup):
    check = State()
    confirm = State()

# --- 🗄️ DATABASE FUNCTIONS ---
async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
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

async def get_db_user(user_id):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)) as cursor:
            return await cursor.fetchone()

async def is_admin(user_id):
    user = await get_db_user(user_id)
    return bool(user and user[5])

# --- 🛠️ HELPERS ---
def get_main_menu_kb(is_admin_flag: bool):
    keyboard = [
        [
            InlineKeyboardButton(text="📊 Моя Статистика", callback_data="menu_stats"),
            InlineKeyboardButton(text="🦣 Мои Мамонты", callback_data="menu_clients_0")
        ],
        [
            InlineKeyboardButton(text="💳 История Выплат", callback_data="menu_salary"),
            InlineKeyboardButton(text="📈 Лог Профитов", callback_data="menu_profits")
        ]
    ]
    if is_admin_flag:
        keyboard.append([InlineKeyboardButton(text="⚡️ ADMIN PANEL", callback_data="admin_dashboard")])
    
    keyboard.append([InlineKeyboardButton(text="🔄 Обновить данные", callback_data="menu_main")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_back_kb(target="menu_main"):
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data=target)]])

async def send_screen(event, text: str, photo_key: str = None, markup=None):
    """
    Robust message sender for Aiogram 3. Handles edits vs new messages and image presence.
    Event can be Message or CallbackQuery.
    """
    is_callback = isinstance(event, CallbackQuery)
    message = event.message if is_callback else event
    
    file_path = IMG_PATHS.get(photo_key)
    has_photo = file_path and os.path.exists(file_path)
    
    try:
        if is_callback:
            # Editing existing message
            if has_photo:
                photo_file = FSInputFile(file_path)
                if message.photo:
                    media = InputMediaPhoto(media=photo_file, caption=text, parse_mode="HTML")
                    await message.edit_media(media=media, reply_markup=markup)
                else:
                    # Message had no photo, delete and send new
                    await message.delete()
                    await message.answer_photo(photo=photo_file, caption=text, reply_markup=markup, parse_mode="HTML")
            else:
                # No photo needed
                if message.photo:
                    await message.delete()
                    await message.answer(text, reply_markup=markup, parse_mode="HTML")
                else:
                    await message.edit_text(text, reply_markup=markup, parse_mode="HTML")
        else:
            # New message
            if has_photo:
                photo_file = FSInputFile(file_path)
                await message.answer_photo(photo=photo_file, caption=text, reply_markup=markup, parse_mode="HTML")
            else:
                await message.answer(text, reply_markup=markup, parse_mode="HTML")
                
    except TelegramBadRequest as e:
        if "message is not modified" in str(e).lower():
            pass
        else:
            logger.error(f"Send Screen Error: {e}")
            if not is_callback:
                await message.answer(text, reply_markup=markup, parse_mode="HTML")

# --- 🎮 HANDLERS ---
dp = Dispatcher(storage=MemoryStorage())

@dp.message(Command("start"))
async def start(message: Message, state: FSMContext):
    await state.clear()
    user = message.from_user
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
        await send_screen(message, text, "welcome", get_main_menu_kb(is_admin_flag))
    else:
        text = (
            f"⛔️ <b>ACCESS DENIED</b>\n\n"
            f"Система закрытого доступа. Ваша учетная запись не обнаружена.\n"
            f"<i>Введите ключ доступа для активации рабочего места:</i>"
        )
        await message.answer(text, parse_mode="HTML")
        await state.set_state(AuthStates.pwd)

@dp.message(AuthStates.pwd)
async def auth_password(message: Message, state: FSMContext):
    try: await message.delete()
    except: pass
    
    if message.text == ACCESS_PASSWORD:
        user = message.from_user
        is_admin_flag = 1 if user.id in ADMIN_IDS else 0
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute(
                "INSERT OR IGNORE INTO users (user_id, username, full_name, is_admin) VALUES (?, ?, ?, ?)",
                (user.id, user.username or "Anon", user.full_name, is_admin_flag)
            )
            await db.commit()
        
        text = f"✅ <b>Доступ разрешен.</b>\nДобро пожаловать в команду."
        await state.clear()
        await send_screen(message, text, "welcome", get_main_menu_kb(bool(is_admin_flag)))
    else:
        reply = await message.answer("❌ Неверный пароль.")
        await asyncio.sleep(2)
        try: await reply.delete()
        except: pass
        # Remain in AuthStates.pwd

@dp.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("🚫 Авторизация отменена.")

# --- 📊 MENU CALLBACKS ---

@dp.callback_query(F.data == "menu_main")
async def cb_menu_main(query: CallbackQuery, state: FSMContext):
    await start(query.message, state)

@dp.callback_query(F.data == "menu_stats")
async def menu_stats(query: CallbackQuery):
    user_id = query.from_user.id
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT full_name, total_earned, balance FROM users WHERE user_id = ?", (user_id,)) as cursor:
            user_data = await cursor.fetchone()
        
        now = datetime.now()
        month_start = now.replace(day=1, hour=0, minute=0, second=0)
        async with db.execute("SELECT SUM(worker_share) FROM profits WHERE worker_id = ? AND timestamp >= ?", (user_id, month_start)) as cursor:
            month_profit = (await cursor.fetchone())[0] or 0.0
        async with db.execute("SELECT COUNT(*) FROM clients WHERE worker_id = ?", (user_id,)) as cursor:
            clients_count = (await cursor.fetchone())[0]

    if not user_data: return # Safety check

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
    await send_screen(query, text, "profile", get_back_kb())

@dp.callback_query(F.data.startswith("menu_clients"))
async def menu_clients(query: CallbackQuery):
    page = int(query.data.split("_")[-1])
    user_id = query.from_user.id
    limit = 6
    offset = page * limit

    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT id, name, total_squeezed FROM clients WHERE worker_id = ? ORDER BY total_squeezed DESC LIMIT ? OFFSET ?", (user_id, limit, offset)) as cursor:
            clients = await cursor.fetchall()
        async with db.execute("SELECT COUNT(*) FROM clients WHERE worker_id = ?", (user_id,)) as cursor:
            total_count = (await cursor.fetchone())[0]

    keyboard = []
    for c in clients:
        keyboard.append([InlineKeyboardButton(text=f"{c[1]} | ${c[2]:.0f}", callback_data=f"client_view_{c[0]}")])
    
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton(text="⬅️", callback_data=f"menu_clients_{page-1}"))
    if offset + limit < total_count:
        nav_row.append(InlineKeyboardButton(text="➡️", callback_data=f"menu_clients_{page+1}"))
    if nav_row:
        keyboard.append(nav_row)
    
    keyboard.append([InlineKeyboardButton(text="🔙 Меню", callback_data="menu_main")])

    text = f"🦣 <b>ВАШИ КЛИЕНТЫ ({total_count})</b>\nНажмите для просмотра истории:"
    await send_screen(query, text, None, InlineKeyboardMarkup(inline_keyboard=keyboard))

@dp.callback_query(F.data.startswith("client_view_"))
async def client_view(query: CallbackQuery):
    client_id = int(query.data.split("_")[-1])
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

    await send_screen(query, text, None, InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 К списку", callback_data="menu_clients_0")]]))

@dp.callback_query(F.data == "menu_profits")
async def menu_profits(query: CallbackQuery):
    user_id = query.from_user.id
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
    await send_screen(query, text, None, get_back_kb())

@dp.callback_query(F.data == "menu_salary")
async def menu_salary(query: CallbackQuery):
    user_id = query.from_user.id
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
    await send_screen(query, text, "pay", get_back_kb())

# --- 🔐 ADMIN PANEL ---

@dp.callback_query(F.data == "admin_dashboard")
async def admin_dashboard(query: CallbackQuery):
    if not await is_admin(query.from_user.id): return

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
        [InlineKeyboardButton(text="💵 Внести профит", callback_data="adm_start_profit")],
        [InlineKeyboardButton(text="💸 Выплатить ЗП", callback_data="adm_start_pay")],
        [InlineKeyboardButton(text="📋 Список воркеров", callback_data="adm_users_list")],
        [InlineKeyboardButton(text="🔙 В меню", callback_data="menu_main")]
    ]
    await send_screen(query, text, None, InlineKeyboardMarkup(inline_keyboard=keyboard))

@dp.callback_query(F.data == "adm_users_list")
async def adm_users_list(query: CallbackQuery):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT full_name, balance, total_earned FROM users ORDER BY balance DESC") as cursor:
            users = await cursor.fetchall()
    text = "📋 <b>ТОП ВОРКЕРОВ</b>\n\n"
    for u in users:
        text += f"👤 <b>{u[0]}</b>\n💵 Баланс: ${u[1]:.2f} | Всего: ${u[2]:.2f}\n\n"
    await send_screen(query, text, None, InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="admin_dashboard")]]))

# --- 🔄 ADMIN FLOW: ADD PROFIT ---

@dp.callback_query(F.data == "adm_start_profit")
async def prof_start(query: CallbackQuery, state: FSMContext):
    await state.clear()
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT user_id, full_name FROM users ORDER BY full_name") as cursor:
            workers = await cursor.fetchall()
            
    keyboard = []
    for w in workers:
        keyboard.append([InlineKeyboardButton(text=w[1], callback_data=f"prof_sel_{w[0]}")])
    keyboard.append([InlineKeyboardButton(text="🔙 Отмена", callback_data="cancel_prof")])
    
    await send_screen(query, "👤 <b>Выберите воркера:</b>", None, InlineKeyboardMarkup(inline_keyboard=keyboard))
    await state.set_state(ProfitStates.worker)

@dp.callback_query(ProfitStates.worker, F.data.startswith("prof_sel_"))
async def prof_worker_sel(query: CallbackQuery, state: FSMContext):
    worker_id = int(query.data.split("_")[-1])
    await state.update_data(worker_id=worker_id)
    
    kb = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text='❌ Отмена')]], resize_keyboard=True)
    await query.message.answer("✍️ <b>Введите имя Мамонта (или username):</b>", parse_mode="HTML", reply_markup=kb)
    await state.set_state(ProfitStates.client)

@dp.message(ProfitStates.client, F.text)
async def prof_client(message: Message, state: FSMContext):
    if message.text == "❌ Отмена": return await cancel_op(message, state)
    await state.update_data(client_name=message.text)
    await message.answer("💰 <b>Сумма залета (в $):</b>\nПример: 1500.50", parse_mode="HTML")
    await state.set_state(ProfitStates.amount)

@dp.message(ProfitStates.amount, F.text)
async def prof_amount(message: Message, state: FSMContext):
    if message.text == "❌ Отмена": return await cancel_op(message, state)
    try:
        raw = message.text.replace(",", ".")
        amt = float(raw)
        if amt <= 0: raise ValueError
        await state.update_data(amount=amt)
        kb = ReplyKeyboardMarkup(keyboard=[
            [KeyboardButton(text="BTC"), KeyboardButton(text="USDT"), KeyboardButton(text="Card")],
            [KeyboardButton(text="❌ Отмена")]
        ], resize_keyboard=True)
        await message.answer("🏦 <b>Выберите направление:</b>", reply_markup=kb, parse_mode="HTML")
        await state.set_state(ProfitStates.direction)
    except ValueError:
        await message.answer("⚠️ Введите корректное число (например 1500.50).")

@dp.message(ProfitStates.direction, F.text)
async def prof_dir(message: Message, state: FSMContext):
    if message.text == "❌ Отмена": return await cancel_op(message, state)
    await state.update_data(direction=message.text)
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="Депозит"), KeyboardButton(text="Комиссия"), KeyboardButton(text="Налог")],
        [KeyboardButton(text="❌ Отмена")]
    ], resize_keyboard=True)
    await message.answer("📑 <b>Стадия обработки:</b>", reply_markup=kb, parse_mode="HTML")
    await state.set_state(ProfitStates.stage)

@dp.message(ProfitStates.stage, F.text)
async def prof_stage(message: Message, state: FSMContext):
    if message.text == "❌ Отмена": return await cancel_op(message, state)
    await state.update_data(stage=message.text)
    kb = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text='❌ Отмена')]], resize_keyboard=True)
    await message.answer("📊 <b>Процент воркера?</b> (Только число, например 50):", reply_markup=kb, parse_mode="HTML")
    await state.set_state(ProfitStates.percent)

@dp.message(ProfitStates.percent, F.text)
async def prof_percent(message: Message, state: FSMContext):
    if message.text == "❌ Отмена": return await cancel_op(message, state)
    try:
        percent = float(message.text)
        data = await state.get_data()
        worker_share = data['amount'] * (percent / 100)
        await state.update_data(percent=percent, worker_share=worker_share)
        
        text = (
            f"⚠️ <b>ПРОВЕРКА ДАННЫХ</b>\n"
            f"👤 Воркер ID: {data['worker_id']}\n"
            f"🦣 Мамонт: {data['client_name']}\n"
            f"💰 Сумма: ${data['amount']}\n"
            f"📊 Процент: {percent}%\n"
            f"💵 <b>Доля воркера: ${worker_share:.2f}</b>\n\n"
            f"Все верно?"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Подтвердить", callback_data="prof_commit")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_prof")]
        ])
        await message.answer(text, reply_markup=kb, parse_mode="HTML")
        await state.set_state(ProfitStates.confirm)
    except ValueError:
        await message.answer("⚠️ Введите число (например 50).")

@dp.callback_query(ProfitStates.confirm, F.data == "prof_commit")
async def prof_confirm(query: CallbackQuery, state: FSMContext, bot: Bot):
    data = await state.get_data()
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

    await query.message.edit_text(f"✅ <b>Профит добавлен!</b>", parse_mode="HTML")
    
    # Notify Worker
    try:
        await bot.send_message(data['worker_id'], 
            f"🚨 <b>НОВЫЙ ЗАЛЕТ!</b>\n━━━━━━━━━━━━━━━━━━\n"
            f"🦣 <b>Мамонт:</b> {data['client_name']}\n"
            f"💵 <b>Сумма:</b> <code>${data['amount']}</code>\n"
            f"⚙️ <b>Тип:</b> {data['direction']} ({data['stage']})\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"💰 <b>ТВОЯ ДОЛЯ:</b> <b>${data['worker_share']:.2f}</b>\n"
            f"🚀 <i>Keep pushing!</i>", 
            parse_mode="HTML")
    except Exception as e:
        logger.warning(f"Notify failed: {e}")
        
    await state.clear()
    await admin_dashboard(query)

# --- 🔄 ADMIN FLOW: PAYOUT ---

@dp.callback_query(F.data == "adm_start_pay")
async def pay_start(query: CallbackQuery, state: FSMContext):
    await state.clear()
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT user_id, full_name, balance FROM users WHERE balance > 0 ORDER BY balance DESC") as cursor:
            users = await cursor.fetchall()
            
    if not users:
        await query.answer("🤷‍♂️ Все выплачено!", show_alert=True)
        return
        
    keyboard = []
    for u in users:
        keyboard.append([InlineKeyboardButton(text=f"{u[1]} (${u[2]:.2f})", callback_data=f"pay_sel_{u[0]}")])
    keyboard.append([InlineKeyboardButton(text="🔙 Отмена", callback_data="cancel_pay")])
    
    await send_screen(query, "💸 <b>Кому выплачиваем?</b>", None, InlineKeyboardMarkup(inline_keyboard=keyboard))
    await state.set_state(PayStates.check)

@dp.callback_query(PayStates.check, F.data.startswith("pay_sel_"))
async def pay_user_sel(query: CallbackQuery, state: FSMContext):
    user_id = int(query.data.split("_")[-1])
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT full_name, balance FROM users WHERE user_id = ?", (user_id,)) as cursor:
            u_data = await cursor.fetchone()
            
    await state.update_data(pay_id=user_id, pay_amount=u_data[1], pay_name=u_data[0])
    
    kb = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text='❌ Отмена')]], resize_keyboard=True)
    await query.message.answer(
        f"💳 Выплата для <b>{u_data[0]}</b>\nСумма: <b>${u_data[1]:.2f}</b>\n\n⬇️ Вставьте чек CryptoBot или код транзакции:", 
        parse_mode="HTML", reply_markup=kb
    )
    await state.set_state(PayStates.confirm)

@dp.message(PayStates.confirm, F.text)
async def pay_confirm_input(message: Message, state: FSMContext):
    if message.text == "❌ Отмена": return await cancel_op(message, state)
    await state.update_data(check_code=message.text)
    data = await state.get_data()
    text = (
        f"⚠️ <b>ПОДТВЕРЖДЕНИЕ ВЫПЛАТЫ</b>\n"
        f"👤 Воркер: {data['pay_name']}\n"
        f"💰 Сумма: ${data['pay_amount']:.2f}\n"
        f"🧾 Чек: {data['check_code']}\n\n"
        f"Обнуляем баланс и отправляем?"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ ВЫПЛАТИТЬ", callback_data="pay_commit")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_pay")]
    ])
    await message.answer(text, reply_markup=kb, parse_mode="HTML")

@dp.callback_query(PayStates.confirm, F.data == "pay_commit")
async def pay_execute(query: CallbackQuery, state: FSMContext, bot: Bot):
    data = await state.get_data()
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE users SET balance = 0 WHERE user_id = ?", (data['pay_id'],))
        await db.execute("INSERT INTO payouts (worker_id, check_code, amount) VALUES (?, ?, ?)", 
                         (data['pay_id'], data['check_code'], data['pay_amount']))
        await db.commit()
        
    await query.message.edit_text("✅ <b>Выплата проведена успешно!</b>", parse_mode="HTML")
    
    try:
        await bot.send_message(data['pay_id'], 
            f"💸 <b>ВЫПЛАТА ПОЛУЧЕНА</b>\n━━━━━━━━━━━━━━━━━━\n"
            f"💳 <b>Сумма:</b> <code>${data['pay_amount']:.2f}</code>\n"
            f"🧾 <b>Чек:</b> <code>{data['check_code']}</code>\n"
            f"📅 <b>Дата:</b> {datetime.now().strftime('%d.%m.%Y')}\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🫡 <i>Спасибо за отличную работу.</i>",
            parse_mode="HTML")
    except: pass
    
    await state.clear()
    await admin_dashboard(query)

# --- CANCEL HANDLERS FOR CALLBACKS ---
@dp.callback_query(F.data.in_({"cancel_prof", "cancel_pay"}))
async def cancel_cb(query: CallbackQuery, state: FSMContext):
    await state.clear()
    await query.message.edit_text("🚫 Операция отменена.")
    await admin_dashboard(query)

async def cancel_op(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("🚫 Операция отменена.", reply_markup=ReplyKeyboardRemove())
    # Try to return to main menu visualization
    user_id = message.from_user.id
    db_user = await get_db_user(user_id)
    is_admin_flag = bool(db_user[5]) if db_user else False
    text = (
        f"👋 <b>Добро пожаловать.</b>\n\n"
        f"🖥 <b>Рабочее пространство:</b> <code>Active</code>\n"
        f"🛡 <b>Статус:</b> {'👨‍💻 Администратор' if is_admin_flag else '👤 Воркер'}\n\n"
        f"👇 <i>Используйте навигацию ниже:</i>"
    )
    await send_screen(message, text, "welcome", get_main_menu_kb(is_admin_flag))

# --- 🚀 BOOTSTRAP ---
async def main():
    if not BOT_TOKEN:
        sys.exit("❌ Error: BOT_TOKEN missing in .env file")

    await init_db()
    
    bot = Bot(token=BOT_TOKEN)
    print("✅ Bot is running (Aiogram v3)...")
    
    # Drop pending updates to avoid flooding
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
