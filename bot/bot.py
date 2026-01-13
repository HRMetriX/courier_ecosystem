import logging
import os
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
)
from flask import Flask
from threading import Thread
import time

# === КОНФИГУРАЦИЯ ===
BOT_TOKEN = os.getenv("TG_HELPER_BOT_TOKEN")
if not BOT_TOKEN:
    logging.error("Токен бота не найден! Установите TG_HELPER_BOT_TOKEN")
    exit(1)

GUIDE_URL = "https://hrmetrix.github.io/courier_ecosystem/"
REFERRAL_LINK = "https://ya.cc/8UiUqj"
AUTHOR_CONTACT = "@OlegBorisov_hr"

CITIES = {
    "moscow": {"name": "Москва", "channel": "@courier_jobs_msk"},
    "spb": {"name": "Санкт-Петербург", "channel": "@courier_jobs_spb"},
    "kazan": {"name": "Казань", "channel": "@courier_jobs_kzn"},
    "ekb": {"name": "Екатеринбург", "channel": "@courier_jobs_ekb"},
    "novosib": {"name": "Новосибирск", "channel": "@courier_jobs_nsk"},
    "nnov": {"name": "Нижний Новгород", "channel": "@courier_jobs_nng"},
    "chelyabinsk": {"name": "Челябинск", "channel": "@courier_jobs_che"},
    "krasnoyarsk": {"name": "Красноярск", "channel": "@courier_jobs_krk"},
}

# === ЛОГГИРОВАНИЕ ===
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# === FLASK ДЛЯ HEALTH-CHECK ===
app = Flask(__name__)

@app.route('/')
@app.route('/health')
def health_check():
    return "Bot is alive", 200

def run_flask():
    """Запускает Flask сервер в отдельном потоке"""
    port = int(os.getenv("PORT", 8080))
    logger.info(f"Starting Flask server on port {port}")
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)

# === ФУНКЦИИ БОТА ===
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("🚀 Начать регистрацию", callback_data="register")],
        [InlineKeyboardButton("❓ Помощь с самозанятостью", callback_data="smz")],
        [InlineKeyboardButton("🗺️ Выбрать город", callback_data="city_select")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        "Привет! Я помогаю новичкам начать работу курьером в Яндекс Еда — без ошибок и с поддержкой.\n\n"
        "Выбери, что тебе нужно:",
        reply_markup=reply_markup
    )

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data == "register":
        text = (
            "Отлично! Вот ссылка для регистрации:\n"
            f'👉 <a href="{REFERRAL_LINK}">Начать регистрацию в Яндекс Про</a>\n\n'
            "После перехода установи приложение «Яндекс Про» (не путай с «Яндекс Еда»!).\n\n"
            "Подробный гайд с советами и чек-листами:\n"
            f'<a href="{GUIDE_URL}">Открыть гайд</a>\n\n'
            f"Если запутаешься — напиши мне лично: {AUTHOR_CONTACT}. Помогу бесплатно."
        )
        await query.edit_message_text(text=text, parse_mode="HTML")

    elif query.data == "smz":
        text = (
            "Оформление самозанятости занимает 7–10 минут и делается через официальное приложение «Мой налог».\n\n"
            "🔹 Скачай его в App Store / Google Play\n"
            "🔹 Выбери «Стать самозанятым» → «По паспорту РФ»\n"
            "🔹 Сделай фото паспорта и селфи\n"
            "🔹 Готово!\n\n"
            f"Если не получается — напиши мне: {AUTHOR_CONTACT}. Разберём по шагам."
        )
        await query.edit_message_text(text=text)

    elif query.data == "city_select":
        keyboard = [
            [InlineKeyboardButton(CITIES[city]["name"], callback_data=f"city_{city}")]
            for city in CITIES
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            "Выбери свой город:", reply_markup=reply_markup
        )

    elif query.data.startswith("city_"):
        city_key = query.data.replace("city_", "")
        city = CITIES[city_key]
        channel_name = city["channel"][1:]
        text = (
            f"Подпишись на канал «Работа курьером | {city['name']}»:\n"
            f'<a href="https://t.me/{channel_name}">Открыть канал</a>\n\n'
            "Там ежедневно публикуются свежие вакансии и советы.\n\n"
            f"Вопросы по каналу или работе? Пиши мне: {AUTHOR_CONTACT} — отвечу лично."
        )
        await query.edit_message_text(text=text, parse_mode="HTML")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        f"Привет! Если у тебя вопрос по работе курьером — напиши мне напрямую: {AUTHOR_CONTACT}\n\n"
        "Или воспользуйся командой /start, чтобы выбрать нужную помощь."
    )

def start_bot():
    """Запускает Telegram бота"""
    logger.info("Starting Telegram bot...")
    application = Application.builder().token(BOT_TOKEN).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    
    application.run_polling(
        drop_pending_updates=True,
        allowed_updates=Update.ALL_TYPES,
    )

# === ОСНОВНАЯ ФУНКЦИЯ ===
def main():
    logger.info("Starting bot application...")
    
    # Запускаем Flask в отдельном потоке (для health-check)
    flask_thread = Thread(target=run_flask, daemon=True)
    flask_thread.start()
    
    # Даем Flask время запуститься
    time.sleep(2)
    
    # Запускаем бота в основном потоке
    start_bot()

if __name__ == "__main__":
    main()
