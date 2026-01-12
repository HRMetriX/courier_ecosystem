import logging
import os
import asyncio
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
)
from aiohttp import web

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
}

# === ЛОГГИРОВАНИЕ ===
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# === ПРОСТОЙ ВЕБ-СЕРВЕР ДЛЯ RENDER ===
async def health_check(request):
    """Обработчик для health-check"""
    return web.Response(text="Bot is alive")

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

# === ОСНОВНАЯ ФУНКЦИЯ ===
async def main():
    """Запускает веб-сервер и бота"""
    logger.info("Starting bot application...")
    
    # Создаем и настраиваем веб-сервер
    app = web.Application()
    app.router.add_get('/', health_check)
    app.router.add_get('/health', health_check)
    
    port = int(os.getenv("PORT", 8080))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    logger.info(f"Web server started on port {port}")
    
    # Создаем приложение бота
    application = Application.builder().token(BOT_TOKEN).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    
    # Запускаем бота
    logger.info("Bot is starting polling...")
    await application.run_polling(
        drop_pending_updates=True,
        allowed_updates=Update.ALL_TYPES,
        close_loop=False  # Важно: не закрывать event loop
    )

if __name__ == "__main__":
    # Создаем новый event loop вручную
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Bot crashed: {e}")
        raise
    finally:
        if not loop.is_closed():
            loop.close()
