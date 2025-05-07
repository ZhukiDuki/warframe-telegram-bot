import telebot
import requests
from datetime import datetime, timedelta
import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from dateutil import parser as date_parser
import sqlite3
import logging
import json
import os
from flask import Flask
import threading

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename='warframe_bot.log',
    filemode='a'
)

# Конфигурация
BOT_TOKEN = os.getenv("TELEGRAM_TOKEN")
API_URL = 'https://api.warframestat.us/pc?language=ru'
CACHE_TIMEOUT = 120
DATABASE = 'users.db'

# Локализация
LOCALE = {
    'MENU': ['События 🎮', 'Вторжения 🌍', 'Разрывы Бездны ⚡', 'Баро Ки’Тиир 🚀', 'Настройки ⚙️'],
    'SELECT_SUBSCRIPTION': 'Выберите уведомления:',
    'NOTIFICATIONS_ENABLED': 'Уведомления обновлены!',
    'ERROR': 'Ошибка получения данных',
    'NO_DATA': 'Данные отсутствуют',
    'SET_TIMEZONE': 'Настроить часовой пояс',
    'SUBSCRIPTIONS': 'Уведомления',
    'MY_FILTERS': 'Мои фильтры',
    'CLEAR_FILTERS': 'Очистить фильтры',
    'FISSURE_FILTERS': 'Настройка фильтров разрывов',
    'FISSURE_TYPES': 'Тип миссии',
    'FISSURE_TIERS': 'Уровень разлома',
    'FISSURE_HARD': 'Стальной Путь',
    'FISSURE_STORM': 'Буря Бездны',
    'BACK': '⬅️ Назад'
}

# Переводы типов миссий
MISSION_TYPES_TRANSLATION = {
    "Survival": "🪓 Выживание",
    "Interception": "🎯 Перехват",
    "Sabotage": "💣 Диверсия",
    "Mobile Defense": "🛡️ Мобильная оборона",
    "Defense": "🧱 Оборона",
    "Skirmish": "⚔️ Стычка",
    "Exterminate": "☠️ Зачистка",
    "Excavation": "⛏ Раскопки",
    "Disruption": "🧨 Сбой",
    "Void Cascade": "🌀 Каскад Бездны",
    "Void Flood": "🌊 Потоп Бездны",
    "Alchemy": "🧪 Алхимия",
    "Rescue": "🚑 Спасение",
    "Capture": "🧟 Захват",
    "Orphix": "🤖 Орфикс",
    "Spy": "🕵 Шпионаж",
    "Volatile": "🔫 Налёт"
}

# Уровни разрывов Бездны
TIER_TRANSLATION = {
    "Lith": "Лит",
    "Meso": "Мезо",
    "Neo": "Нео",
    "Axi": "Акси",
    "Requiem": "Реквием",
    "Omnia": "Омниа"
}

TIER_REVERSE_TRANSLATION = {v: k for k, v in TIER_TRANSLATION.items()}

# Функции валидации данных
def validate_api_data(data, key):
    if not data:
        logging.warning(f"Пустые данные из API для {key}")
        return []
    
    result = data.get(key, [])
    if not isinstance(result, (list, dict)):
        logging.warning(f"Неверный формат данных для {key}: {type(result)}")
        return []
    
    return result

def is_data_valid(data):
    """Проверяет, что данные содержат нужные ключи и они валидны"""
    if not data:
        logging.warning("Пустой объект данных")
        return False
        
    required_keys = ['events', 'invasions', 'fissures']
    
    for key in required_keys:
        result = data.get(key)
        if not result:
            logging.warning(f"Отсутствуют данные для ключа: {key}")
            return False
            
        if not isinstance(result, (list, dict)):
            logging.warning(f"Неверный тип данных для ключа {key}: {type(result)}")
            return False
    
    # voidTraders может отсутствовать - это нормально
    void_traders = data.get('voidTraders', [])
    if void_traders and not isinstance(void_traders, list):
        logging.warning(f"Неверный формат данных для voidTraders: {type(void_traders)}")
        return False
    
    return True

# Инициализация бота
bot = telebot.TeleBot(BOT_TOKEN)
scheduler = BackgroundScheduler()
logging.basicConfig(level=logging.INFO)

# Работа с базой данных
def init_db():
    with sqlite3.connect(DATABASE) as conn:
        c = conn.cursor()
        c.execute('''
            CREATE TABLE IF NOT EXISTS users (
                chat_id INTEGER PRIMARY KEY, 
                timezone TEXT DEFAULT 'Europe/Moscow',
                subscriptions TEXT DEFAULT '',
                fissure_filters TEXT DEFAULT '{"types": [], "tiers": [], "hard": false, "storm": false}'
            )
        ''')
        conn.commit()

def check_db_structure():
    with sqlite3.connect(DATABASE) as conn:
        c = conn.cursor()
        c.execute("PRAGMA table_info(users)")
        print(c.fetchall())

def get_user(chat_id):
    logging.info(f"[get_user] Получен chat_id: {chat_id} (тип: {type(chat_id)})")
    
    # Если передан объект Message, извлекаем chat_id
    if isinstance(chat_id, telebot.types.Message):
        chat_id = chat_id.chat.id
    
    with sqlite3.connect(DATABASE) as conn:
        c = conn.cursor()
        c.execute("SELECT * FROM users WHERE chat_id=?", (chat_id,))
        row = c.fetchone()
        if not row:
            return None
        try:
            fissure_filters = row[3]
            if isinstance(fissure_filters, str):
                try:
                    fissure_filters = json.loads(fissure_filters)
                except json.JSONDecodeError:
                    logging.error(f"Ошибка JSON для пользователя {chat_id}, используется дефолт")
                    fissure_filters = {
                        "types": [], 
                        "tiers": [], 
                        "hard": False, 
                        "storm": False
                    }
            return {
                'chat_id': row[0],
                'timezone': row[1],
                'subscriptions': row[2].split(',') if row[2] else [],
                'fissure_filters': fissure_filters
            }
        except Exception as e:
            logging.error(f"Ошибка парсинга данных пользователя: {e}")
            return None

def save_user(chat_id, data):
    with sqlite3.connect(DATABASE) as conn:
        c = conn.cursor()
        c.execute(
            "REPLACE INTO users VALUES (?,?,?,?)", 
            (
                chat_id, 
                data['timezone'], 
                ','.join(data['subscriptions']), 
                json.dumps(data.get('fissure_filters', {
                    "types": [], 
                    "tiers": [], 
                    "hard": False, 
                    "storm": False
                }))
            )
        )
        conn.commit()

# Глобальный кэш
CACHE = {}

# Проверка валидности кэша
def is_cache_valid():
    """Проверяет, что кэш существует и не истёк"""
    return (
        'data' in CACHE and 
        datetime.now() < CACHE.get('expires', datetime.min)
    )

# Получение данных из API
def get_api_data():
    global CACHE
    if is_cache_valid():
        return CACHE['data']
    try:
        # Добавлены заголовки для обхода блокировки
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
        }
        response = requests.get(API_URL, timeout=20, headers=headers)  # Заголовки добавлены
        response.raise_for_status()
        data = response.json()
        CACHE.update({
            'data': data,
            'expires': datetime.now() + timedelta(seconds=CACHE_TIMEOUT)
        })
        return data
    except Exception as e:
        logging.error(f"Ошибка API: {e}", exc_info=True)
        return {}

def check_api_update():
    try:
        response = requests.head(API_URL, timeout=20)
        last_modified = response.headers.get('Last-Modified')
        if last_modified:
            logging.info(f"API обновлён: {last_modified}")
    except Exception as e:
        logging.warning(f"Ошибка проверки API: {e}")

@bot.message_handler(commands=['test_api'])
def test_api(message):
    try:
        response = requests.get(API_URL, timeout=10)
        bot.send_message(message.chat.id, f"Статус: {response.status_code}\nОтвет: {response.text[:200]}...")
    except Exception as e:
        bot.send_message(message.chat.id, f"Ошибка: {e}")

# Форматирование даты
def format_date(timestamp, timezone):
    """
    Форматирует дату из API в локальное время пользователя
    Поддерживает форматы:
    - ISO-строка ("2025-05-16T13:00:00.000Z")
    - datetime-объект
    """
    try:
        tz = pytz.timezone(timezone)
        
        # Если timestamp уже объект datetime, используем его напрямую
        if isinstance(timestamp, datetime):
            dt = timestamp
        # Если это строка, парсим её
        elif isinstance(timestamp, str):
            # Обработка разных форматов даты
            if timestamp.endswith('Z'):
                timestamp = timestamp.replace('Z', '+00:00')
            dt = date_parser.isoparse(timestamp)
        else:
            raise ValueError(f"Неверный тип данных: {type(timestamp)}")
            
        # Конвертируем в локальное время
        dt_local = dt.astimezone(tz)
        return dt_local.strftime("%d.%m.%Y %H:%M")
        
    except pytz.UnknownTimeZoneError:
        logging.warning(f"Неизвестный часовой пояс: {timezone}")
        return "Неверный часовой пояс"
        
    except ValueError as ve:
        logging.warning(f"Ошибка форматирования даты '{timestamp}': {ve}")
        return "Ошибка времени"
        
    except Exception as e:
        logging.error(f"Критическая ошибка форматирования даты: {e}", exc_info=True)
        return "Ошибка времени"

# Меню
def create_main_menu():
    markup = telebot.types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    buttons = [telebot.types.KeyboardButton(text) for text in LOCALE['MENU']]
    markup.add(*buttons)
    return markup

@bot.message_handler(func=lambda m: m.text == 'Настройки ⚙️')
def settings_menu(message):
    try:
        chat_id = message.chat.id if isinstance(message, telebot.types.Message) else int(message)
        user = get_user(chat_id)
        if not user:
            default_filters = {"types": [], "tiers": [], "hard": False, "storm": False}
            save_user(chat_id, {
                'timezone': 'Europe/Moscow',
                'subscriptions': [],
                'fissure_filters': default_filters
            })
            user = get_user(chat_id)
        # Добавлена кнопка для настройки фильтров разрывов
        markup = telebot.types.ReplyKeyboardMarkup(resize_keyboard=True)
        markup.add(telebot.types.KeyboardButton(LOCALE['SET_TIMEZONE']))
        markup.add(telebot.types.KeyboardButton(LOCALE['SUBSCRIPTIONS']))
        markup.add(telebot.types.KeyboardButton(LOCALE['MY_FILTERS']))
        markup.add(telebot.types.KeyboardButton(LOCALE['FISSURE_FILTERS']))  # ✅ Добавлена кнопка
        markup.row(telebot.types.KeyboardButton(LOCALE['BACK']))
        bot.send_message(chat_id, "Настройки:", reply_markup=markup)
    except Exception as e:
        logging.error(f"Ошибка в settings_menu: {e}", exc_info=True)
        bot.send_message(message.chat.id, "Произошла ошибка при открытии настроек")

def create_subscriptions_menu(chat_id):
    markup = telebot.types.InlineKeyboardMarkup(row_width=2)
    user = get_user(chat_id)
    user_subs = user['subscriptions'] if user else []  # ✅ Уже список

    categories = ['events', 'invasions', 'fissures']
    for cat in categories:
        markup.add(telebot.types.InlineKeyboardButton(
            text=f"{'✅' if cat in user_subs else '❌'} {cat}",
            callback_data=f"toggle_{cat}"
        ))
    
    return markup

# Форматирование наград
def format_rewards(rewards):
    if not rewards:
        return "Нет наград"

    reward_items = []
    for reward in rewards:
        item = reward.get('type', 'Неизвестно')
        count = reward.get('count', 1)
        reward_items.append(f"{item} x{count}")

    return ', '.join(reward_items)

# Уведомления
def check_notifications():
    """Проверяет события, вторжения и разрывы Бездны для всех пользователей"""
    data = get_api_data()
    
    if not is_data_valid(data):
        logging.warning("Получены устаревшие или неполные данные")
        return
    
    with sqlite3.connect(DATABASE) as conn:
        c = conn.cursor()
        c.execute("SELECT chat_id, timezone, subscriptions, fissure_filters FROM users")
        
        for row in c.fetchall():
            try:
                chat_id, tz, subs, filters_str = row
                
                # Извлечение подписок
                try:
                    subscriptions = subs.split(',') if isinstance(subs, str) and subs else []
                except:
                    subscriptions = []
                
                # Извлечение и парсинг фильтров
                try:
                    fissure_filters = json.loads(filters_str) if isinstance(filters_str, str) and filters_str else {
                        "types": [], "tiers": [], "hard": False, "storm": False
                    }
                except json.JSONDecodeError:
                    logging.warning(f"Ошибка декодирования fissure_filters для {chat_id}")
                    fissure_filters = {
                        "types": [], "tiers": [], "hard": False, "storm": False
                    }
                
                # Проверка разрывов Бездны
                if 'fissures' in subscriptions:
                    fissures = validate_api_data(data, 'fissures')
                    
                    for fissure in fissures:
                        mission_type = fissure.get('missionType')
                        tier = fissure.get('tier')
                        is_hard = fissure.get('isHard', False)
                        is_storm = fissure.get('isStorm', False)
                        eta = fissure.get('eta', 'Неизвестно')  # Получаем время до окончания
                        
                        # Проверка по фильтрам
                        type_ok = not fissure_filters['types'] or mission_type in fissure_filters['types']
                        
                        # Используем обратный перевод для уровней
                        tier_for_check = TIER_REVERSE_TRANSLATION.get(tier, tier)
                        tier_ok = not fissure_filters['tiers'] or tier_for_check in fissure_filters['tiers']
                        
                        # Проверка флагов
                        hard_ok = not fissure_filters['hard'] or is_hard == fissure_filters['hard']
                        storm_ok = not fissure_filters['storm'] or is_storm == fissure_filters['storm']
                        
                        if type_ok and tier_ok and hard_ok and storm_ok:
                            bot.send_message(chat_id, 
                                f"⚡ Разрыв Бездны: {fissure.get('node', 'Неизвестно')}\n"
                                f"Тип: {MISSION_TYPES_TRANSLATION.get(mission_type, mission_type)}\n"
                                f"Уровень: {TIER_TRANSLATION.get(tier, tier)}\n"
                                f"⏳ Осталось: {eta}",
                                parse_mode='Markdown'
                            )
            except Exception as e:
                logging.error(f"Ошибка обработки уведомлений для {chat_id}: {e}", exc_info=True)
                continue

@bot.message_handler(func=lambda m: m.text in LOCALE['MENU'])
def handle_menu(message):
    user_id = message.chat.id
    data = get_api_data()
    
    if not is_data_valid(data):
        bot.send_message(user_id, LOCALE['ERROR'])
        return
    
    if message.text == 'События 🎮':
        events_info(message)
    elif message.text == 'Вторжения 🌍':
        invasions_info(message)
    elif message.text == 'Разрывы Бездны ⚡':
        show_fissure_submenu(message)
    elif message.text == 'Баро Ки’Тиир 🚀':
        baro_info(message)
    elif message.text == 'Настройки ⚙️':
        settings_menu(message)  # Передаём message, но внутри извлекаем chat_id
    else:
        bot.send_message(user_id, LOCALE['NO_DATA'])

# Обработчики
@bot.message_handler(commands=['start'])
def start(message):
    user = get_user(message.chat.id)
    if not user:
        default_filters = {
            "types": [],
            "tiers": [],
            "hard": False,
            "storm": False
        }
        save_user(message.chat.id, {
            'timezone': 'Europe/Moscow',
            'subscriptions': [],
            'fissure_filters': default_filters
        })

    bot.send_message(
        message.chat.id,
        "Добро пожаловать в Warframe Helper!",
        reply_markup=create_main_menu()
    )

@bot.message_handler(commands=['refresh'])
def refresh_cache(message):
    global CACHE
    try:
        response = requests.get(API_URL, timeout=20)
        response.raise_for_status()
        CACHE.update({
            'data': response.json(),
            'expires': datetime.now() + timedelta(seconds=CACHE_TIMEOUT)
        })
        bot.send_message(message.chat.id, "Кэш обновлён")
    except Exception as e:
        logging.error(f"Ошибка обновления кэша: {e}", exc_info=True)
        bot.send_message(message.chat.id, "Не удалось обновить кэш")

@bot.message_handler(func=lambda m: m.text == 'Баро Ки’Тиир 🚀')
def baro_info(message):
    user_id = message.chat.id
    data = get_api_data()
    
    if not is_data_valid(data):
        bot.send_message(user_id, LOCALE['NO_DATA'])
        return
    
    trader = data.get('voidTraders', [{}])[0]
    user = get_user(user_id)
    user_tz = user['timezone'] if user else 'Europe/Moscow'
    
    # Извлечение данных из API
    location = trader.get('location', 'Неизвестно')
    activation = trader.get('activation', 'Неизвестно')
    expiry = trader.get('expiry', 'Неизвестно')
    start_string = trader.get('startString', 'Неизвестно')
    end_string = trader.get('endString', 'Неизвестно')
    active = trader.get('active', False)
    inventory = trader.get('inventory', [])
    
    # Формирование текста времени
    time_text = ""
    try:
        if active and expiry != 'Неизвестно':
            # Используем исходную строку для форматирования
            formatted_expiry = format_date(expiry, user_tz)
            time_text = f"Окончание: {formatted_expiry}\nОсталось: {end_string}"
        
        elif activation != 'Неизвестно':
            # Используем исходную строку для форматирования
            formatted_activation = format_date(activation, user_tz)
            time_text = f"Прибудет: {formatted_activation}\nДо прибытия: {start_string}"
        
        else:
            time_text = "Время: Неизвестно"
    
    except Exception as e:
        logging.error(f"Ошибка форматирования времени: {e}")
        time_text = f"До прибытия: {start_string}" if not active else f"Осталось: {end_string}"

    # Формирование списка товаров
    items_text = ""
    if active and inventory and isinstance(inventory, list):
        items_text = "**Товары:**\n"
        for item in inventory:
            price_parts = []
            if item.get('ducats'):
                price_parts.append(f"{item['ducats']} дукатов")
            if item.get('credits'):
                price_parts.append(f"{item['credits']} кредитов")
            items_text += f"- {item.get('item', 'Неизвестно')} ({', '.join(price_parts)})\n"

    # Формирование итогового сообщения
    status_emoji = "🟢" if active else "🟠"
    text = f"{status_emoji} **Баро Ки’Тиир**\n"
    text += f"Локация: {location}\n"
    text += f"{time_text}\n"
    text += items_text.strip()
    
    bot.send_message(user_id, text, parse_mode='Markdown')

@bot.message_handler(func=lambda m: m.text == LOCALE['SUBSCRIPTIONS'])
def subscriptions(message):
    bot.send_message(
        message.chat.id,
        LOCALE['SELECT_SUBSCRIPTION'],
        reply_markup=create_subscriptions_menu(message.chat.id)
    )

@bot.callback_query_handler(func=lambda call: call.data.startswith('toggle_'))
def toggle_subscription(call):
    try:
        user = get_user(call.message.chat.id)
        if not user:
            bot.answer_callback_query(call.id, "Ошибка: пользователь не найден")
            return
        
        subscriptions = user.get('subscriptions', [])
        category = call.data.split('_')[1]
        
        if category in subscriptions:
            subscriptions.remove(category)
        else:
            subscriptions.append(category)
        
        save_user(call.message.chat.id, {
            'timezone': user['timezone'],
            'subscriptions': subscriptions,
            'fissure_filters': user.get('fissure_filters', {
                "types": [], 
                "tiers": [], 
                "hard": False, 
                "storm": False
            })
        })
        
        bot.answer_callback_query(call.id, LOCALE['NOTIFICATIONS_ENABLED'])
    
    except Exception as e:
        # Логирование ошибки сохранения
        logging.error(f"Ошибка обработки подписок: {e}", exc_info=True)
        bot.answer_callback_query(call.id, "Ошибка обработки")

@bot.message_handler(func=lambda m: m.text == 'События 🎮')
def events_info(message):
    user_id = message.chat.id
    data = get_api_data()

    if not is_data_valid(data):
        bot.send_message(user_id, "Данные устарели или некорректны")
        return

    events = validate_api_data(data, 'events')
    if not events:
        bot.send_message(user_id, LOCALE['NO_DATA'])
        return

    text = "**Текущие события:**\n"
    for event in events:
        # Извлечение данных
        title = event.get('description', 'Без названия')  # Используем description вместо title
        location = event.get('node', 'Неизвестно')
        expiry = event.get('expiry', 'Неизвестно')
        active = event.get('active', False)

        # Рассчитываем оставшееся время
        try:
            expiry_dt = datetime.fromisoformat(expiry.replace('Z', '+00:00'))
            now = datetime.now(pytz.utc)
            eta = str(expiry_dt - now).split('.')[0]  # Убираем микросекунды
        except:
            eta = 'Неизвестно'

        # Извлечение наград
        rewards = event.get('rewards', [])
        reward_text = "Нет наград"
        if rewards:
            reward_items = []
            for reward in rewards:
                items = reward.get('items', [])
                if items:
                    reward_items.extend(items)
            if reward_items:
                reward_text = ", ".join(reward_items)

        status = "✅ Активно" if active else "⏸ Неактивно"

        text += f"• **{title}**\n"
        text += f"  Локация: {location}\n"
        text += f"  Награды: {reward_text}\n"
        text += f"  Осталось: {eta}\n"
        text += f"  Статус: {status}\n\n"

    bot.send_message(user_id, text, parse_mode='Markdown')

@bot.message_handler(func=lambda m: m.text == 'Вторжения 🌍')
def invasions_info(message):
    user_id = message.chat.id
    data = get_api_data()

    if not is_data_valid(data):
        bot.send_message(user_id, "Данные устарели или некорректны")
        return

    invasions = validate_api_data(data, 'invasions')
    if not invasions:
        bot.send_message(user_id, LOCALE['NO_DATA'])
        return

    text = "**Текущие вторжения:**\n"
    for inv in invasions:
        completed = inv.get('completed', False)
        if completed:
            continue  # Пропускаем завершённые вторжения

        node = inv.get('node', 'Неизвестно')
        attacker = inv.get('attacker', {}).get('faction', 'Неизвестно')
        defender = inv.get('defender', {}).get('faction', 'Неизвестно')
        eta = inv.get('eta', 'Неизвестно')

        # Извлечение наград атакующих
        attacker_reward = inv.get('attacker', {}).get('reward', {}).get('countedItems', [])
        attacker_reward_text = format_rewards(attacker_reward)

        # Извлечение наград защитников
        defender_reward = inv.get('defender', {}).get('reward', {}).get('countedItems', [])
        defender_reward_text = format_rewards(defender_reward)

        text += f"• **Локация:** {node}\n"
        text += f"  Атакующие: {attacker} | Защитники: {defender}\n"
        text += f"  Статус: ⏳ Осталось: {eta}\n"
        text += f"  Награды атакующих: {attacker_reward_text}\n"
        text += f"  Награды защитников: {defender_reward_text}\n\n"

    if text == "**Текущие вторжения:**\n":
        text = "Активных вторжений нет"

    bot.send_message(user_id, text, parse_mode='Markdown')

def format_rewards(rewards):
    """Форматирует награды с указанием количества предметов"""
    if not rewards:
        return "Нет наград"

    reward_items = []
    for reward in rewards:
        item = reward.get('type', 'Неизвестно')
        count = reward.get('count', 1)  # Используем count если доступен, иначе 1
        reward_items.append(f"{item} x{count}")

    return ", ".join(reward_items)

@bot.message_handler(func=lambda m: m.text == 'Разрывы Бездны ⚡')
def show_fissure_submenu(message):
    chat_id = message.chat.id
    markup = telebot.types.ReplyKeyboardMarkup(resize_keyboard=True)
    markup.add(
        telebot.types.KeyboardButton("Стальной Путь 💎"),
        telebot.types.KeyboardButton("Буря Бездны 🌪️"),
        telebot.types.KeyboardButton("Обычные разрывы 🌌")
    )
    markup.row(telebot.types.KeyboardButton(LOCALE['BACK']))
    bot.send_message(chat_id, "Выберите тип разрывов:", reply_markup=markup)

@bot.message_handler(func=lambda m: m.text in ["Стальной Путь 💎", "Буря Бездны 🌪️", "Обычные разрывы 🌌"])
def handle_fissure_subcategories(message):
    chat_id = message.chat.id
    data = get_api_data()
    
    if not is_data_valid(data):
        bot.send_message(chat_id, LOCALE['ERROR'])
        return
    
    fissures = validate_api_data(data, 'fissures')
    filtered_fissures = []
    
    if message.text == "Стальной Путь 💎":
        filtered_fissures = [f for f in fissures if f.get('isHard', False)]
    elif message.text == "Буря Бездны 🌪️":
        filtered_fissures = [f for f in fissures if f.get('isStorm', False)]
    elif message.text == "Обычные разрывы 🌌":
        filtered_fissures = [f for f in fissures if not f.get('isHard', False) and not f.get('isStorm', False)]
    
    if not filtered_fissures:
        bot.send_message(chat_id, "Нет активных разрывов для этой категории.")
        return
    
    text = f"**{message.text}**\n\n"
    for fissure in filtered_fissures:
        node = fissure.get('node', 'Неизвестно')
        mission_type = fissure.get('missionType', 'Неизвестно')
        tier = fissure.get('tier', 'Неизвестно')
        eta = fissure.get('eta', 'Неизвестно')
        
        mission_type_ru = MISSION_TYPES_TRANSLATION.get(mission_type, mission_type)
        tier_ru = TIER_TRANSLATION.get(tier, tier)
        
        text += f"• Локация: {node}\n"
        text += f"  Тип: {mission_type_ru} | Уровень: {tier_ru}\n"
        text += f"  Осталось: {eta}\n\n"
    
    bot.send_message(chat_id, text, parse_mode='Markdown')

# Новое меню настроек фильтров разрывов
def create_fissure_filters_menu(chat_id):
    """Создает меню фильтров с правильными данными для сравнения"""
    user = get_user(chat_id)
    if not user:
        return None
    
    try:
        filters = user.get('fissure_filters', {
            "types": [], 
            "tiers": [], 
            "hard": False, 
            "storm": False
        })
        if isinstance(filters, str):
            filters = json.loads(filters)
    except json.JSONDecodeError:
        filters = {
            "types": [], 
            "tiers": [], 
            "hard": False, 
            "storm": False
        }
    
    markup = telebot.types.InlineKeyboardMarkup(row_width=2)
    
    # Типы миссий (используем оригинальные ключи для callback)
    for key, value in MISSION_TYPES_TRANSLATION.items():
        markup.add(telebot.types.InlineKeyboardButton(
            text=f"{'✅' if key in filters.get('types', []) else '❌'} {value}",
            callback_data=f"fissure_type_{key}"  # Сохраняем оригинальный ключ
        ))
    
    # Уровни разлома (используем оригинальные ключи для callback)
    for key, value in TIER_TRANSLATION.items():
        markup.add(telebot.types.InlineKeyboardButton(
            text=f"{'✅' if key in filters.get('tiers', []) else '❌'} {value}",
            callback_data=f"fissure_tier_{key}"  # Сохраняем оригинальный ключ
        ))
    
    # Стальной Путь и Буря Бездны остаются без изменений
    markup.add(telebot.types.InlineKeyboardButton(
        text=f"{'✅' if filters.get('hard', False) else '❌'} Стальной Путь 💎",
        callback_data="fissure_hard"
    ))
    
    markup.add(telebot.types.InlineKeyboardButton(
        text=f"{'✅' if filters.get('storm', False) else '❌'} Буря Бездны 🌪️",
        callback_data="fissure_storm"
    ))
    
    # Очистка и сохранение
    markup.add(telebot.types.InlineKeyboardButton(
        text="🗑 Очистить всё",
        callback_data="fissure_clear_all"
    ))
    
    markup.add(telebot.types.InlineKeyboardButton(
        text="✔️ Сохранить",
        callback_data="fissure_filter_save"
    ))
    
    return markup

@bot.message_handler(func=lambda m: m.text == LOCALE['FISSURE_FILTERS'])
def open_fissure_filters(message):
    chat_id = message.chat.id
    user = get_user(chat_id)
    
    if not user:
        bot.send_message(chat_id, "Ошибка: пользователь не найден")
        return
    
    # Отправка меню с текущими фильтрами
    bot.send_message(
        chat_id,
        "Настройте фильтры разрывов Бездны:",
        reply_markup=create_fissure_filters_menu(chat_id)
    )

# Обработчик inline-кнопок
@bot.callback_query_handler(func=lambda call: call.data.startswith('fissure_'))
def toggle_fissure_filter(call):
    chat_id = call.message.chat.id
    data_parts = call.data.split('_')
    
    if len(data_parts) < 2:
        bot.answer_callback_query(call.id, "Ошибка формата данных")
        return
    
    data_type = data_parts[1]
    value = data_parts[2] if len(data_parts) > 2 else ''
    user = get_user(chat_id)
    
    if not user:
        bot.answer_callback_query(call.id, "Ошибка: пользователь не найден")
        return
    
    try:
        # Получаем и парсим фильтры
        filters = user.get('fissure_filters', {
            "types": [], 
            "tiers": [], 
            "hard": False, 
            "storm": False
        })
        
        if isinstance(filters, str):
            filters = json.loads(filters)
    except json.JSONDecodeError:
        filters = {
            "types": [], 
            "tiers": [], 
            "hard": False, 
            "storm": False
        }
        logging.warning(f"Ошибка парсинга JSON для {chat_id}, используется дефолт")
    
    # Обработка событий
    if data_type == 'type':
        if value in filters['types']:
            filters['types'].remove(value)
        else:
            filters['types'].append(value)
    
    elif data_type == 'tier':
        if value in filters['tiers']:
            filters['tiers'].remove(value)
        else:
            filters['tiers'].append(value)
    
    elif data_type == 'hard':
        filters['hard'] = not filters.get('hard', False)
    
    elif data_type == 'storm':
        filters['storm'] = not filters.get('storm', False)
    
    elif data_type == 'clear' and value == 'all':
        filters = {
            "types": [], 
            "tiers": [], 
            "hard": False, 
            "storm": False
        }
        bot.answer_callback_query(call.id, "Все фильтры сброшены")
    
    elif data_type == 'filter' and value == 'save':
        save_user(chat_id, {
            'timezone': user['timezone'],
            'subscriptions': user['subscriptions'],
            'fissure_filters': filters
        })
        
        bot.edit_message_text(
            message_id=call.message.message_id,
            chat_id=chat_id,
            text="✅ Фильтры разрывов Бездны сохранены!",
            reply_markup=None
        )
        bot.answer_callback_query(call.id, "Фильтры сохранены")
        return
    
    else:
        bot.answer_callback_query(call.id, "Неизвестная команда")
        return
    
    # Сохраняем обновленные фильтры
    save_user(chat_id, {
        'timezone': user['timezone'],
        'subscriptions': user['subscriptions'],
        'fissure_filters': filters
    })
    
    # Обновляем меню
    new_markup = create_fissure_filters_menu(chat_id)
    
    try:
        bot.edit_message_reply_markup(
            message_id=call.message.message_id,
            chat_id=chat_id,
            reply_markup=new_markup
        )
        bot.answer_callback_query(call.id, "Фильтры обновлены")
    except telebot.apihelper.ApiTelegramException as e:
        if "message is not modified" in str(e):
            bot.answer_callback_query(call.id, "Фильтры уже установлены")
        else:
            logging.warning(f"Telegram API ошибка: {e.result_json.get('description', 'Неизвестная ошибка')}")
            bot.answer_callback_query(call.id, "Ошибка обновления меню")

# Обработчик команды настройки фильтров
@bot.message_handler(func=lambda m: m.text == 'Разрывы Бездны ⚡')
def show_fissure_settings(message):
    chat_id = message.chat.id
    user = get_user(chat_id)
    
    if not user:
        bot.send_message(chat_id, "Ошибка: пользователь не найден")
        return
    
    try:
        # Получение фильтров
        filters = user.get('fissure_filters', {
            "types": [], 
            "tiers": [], 
            "hard": False, 
            "storm": False
        })
        
        if isinstance(filters, str):
            filters = json.loads(filters)
    
    except json.JSONDecodeError:
        filters = {
            "types": [], 
            "tiers": [], 
            "hard": False, 
            "storm": False
        }
        bot.send_message(chat_id, "Ошибка: фильтры повреждены, установлены дефолтные значения")
    
    try:
        # Обновляем фильтры у пользователя
        user['fissure_filters'] = filters
        
        # Отправляем меню
        bot.send_message(
            chat_id,
            "Настройте фильтры разрывов Бездны:",
            reply_markup=create_fissure_filters_menu(chat_id)
        )
    
    except Exception as e:
        logging.error(f"Ошибка открытия меню фильтров: {e}", exc_info=True)
        bot.send_message(chat_id, "Не удалось загрузить фильтры разрывов")

@bot.message_handler(commands=['myfilters'])
def show_filters(message):
    chat_id = message.chat.id
    user = get_user(chat_id)
    
    if not user:
        bot.send_message(chat_id, "Ошибка: пользователь не найден")
        return
    
    try:
        # Получение и парсинг фильтров
        filters = user.get('fissure_filters', {
            "types": [], 
            "tiers": [], 
            "hard": False, 
            "storm": False
        })
        
        if isinstance(filters, str):
            filters = json.loads(filters)
    
    except json.JSONDecodeError:
        filters = {
            "types": [], 
            "tiers": [], 
            "hard": False, 
            "storm": False
        }
        bot.send_message(chat_id, "Ошибка: фильтры повреждены, установлены дефолтные значения")
    
    types = ', '.join(filters.get('types', [])) if filters.get('types') else 'Не выбрано'
    tiers = ', '.join(filters.get('tiers', [])) if filters.get('tiers') else 'Не выбрано'
    hard_status = 'ВКЛ' if filters.get('hard', False) else 'ВЫКЛ'
    storm_status = 'ВКЛ' if filters.get('storm', False) else 'ВЫКЛ'
    
    bot.send_message(chat_id, f"""
⚙️ *Ваши текущие фильтры разрывов Бездны:*

▫️ Типы миссий: {types}
▫️ Уровни разлома: {tiers}
▫️ Стальной Путь: {hard_status}
▫️ Буря Бездны: {storm_status}
""", parse_mode='Markdown')

@bot.message_handler(func=lambda m: m.text == LOCALE['MY_FILTERS'])
def show_filters_menu(message):
    chat_id = message.chat.id
    show_filters(message)  # Вызываем команду /myfilters

@bot.message_handler(func=lambda m: m.text == LOCALE['MY_FILTERS'])
def show_current_filters(message):
    show_filters(message)

@bot.message_handler(commands=['clearfilters'])
def reset_filters(message):
    chat_id = message.chat.id
    user = get_user(chat_id)
    
    if not user:
        bot.send_message(chat_id, "Ошибка: пользователь не найден")
        return
    
    save_user(chat_id, {
        'timezone': user['timezone'],
        'subscriptions': user['subscriptions'],
        'fissure_filters': {
            "types": [], 
            "tiers": [], 
            "hard": False, 
            "storm": False
        }
    })
    
    bot.send_message(chat_id, "🗑 Все фильтры разрывов Бездны сброшены")

@bot.message_handler(func=lambda m: m.text == LOCALE['BACK'])
def back_to_menu(message):
    bot.send_message(message.chat.id, "Главное меню:", reply_markup=create_main_menu())

@bot.message_handler(func=lambda m: m.text == LOCALE['SET_TIMEZONE'])
def set_timezone(message):
    markup = telebot.types.ReplyKeyboardMarkup(resize_keyboard=True)
    
    # Сначала объявите списки отдельно
    europe = [
        "Europe/Moscow (UTC+3)", 
        "Europe/London (UTC+0)", 
        "Europe/Paris (UTC+1)", 
        "Europe/Berlin (UTC+1)", 
        "Europe/Rome (UTC+1)", 
        "Europe/Madrid (UTC+1)"
    ]
    
    north_america = [
        "America/New_York (UTC-4)", 
        "America/Chicago (UTC-5)", 
        "America/Denver (UTC-6)", 
        "America/Los_Angeles (UTC-7)", 
        "America/Phoenix (UTC-7)"
    ]
    
    south_america = [
        "America/Sao_Paulo (UTC-3)", 
        "America/Buenos_Aires (UTC-3)", 
        "America/Lima (UTC-5)"
    ]
    
    asia = [
        "Asia/Tokyo (UTC+9)", 
        "Asia/Shanghai (UTC+8)", 
        "Asia/Dubai (UTC+4)", 
        "Asia/Singapore (UTC+8)", 
        "Asia/Manila (UTC+8)", 
        "Asia/Dhaka (UTC+6)", 
        "Asia/Bangkok (UTC+7)"
    ]
    
    oceania = [
        "Australia/Sydney (UTC+10)", 
        "Australia/Melbourne (UTC+10)", 
        "Pacific/Auckland (UTC+12)", 
        "Pacific/Fiji (UTC+12)"
    ]
    
    africa = [
        "Africa/Cairo (UTC+2)", 
        "Africa/Nairobi (UTC+3)", 
        "Africa/Lagos (UTC+1)", 
        "Africa/Casablanca (UTC+0)"
    ]
    
    # Объедините списки в один
    all_timezones = europe + north_america + south_america + asia + oceania + africa
    
    # Добавьте кнопки
    for tz in all_timezones:
        markup.add(telebot.types.KeyboardButton(tz))
    
    markup.row(telebot.types.KeyboardButton(LOCALE['BACK']))
    bot.send_message(message.chat.id, "Выберите часовой пояс:", reply_markup=markup)

@bot.message_handler(func=lambda m: m.text.startswith('+') or m.text.startswith('-'))
def custom_timezone(message):
    try:
        offset = int(message.text.split(':')[0])
        tz = pytz.FixedOffset(offset * 60, 'Custom')
        user = get_user(message.chat.id)
        save_user(message.chat.id, {
            'timezone': str(tz),
            'subscriptions': user['subscriptions'],
            'fissure_filters': user.get('fissure_filters', {"types": [], "tiers": [], "hard": False, "storm": False})
        })
        bot.send_message(message.chat.id, f"Часовой пояс установлен: {tz}")
    except ValueError:
        bot.send_message(message.chat.id, "Неверный формат. Используйте +HH:MM или -HH:MM")

@bot.message_handler(func=lambda m: "(UTC" in m.text)
def handle_timezone_selection(message):
    """Обрабатывает выбор пользовательского часового пояса"""
    try:
        # Извлекаем только IANA-название
        timezone = message.text.split(" ")[0]
        user = get_user(message.chat.id)

        if not user:
            bot.send_message(message.chat.id, "Ошибка: пользователь не найден")
            return

        # Сохраняем настройки
        save_user(message.chat.id, {
            'timezone': timezone,
            'subscriptions': user['subscriptions'],
            'fissure_filters': user['fissure_filters']
        })

        bot.send_message(message.chat.id, f"Часовой пояс установлен: {timezone}", reply_markup=create_main_menu())

    except Exception as e:
        logging.error(f"Ошибка выбора часового пояса: {e}")
        bot.send_message(message.chat.id, "Ошибка установки часового пояса")

# Инициализация
init_db()
scheduler.add_job(check_notifications, 'interval', minutes=5)
scheduler.start()

app = Flask(__name__)

@app.route('/')
def home():
    return "Бот работает!", 200

def run_server():
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)

# Запуск сервера в отдельном потоке
server_thread = threading.Thread(target=run_server)
server_thread.daemon = True
server_thread.start()

# Запуск бота
bot.infinity_polling()