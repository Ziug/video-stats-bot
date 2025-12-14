import os
import re
import json
import asyncio
import asyncpg
from urllib.parse import urlparse
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from mistralai import Mistral
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
MISTRAL_API_KEY = os.getenv("MISTRAL_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")

mistral_client = Mistral(api_key=MISTRAL_API_KEY)

def parse_db_url(url):
    parsed = urlparse(url)
    user = parsed.username or ""
    pwd = parsed.password or ""
    host = parsed.hostname or "localhost"
    port = parsed.port or 5432
    path = parsed.path.lstrip("/") or "postgres"
    
    creds = f"{user}:{pwd}@" if user else ""
    return f"postgresql://{creds}{host}:{port}/{path}"

PROMPT = """Ты помощник по генерации SQL запросов для аналитики видео.

Таблица videos содержит итоговую статистику по каждому видео:
- id: идентификатор видео
- creator_id: идентификатор креатора
- video_created_at: дата и время публикации видео
- views_count: финальное количество просмотров
- likes_count: финальное количество лайков
- comments_count: финальное количество комментариев
- reports_count: финальное количество жалоб
- created_at: дата создания записи
- updated_at: дата обновления записи

Таблица video_snapshots содержит почасовые снапшоты статистики:
- id: идентификатор снапшота
- video_id: ссылка на видео
- views_count: количество просмотров на момент замера
- likes_count: количество лайков на момент замера
- comments_count: количество комментариев на момент замера
- reports_count: количество жалоб на момент замера
- delta_views_count: прирост просмотров с предыдущего снапшота
- delta_likes_count: прирост лайков с предыдущего снапшота
- delta_comments_count: прирост комментариев с предыдущего снапшота
- delta_reports_count: прирост жалоб с предыдущего снапшота
- created_at: время снапшота (раз в час)
- updated_at: дата обновления

ВАЖНО: Ответ ДОЛЖЕН быть JSON {"sql": "<SQL>"} и SQL ДОЛЖЕН ВОЗВРАЩАТЬ ОДНО ЧИСЛО!

Примеры запросов и ответов:
"Сколько всего видео?" -> {"sql": "SELECT COUNT(*) FROM videos"}
"Сколько видео у креатора abc?" -> {"sql": "SELECT COUNT(*) FROM videos WHERE creator_id = 'abc'"}
"Сколько видео с более чем 100000 просмотров?" -> {"sql": "SELECT COUNT(*) FROM videos WHERE views_count > 100000"}
"На сколько выросли просмотры 28 ноября?" -> {"sql": "SELECT COALESCE(SUM(delta_views_count),0) FROM video_snapshots WHERE date(created_at) = '2025-11-28'"}
"Сколько уникальных видео получали просмотры 27 ноября?" -> {"sql": "SELECT COUNT(DISTINCT video_id) FROM video_snapshots WHERE delta_views_count > 0 AND date(created_at) = '2025-11-27'"}
"Какое суммарное количество просмотров в июне 2025?" -> {"sql": "SELECT COALESCE(SUM(views_count),0) FROM videos WHERE EXTRACT(MONTH FROM video_created_at) = 6 AND EXTRACT(YEAR FROM video_created_at) = 2025"}

Правила:
1. SQL ДОЛЖЕН ВОЗВРАЩАТЬ РОВНО ОДНО ЧИСЛО
2. Используй COUNT, SUM, MAX, MIN, AVG для получения чисел
3. Никогда не возвращай названия, ID или текст - только числовые агрегаты
4. Возвращай ТОЛЬКО JSON {"sql": "..."}
5. Используй ТОЛЬКО таблицы videos и video_snapshots
6. Допускаются функции: COUNT, SUM, AVG, MIN, MAX, DISTINCT, date, COALESCE, EXTRACT
7. Без точек с запятой в конце SQL"""

ALLOWED_TABLES = {"videos", "video_snapshots"}

def validate_sql(sql: str) -> bool:
    s = sql.strip()
    if not s.lower().startswith("select"):
        return False
    if ";" in s:
        return False
    forbidden = ["insert ", "update ", "delete ", "drop ", "alter ", "create ", "grant ", "revoke ", "truncate "]
    low = s.lower()
    for f in forbidden:
        if f in low:
            return False
    
    sql_lower = low
    where_pos = sql_lower.find(" where ")
    if where_pos == -1:
        where_pos = sql_lower.find(" group ")
    if where_pos == -1:
        where_pos = sql_lower.find(" order ")
    if where_pos == -1:
        where_pos = len(sql_lower)
    
    from_to_where = sql_lower[:where_pos]
    tables = set(re.findall(r"(?:from|join)\s+([a-z_]+)", from_to_where))
    print(f"Found tables: {tables}")
    
    if not tables:
        return False
    for t in tables:
        if t not in ALLOWED_TABLES:
            print(f"Table {t} not in allowed: {ALLOWED_TABLES}")
            return False
    return True

async def sql_from_llm(user_text: str) -> str:
    messages = [
        {"role": "system", "content": PROMPT},
        {"role": "user", "content": user_text}
    ]
    resp = await asyncio.to_thread(lambda: mistral_client.chat.complete(model="mistral-small-latest", messages=messages, temperature=0))
    content = resp.choices[0].message.content.strip()
    print(f"Ответ LLMки: {content}")

    m = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', content, re.DOTALL)
    if m:
        try:
            json_str = m.group(1)
            j = json.loads(json_str)
            sql = j.get("sql", "").strip()
            if sql:
                return sql
        except Exception as e:
            print(f"ошибка парсинга JSON из markdown: {e}")
    
    try:
        j = json.loads(content)
        sql = j.get("sql", "").strip()
        if sql:
            return sql
    except Exception as e:
        print(f"ошибка парсинга JSON: {e}")
    
    m = re.search(r'"sql"\s*:\s*"([^"]*)"', content)
    if m:
        sql = m.group(1).strip()
        if sql:
            return sql
    
    m = re.search(r"SELECT[\s\S]*?(?=[\)\"]|$)", content, re.IGNORECASE)
    if m:
        sql = m.group(0).strip()
        if sql and not sql.endswith('"'):
            return sql
    
    return None


async def handle_message(event: types.Message):
    text = (event.text or "").strip()
    if not text:
        await event.answer("0")
        return
    sql = await sql_from_llm(text)
    print(f"Юзер: {text}")
    print(f"SQL: {sql}")

    if not sql:
        await event.answer("0")
        return
    is_valid = validate_sql(sql)
    print(f"Valid: {is_valid}")
    if not is_valid:
        await event.answer("0")
        return
    db_url = parse_db_url(DATABASE_URL)
    conn = await asyncpg.connect(db_url)
    try:
        val = await conn.fetchval(sql)
        print(f"Result: {val}")
        if val is None:
            val = 0
        await event.answer(str(int(val)))
    except Exception as e:
        print(f"DB Error: {e}")
        await event.answer("0")
    finally:
        await conn.close()

async def main():
    bot = Bot(token=TELEGRAM_BOT_TOKEN)
    dp = Dispatcher()
    
    async def start_handler(message: types.Message):
        await message.answer("Привет!\nЗадавай вопросы на русском о видеостатистике")
    
    dp.message.register(start_handler, Command("start"))
    dp.message.register(handle_message)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
