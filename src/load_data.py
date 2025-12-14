
import os
import json
import asyncio
import asyncpg
from urllib.parse import urlparse
from datetime import datetime
from db import create_tables
from dotenv import load_dotenv

load_dotenv()

DATA_FILE = os.path.join(os.path.dirname(__file__), "..", "videos.json")
DATABASE_URL = os.getenv("DATABASE_URL")

def parse_db_url(url):
    parsed = urlparse(url)
    user = parsed.username or ""
    pwd = parsed.password or ""
    host = parsed.hostname or "localhost"
    port = parsed.port or 5432
    path = parsed.path.lstrip("/") or "postgres"
    
    creds = f"{user}:{pwd}@" if user else ""
    return f"postgresql://{creds}{host}:{port}/{path}"

def parse_datetime(dt_str):
    if isinstance(dt_str, str):
        dt = datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
        if dt.tzinfo is not None:
            dt = dt.replace(tzinfo=None)
        return dt
    return dt_str

async def load():
    if DATABASE_URL is None:
        raise RuntimeError("DATABASE_URL is not set")
    await create_tables()
    with open(os.path.join(os.path.dirname(__file__), "..", "videos.json"), "r", encoding="utf-8") as f:
        data = json.load(f)
    db_url = parse_db_url(DATABASE_URL)
    conn = await asyncpg.connect(db_url)
    try:
        videos = data.get("videos", [])
        video_values = []
        snapshot_values = []
        for v in videos:
            video_values.append((v.get("id"), v.get("creator_id"), parse_datetime(v.get("video_created_at")), v.get("views_count"), v.get("likes_count"), v.get("comments_count"), v.get("reports_count"), parse_datetime(v.get("created_at")), parse_datetime(v.get("updated_at"))))
            for s in v.get("snapshots", []):
                snapshot_values.append((s.get("id"), s.get("video_id"), s.get("views_count"), s.get("likes_count"), s.get("comments_count"), s.get("reports_count"), s.get("delta_views_count"), s.get("delta_likes_count"), s.get("delta_comments_count"), s.get("delta_reports_count"), parse_datetime(s.get("created_at")), parse_datetime(s.get("updated_at"))))
        if video_values:
            await conn.executemany('''INSERT INTO videos(id, creator_id, video_created_at, views_count, likes_count, comments_count, reports_count, created_at, updated_at)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT DO NOTHING''', video_values)
        if snapshot_values:
            await conn.executemany('''INSERT INTO video_snapshots(id, video_id, views_count, likes_count, comments_count, reports_count, delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count, created_at, updated_at)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) ON CONFLICT DO NOTHING''', snapshot_values)
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(load())
