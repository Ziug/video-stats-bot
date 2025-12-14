
import os
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy import String, DateTime, Integer, ForeignKey, func
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

class Base(DeclarativeBase):
    pass

class Video(Base):
    __tablename__ = "videos"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    creator_id: Mapped[str] = mapped_column(String)
    video_created_at: Mapped[DateTime] = mapped_column(DateTime)
    views_count: Mapped[int] = mapped_column(Integer)
    likes_count: Mapped[int] = mapped_column(Integer)
    comments_count: Mapped[int] = mapped_column(Integer)
    reports_count: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[DateTime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[DateTime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())
    snapshots = relationship("VideoSnapshot", back_populates="video")

class VideoSnapshot(Base):
    __tablename__ = "video_snapshots"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    video_id: Mapped[str] = mapped_column(ForeignKey("videos.id"))
    views_count: Mapped[int] = mapped_column(Integer)
    likes_count: Mapped[int] = mapped_column(Integer)
    comments_count: Mapped[int] = mapped_column(Integer)
    reports_count: Mapped[int] = mapped_column(Integer)
    delta_views_count: Mapped[int] = mapped_column(Integer)
    delta_likes_count: Mapped[int] = mapped_column(Integer)
    delta_comments_count: Mapped[int] = mapped_column(Integer)
    delta_reports_count: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[DateTime] = mapped_column(DateTime)
    updated_at: Mapped[DateTime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())
    video = relationship("Video", back_populates="snapshots")

def get_engine():
    if DATABASE_URL is None:
        raise RuntimeError("DATABASE_URL is not set")
    return create_async_engine(DATABASE_URL, echo=False)

async def create_tables():
    engine = get_engine()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    await engine.dispose()
