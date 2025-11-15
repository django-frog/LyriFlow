from prefect import task, get_run_logger
import os
import sqlite3
from datetime import datetime

DB_PATH = "data/output/songs.db"

@task(name="Load Results Task")
def load_results_task(song: dict):
    logger = get_run_logger()
    os.makedirs("data/output", exist_ok=True)

    song_id = song.get("id", "unknown")
    logger.info(f"[LOAD] Writing song ID={song_id} to SQLite...")

    # Extract sentiment info
    sentiment = song.get("sentiment")
    s_label = sentiment[0]["label"] if sentiment else None
    s_score = sentiment[0]["score"] if sentiment else None

    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()

        # Create table if not exists
        cur.execute("""
            CREATE TABLE IF NOT EXISTS songs (
                id TEXT PRIMARY KEY,
                title TEXT,
                artist TEXT,
                lyrics TEXT,
                lyrics_clean TEXT,
                sentiment_label TEXT,
                sentiment_score REAL,
                updated_at TEXT
            )
        """)

        # Check if song already exists
        cur.execute("SELECT 1 FROM songs WHERE id = ?", (song_id,))
        if cur.fetchone():
            logger.info(f"[LOAD] Song ID={song_id} already exists. Skipping insert.")
            return song

        # Insert song
        cur.execute("""
            INSERT INTO songs
            (id, title, artist, lyrics, lyrics_clean, sentiment_label, sentiment_score, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            song_id,
            song.get("title", "unknown"),
            song.get("artist", "unknown"),
            song.get("lyrics", ""),
            song.get("lyrics_clean", ""),
            s_label,
            s_score,
            datetime.now().isoformat() + "Z",
        ))

        conn.commit()

    logger.info(f"[LOAD] Successfully stored song ID={song_id}")
    return song
