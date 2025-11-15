from prefect import task, get_run_logger
import re
import string

from utils.cache import cache_load, cache_save

@task(name="Preprocess Lyrics Task")
def preprocess_lyrics_task(song: dict) -> dict:
    logger = get_run_logger()
    song_id = song.get("id")

    # -----------------------------------------------------------------
    # 1) Try cache
    # -----------------------------------------------------------------
    cached = cache_load("preprocessed", song_id)
    if cached is not None:
        logger.info(f"Cache hit (preprocess) for song ID {song_id}")
        song["lyrics_clean"] = cached["lyrics_clean"]
        return song

    logger.info(f"Cache miss (preprocess) → cleaning lyrics for {song.get('title')}")

    lyrics = song.get("lyrics")
    if not lyrics:
        logger.warning(f"No lyrics found for {song.get('title')}")
        song["lyrics_clean"] = None
        return song

    # Clean lyrics
    lyrics_clean = re.sub(r"(?<=[a-zA-Z])\n(?=[a-zA-Z])", " ", lyrics)
    lyrics_clean = re.sub(r"\n", " ", lyrics_clean)
    lyrics_clean = re.sub(r"\[.*?\]", "", lyrics_clean)
    lyrics_clean = re.sub(r"\s+", " ", lyrics_clean)
    lyrics_clean = lyrics_clean.translate(str.maketrans("", "", string.punctuation))
    lyrics_clean = lyrics_clean.lower()

    if len(lyrics_clean.split()) < 5:
        logger.warning(f"Lyrics too short for {song.get('title')}")
        lyrics_clean = None

    # Save to cache
    cache_save("preprocessed", song_id, {"lyrics_clean": lyrics_clean})
    logger.info(f"Cached preprocessed lyrics for song ID {song_id}")

    song["lyrics_clean"] = lyrics_clean
    return song
