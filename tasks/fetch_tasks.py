from prefect import task, get_run_logger
from services.genius_fetch_lyrics import GeniusService
from utils.cache import cache_load, cache_save


@task(
    retries=3,
    retry_delay_seconds=5,
    name="Fetch Lyrics Task",
)
def fetch_lyrics_task(song: dict, access_token: str) -> dict:
    """
    Fetch lyrics for a song.
    Uses disk caching to avoid re-fetching the same song twice.
    """
    logger = get_run_logger()
    song_id = song.get("id")
    title = song.get("title")
    artist = song.get("artist")

    # ------------------------
    # 1) Check cache
    # ------------------------
    cached = cache_load("lyrics", song_id)
    if cached is not None:
        logger.info(f"Cache hit (lyrics) for song ID {song_id} → skipping API call")
        song["lyrics"] = cached["lyrics"]
        return song

    # ------------------------
    # 2) Cache miss → call Genius API
    # ------------------------
    logger.info(f"Cache miss → Fetching lyrics for: {title} - {artist}")
    genius = GeniusService(access_token=access_token)

    try:
        lyrics = genius.get_lyrics(title, artist)
    except Exception as exp:
        logger.error(str(exp))
        raise  # triggers Prefect retry

    # ------------------------
    # 3) Save to cache
    # ------------------------
    cache_save("lyrics", song_id, {"lyrics": lyrics})
    logger.info(f"Fetched and cached lyrics for song ID {song_id}")

    song["lyrics"] = lyrics
    return song
