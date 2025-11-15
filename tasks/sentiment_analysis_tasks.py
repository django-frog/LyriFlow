import time
from hashlib import sha256
from datetime import datetime

from prefect import task, get_run_logger
from huggingface_hub import InferenceClient

from utils.cache import cache_load, cache_save

def make_cache_key(song: dict) -> str:
    """
    Prefer song['id'].
    If no ID exists, use a hash of the cleaned lyrics.
    """
    if song.get("id"):
        return str(song["id"])

    text = song.get("lyrics_clean", "")
    return sha256(text.encode()).hexdigest()


def get_hf_client(token: str) -> InferenceClient:
    return InferenceClient(
        provider="hf-inference",
        api_key=token,
    )

def call_with_retries(func, logger, max_attempts=3, delay=2):
    """
    Generic retry handler for HF API calls.
    """
    for attempt in range(max_attempts):
        try:
            return func()
        except Exception as e:
            logger.error(f"HF error on attempt {attempt+1}: {e}")
            if attempt == max_attempts - 1:
                raise
            time.sleep(delay)


@task(name="Sentiment Analysis Task")
def analyze_sentiment_task(song: dict, hf_token: str) -> dict:
    logger = get_run_logger()

    text = song.get("lyrics_clean")
    if not text:
        song["sentiment"] = None
        return song

    # ---------------------------------------
    # 1. Check cache
    # ---------------------------------------
    cache_key = make_cache_key(song)
    cached = cache_load("sentiment", cache_key)

    if cached:
        logger.info(f"Cache hit for sentiment → {cache_key}")
        return cached

    logger.info(f"Cache miss for sentiment → {cache_key}")

    # ---------------------------------------
    # 2. Infer with HuggingFace (with retries)
    # ---------------------------------------
    client = get_hf_client(hf_token)

    def infer():
        return client.text_classification(
            text,
            model="distilbert/distilbert-base-uncased-finetuned-sst-2-english",
        )

    result = call_with_retries(infer, logger)

    # ---------------------------------------
    # 3. Normalize HF results
    # ---------------------------------------
    sentiment = [
        {"label": r.label, "score": float(r.score)}
        for r in result
    ]

    enriched = {
        "id": song.get("id"),
        "title": song.get("title"),
        "artist":song.get("artist"),
        "lyrics": song.get("lyrics"),
        "lyrics_clean": text,
        "sentiment": sentiment,
        "cached_at": datetime.now().isoformat() + "Z",
    }

    # ---------------------------------------
    # 4. Save to cache
    # ---------------------------------------
    cache_save("sentiment", cache_key, enriched)
    logger.info(f"Sentiment saved to cache (key={cache_key})")

    return enriched
