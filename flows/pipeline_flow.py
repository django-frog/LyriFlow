from prefect import flow
from prefect.blocks.system import Secret
from tasks.extract_tasks import load_song_metadata
from tasks.fetch_tasks import fetch_lyrics_task
from tasks.preprocess_tasks import preprocess_lyrics_task
from tasks.sentiment_analysis_tasks import analyze_sentiment_task
from tasks.load_tasks import load_results_task

@flow(name="Lyrics ETL Pipeline")
def lyrics_pipeline():
    songs = load_song_metadata("data/raw/songs_metadata.json")
    token_block = Secret.load("genius-token")
    access_token = token_block.get()

    enriched = fetch_lyrics_task.map(
        songs,
        access_token=access_token
    )

    preprocessed = preprocess_lyrics_task.map(enriched)

    hf_token = Secret.load("hugging-face-token").get()
    analyzed = analyze_sentiment_task.map(preprocessed, hf_token=hf_token)
    
    loaded = load_results_task.map(analyzed)
    return loaded