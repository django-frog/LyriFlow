from prefect import task
from services.metadata_loader import MetadataLoader

@task(name="Load Song Metadata", retries=3, retry_delay_seconds=5)
def load_song_metadata(path: str):
    return MetadataLoader.load_metadata(path)
