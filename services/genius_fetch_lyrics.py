import lyricsgenius

class GeniusService:
    def __init__(self, access_token: str):
        self.client = lyricsgenius.Genius(
            access_token,
            timeout=10,
            sleep_time=1,
            remove_section_headers=True,
        )

    def get_lyrics(self, title: str, artist: str) -> str:
        """
        Fetch lyrics using Genius API.
        Raises a descriptive exception if something goes wrong.
        """
        try:
            song = self.client.search_song(title=title, artist=artist)
            if not song:
                # This triggers a Prefect retry because it's an exception
                raise ValueError(f"Lyrics not found for '{title}' by '{artist}'")
            return song.lyrics
        except Exception as exp:
            # Wrap any exception with a friendly message
            raise RuntimeError(f"Failed to fetch lyrics for '{title}' by '{artist}': {exp}") from exp
