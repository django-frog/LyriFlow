import json
from pathlib import Path

class MetadataLoader:

    @staticmethod
    def load_metadata(path: str):
        file_path = Path(path)

        if not file_path.exists():
            raise FileNotFoundError(f"Metadata file not found: {file_path}")

        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Validate metadata
        for item in data:
            if not all(key in item for key in ("title", "artist")):
                raise ValueError(f"Invalid metadata format: {item}")

        return data
