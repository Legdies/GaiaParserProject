import requests
from pathlib import Path
import io


def download(url: str, save_to: Path | None):
    r = requests.get(url, stream=True)
    r.raise_for_status()

    total = int(r.headers.get("Content-Length", "0"))
    done = 0

    if save_to:
        with save_to.open("wb") as f:
            for chunk in r.iter_content(1024 * 1024):
                f.write(chunk)
        return None, save_to

    bio = io.BytesIO()
    for chunk in r.iter_content(1024 * 1024):
        bio.write(chunk)
    bio.seek(0)
    return bio, None
