import requests
from pathlib import Path
import io

try:
    from tqdm import tqdm
except Exception:
    tqdm = None


def download(url: str, save_to: Path | None, logger=None, multi_thread: bool = True):
    """
    If save_to is not None: stream to file and return (None, save_to)
    else: stream to BytesIO and return (bio, None)

    multi_thread=True disables tqdm per-file to avoid mangled output.
    """
    r = requests.get(url, stream=True, timeout=120)
    r.raise_for_status()

    total = int(r.headers.get("Content-Length", "0"))
    chunk_size = 1024 * 1024
    iterator = r.iter_content(chunk_size)

    use_tqdm = (tqdm is not None) and (total > 0) and (not multi_thread)

    if use_tqdm:
        iterator = tqdm(
            iterator,
            total=total // chunk_size + 1,
            unit="MB",
            desc=save_to.name if save_to else "stream"
        )

    if save_to:
        with save_to.open("wb") as f:
            for chunk in iterator:
                if chunk:
                    f.write(chunk)
        if logger:
            logger.info(f"Downloaded → {save_to.name}")
        return None, save_to

    bio = io.BytesIO()
    for chunk in iterator:
        if chunk:
            bio.write(chunk)
    bio.seek(0)
    if logger:
        logger.info("Downloaded → memory stream")
    return bio, None
