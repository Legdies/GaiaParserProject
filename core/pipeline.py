import gzip
import concurrent.futures
from pathlib import Path
import polars as pl

from .state import StateManager
from .parse import parse_text
from .download import download
from .utils import gzip_is_valid


class Pipeline:

    def __init__(self, state: StateManager, cache_dir: Path, parsed_dir: Path):
        self.state = state
        self.cache = cache_dir
        self.parsed = parsed_dir

    def parse_from_cache(self, cache_path, fname, idx):
        if not gzip_is_valid(cache_path):
            self.state.update(fname, None, "failed", cache=cache_path)
            return None

        with gzip.open(cache_path, "rt", encoding="utf-8", errors="ignore") as gz:
            text = gz.read()

        out = self.parsed / f"chunk_{idx:05d}.parquet"
        res = parse_text(text, out)
        if res:
            self.state.update(fname, None, "ok", path=out, cache=cache_path)
        return res

    def process(self, url, fname, idx, save_cache):
        entry = self.state.get(fname)

        # existing cache
        if entry and entry.get("cache"):
            p = Path(entry["cache"])
            if p.exists():
                res = self.parse_from_cache(p, fname, idx)
                if res:
                    return res

        # download
        cache_path = self.cache / fname if save_cache else None
        bio, tmp = download(url, cache_path)

        if save_cache:
            if not gzip_is_valid(cache_path):
                self.state.update(fname, url, "failed", cache=cache_path)
                return None
            return self.parse_from_cache(cache_path, fname, idx)

        # memory-mode
        try:
            with gzip.open(bio, "rt", encoding="utf-8", errors="ignore") as gz:
                text = gz.read()
        except:
            self.state.update(fname, url, "failed")
            return None

        out = self.parsed / f"chunk_{idx:05d}.parquet"
        res = parse_text(text, out)
        if res:
            self.state.update(fname, url, "ok", path=out)
        return res

    def merge(self, paths, out_path):
        scans = [pl.scan_parquet(p) for p in paths]
        final = pl.concat(scans).collect()
        final.write_parquet(out_path, compression="zstd")
