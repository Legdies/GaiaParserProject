import time
from pathlib import Path
from .utils import save_json


class Stats:
    def __init__(self):
        self.start = time.time()
        self.ok = 0
        self.failed = 0
        self.skipped_parquet = 0
        self.skipped_cache = 0
        self.redownloaded = 0
        self.total_files = 0
        self.parquet_files = 0

    def finish(self, out_path: Path):
        duration = time.time() - self.start
        data = {
            "total_files": self.total_files,
            "ok": self.ok,
            "failed": self.failed,
            "skipped_parquet": self.skipped_parquet,
            "skipped_cache": self.skipped_cache,
            "redownloaded": self.redownloaded,
            "parquet_files": self.parquet_files,
            "duration_sec": round(duration, 3),
        }
        save_json(out_path, data)
        return data
