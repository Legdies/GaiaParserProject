import gzip
import queue
import threading
import concurrent.futures
from pathlib import Path

import polars as pl

from .parse import parse_text
from .download import download
from .utils import gzip_is_valid


class Pipeline:
    def __init__(
        self,
        db,
        cache_dir: Path,
        parsed_dir: Path,
        logger,
        stats,
        test_parse: bool = False,
        download_threads_ratio: float = 0.25,
        save_cache: bool = True,
        queue_size: int = 32,
        parse_needed: bool = False,
    ):
        self.db = db
        self.cache = cache_dir
        self.parsed = parsed_dir
        self.log = logger
        self.stats = stats
        self.test_parse = test_parse
        self.download_ratio = download_threads_ratio
        self.save_cache = save_cache
        self.parse_needed = parse_needed

        self.task_q = queue.Queue(maxsize=queue_size)
        self.stop_token = object()

    # ---------------------------
    # internal helpers
    # ---------------------------

    def _after_parse(self, out_path: Path, fname: str, url: str | None, cache_path: Path | None):
        if self.test_parse:
            try:
                df = pl.read_parquet(out_path)
                self.log.info("=== TEST-PARSE: SCHEMA ===")
                self.log.info(df.schema)
                self.log.info("=== TEST-PARSE: FIRST ROW ===")
                self.log.info(df.head(1).to_dict())
            except Exception as e:
                self.log.error(f"Failed test-parse read: {e}")

        self.db.upsert(fname, url=url, status="ok", path=out_path, cache=cache_path)
        self.stats.ok += 1
        self.stats.parquet_files += 1
        return out_path

    # ---------------------------
    # download worker
    # ---------------------------

    def _download_worker(self, files, start_idx: int, step: int):
        """
        Каждый downloader берёт свою подпоследовательность файлов.
        Кидает в очередь либо cache_path, либо memory BytesIO.
        """
        for i in range(start_idx, len(files), step):
            f = files[i]
            fname, url = f["name"], f["url"]

            entry = self.db.get(fname)

            # 1) reuse parsed
            if entry and entry.get("path") and Path(entry["path"]).exists():
                self.stats.skipped_parquet += 1
                continue

            # 2) reuse cache
            if entry and entry.get("cache"):
                cp = Path(entry["cache"])
                if cp.exists() and gzip_is_valid(cp):
                    self.task_q.put(("cache", fname, url, i, cp))
                    continue

            # 3) download
            self.db.upsert(fname, url=url, status="downloading")
            cache_path = (self.cache / fname) if self.save_cache else None

            try:
                bio, saved = download(url, cache_path, logger=self.log, multi_thread=True)
                self.log.info(f"[DL DONE] {fname}")

                if self.save_cache:
                    # validate
                    if not gzip_is_valid(saved):
                        self.log.error(f"[BAD DOWNLOAD] Invalid gzip: {fname}")
                        self.db.upsert(fname, url=url, status="failed", cache=saved)
                        self.stats.failed += 1
                        continue
                    self.task_q.put(("cache", fname, url, i, saved))
                else:
                    # in-memory validate by open
                    try:
                        gzip.open(bio, "rb").read(4)
                    except Exception as e:
                        self.log.error(f"[BAD STREAM] {fname}: {e}")
                        self.db.upsert(fname, url=url, status="failed")
                        self.stats.failed += 1
                        continue
                    self.task_q.put(("bio", fname, url, i, bio))

            except Exception as e:
                self.log.error(f"[DL FAIL] {fname}: {e}")
                self.db.upsert(fname, url=url, status="failed")
                self.stats.failed += 1

    # ---------------------------
    # parse worker
    # ---------------------------

    def _parse_worker(self, worker_id: int):
        while True:
            item = self.task_q.get()
            if item is self.stop_token:
                self.task_q.put(self.stop_token)  # передаем дальше
                return

            mode, fname, url, idx, payload = item
            out_path = self.parsed / f"chunk_{idx:05d}.parquet"

            try:
                if mode == "cache":
                    cache_path: Path = payload

                    if not gzip_is_valid(cache_path):
                        self.log.warning(f"[BAD GZIP] Cache invalid: {fname}")
                        self.db.upsert(fname, url=url, status="failed", cache=cache_path)
                        self.stats.failed += 1
                        continue

                    with gzip.open(cache_path, "rt", encoding="utf-8", errors="ignore") as gz:
                        text = gz.read()

                    if not parse_text(text, out_path):
                        self.log.warning(f"[PARSE FAIL] {fname}")
                        self.db.upsert(fname, url=url, status="failed", cache=cache_path)
                        self.stats.failed += 1
                        continue

                    self._after_parse(out_path, fname, url, cache_path)

                else:
                    bio = payload
                    with gzip.open(bio, "rt", encoding="utf-8", errors="ignore") as gz:
                        text = gz.read()

                    if not parse_text(text, out_path):
                        self.log.warning(f"[PARSE FAIL] {fname}")
                        self.db.upsert(fname, url=url, status="failed")
                        self.stats.failed += 1
                        continue

                    self._after_parse(out_path, fname, url, None)

            except Exception as e:
                self.log.error(f"[PARSE ERROR] {fname}: {e}")
                self.db.upsert(fname, url=url, status="failed")
                self.stats.failed += 1

    # ---------------------------
    # public API
    # ---------------------------

    def run(self, files, threads: int):
        """
        Starts producer/consumer:
          - downloaders = ceil(threads * 0.25)
          - parsers = everything else
        """
        if threads < 1:
            threads = 1

        dl_threads = max(1, int(round(threads * self.download_ratio)))
        parse_threads = max(1, threads - dl_threads)

        self.log.info(f"[THREADS] download={dl_threads}, parse={parse_threads}")

        # start parsers
        parsers = []
        for pid in range(parse_threads):
            t = threading.Thread(target=self._parse_worker, args=(pid,), daemon=True)
            t.start()
            parsers.append(t)

        # start downloaders
        dls = []
        for k in range(dl_threads):
            t = threading.Thread(
                target=self._download_worker,
                args=(files, k, dl_threads),
                daemon=True
            )
            t.start()
            dls.append(t)

        # wait downloaders
        for t in dls:
            t.join()

        # stop parsers
        self.task_q.put(self.stop_token)
        for t in parsers:
            t.join()

        # collect results from db (ok + path exists)
        results = []
        for f in files:
            entry = self.db.get(f["name"])
            if entry and entry.get("status") == "ok" and entry.get("path"):
                p = Path(entry["path"])
                if p.exists():
                    results.append(p)

        return results

    def merge(self, paths, out_path: Path):
        self.log.info("[MERGE] Performing final bulk merge…")
        scans = [pl.scan_parquet(p) for p in paths]
        final = pl.concat(scans).collect()
        final.write_parquet(out_path, compression="zstd", compression_level=7)
        self.log.info(f"[OK] → {out_path}")
