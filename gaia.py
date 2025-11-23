import argparse
from pathlib import Path

from core.index import IndexManager
from core.state_sqlite import StateDB
from core.pipeline import Pipeline
from core.logging_setup import setup_logging
from core.stats import Stats
from core.utils import gzip_is_valid
from core.download import download

BASE = "https://cdn.gea.esac.esa.int/Gaia/gdr3/gaia_source/"

CACHE = Path("gaiaCache")
PARSED = Path("gaia_parsed")
INDEX_DIR = Path("indexes")

STATE_DB = INDEX_DIR / "state.sqlite3"
FILE_INDEX = INDEX_DIR / "file_index.json"
GAIA_INDEX = PARSED / "gaia_index.parquet"
LOG_FILE = INDEX_DIR / "run.log"
STATS_FILE = INDEX_DIR / "stats.json"


def cmd_verify(args):
    log = setup_logging(LOG_FILE)
    log.info("VERIFY: checking gzip cache integrity")

    db = StateDB(STATE_DB)
    corrupted = []

    for f in db.iter_all():
        cache = f.get("cache")
        if cache and Path(cache).exists():
            if not gzip_is_valid(Path(cache)):
                corrupted.append(f["name"])
                log.warning(f"BAD: {f['name']}")

    if not corrupted:
        log.info("All cached gzip files are valid.")
    else:
        log.error(f"Corrupted files: {len(corrupted)}")


def cmd_download(args):
    log = setup_logging(LOG_FILE)
    log.info("DOWNLOAD: cache-only mode")

    INDEX_DIR.mkdir(exist_ok=True)
    CACHE.mkdir(exist_ok=True)

    idx_mgr = IndexManager(FILE_INDEX, BASE)
    idx = idx_mgr.load() or idx_mgr.scan_remote()
    files = idx["files"]

    db = StateDB(STATE_DB)

    for f in files:
        fname, url = f["name"], f["url"]
        cache_path = CACHE / fname

        if cache_path.exists() and gzip_is_valid(cache_path):
            log.info(f"SKIP: {fname} already in cache")
            db.upsert(fname, url=url, status="ok", cache=cache_path)
            continue

        log.info(f"DL: {fname}")
        _, saved = download(url, cache_path, logger=log, multi_thread=False)

        if gzip_is_valid(saved):
            log.info(f"OK: {fname}")
            db.upsert(fname, url=url, status="ok", cache=saved)
        else:
            log.error(f"BAD: {fname}")
            db.upsert(fname, url=url, status="failed", cache=saved)


def cmd_pipeline(args):
    log = setup_logging(LOG_FILE)
    stats = Stats()

    INDEX_DIR.mkdir(exist_ok=True)
    CACHE.mkdir(exist_ok=True)
    PARSED.mkdir(exist_ok=True)

    log.info("PIPELINE: starting ETL")

    idx_mgr = IndexManager(FILE_INDEX, BASE)
    idx = idx_mgr.load() or idx_mgr.scan_remote()
    files = idx["files"]

    if args.test or args.test_parse:
        files = files[:1]

    stats.total_files = len(files)

    db = StateDB(STATE_DB)
    db.cleanup_incomplete()

    pipeline = Pipeline(
        db=db,
        cache_dir=CACHE,
        parsed_dir=PARSED,
        logger=log,
        stats=stats,
        test_parse=args.test_parse,
        download_threads_ratio=0.25,   # 25% download
        save_cache=not args.NoCache,
    )

    results = pipeline.run(files, threads=args.thread)

    if args.test or args.test_parse:
        log.info("DONE: test mode")
        stats.finish(STATS_FILE)
        return

    if args.multiple:
        log.info("DONE: multiple chunks mode")
        stats.finish(STATS_FILE)
        return

    log.info("MERGE: building gaia_index.parquet")
    pipeline.merge(results, GAIA_INDEX)

    report = stats.finish(STATS_FILE)
    log.info(f"STATS saved → {STATS_FILE}")
    log.info(f"Run summary: {report}")


def main():
    parser = argparse.ArgumentParser(
        description="Gaia GDR3 ETL Tool",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    d = sub.add_parser("download", help="Download .csv.gz into cache")
    d.set_defaults(func=cmd_download)

    v = sub.add_parser("verify", help="Verify cached gzip integrity")
    v.set_defaults(func=cmd_verify)

    p = sub.add_parser("pipeline", help="Full ETL + merge")
    p.add_argument("--test", action="store_true")
    p.add_argument("--test-parse", action="store_true")
    p.add_argument("--NoCache", action="store_true")
    p.add_argument("--multiple", action="store_true")
    p.add_argument("--thread", type=int, default=6)
    p.set_defaults(func=cmd_pipeline)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
