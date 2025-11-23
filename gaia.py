import argparse
from pathlib import Path

from core.index import IndexManager
from core.state import StateManager
from core.pipeline import Pipeline
from core.utils import gzip_is_valid

BASE = "https://cdn.gea.esac.esa.int/Gaia/gdr3/gaia_source/"

CACHE = Path("gaiaCache")
PARSED = Path("gaia_parsed")
INDEX_DIR = Path("indexes")

STATE_FILE = INDEX_DIR / "state.json"
FILE_INDEX = INDEX_DIR / "file_index.json"
GAIA_INDEX = PARSED / "gaia_index.parquet"


def cmd_verify(args):
    print("[VERIFY] Checking gzip integrity…")
    state = StateManager(STATE_FILE).data
    corrupted = []

    for f in state["files"]:
        cache = f.get("cache")
        if cache and Path(cache).exists():
            if not gzip_is_valid(Path(cache)):
                corrupted.append(f["name"])
                print("[BAD]", f["name"])

    if not corrupted:
        print("[OK] All fine.")
    else:
        print("Corrupted:", corrupted)


def cmd_download(args):
    print("[DOWNLOAD] Cache-only mode.")

    idx_mgr = IndexManager(FILE_INDEX, BASE)
    idx = idx_mgr.load() or idx_mgr.scan_remote()
    files = idx["files"]

    for f in files:
        fname = f["name"]
        url = f["url"]
        cache_path = CACHE / fname

        if cache_path.exists() and gzip_is_valid(cache_path):
            print("[SKIP]", fname)
            continue

        print("[DL]", fname)
        from core.download import download
        _, saved = download(url, cache_path)

        if gzip_is_valid(saved):
            print("[OK]", fname)
        else:
            print("[BAD]", fname)


def cmd_pipeline(args):
    print("[PIPELINE] Starting ETL…")

    INDEX_DIR.mkdir(exist_ok=True)
    CACHE.mkdir(exist_ok=True)
    PARSED.mkdir(exist_ok=True)

    idx_mgr = IndexManager(FILE_INDEX, BASE)
    idx = idx_mgr.load() or idx_mgr.scan_remote()
    files = idx["files"]

    if args.test or args.test_parse:
        files = files[:1]

    state = StateManager(STATE_FILE)
    state.cleanup_incomplete()

    pipeline = Pipeline(state, CACHE, PARSED)

    results = []

    # reuse
    for i, f in enumerate(files):
        entry = state.get(f["name"])
        if entry and entry.get("path") and Path(entry["path"]).exists():
            results.append(Path(entry["path"]))
            continue
        if entry and entry.get("cache") and Path(entry["cache"]).exists():
            out = pipeline.parse_from_cache(Path(entry["cache"]), f["name"], i)
            if out:
                results.append(out)

    # missing
    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(args.thread) as ex:
        futures = []
        for i, f in enumerate(files):
            fname = f["name"]
            entry = state.get(fname)
            parsed_ok = entry and entry.get("path") and Path(entry["path"]).exists()
            cached_ok = entry and entry.get("cache") and Path(entry["cache"]).exists()

            if parsed_ok or cached_ok:
                continue

            futures.append(ex.submit(
                pipeline.process,
                f["url"], fname, i,
                save_cache=not args.NoCache
            ))

        for fut in concurrent.futures.as_completed(futures):
            res = fut.result()
            if res:
                results.append(res)

    if args.test or args.test_parse:
        print("[DONE] Test mode.")
        return

    if args.multiple:
        print("[DONE] Separate parquet chunks.")
        return

    print("[MERGE] Building final parquet index…")
    pipeline.merge(results, GAIA_INDEX)
    print("[DONE] →", GAIA_INDEX)


def main():
    parser = argparse.ArgumentParser(
        description="Gaia GDR3 ETL Tool",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    sub = parser.add_subparsers(dest="cmd", required=True)

    d = sub.add_parser("download")
    d.set_defaults(func=cmd_download)

    v = sub.add_parser("verify")
    v.set_defaults(func=cmd_verify)

    p = sub.add_parser("pipeline")
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
