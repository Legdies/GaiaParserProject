import argparse
import requests
import polars as pl
import numpy as np
import io
import gzip
import zlib
import os
import json
import re
import threading
import concurrent.futures
from pathlib import Path

# ======================================================
# DIRECTORY SETUP
# ======================================================

BASE = "https://cdn.gea.esac.esa.int/Gaia/gdr3/gaia_source/"

CACHE = Path("gaiaCache")
CACHE.mkdir(exist_ok=True)

PARSED = Path("gaia_parsed")
PARSED.mkdir(exist_ok=True)

INDEX_DIR = Path("indexes")
INDEX_DIR.mkdir(exist_ok=True)

STATE_FILE = INDEX_DIR / "state.json"
FILE_INDEX = INDEX_DIR / "file_index.json"

GAIA_INDEX = PARSED / "gaia_index.parquet"

LOCK = threading.Lock()

# ======================================================
# JSON UTILITIES
# ======================================================

def load_json(path, default):
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return default


def save_json(path, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2)

# ======================================================
# STATE HANDLING
# ======================================================

def cleanup_incomplete(state):
    """Mark 'downloading' as failed and remove broken cache."""
    for f in state["files"]:
        if f["status"] == "downloading":
            cache = f.get("cache")
            if cache and os.path.exists(cache):
                os.remove(cache)
            f["status"] = "failed"
    save_json(STATE_FILE, state)


def update_state(state, name, url, status, path=None, cache_path=None):
    for f in state["files"]:
        if f["name"] == name:
            f["status"] = status
            if path:
                f["path"] = str(path)
            if cache_path is not None:
                f["cache"] = str(cache_path) if cache_path else None
            save_json(STATE_FILE, state)
            return

    state["files"].append({
        "name": name,
        "url": url,
        "status": status,
        "path": str(path) if path else None,
        "cache": str(cache_path) if cache_path else None
    })
    save_json(STATE_FILE, state)


def find_state_entry(state, name):
    for f in state["files"]:
        if f["name"] == name:
            return f
    return None

# ======================================================
# UTILS
# ======================================================

NEEDED = [
    "ra","dec","distance_gspphot","distance_gspphot_lower",
    "distance_gspphot_upper","ruwe","duplicated_source"
]

SCHEMA = {
    "ra": pl.Float64,
    "dec": pl.Float64,
    "distance_gspphot": pl.Float64,
    "distance_gspphot_lower": pl.Float64,
    "distance_gspphot_upper": pl.Float64,
    "ruwe": pl.Float64,
    "duplicated_source": pl.Boolean
}


def progress_bar(done, total):
    width = 30
    if total <= 0:
        print(f"\r[{'.'*width}] {done/1e6:.1f}MB", end="", flush=True)
        return
    ratio = done / total
    filled = int(ratio * width)
    bar = "#" * filled + "-" * (width - filled)
    print(f"\r[{bar}] {done/1e6:.1f}MB / {total/1e6:.1f}MB", end="", flush=True)


def gzip_is_valid(path):
    """Validate gzip CRC and file structure by full stream read."""
    try:
        with gzip.open(path, "rb") as f:
            while f.read(1024 * 1024):
                pass
        return True
    except (OSError, EOFError, zlib.error):
        return False


def filter_good(df):
    return df.filter(
        (pl.col("distance_gspphot").is_finite()) &
        (pl.col("distance_gspphot") > 0) &
        (pl.col("distance_gspphot_upper") < pl.col("distance_gspphot") * 3) &
        (pl.col("ruwe") < 1.4) &
        (~pl.col("duplicated_source"))
    )


def calc_xyz(df):
    ra = np.radians(df["ra"].to_numpy())
    dec = np.radians(df["dec"].to_numpy())
    dist = df["distance_gspphot"].to_numpy()
    return pl.DataFrame({
        "x_pc": (dist * np.cos(dec) * np.cos(ra)).astype("float32"),
        "y_pc": (dist * np.cos(dec) * np.sin(ra)).astype("float32"),
        "z_pc": (dist * np.sin(dec)).astype("float32"),
    })

# ======================================================
# PARSE FROM CACHE / MEMORY
# ======================================================

def parse_text_to_chunk(text, fname, idx, state, cache_path=None):
    clean = "\n".join(l for l in text.splitlines() if not l.startswith("#"))
    if not clean.strip():
        print(f"[EMPTY] {fname}")
        update_state(state, fname, None, "failed", cache_path=cache_path)
        return None

    try:
        df = pl.read_csv(
            io.BytesIO(clean.encode()),
            separator=",",
            null_values=["null","NaN",""],
            schema_overrides=SCHEMA,
            ignore_errors=True,
            infer_schema_length=0
        )
    except Exception as e:
        print(f"[PARSE FAIL] {fname}: {e}")
        update_state(state, fname, None, "failed", cache_path=cache_path)
        return None

    df = df.select([c for c in NEEDED if c in df.columns])
    df = filter_good(df)

    if df.is_empty():
        print(f"[SKIP] {fname} (empty after filters)")
        update_state(state, fname, None, "failed", cache_path=cache_path)
        return None

    xyz = calc_xyz(df)
    out = PARSED / f"chunk_{idx:05d}.parquet"
    xyz.write_parquet(out, compression="zstd")

    update_state(state, fname, None, "ok", path=out, cache_path=cache_path)
    print(f"[OK] → {out}")
    return out


def parse_from_cache(cache_path, fname, idx, state):
    print(f"[CACHE] Parsing {fname}")

    if not gzip_is_valid(cache_path):
        print(f"[BAD GZIP] {fname} → rebuild required")
        update_state(state, fname, None, "failed", cache_path=cache_path)
        return None

    try:
        with gzip.open(cache_path, "rt", encoding="utf-8", errors="ignore") as gz:
            text = gz.read()
    except Exception as e:
        print(f"[CORRUPT CACHE] {fname}: {e}")
        update_state(state, fname, None, "failed", cache_path=cache_path)
        return None

    return parse_text_to_chunk(text, fname, idx, state, cache_path=cache_path)

# ======================================================
# DOWNLOADER
# ======================================================

def download_file(url, fname, save_cache):
    """
    If save_cache=True: download to gaiaCache/fname and return cache_path.
    If save_cache=False: download to memory (BytesIO) and return (bio, None).
    """
    r = requests.get(url, stream=True)
    r.raise_for_status()

    total = int(r.headers.get("Content-Length", "0"))
    done = 0

    if save_cache:
        cache_path = CACHE / fname
        with open(cache_path, "wb") as f:
            for chunk in r.iter_content(1024 * 1024):
                f.write(chunk)
                done += len(chunk)
                progress_bar(done, total)
        print()
        return None, cache_path
    else:
        bio = io.BytesIO()
        for chunk in r.iter_content(1024 * 1024):
            bio.write(chunk)
            done += len(chunk)
            progress_bar(done, total)
        print()
        bio.seek(0)
        return bio, None

# ======================================================
# PROCESSOR
# ======================================================

def process_file(url, idx, save_cache, state):
    fname = url.split("/")[-1]
    entry = find_state_entry(state, fname)

    # If cache exists and state says ok/failed → parse cache, no download
    if entry and entry.get("cache"):
        cpath = Path(entry["cache"])
        if cpath.exists():
            out = parse_from_cache(cpath, fname, idx, state)
            if out:
                return out

    print(f"\n[DL] {fname}")
    update_state(state, fname, url, "downloading")

    bio, cache_path = download_file(url, fname, save_cache)

    # Validate gzip
    if save_cache:
        if not gzip_is_valid(cache_path):
            print(f"[BAD GZIP] {fname} after download")
            update_state(state, fname, url, "failed", cache_path=cache_path)
            return None
        return parse_from_cache(cache_path, fname, idx, state)

    # No cache: validate by trying to read stream
    try:
        text = gzip.open(bio, "rt", encoding="utf-8", errors="ignore").read()
    except Exception as e:
        print(f"[BAD GZIP STREAM] {fname}: {e}")
        update_state(state, fname, url, "failed", cache_path=None)
        return None

    return parse_text_to_chunk(text, fname, idx, state, cache_path=None)

# ======================================================
# MAIN
# ======================================================

def print_help(args = None):
    if args.save_cache:
        print("[NoCache] Is used to stream downloads directly to parser, converting file to parquet without storing cache on drive. NOT RECOMMENDED. [Default: disabled]")
    if args.save_cache:
        print("[SaveCache] Saves cache to allow reusing and saving time later. Requires more drive space! [Default: enabled]")
    if args.test:
        print("[Test] Required for testing, usually not required. Downloads one file.")
    if args.multiple:
        print("[Multiple] Creates multiple parquet files.")
def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--test", action="store_true")
    parser.add_argument("--test-parse", action="store_true")
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--full", action="store_true")

    cache_group = parser.add_mutually_exclusive_group()
    cache_group.add_argument("--SaveCache", action="store_true",
                             help="Save CSV.gz to gaiaCache (default)")
    cache_group.add_argument("--NoCache", action="store_true",
                             help="Do not save cache (asks confirmation)")

    parser.add_argument("--thread", type=int, default=6)
    parser.add_argument("--SingleFile", action="store_true")
    parser.add_argument("--multiple", action="store_true")

    args = parser.parse_args()
    if not args:
        parser.print_help()
        raise Exception(SystemExit)



    # default: cache ON unless NoCache
    save_cache = not args.NoCache

    if args.NoCache:
        print("[WARNING] Cache will NOT be created. Re-downloads will be required.")
        confirm = input("Continue? Type 'yes': ").strip().lower()
        if confirm != "yes":
            print("Aborted.")
            return

    # state
    state = load_json(STATE_FILE, {"files": []})
    cleanup_incomplete(state)

    # file_index.json is source of truth
    if FILE_INDEX.exists():
        idx_data = load_json(FILE_INDEX, {"files": []})
        if not idx_data["files"]:
            idx_data = None
    else:
        idx_data = None

    if idx_data is None:
        print("[SCAN] Downloading index...")
        html = requests.get(BASE).text
        found = re.findall(r'href=[\'"]([^\'"]*?\.csv\.gz)[\'"]', html)
        files = [{"name": f, "url": BASE + f} for f in found]
        save_json(FILE_INDEX, {"files": files})
        idx_data = {"files": files}

    files = idx_data["files"]
    print(f"[FOUND] {len(files)} files")

    if args.test:
        files = files[:1]
    elif args.test_parse:
        files = files[:1]
    elif not (args.all or args.full):
        print("ERROR: select --all or --full")
        return

    results = []

    # First pass: reuse parsed or cached
    for i, f in enumerate(files):
        fname = f["name"]
        entry = find_state_entry(state, fname)

        if entry and entry["status"] == "ok" and entry.get("path"):
            p = Path(entry["path"])
            if p.exists():
                print(f"[SKIP] {fname} already parsed.")
                results.append(p)
                continue

        if entry and entry.get("cache"):
            cpath = Path(entry["cache"])
            if cpath.exists():
                out = parse_from_cache(cpath, fname, i, state)
                if out:
                    results.append(out)

    # Second pass: download missing/failed
    with concurrent.futures.ThreadPoolExecutor(args.thread) as ex:
        futures = []
        for i, f in enumerate(files):
            fname = f["name"]
            entry = find_state_entry(state, fname)

            if entry and entry["status"] == "ok" and entry.get("path") and Path(entry["path"]).exists():
                continue

            if entry and entry["status"] == "ok" and entry.get("cache") and Path(entry["cache"]).exists():
                continue

            futures.append(ex.submit(process_file, f["url"], i, save_cache, state))

        for fut in concurrent.futures.as_completed(futures):
            out = fut.result()
            if out:
                results.append(out)

    if args.test or args.test_parse:
        print("[DONE] test mode.")
        return

    if args.multiple:
        print("[DONE] multiple chunks complete.")
        return

    print("[MERGE] building gaia_index.parquet...")
    scans = [pl.scan_parquet(p) for p in results]
    final = pl.concat(scans).collect()
    final.write_parquet(GAIA_INDEX, compression="zstd", compression_level=7)
    print("[DONE] →", GAIA_INDEX)



# ======================================================
# COMMAND: VERIFY
# ======================================================

def cmd_verify(args):
    print("[VERIFY] Checking cached gzip integrity…")

    state = load_json(STATE_FILE, {"files": []})
    corrupted = []

    for f in state["files"]:
        cache = f.get("cache")
        if cache and Path(cache).exists():
            if not gzip_is_valid(cache):
                corrupted.append(f["name"])
                print(f"[BAD] {f['name']}")
        else:
            print(f"[MISS] {f['name']} (no cache)")

    if not corrupted:
        print("[OK] All cached files are valid.")
    else:
        print(f"[FAIL] {len(corrupted)} corrupted files:")
        for name in corrupted:
            print(" -", name)


# ======================================================
# COMMAND: DOWNLOAD (cache only)
# ======================================================

def cmd_download(args):
    print("[MODE] DOWNLOAD — only downloading CSV.gz")

    # Load or build index
    if FILE_INDEX.exists():
        idx = load_json(FILE_INDEX, {"files": []})
        if not idx["files"]:
            idx = None
    else:
        idx = None

    if idx is None:
        print("[SCAN] Building index…")
        html = requests.get(BASE).text
        found = re.findall(r'href=[\'\"]([^\'\"]*?\\.csv\\.gz)[\'\"]', html)
        files = [{"name": f, "url": BASE + f} for f in found]
        save_json(FILE_INDEX, {"files": files})
        idx = {"files": files}

    files = idx["files"]
    print(f"[FOUND] {len(files)} files")

    for f in files:
        fname = f["name"]
        url = f["url"]
        cp = CACHE / fname

        if cp.exists():
            print(f"[SKIP] {fname} exists.")
            continue

        print(f"[DL] {fname}")
        _, saved = download_file(url, fname, save_cache=True)

        if gzip_is_valid(saved):
            print(f"[OK] {fname}")
        else:
            print(f"[BAD] {fname} corrupted.")


# ======================================================
# COMMAND: PIPELINE (your full ETL)
# ======================================================

def cmd_pipeline(args):
    print("[MODE] PIPELINE — full ETL")

    save_cache = not args.NoCache

    if args.NoCache:
        print("[WARNING] Cache disabled. Downloading will be repeated each run.")
        if input("Type 'yes' to continue: ").strip().lower() != "yes":
            print("Aborted.")
            return

    # Load state
    state = load_json(STATE_FILE, {"files": []})
    cleanup_incomplete(state)

    # Load index
    if FILE_INDEX.exists():
        idx = load_json(FILE_INDEX, {"files": []})
        if not idx["files"]:
            idx = None
    else:
        idx = None

    if idx is None:
        print("[SCAN] Building index…")
        html = requests.get(BASE).text
        found = re.findall(r'href=[\'\"]([^\'\"]*?\\.csv\\.gz)[\'\"]', html)
        files = [{"name": f, "url": BASE + f} for f in found]
        save_json(FILE_INDEX, {"files": files})
        idx = {"files": files}

    files = idx["files"]
    print(f"[FOUND] {len(files)} files")

    # Modes
    if args.test:
        files = files[:1]
    if args.test_parse:
        files = files[:1]

    results = []

    # Reuse cache / parsed
    for i, f in enumerate(files):
        fname = f["name"]
        entry = find_state_entry(state, fname)

        # Already parsed
        if entry and entry["status"] == "ok" and entry.get("path"):
            path = Path(entry["path"])
            if path.exists():
                print(f"[SKIP] {fname}")
                results.append(path)
                continue

        # Parse from existing cache
        if entry and entry.get("cache"):
            cp = Path(entry["cache"])
            if cp.exists():
                out = parse_from_cache(cp, fname, i, state)
                if out:
                    results.append(out)

    # Download + process missing
    with concurrent.futures.ThreadPoolExecutor(args.thread) as ex:
        futures = []
        for i, f in enumerate(files):
            fname = f["name"]
            entry = find_state_entry(state, fname)

            parsed_ok = (
                entry and entry["status"] == "ok"
                and entry.get("path")
                and Path(entry["path"]).exists()
            )

            cached_ok = (
                entry and entry["status"] == "ok"
                and entry.get("cache")
                and Path(entry["cache"]).exists()
            )

            if parsed_ok or cached_ok:
                continue

            futures.append(ex.submit(
                process_file, f["url"], i, save_cache, state
            ))

        for fut in concurrent.futures.as_completed(futures):
            out = fut.result()
            if out:
                results.append(out)

    # Test-only modes
    if args.test or args.test_parse:
        print("[DONE] test mode.")
        return

    # Stop if user only wants separate parquet files
    if args.multiple:
        print("[DONE] multiple chunks.")
        return

    # Merge
    print("[MERGE] Building gaia_index.parquet…")
    scans = [pl.scan_parquet(p) for p in results]
    final = pl.concat(scans).collect()
    final.write_parquet(GAIA_INDEX, compression="zstd")
    print("[DONE] →", GAIA_INDEX)


# ======================================================
# MAIN — SUBCOMMAND ROUTER
# ======================================================

def main():
    parser = argparse.ArgumentParser(
        description="Gaia GDR3 ETL Tool",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    sub = parser.add_subparsers(dest="command", required=True)

    # download
    dl = sub.add_parser("download", help="Download .csv.gz files into cache")
    dl.set_defaults(func=cmd_download)

    # verify
    vr = sub.add_parser("verify", help="Verify integrity of cached gzip files")
    vr.set_defaults(func=cmd_verify)

    # pipeline
    plp = sub.add_parser("pipeline", help="Full pipeline: ETL + merge")
    plp.add_argument("--test", action="store_true")
    plp.add_argument("--test-parse", action="store_true")
    plp.add_argument("--NoCache", action="store_true")
    plp.add_argument("--multiple", action="store_true")
    plp.add_argument("--thread", type=int, default=6)
    plp.set_defaults(func=cmd_pipeline)

    args = parser.parse_args()
    args.func(args)

