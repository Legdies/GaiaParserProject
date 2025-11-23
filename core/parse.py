import io
import numpy as np
import polars as pl
from pathlib import Path


NEEDED = [
    "ra", "dec",
    "distance_gspphot", "distance_gspphot_lower",
    "distance_gspphot_upper", "ruwe",
    "duplicated_source"
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


def filter_good(df):
    # Basic astrophysical and quality filters for distance and RUWE
    return df.filter(
        (pl.col("distance_gspphot").is_finite())
        & (pl.col("distance_gspphot") > 0)
        & (pl.col("distance_gspphot_upper") < pl.col("distance_gspphot") * 3)
        & (pl.col("ruwe").is_finite())
        & (pl.col("ruwe") < 1.4)
        & (~pl.col("duplicated_source"))
    )


def calc_xyz(df):
    # Convert spherical coordinates (RA/DEC + distance) to Cartesian parsecs
    ra = np.radians(df["ra"].to_numpy())
    dec = np.radians(df["dec"].to_numpy())
    dist = df["distance_gspphot"].to_numpy()

    return pl.DataFrame({
        "x_pc": (dist * np.cos(dec) * np.cos(ra)).astype("float32"),
        "y_pc": (dist * np.cos(dec) * np.sin(ra)).astype("float32"),
        "z_pc": (dist * np.sin(dec)).astype("float32")
    })


def parse_text(text: str, out_path: Path, parse_needed: bool = False):
    """
    parse_needed=False → Save the full CSV schema without filtering.
    parse_needed=True  → Extract only NEEDED columns, apply scientific filters, and compute xyz.
    """
    # Strip comment lines starting with '#'
    clean = "\n".join(
        l for l in text.splitlines()
        if not l.startswith("#")
    )
    if not clean.strip():
        return None

    # In full-schema mode we load the CSV without enforcing SCHEMA
    df = pl.read_csv(
        io.BytesIO(clean.encode()),
        separator=",",
        null_values=["null", "NaN", ""],
        schema_overrides=SCHEMA if parse_needed else None,
        ignore_errors=True,
        infer_schema_length=0
    )

    # ---------------- FULL SCHEMA MODE ----------------
    if not parse_needed:
        try:
            df.write_parquet(out_path, compression="zstd")
            return out_path
        except Exception:
            return None

    # ---------------- NEEDED MODE ---------------------

    # Check that all required columns exist
    have = set(df.columns)
    missing = set(NEEDED) - have
    if missing:
        return None

    df = df.select(NEEDED)
    df = filter_good(df)
    if df.is_empty():
        return None

    # Compute x/y/z in parsecs
    xyz = calc_xyz(df)
    xyz.write_parquet(out_path, compression="zstd")
    return out_path
