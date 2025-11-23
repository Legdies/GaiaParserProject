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
    return df.filter(
        (pl.col("distance_gspphot").is_finite())
        & (pl.col("distance_gspphot") > 0)
        & (pl.col("distance_gspphot_upper") < pl.col("distance_gspphot") * 3)
        & (pl.col("ruwe") < 1.4)
        & (~pl.col("duplicated_source"))
    )


def calc_xyz(df):
    ra = np.radians(df["ra"].to_numpy())
    dec = np.radians(df["dec"].to_numpy())
    dist = df["distance_gspphot"].to_numpy()

    return pl.DataFrame({
        "x_pc": (dist * np.cos(dec) * np.cos(ra)).astype("float32"),
        "y_pc": (dist * np.cos(dec) * np.sin(ra)).astype("float32"),
        "z_pc": (dist * np.sin(dec)).astype("float32")
    })


def parse_text(text: str, out_path: Path):
    clean = "\n".join(
        l for l in text.splitlines()
        if not l.startswith("#")
    )
    if not clean.strip():
        return None

    df = pl.read_csv(
        io.BytesIO(clean.encode()),
        separator=",",
        null_values=["null", "NaN", ""],
        schema_overrides=SCHEMA,
        ignore_errors=True,
        infer_schema_length=0
    )

    df = df.select([c for c in NEEDED if c in df.columns])
    df = filter_good(df)
    if df.is_empty():
        return None

    xyz = calc_xyz(df)
    xyz.write_parquet(out_path, compression="zstd")
    return out_path
