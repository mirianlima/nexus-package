"""Datamap tool implementation."""

from typing import List, Optional
import polars as pl

def datamap(
    df: pl.LazyFrame,
    source_metadata_columns: List[str],
    indicator_metadata_columns: Optional[List[str]] = None
) -> pl.DataFrame:
    """
    Count rows by source metadata (and optionally by indicator metadata).

    Parameters
    ----------
    df
        LazyFrame containing at least the columns in `source_metadata_columns`
        and (if indicator_metadata_columns is provided) those in `indicator_metadata_columns`.
    source_metadata_columns
        List of columns to group by for source metadata
        (e.g. ["source", "database", "collection"]).
    indicator_metadata_columns
        List of columns to group by for indicator metadata
        (e.g. ["indicator_label", "value_meta"]). If None, only source metadata is used.

    Returns
    -------
    pl.DataFrame
        Eagerly‑collected counts, with one row per unique metadata group.
    """
    
    group_cols = source_metadata_columns + (indicator_metadata_columns or [])

    return (
        df
        .select(*group_cols)
        .group_by(group_cols)
        .agg(pl.count().alias("count"))
        .sort(group_cols + ["count"])
        .collect()
    )