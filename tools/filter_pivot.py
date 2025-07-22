"""Filter-then-pivot-pattern tool implementation."""

from typing import Literal, List
import polars as pl

def filter_pivot(
    df: pl.LazyFrame,
    index_cols: List[str],
    ind: Literal['code', 'label'] = 'label'
) -> pl.DataFrame:
    """
    Pivot a LazyFrame on country and year, spreading each indicator's values into its own column.

    Parameters
    ----------
    df
        A LazyFrame containing at least the columns from index_cols.
    index_cols
        List of columns from config.yaml's index_columns.
    ind
        If 'code', pivot on the 'indicator_code' column; if 'label', pivot on 'indicator_label'.

    Returns
    -------
    pl.DataFrame
        An eagerly‑collected DataFrame indexed by country and year, with one column per indicator.
    """
    # Build cols based on ind parameter
    if ind == "code":
        # Replace 'indicator_label' with 'indicator_code' in index_cols
        cols = [col.replace("indicator_label", "indicator_code") for col in index_cols]
        ind_col = "indicator_code"
    else:
        # Use index_cols as is (already has indicator_label)
        cols = index_cols
        ind_col = "indicator_label"

    # Select lazily, collect to eager DataFrame, then pivot
    eager = (
        df
        .select(*cols)                    # unpack as varargs
        .collect()
    )

    return eager.pivot(
        values="value",                  # fill values from this column
        index=["country", "year"],       # group by these cols
        on=ind_col,                      # spread unique values here as new cols
        aggregate_function="first"
    )
