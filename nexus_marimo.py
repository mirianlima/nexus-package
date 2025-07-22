import marimo

__generated_with = "0.14.11"
app = marimo.App(width="columns")


@app.cell(column=0)
def _():
    import pandas as pd
    import altair as alt
    import marimo as mo
    from pathlib import Path
    import polars as pl
    import tools as nx
    import yaml
    #from utils import load_config
    return Path, alt, mo, nx, pl, yaml


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Force reload tools""")
    return


@app.cell
def _(nx):
    import importlib
    importlib.reload(nx)
    print(nx.__file__)
    print(dir(nx))
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Setup""")
    return


@app.cell
def _(mo):
    mo.md(r"""Config yaml, project data path, altair options""")
    return


@app.cell
def _(Path, alt, yaml):
    # 1. Load configuration
    with open("config.yaml") as f:
        config = yaml.safe_load(f)

    # 2. Project Paths
    PROJECT_ROOT = Path.cwd()
    DATA_PATH    = PROJECT_ROOT / config["data"]["nexus_path"]

    # 3. Query settings
    INDEX_COLS      = config["query"]["index_columns"]
    SOURCE_META     = config["query"]["source_metadata_columns"]
    IND_META        = config["query"]["indicator_metadata_columns"]
    COUNTRY_CLASSES = config["query"]["country_classification_columns"]

    # 4. Altair transformer
    alt.data_transformers.enable('default', max_rows=None)
    return COUNTRY_CLASSES, DATA_PATH, INDEX_COLS, IND_META, SOURCE_META


@app.cell
def _(mo):
    mo.md(r"""## Nexus Transform""")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""Nexus table with income level categorical + "country" rename""")
    return


@app.cell
def _(DATA_PATH, pl):
    # 1️⃣ Lazy‑load the Parquet into a LazyFrame
    nexus = (
        pl.scan_parquet(DATA_PATH)
        .rename({"country_or_area": "country"})
        .with_columns(
            pl.when(pl.col("high_income") == "High income")
            .then(pl.lit("High Income"))
            .when(pl.col("upper_middle_income") == "Upper middle income")
            .then(pl.lit("Upper Middle Income"))
            .when(pl.col("lower_middle_income") == "Lower middle income")
            .then(pl.lit("Lower Middle Income"))
            .when(pl.col("low_income") == "Low income")
            .then(pl.lit("Low Income"))
            .otherwise(None)
            .cast(pl.Categorical)
            .alias("income_level")
        )
    )
    return (nexus,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""Nexus Indicators meta tab""")
    return


@app.cell
def _():
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""Testing the income_level classification""")
    return


@app.cell
def _(nexus, pl):
    # Check income_level for a sample of countries
    test_countries = ["United States", "China", "Nigeria", "Bangladesh", "Brazil"]

    income_check = (
        nexus
        .filter(pl.col("country").is_in(test_countries))
        .select(["country", "income_level", "high_income", "upper_middle_income", "lower_middle_income", "low_income"])
        .unique(["country"])
        .collect()
    )

    print(income_check)

    # Also check the distribution of income levels
    income_distribution = (
        nexus
        .select("income_level")
        .group_by("income_level")
        .agg(pl.len().alias("count"))
        .collect()
    )

    print("\nIncome Level Distribution:")
    print(income_distribution.sort("count", descending=True))
    return


@app.cell
def _(nexus, pl):
    # Count distinct countries with null income_level
    null_income_countries = (
       nexus
       .filter(pl.col("income_level").is_null())
       .select(pl.col("country").n_unique())
       .collect()
       .item()
    )

    print(f"Number of distinct countries with null income_level: {null_income_countries}")

    # Optionally, see which countries have null income_level
    countries_with_null_income = (
       nexus
       .filter(pl.col("income_level").is_null())
       .select("country")
       .unique()
       .collect()
       .sort("country")
    )

    print(f"\nCountries with null income_level:")
    print(countries_with_null_income)
    # countries_with_null_income.write_csv("countries_with_null_income.csv")
    return (countries_with_null_income,)


@app.cell
def _(countries_with_null_income):
    countries_with_null_income
    return


@app.cell
def _(mo):
    mo.md(r"""## TODOS""")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    1. Create categorical income_level
    2. Clean income_level nulls
    3. Fix missing values in index columns
    """
    )
    return


@app.cell(column=1, hide_code=True)
def _(mo):
    mo.md(r"""# NEXUS OBT""")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Nexus Data""")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""Inspect the schema without fetching all the data""")
    return


@app.cell
def _(nexus):
    nexus.collect_schema()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""Or the full collect shabang""")
    return


@app.cell
def _(nexus):
    df = nexus.collect()      # run the query
    print(df.shape)           # (n_rows, n_cols)
    print(df.dtypes)          # list of dtypes
    df.describe()             # summary stats for each column
    return


@app.cell
def _(nexus):
    nexus.inspect()
    return


@app.cell
def _(alt):
    _chart = (
        alt.Chart(_df)
        .mark_bar()
        .transform_aggregate(count="count()", groupby=["indicator_label"])
        .transform_window(
            rank="rank()",
            sort=[
                alt.SortField("count", order="descending"),
                alt.SortField("indicator_label", order="ascending"),
            ],
        )
        .transform_filter(alt.datum.rank <= 10)
        .encode(
            y=alt.Y(
                "indicator_label:N",
                sort="-x",
                axis=alt.Axis(title=None),
            ),
            x=alt.X("count:Q", title="Number of records"),
            tooltip=[
                alt.Tooltip("indicator_label:N"),
                alt.Tooltip("count:Q", format=",.0f", title="Number of records"),
            ],
        )
        .properties(title="Top 10 indicator_label", width="container")
        .configure_view(stroke=None)
        .configure_axis(grid=False)
    )
    _chart
    return


@app.cell
def _(nexus, pl):
    nexus.select(pl.col("collection")).n_unique()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Nexus metadata""")
    return


@app.cell
def _(COUNTRY_CLASSES, IND_META, SOURCE_META, nexus, pl):
    # Get distinct counts for all metadata columns
    all_metadata_cols = SOURCE_META + IND_META + COUNTRY_CLASSES

    distinct_counts = (
       nexus
       .select([pl.col(c).n_unique().alias(c) for c in all_metadata_cols])
       .collect()
    )

    # Display as a transposed view
    distinct_counts.transpose(include_header=True, header_name="column", column_names=["distinct_count"])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Utils""")
    return


@app.cell
def _():
    from utils import (
        get_sub_region_info,
        calculate_base_stats,
        calculate_sub_region_coverage,
        style_sub_region_coverage_heatmap
    )
    return (
        calculate_base_stats,
        calculate_sub_region_coverage,
        get_sub_region_info,
        style_sub_region_coverage_heatmap,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""Geo info""")
    return


@app.cell
def _(get_sub_region_info, nexus):
    get_sub_region_info(nexus)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""Basic stats""")
    return


@app.cell
def _(calculate_base_stats, nexus):
    calculate_base_stats(nexus)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""Sub region coverage""")
    return


@app.cell
def _(calculate_sub_region_coverage, get_sub_region_info, nexus):
    cvg_geo=calculate_sub_region_coverage(
        nexus,
        get_sub_region_info(nexus)
    )

    cvg_geo
    return (cvg_geo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""subregion coverage with style""")
    return


@app.cell
def _(cvg_geo, style_sub_region_coverage_heatmap):
    style_sub_region_coverage_heatmap(cvg_geo)
    return


@app.cell
def _(mo):
    mo.md(r"""## Final result""")
    return


@app.cell
def _(
    calculate_base_stats,
    calculate_sub_region_coverage,
    get_sub_region_info,
    nexus,
    pl,
):
    """Nexus indicators metadata analysis tool."""

    # import polars as pl
    # from utils import (
    #    get_sub_region_info,
    #    calculate_base_stats,
    #    calculate_sub_region_coverage
    # )


    def create_indicators_metadata(df: pl.LazyFrame) -> pl.DataFrame:
       """
       Create comprehensive metadata table for indicators in nexus dataset.
   
       Parameters
       ----------
       df : pl.LazyFrame
           LazyFrame containing nexus data
       
       Returns
       -------
       pl.DataFrame
           Metadata table with one row per indicator
       """
   
       def calculate_temporal_completeness() -> pl.Expr:
           """Calculate temporal completeness expression."""
           return (
               pl.col("num_years_with_data") / (pl.col("max_year") - pl.col("min_year") + 1) * 100
           ).fill_null(0)

       def calculate_geographic_completeness(sub_region_info: pl.DataFrame) -> pl.Expr:
           """Calculate geographic completeness expression."""
           regions = sub_region_info.get_column("sub_region_name").to_list()
       
           return pl.concat_list([
               pl.col(f"pct_coverage_{region.replace(' ', '_')}") for region in regions
           ]).list.mean()
   
       # Get sub-region information
       sub_region_info = get_sub_region_info(df)
   
       # Calculate base statistics
       base_stats = calculate_base_stats(df)
   
       # Calculate sub-region coverage percentages (returns raw DataFrame, not styled)
       sub_region_coverage=calculate_sub_region_coverage(nexus, sub_region_info)
   
       # Calculate completeness components
       temporal_completeness = calculate_temporal_completeness()
       geographic_completeness = calculate_geographic_completeness(sub_region_info)

       # Calculate final metrics with completeness score
       final_result = (
           base_stats
           .join(sub_region_coverage, on="indicator_label", how="left")
           .with_columns([
               ((temporal_completeness + geographic_completeness) / 2).alias("completeness_score")
           ])
           .sort("completeness_score", descending=True)
       )
   
       return final_result

    create_indicators_metadata(nexus)
    return


@app.cell(column=2, hide_code=True)
def _(mo):
    mo.md(r"""## `datamap` tool""")
    return


@app.cell
def _(SOURCE_META, nexus, nx):
    dtmap=nx.datamap(
        nexus,
        source_metadata_columns=SOURCE_META
    )
    return (dtmap,)


@app.cell
def _(dtmap, mo):
    table=mo.ui.table(data=dtmap, pagination=True)
    return (table,)


@app.cell
def _(dtmap):
    dtmap
    return


@app.cell
def _(table):
    table
    return


@app.cell
def _(table):
    collections=table.value.get_column("collection").to_list()
    return (collections,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## `filter_pivot` tool""")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""Using `filter_pivot` with config values""")
    return


@app.cell
def _(INDEX_COLS, collections, nexus, nx, pl):
    nexus_pivot=nx.filter_pivot(
        nexus.filter(pl.col("collection").is_in(collections)),
        index_cols=INDEX_COLS,
        ind='label'  # or 'code' to pivot on indicator_code
    )
    nexus_pivot
    return (nexus_pivot,)


@app.cell
def _(mo, nexus_pivot):
    mo.ui.data_explorer(nexus_pivot)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""Chainning filters before pivoting""")
    return


@app.cell
def _(INDEX_COLS, nexus, nx, pl):
    nx.filter_pivot(
        nexus.filter(
            (pl.col("source") == "ISORA") & 
            (pl.col("year") == 2020) 
        ),
        index_cols=INDEX_COLS,
        ind='label'
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""filter for specific countries""")
    return


@app.cell
def _(INDEX_COLS, nexus, nx, pl):
    nx.filter_pivot(
        nexus.filter(
            (pl.col("source") == "World Bank") & 
            (pl.col("region_name") == "Africa")
        ),
        index_cols=INDEX_COLS,
        ind='code'  # Use indicator codes instead of labels
    )
    return


@app.cell
def _(INDEX_COLS, nexus, nx, pl):
    nx.filter_pivot(
        nexus.filter(
            (pl.col("country") == "Liberia")
        ),
        index_cols=INDEX_COLS,
        ind='code'  # Use indicator codes instead of labels
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
