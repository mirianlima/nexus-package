"""Utility functions for nexus indicators metadata analysis."""

import polars as pl


def get_sub_region_info(df: pl.LazyFrame) -> pl.DataFrame:
    """Get sub-region information with total countries per region."""
    return (
        df
        .select(["country", "sub_region_name"])
        .unique()
        .drop_nulls()
        .group_by("sub_region_name")
        .agg(
            total_countries=pl.len(),
            countries=pl.col("country")
        )
        .collect()
    )


def calculate_base_stats(df: pl.LazyFrame) -> pl.DataFrame:
    """Calculate base statistics for each indicator."""
    return (
        df
        .group_by("source","collection","indicator_label")
        .agg([
            # Counts and coverage
            pl.len().alias("count_rows"),
            pl.col("value").null_count().alias("count_missing_value"),
            pl.col("country").n_unique().alias("num_countries_with_data"),
            pl.col("year").filter(pl.col("value").is_not_null()).n_unique().alias("num_years_with_data"),
            
            # Value statistics
            pl.col("value").mean().alias("mean_value"),
            pl.col("value").median().alias("median_value"), 
            pl.col("value").min().alias("min_value"),
            pl.col("value").max().alias("max_value"),
            (pl.col("value").quantile(0.75) - pl.col("value").quantile(0.25)).alias("iqr_value"),
            
            # Temporal coverage
            pl.col("year").filter(pl.col("value").is_not_null()).min().alias("min_year"),
            pl.col("year").filter(pl.col("value").is_not_null()).max().alias("max_year"),
        ])
        .with_columns([
            (pl.col("count_missing_value") / pl.col("count_rows") * 100).alias("pct_missing_value")
        ])
        .sort("source","collection","indicator_label")
        .collect()
    )


def calculate_sub_region_coverage(df: pl.LazyFrame, sub_region_info: pl.DataFrame) -> pl.DataFrame:
   """Calculate sub-region coverage percentages for each indicator."""
   # Create lookup dict and regions list from sub_region_info
   region_totals = dict(sub_region_info.select(["sub_region_name", "total_countries"]).rows())
   regions = sub_region_info.get_column("sub_region_name").to_list()
   
   return (
       df
       .filter(pl.col("value").is_not_null())
       .with_columns(
           pl.col("sub_region_name").str.replace_all(" ", "_").alias("sub_region_name_clean")
       )
       .group_by(["indicator_label", "sub_region_name_clean"])
       .agg(pl.col("country").n_unique().alias("countries_with_data"))
       .collect()
       .pivot(on="sub_region_name_clean", index="indicator_label", values="countries_with_data")
       .fill_null(0)
       .with_columns([
           (pl.col(region.replace(" ", "_")) / region_totals[region] * 100).round(3)
           .alias(f"pct_coverage_{region.replace(' ', '_')}")
           for region in regions
       ])
       .drop([region.replace(" ", "_") for region in regions])  # Keep only percentage columns
       .sort("indicator_label")
   )


def style_sub_region_coverage_heatmap(coverage_df: pl.DataFrame):
   """Apply heatmap styling to sub-region coverage DataFrame."""
   import marimo as mo
   
   def style_cell(row_id, column_name, value):
       """Style cells with simple red-green gradient through bright yellow."""
       if column_name.startswith("pct_coverage_") and isinstance(value, (int, float)):
           if value <= 50:
               # Red to bright yellow (0-50%)
               red = 255
               green = int(195 * (value / 50))  # 195 from #ffc300
               blue = 0
           else:
               # Bright yellow to green (50-100%)
               red = int(255 * (1 - (value - 50) / 50))
               green = int(195 + 60 * ((value - 50) / 50))  # 195 to 255
               blue = 0
           
           return {
               "backgroundColor": f"rgb({red}, {green}, {blue})",
               "color": "black",
               "fontWeight": "bold"
           }
       return {}
   
   return mo.ui.table(
       data=coverage_df,
       style_cell=style_cell,
       pagination=True,
       label="Sub-Region Coverage Heatmap"
   )