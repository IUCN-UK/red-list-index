import numpy as np
import polars as pl


def extrapolate_trends_for(trends_df):
    """
    Extrapolates trends for each group in the given DataFrame by fitting a linear model
    and extending the trend across the full range of years.

    Parameters:
        trends_df (pl.DataFrame): A Polars DataFrame containing columns:
            - "year" (int): The year of the trend data.
            - "rli" (float): The Red List Index value for the corresponding year.
            - "group" (str): The group identifier.

    Returns:
        pl.DataFrame: A Polars DataFrame containing extrapolated trends for all groups
        across the full range of years. The resulting DataFrame includes columns:
            - "year" (int): The year of the extrapolated trend data.
            - "rli" (float): The extrapolated Red List Index value, clipped between 0.0 and 1.0.
            - "group" (str): The group identifier.
    """
    df_full_extrapolated = pl.DataFrame(
        {
            "year": pl.Series([], dtype=pl.Int64),
            "rli": pl.Series([], dtype=pl.Float64),
            "group": pl.Series([], dtype=pl.Utf8),
        }
    )

    groups = trends_df.select("group").unique()["group"].to_list()

    # Get full year range across all groups
    all_years = trends_df.select(["year"]).unique().sort("year")

    for group in groups:
        group_df = trends_df.filter(pl.col("group") == group).select(["year", "rli"])

        slope, intercept = np.polyfit(
            group_df["year"].to_numpy(), group_df["rli"].to_numpy(), deg=1
        )

        full_group_df = all_years.with_columns(
            [
                (pl.col("year") * slope + intercept)
                .clip(lower_bound=0.0, upper_bound=1.0)
                .alias("rli"),
                pl.lit(group).alias("group"),
            ]
        )

        df_full_extrapolated = df_full_extrapolated.vstack(full_group_df)

    return df_full_extrapolated


def calculate_aggregate_for(df_rli_data):
    """
    Calculate the aggregate value for the given Red List Index (RLI) data.

    This function first extrapolates trends from the provided RLI data using
    the `extrapolate_trends_for` function, and then calculates the aggregate
    value from the extrapolated data using the `calculate_aggregate_from` function.

    Args:
        df_rli_data (pd.DataFrame): A DataFrame containing Red List Index data.

    Returns:
        pd.DataFrame: A DataFrame containing the aggregated results based on
        the extrapolated trends.
    """
    df_rli_extrapolated = extrapolate_trends_for(df_rli_data)
    return calculate_aggregate_from(df_rli_extrapolated)


def calculate_aggregate_from(df_rli_extrapolated_data):
    """
    Aggregates Red List Index (RLI) data for each group across the full range of years.

    This function takes a DataFrame containing extrapolated Red List Index (RLI) data,
    sorts it by the "year" column, groups the data by "year", and computes aggregate
    statistics for each year. The resulting DataFrame includes the following columns:
    - "group": A constant value "Aggregate" for all rows.
    - "rli": The mean value of the "rli" column for each year.
    - "qn_95": A placeholder column with None values.
    - "qn_05": A placeholder column with None values.
    - "n": A placeholder column with None values.
    - "group_sample_sizes": A placeholder column with None values.
    Columns with None values are added so the dataframe remains consistent and joinable to
    the dataframe containing comprehensive group RLI's.

    Args:
        df_rli_extrapolated_data (polars.DataFrame): A DataFrame containing extrapolated
            RLI data with at least a "year" column and an "rli" column.

    Returns:
        polars.DataFrame: A DataFrame with aggregated data grouped by year.
    """

    # Note: Polars .mean() method calculates and returns the arithmetic mean of elements
    #       as specified in Butchart et al., 2010.
    return (
        df_rli_extrapolated_data.sort("year")
        .group_by("year")
        .agg(
            [
                pl.lit("Aggregate").alias("group"),
                pl.mean("rli").alias("rli"),
                pl.lit(None).alias("qn_95"),
                pl.lit(None).alias("qn_05"),
                pl.lit(None).alias("n"),
                pl.lit(None).alias("group_sample_sizes"),
            ]
        )
    )
