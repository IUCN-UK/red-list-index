import numpy as np
import polars as pl
import seaborn as sns
import matplotlib.pyplot as plt


# def plot_global_rli(rli_df, filename="global_rli.png"):
#     """
#     Plots the global RLI by group over time using Seaborn and saves the plot to a file.

#     Args:
#         rli_df (pl.DataFrame): Input Polars DataFrame with columns 'group', 'year', 'rli', 'qn_05', 'qn_95'.
#         filename (str): The filename to save the plot (default: 'global_rli.png').
#     """
#     # Convert to pandas for Seaborn/Matplotlib
#     pdf = rli_df.to_pandas()

#     # Set Seaborn style
#     sns.set(style="whitegrid")

#     plt.figure(figsize=(8, 5))

#     # Plot each group
#     groups = pdf["group"].unique()
#     for group in groups:
#         sub = pdf[pdf["group"] == group]
#         plt.plot(sub["year"], sub["rli"], label=group, lw=0.5)  # No marker
#         plt.fill_between(sub["year"], sub["qn_05"], sub["qn_95"], alpha=0.2)

#     plt.xlabel("Year")
#     plt.ylabel("RLI")
#     plt.title("RLI by Group Over Time")
#     plt.legend(loc="center left", bbox_to_anchor=(1, 0.5))
#     plt.tight_layout()
#     plt.savefig(filename, dpi=300)
#     plt.close()  # Close the figure to avoid display in notebooks


def interpolate_rli_for_missing_years(rli_df):
    """
    Fills in missing years for each group in the RLI DataFrame by linearly interpolating RLI values and associated columns.

    For each unique group in the input DataFrame, this function:
      - Determines the full range of years from the group's minimum to maximum year.
      - Joins the group's data to this full year range, introducing NaNs where data is missing.
      - Linearly interpolates missing values for 'rli', 'qn_05', and 'qn_95' columns.
      - Fills and interpolates the 'n' and 'group_sample_sizes' columns as appropriate.
      - Concatenates the interpolated results for all groups into a single DataFrame.

    Args:
        rli_df (pl.DataFrame): Input Polars DataFrame containing at least 'group', 'year', 'rli', 'qn_05', 'qn_95', 'n', and 'group_sample_sizes' columns.

    Returns:
        pl.DataFrame: A DataFrame with missing years filled and interpolated values for each group.
    """

    unique_groups_list = rli_df["group"].unique()

    df_list = []
    for group in unique_groups_list:
        group_rli_df = rli_df.filter(pl.col("group") == group)

        all_years = pl.DataFrame(
            {
                "year": list(
                    range(group_rli_df["year"].min(), group_rli_df["year"].max() + 1)
                )
            }
        )

        df_full = all_years.join(group_rli_df, on="year", how="left")

        # Fill group and interpolate rli
        df_full = df_full.with_columns(
            [
                pl.lit(group).alias("group"),
                pl.col("rli").interpolate(),
                pl.col("qn_05").interpolate(),
                pl.col("qn_95").interpolate(),
                pl.col("n")
                .fill_null(strategy="forward")
                .interpolate()
                .cast(pl.Int64)
                .alias("n"),
                pl.col("group_sample_sizes")
                .fill_null(strategy="forward")
                .interpolate()
                .alias("group_sample_sizes"),
            ]
        )
        df_list.append(df_full)
    return pl.concat(df_list)


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
