import numpy as np
import polars as pl


class GroupYearExtrapolation:
    def extrapolate_trends_for(trends_df):
        """
        Extrapolates trends for each group in the given DataFrame by fitting a linear model
        and extending the trend across the full range of years.

        Parameters:
            trends_df (pl.DataFrame): A Polars DataFrame containing columns:
                - "year" (int): The year of the trend data.
                - "rli" (float): The Red List Index value for the corresponding year.
                - "taxonomic_group" (str): The group identifier.

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
                "taxonomic_group": pl.Series([], dtype=pl.Utf8),
            }
        )

        groups = trends_df.select("taxonomic_group").unique()["taxonomic_group"].to_list()

        # Get full year range across all groups
        all_years = trends_df.select(["year"]).unique().sort("year")

        for group in groups:
            group_df = trends_df.filter(pl.col("taxonomic_group") == group).select(
                ["year", "rli"]
            )

            slope, intercept = np.polyfit(
                group_df["year"].to_numpy(), group_df["rli"].to_numpy(), deg=1
            )

            full_group_df = all_years.with_columns(
                [
                    (pl.col("year") * slope + intercept)
                    .clip(lower_bound=0.0, upper_bound=1.0)
                    .alias("rli"),
                    pl.lit(group).alias("taxonomic_group"),
                ]
            )

            df_full_extrapolated = df_full_extrapolated.vstack(full_group_df)

        return df_full_extrapolated
