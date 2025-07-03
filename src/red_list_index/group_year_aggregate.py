import polars as pl


class GroupYearAggregate:
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
                    pl.lit("Aggregate").alias("taxonomic_group"),
                    pl.col("rli").mean().alias("rli"),
                    pl.col("qn_95").mean().alias("qn_95"),
                    pl.col("qn_05").mean().alias("qn_05"),
                    pl.col("n").mean().round(0).cast(pl.Int64).alias("n"),
                    pl.col("taxonomic_group_sample_sizes")
                    .unique()
                    .sort()
                    .str.join(";")
                    .alias("taxonomic_group_sample_sizes"),
                ]
            )
        )
