import polars as pl


class YearInterpolation:
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

    def interpolate_rli_for_missing_years(rli_df):
        unique_groups_list = rli_df["group"].unique()
        df_list = []
        for group in unique_groups_list:
            group_rli_df = rli_df.filter(pl.col("group") == group)

            all_years = pl.DataFrame(
                {
                    "year": list(
                        range(
                            group_rli_df["year"].min(), group_rli_df["year"].max() + 1
                        )
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
