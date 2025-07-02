import polars as pl
import numpy as np

from red_list_index.calculate import Calculate


class CalculateGroups:
    """
    CalculateGroups is a class to compute the Red List Index (RLI) values for different groups and years
    based on input data. It provides methods to account for uncertainty due to Data Deficient (DD) species.

    Attributes:
      df (polars.DataFrame): A DataFrame containing the input data for RLI calculations.
      number_of_repetitions (int): The number of repetitions for RLI simulations to account for variability.

    Methods:
      __init__(df, number_of_repetitions=1):
        Initializes the CalculateGroups instance, processes the input DataFrame, and prepares it for RLI calculations.

      _build_global_red_list_indices(df):
        Builds a DataFrame containing Red List Index (RLI) results for each group and year. For each unique group in the
        input DataFrame, this function iterates over all years associated with that group.

        It computes the RLI for each group and year combination by calling `calculate_rli_for`, repeating the calculation
        a specified number of times to account for uncertainty or variability due to any included Data Deficient (DD) species.
        The results for all combinations are collected and returned as a new Polars DataFrame.

      _calculate_rli_for(row_df, number_of_repetitions=1):
        Calculates the RLI and summary statistics for a given group/year subset of data. Performs repeated simulations
        to estimate the RLI, replacing Data Deficient rows and computing the mean, percentiles, and sample sizes.

        It returns the mean RLI, the 95th and 5th percentiles, the number of repetitions, and a dictionary
        of sample sizes for each group in the data.

      _replace_data_deficient_rows(df):
        Replaces Data Deficient (DD) weights in the DataFrame with randomly sampled valid weights.
        As per Butchart et al. (2010), Red List categories (from Least Concern to Extinct) are assigned to all
        Data Deficient species, with a probability proportional to the number of species in non-Data Deficient
        categories for that taxonomic group.
    """

    def __init__(self, df, number_of_repetitions=1):
        self.number_of_repetitions = number_of_repetitions
        self.df = self._build_global_red_list_indices(df)

    def _build_global_red_list_indices(self, df):
        rli_df = []
        for group in df["taxonomic_group"].unique():
            years = df.filter(pl.col("taxonomic_group") == group)["year"].unique()
            for year in years:
                group_rows_by_year = df.filter(
                    (pl.col("year") == year) & (pl.col("taxonomic_group") == group)
                )

                group_year_results = self._calculate_rli_for(
                    group_rows_by_year, self.number_of_repetitions
                )

                rli_df.append(
                    {**{"taxonomic_group": group, "year": year}, **group_year_results}
                )
        return pl.DataFrame(rli_df)

    def _calculate_rli_for(self, row_df, number_of_repetitions=1):
        rlis = []
        for n in range(number_of_repetitions):
            weights_for_group_and_year = self._replace_data_deficient_rows(row_df)
            rli = Calculate(weights_for_group_and_year).red_list_index()
            rlis.append(rli)

        # Note: The numpy .mean() method calculates and returns the arithmetic mean of elements in a NumPy array
        #       as specified in Butchart et al., 2010.
        return {
            "rli": np.mean(rlis),
            "qn_95": np.percentile(rlis, 95),
            "qn_05": np.percentile(rlis, 5),
            "n": number_of_repetitions,
            "taxonomic_group_sample_sizes": self._taxonomic_group_sample_sizes_for(
                row_df
            ),
        }

    def _taxonomic_group_sample_sizes_for(self, row_df):
        counts_df = row_df.select(pl.col("taxonomic_group").value_counts())

        taxonomic_group_counts = counts_df["taxonomic_group"].to_list()
        return f"{taxonomic_group_counts[0]['taxonomic_group']} ({taxonomic_group_counts[0]['count']})"

    def _replace_data_deficient_rows(self, df):
        valid_weights = df.filter(pl.col("weights").is_not_null())["weights"].to_numpy()
        if valid_weights.size == 0:
            raise ValueError("No valid weights found in the DataFrame to sample from.")

        data_deficient_count = df.filter(pl.col("weights").is_null()).height

        random_weights = np.random.choice(
            valid_weights, size=data_deficient_count, replace=False
        )
        return valid_weights.tolist() + random_weights.tolist()
