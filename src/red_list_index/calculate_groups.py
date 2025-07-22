import polars as pl
import numpy as np
import asyncio

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
        # Run the async method synchronously at init
        self.df = asyncio.run(self._build_global_red_list_indices(df))

    async def _build_group_year_rli_async(self, df, group, year):
        # If _build_group_year_rli is synchronous, wrap it in a thread pool
        # Otherwise, just await if you have an async implementation
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None, self._build_group_year_rli, df, group, year
        )

    async def _build_global_red_list_indices(self, df):
        rli_df = []
        tasks = []
        for group in df["taxonomic_group"].unique():
            years = df.filter(pl.col("taxonomic_group") == group)["year"].unique()
            for year in years:
                # Schedule async execution
                tasks.append(self._build_group_year_rli_async(df, group, year))
        results = await asyncio.gather(*tasks)
        return pl.DataFrame(results)

    def _build_group_year_rli(self, df, group, year):
        group_rows_by_year = df.filter(
            (pl.col("year") == year) & (pl.col("taxonomic_group") == group)
        )
        group_year_results = self._calculate_rli_for(
            group_rows_by_year, self.number_of_repetitions
        )
        return {**{"taxonomic_group": group, "year": year}, **group_year_results}

    def _calculate_rli_for(self, row_df, number_of_repetitions=1):
        """Calculate the Red List Index (RLI) and related statistics for the given DataFrame."""
        rli_collection = self._generate_rli_collection(row_df, number_of_repetitions)
        return self._summarize_rli_collection(
            rli_collection, number_of_repetitions, row_df
        )

    def _generate_rli_collection(self, row_df, number_of_repetitions):
        """Generate a list of RLI values by bootstrapping."""
        return [self._single_rli(row_df) for _ in range(number_of_repetitions)]

    def _summarize_rli_collection(self, rli_collection, number_of_repetitions, row_df):
        """Summarize the RLI collection with statistics and metadata."""
        # The RLI (Red List Index) summary statistics are calculated as follows:
        # - The mean RLI is computed using numpy's np.mean, which calculates the arithmetic mean as specified in Butchart et al., 2010.
        # - The 95th and 5th percentiles (qn_95 and qn_05) are computed using np.percentile to provide uncertainty intervals.
        # - The total number of bootstrap repetitions (n) is recorded.
        # - The sample sizes for each taxonomic group are included for reference.

        return {
            "rli": np.mean(rli_collection),
            "qn_95": np.percentile(rli_collection, 95),
            "qn_05": np.percentile(rli_collection, 5),
            "n": number_of_repetitions,
            "taxonomic_group_sample_sizes": self._taxonomic_group_sample_sizes_for(
                row_df
            ),
        }

    def _single_rli(self, row_df):
        """Calculate a single RLI value for the given DataFrame, replacing data deficient rows as needed."""
        weights_for_group_and_year = self._replace_data_deficient_rows(row_df)

        return Calculate(weights_for_group_and_year).red_list_index()

    def _taxonomic_group_sample_sizes_for(self, row_df):
        """Get the count of occurrences for each taxonomic_group in the input DataFrame."""
        counts_df = row_df.select(pl.col("taxonomic_group").value_counts())

        taxonomic_group_counts = counts_df["taxonomic_group"].to_list()
        return f"{taxonomic_group_counts[0]['taxonomic_group']} ({taxonomic_group_counts[0]['count']})"

    def _replace_data_deficient_rows(self, df):
        """Replace null weights in DataFrame with random samples from valid weights."""
        valid_weights = self._get_valid_weights(df)
        data_deficient_count = self._get_data_deficient_count(df)

        random_weights = self._sample_random_weights(
            valid_weights, data_deficient_count
        )
        return valid_weights.tolist() + random_weights.tolist()

    def _get_valid_weights(self, df):
        """Return all valid (non-null) weights as a numpy array."""
        valid_weights = df.filter(pl.col("weights").is_not_null())["weights"].to_numpy()
        if valid_weights.size == 0:
            raise ValueError("No valid weights found in the DataFrame to sample from.")
        return valid_weights

    def _get_data_deficient_count(self, df):
        """Return the number of rows with null weights."""
        return df.filter(pl.col("weights").is_null()).height

    def _sample_random_weights(self, valid_weights, count):
        """Randomly sample 'count' weights from valid_weights (without replacement)."""
        if count == 0:
            return np.array([])
        return np.random.choice(valid_weights, size=count, replace=False)
