import polars as pl
import numpy as np

# from .constants import RED_LIST_CATEGORY_WEIGHTS
from red_list_index.calculate import Calculate


class CalculateGroups:
    def __init__(self, df, number_of_repetitions=1):
        self.number_of_repetitions = number_of_repetitions
        self.df = self._build_global_red_list_indices(df)

    def _build_global_red_list_indices(self, df):
        """
        Builds a DataFrame containing Red List Index (RLI) results for each group and year.
        For each unique group in the input DataFrame, this function iterates over all years associated with that group.
        It computes RLI statistics for each (group, year) combination by calling `calculate_rli_for`, repeating the calculation
        a specified number of times to account for uncertainty or variability due to included Data Deficient (DD) species.
        The results for all combinations are collected and returned as a new Polars DataFrame.
        Args:
            df (pl.DataFrame): Input Polars DataFrame containing "id","red_list_category","year", and "group" columns.
            number_of_repetitions (int, optional): Number of repetitions for the RLI simulation in each group/year. Default is 1.
        Returns:
            pl.DataFrame: A DataFrame where each row corresponds to a group/year with RLI statistics and sample sizes.
        """
        rli_df = []
        for group in df["group"].unique():
            years = df.filter(pl.col("group") == group)["year"].unique()
            for year in years:
                group_rows_by_year = df.filter(
                    (pl.col("year") == year) & (pl.col("group") == group)
                )
                group_year_results = self._calculate_rli_for(
                    group_rows_by_year, self.number_of_repetitions
                )

                rli_df.append({**{"group": group, "year": year}, **group_year_results})
        return pl.DataFrame(rli_df)

    def _calculate_rli_for(self, row_df, number_of_repetitions=1):
        """
        Calculates the Red List Index (RLI) and summary statistics for a given group/year subset of data.

        This function performs repeated simulations to estimate the RLI using the provided data subset.
        For each repetition, it replaces data-deficient rows, computes the RLI, and collects the results.
        It returns the mean RLI, the 95th and 5th percentiles, the number of repetitions, and a dictionary
        of sample sizes for each group in the data.

        Args:
            row_df (pl.DataFrame): A Polars DataFrame containing the subset of data for a particular group/year.
            number_of_repetitions (int, optional): The number of repetitions for the simulation. Default is 1.

        Returns:
            dict: {
                "rli": Mean RLI across repetitions,
                "qn_95": 95th percentile of RLI,
                "qn_05": 5th percentile of RLI,
                "n": Number of repetitions,
                "group_sample_sizes": Dictionary of sample sizes per group
            }
        """
        rlis = []
        for n in range(number_of_repetitions):
            weights_for_group_and_year = self._replace_data_deficient_rows(row_df)
            rli = Calculate(weights_for_group_and_year).red_list_index()
            rlis.append(rli)
        counts_df = row_df.select(pl.col("group").value_counts())
        dicts = counts_df["group"].to_list()
        group_sample_sizes = {k: v for d in dicts for k, v in d.items()}
        # Note: The numpy .mean() method calculates and returns the arithmetic mean of elements in a NumPy array
        #       as specified in Butchart et al., 2010.
        return {
            "rli": np.mean(rlis),
            "qn_95": np.percentile(rlis, 95),
            "qn_05": np.percentile(rlis, 5),
            "n": number_of_repetitions,
            "group_sample_sizes": group_sample_sizes,
        }

    def _replace_data_deficient_rows(self, df):
        """
        Replace Data Deficient (DD) weights in the DataFrame with randomly sampled valid weights.
        Data Deficiency: Red List categories (from Least Concern to Extinct) are assigned to all
        Data Deficient species, with a probability proportional to the number of species in non-Data
        Deficient categories for that taxonomic group (Butchart et al., 2010).
        """
        valid_weights = df.filter(pl.col("weights").is_not_null())["weights"].to_numpy()
        if valid_weights.size == 0:
            raise ValueError("No valid weights found in the DataFrame to sample from.")

        data_deficient_count = df.filter(pl.col("weights").is_null()).height

        random_weights = np.random.choice(
            valid_weights, size=data_deficient_count, replace=False
        )
        return valid_weights.tolist() + random_weights.tolist()
