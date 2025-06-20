import polars as pl
import pytest
import random

from red_list_index.calculate_groups import CalculateGroups


def create_sample_dataframe():
    # Create a sample DataFrame
    data = {
        "sis_taxon_id": [1, 2, 3, 4],
        "red_list_category": ["CR", "CR(PE)", "CR", "CR(PE)"],
        "year": [2020, 2020, 2021, 2021],
        "taxonomic_group": ["Mammals", "Mammals", "Birds", "Birds"],
        "weights": [4, 5, 4, 5],
    }
    return pl.DataFrame(data)


def test_calculate_groups_initialization():
    df = create_sample_dataframe()

    # Initialize CalculateGroups
    calculated_groups = CalculateGroups(df, number_of_repetitions=1)

    # Assert the DataFrame is built correctly
    assert isinstance(calculated_groups.df, pl.DataFrame)

    expected_df = pl.DataFrame(
        [
            {
                "taxonomic_group": "Birds",
                "year": 2021,
                "rli": 0.09999999999999998,
                "qn_95": 0.09999999999999998,
                "qn_05": 0.09999999999999998,
                "n": 1,
                "taxonomic_group_sample_sizes": {"taxonomic_group": "Birds", "count": 2},
            },
            {
                "taxonomic_group": "Mammals",
                "year": 2020,
                "rli": 0.09999999999999998,
                "qn_95": 0.09999999999999998,
                "qn_05": 0.09999999999999998,
                "n": 1,
                "taxonomic_group_sample_sizes": {"taxonomic_group": "Mammals", "count": 2},
            },
        ]
    )

    assert (
        calculated_groups.df.sort("taxonomic_group").to_dicts()
        == expected_df.sort("taxonomic_group").to_dicts()
    ), "DataFrame does not match expected structure"


def test_calculate_groups_initialization_with_10_repetitions():
    df = create_sample_dataframe()

    random_number_of_repetitions = random.choice([2, 4, 6, 8, 10])

    # Initialize CalculateGroups
    calculated_groups = CalculateGroups(
        df, number_of_repetitions=random_number_of_repetitions
    )

    # Assert the DataFrame is built correctly
    assert isinstance(calculated_groups.df, pl.DataFrame)

    unique_n_list = calculated_groups.df["n"].unique().to_list()

    assert unique_n_list == [random_number_of_repetitions], (
        "DataFrame n column does not match expected number of repetitions"
    )


def test_replace_data_deficient_rows():
    # Create a sample DataFrame with some Data Deficient rows
    data = {
        "sis_taxon_id": [1, 2, 3, 4],
        "red_list_category": ["CR", "DD", "CR", "DD"],
        "year": [2020, 2020, 2021, 2021],
        "taxonomic_group": ["Mammals", "Mammals", "Birds", "Birds"],
        "weights": [2, None, 5, None],
    }
    df = pl.DataFrame(data)

    # Initialize CalculateGroups
    calculate_groups = CalculateGroups(df, number_of_repetitions=1)

    # Call _replace_data_deficient_rows
    replaced_weights = calculate_groups._replace_data_deficient_rows(df)

    # Assert that the replaced weights contain no None values
    assert all(weight is not None for weight in replaced_weights), (
        "Weights contain None values"
    )

    # Assert that the replaced weights include valid weights and random samples
    valid_weights = df.filter(pl.col("weights").is_not_null())["weights"].to_numpy()
    assert all(
        weight in valid_weights for weight in replaced_weights[: len(valid_weights)]
    ), "Valid weights are missing"

    # Assert that the length of replaced weights matches the original DataFrame's weights
    assert len(replaced_weights) == len(df["weights"]), (
        "Replaced weights length mismatch"
    )

    # Assert that ValueError is raised when no valid weights are present
    empty_df = pl.DataFrame({"weights": [None, None]})
    with pytest.raises(
        ValueError, match="No valid weights found in the DataFrame to sample from."
    ):
        calculate_groups._replace_data_deficient_rows(empty_df)
