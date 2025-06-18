import polars as pl

from red_list_index.calculate_groups import CalculateGroups
import pytest
import numpy as np


def test_calculate_groups_initialization():
    # Create a sample DataFrame
    data = {
        "id": [1, 2, 3, 4],
        "red_list_category": ["CR", "CR(PE)", "CR", "CR(PE)"],
        "year": [2020, 2020, 2021, 2021],
        "group": ["Mammals", "Mammals", "Birds", "Birds"],
        "weights": [4, 5, 4, 5],
    }
    df = pl.DataFrame(data)

    # Initialize CalculateGroups
    calculate_groups = CalculateGroups(df, number_of_repetitions=1)

    # Assert the DataFrame is built correctly
    assert isinstance(calculate_groups.df, pl.DataFrame)

    expected_df = pl.DataFrame(
        [
            {
                "group": "Birds",
                "year": 2021,
                "rli": 0.09999999999999998,
                "qn_95": 0.09999999999999998,
                "qn_05": 0.09999999999999998,
                "n": 1,
                "group_sample_sizes": {"group": "Birds", "count": 2},
            },
            {
                "group": "Mammals",
                "year": 2020,
                "rli": 0.09999999999999998,
                "qn_95": 0.09999999999999998,
                "qn_05": 0.09999999999999998,
                "n": 1,
                "group_sample_sizes": {"group": "Mammals", "count": 2},
            },
        ]
    )

    assert (
        calculate_groups.df.sort("group").to_dicts()
        == expected_df.sort("group").to_dicts()
    ), "DataFrame does not match expected structure"

    # def test_calculate_groups_initialization():
    #     # Create a sample DataFrame
    #     data = {
    #         "id": [1, 2, 3, 4],
    #         "red_list_category": ["CR", "CR(PE)", "CR", "CR(PE)"],
    #         "year": [2020, 2020, 2021, 2021],
    #         "group": ["Mammals", "Mammals", "Birds", "Birds"],
    #         "weights": [4, 5, 4, 5],
    #     }
    #     df = pl.DataFrame(data)

    #     # Initialize CalculateGroups
    #     calculate_groups = CalculateGroups(df, number_of_repetitions=1)

    #     # Assert the DataFrame is built correctly
    #     assert isinstance(calculate_groups.df, pl.DataFrame)

    #     expected_df = pl.DataFrame(
    #         [
    #             {
    #                 "group": "Birds",
    #                 "year": 2021,
    #                 "rli": 0.09999999999999998,
    #                 "qn_95": 0.09999999999999998,
    #                 "qn_05": 0.09999999999999998,
    #                 "n": 1,
    #                 "group_sample_sizes": {"group": "Birds", "count": 2},
    #             },
    #             {
    #                 "group": "Mammals",
    #                 "year": 2020,
    #                 "rli": 0.09999999999999998,
    #                 "qn_95": 0.09999999999999998,
    #                 "qn_05": 0.09999999999999998,
    #                 "n": 1,
    #                 "group_sample_sizes": {"group": "Mammals", "count": 2},
    #             },
    #         ]
    #     )

    #     assert (
    #         calculate_groups.df.sort("group").to_dicts()
    #         == expected_df.sort("group").to_dicts()
    #     ), "DataFrame does not match expected structure"


def test_replace_data_deficient_rows():
    # Create a sample DataFrame with some Data Deficient rows
    data = {
        "id": [1, 2, 3, 4],
        "red_list_category": ["CR", "DD", "CR", "DD"],
        "year": [2020, 2020, 2021, 2021],
        "group": ["Mammals", "Mammals", "Birds", "Birds"],
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
