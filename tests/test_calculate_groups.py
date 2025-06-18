import polars as pl

from red_list_index.calculate_groups import CalculateGroups


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
