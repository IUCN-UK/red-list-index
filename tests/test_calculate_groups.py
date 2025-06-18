import polars as pl

from red_list_index.calculate_groups import CalculateGroups


def test_calculate_groups_initialization():
    # Create a sample DataFrame
    data = {
        "id": [1, 2, 3, 4],
        "red_list_category": ["LC", "VU", "EN", "DD"],
        "year": [2020, 2020, 2021, 2021],
        "group": ["Mammals", "Mammals", "Birds", "Birds"],
        "weights": [1.0, 0.8, 0.6, None],
    }
    df = pl.DataFrame(data)

    # Initialize CalculateGroups
    calculate_groups = CalculateGroups(df, number_of_repetitions=2)

    # Assert the DataFrame is built correctly
    assert isinstance(calculate_groups.df, pl.DataFrame)
    required_columns = [
        "group",
        "year",
        "rli",
        "qn_95",
        "qn_05",
        "n",
        "group_sample_sizes",
    ]
    assert set(required_columns).issubset(calculate_groups.df.columns), (
        f"Missing columns: {set(required_columns) - set(calculate_groups.df.columns)}"
    )
