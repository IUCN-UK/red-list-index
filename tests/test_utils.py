import polars as pl

from red_list_index.utils import interpolate_rli_for_missing_years
from red_list_index.utils import calculate_aggregate_from


def test_interpolate_rli_for_missing_years_valid_input():
    rli_df = pl.DataFrame(
        {
            "year": [2000, 2002, 2004, 2001, 2003],
            "group": ["Bird", "Bird", "Bird", "Mammal", "Mammal"],
            "rli": [0.5, 0.6, 0.7, 0.8, 0.9],
            "qn_05": [0.4, 0.5, 0.6, 0.7, 0.8],
            "qn_95": [0.6, 0.7, 0.8, 0.9, 1.0],
            "n": [1, 2, 3, 4, 5],
            "group_sample_sizes": [
                [{"group": "Bird", "count": 20}],
                [{"group": "Bird", "count": 30}],
                [{"group": "Bird", "count": 40}],
                [{"group": "Mammal", "count": 50}],
                [{"group": "Mammal", "count": 60}],
            ],
        }
    )

    expected = pl.DataFrame(
        {
            "year": [2000, 2001, 2002, 2003, 2004, 2001, 2002, 2003],
            "group": [
                "Bird",
                "Bird",
                "Bird",
                "Bird",
                "Bird",
                "Mammal",
                "Mammal",
                "Mammal",
            ],
            "rli": [
                0.5,
                0.55,
                0.6,
                0.6499999999999999,
                0.7,
                0.8,
                0.8500000000000001,
                0.9,
            ],
            "qn_05": [0.4, 0.45, 0.5, 0.55, 0.6, 0.7, 0.75, 0.8],
            "qn_95": [0.6, 0.6499999999999999, 0.7, 0.75, 0.8, 0.9, 0.95, 1.0],
            "n": [1, 1, 2, 2, 3, 4, 4, 5],
            "group_sample_sizes": [
                [{"group": "Bird", "count": 20}],
                [{"group": "Bird", "count": 20}],
                [{"group": "Bird", "count": 30}],
                [{"group": "Bird", "count": 30}],
                [{"group": "Bird", "count": 40}],
                [{"group": "Mammal", "count": 50}],
                [{"group": "Mammal", "count": 50}],
                [{"group": "Mammal", "count": 60}],
            ],
        }
    )

    result = interpolate_rli_for_missing_years(rli_df).sort(["group", "year"])

    assert result.shape == expected.shape, (
        f"Expected shape {expected.shape}, but got {result.shape}"
    )
    assert result["year"].to_list() == expected["year"].to_list()
    assert result["group"].to_list() == expected["group"].to_list()
    assert result["rli"].to_list() == expected["rli"].to_list()
    assert result["qn_05"].to_list() == expected["qn_05"].to_list()
    assert result["qn_95"].to_list() == expected["qn_95"].to_list()
    assert result["n"].to_list() == expected["n"].to_list()
    assert (
        result["group_sample_sizes"].to_list()
        == expected["group_sample_sizes"].to_list()
    )


def test_calculate_aggregate_from_valid_input():
    df_rli_extrapolated_data = pl.DataFrame(
        {
            "year": [2000, 2000, 2001, 2001, 2002],
            "rli": [0.5, 0.6, 0.7, 0.8, 0.9],
            "group": ["Bird", "Mammal", "Bird", "Mammal", "Bird"],
        }
    )
    expected = pl.DataFrame(
        {
            "year": [2000, 2001, 2002],
            "group": ["Aggregate", "Aggregate", "Aggregate"],
            "rli": [0.55, 0.75, 0.9],
            "qn_95": [None, None, None],
            "qn_05": [None, None, None],
            "n": [None, None, None],
            "group_sample_sizes": [None, None, None],
        }
    )
    result = calculate_aggregate_from(df_rli_extrapolated_data)
    assert result.shape == expected.shape, (
        f"Expected shape {expected.shape}, but got {result.shape}"
    )
    assert result["year"].to_list() == expected["year"].to_list()
    assert result["group"].to_list() == expected["group"].to_list()
    assert result["rli"].to_list() == expected["rli"].to_list()
    assert result["qn_95"].to_list() == expected["qn_95"].to_list()
    assert result["qn_05"].to_list() == expected["qn_05"].to_list()
    assert result["n"].to_list() == expected["n"].to_list()
    assert (
        result["group_sample_sizes"].to_list()
        == expected["group_sample_sizes"].to_list()
    )
