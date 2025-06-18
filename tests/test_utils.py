import pytest
import polars as pl
import numpy as np
import random

from red_list_index.utils import replace_data_deficient_rows
from red_list_index.utils import calculate_rli_for
from red_list_index.utils import interpolate_rli_for_missing_years
from red_list_index.utils import calculate_aggregate_from

from red_list_index.constants import RED_LIST_CATEGORY_WEIGHTS


def test_replace_data_deficient_rows_valid_input():
    # This test checks if the function correctly replaces all None values with
    # values sampled from non-null values in the 'weights' column
    valid_weights = random.sample([0, 1, 2, 3, 4, 5], 4)
    data_deficient_weights = [None, None]

    df = pl.DataFrame({"weights": valid_weights + data_deficient_weights})
    result = replace_data_deficient_rows(df)
    assert len(result) == 6
    assert all(isinstance(weight, int) for weight in result)
    assert all(x in result for x in valid_weights), (
        "Not all values in results are present in the initial valid_weights"
    )


def test_replace_data_deficient_rows_no_null_weights():
    df = pl.DataFrame({"weights": [1, 2, 3, 4, 5]})
    result = replace_data_deficient_rows(df)
    assert result == [1, 2, 3, 4, 5]


def test_calculate_rli_for_valid_input():
    df = pl.DataFrame({"group": ["Bird", "Bird", "Bird"], "weights": [1, 2, 1]})

    result = calculate_rli_for(df, number_of_repetitions=3)

    assert result == {
        "rli": np.float64(0.7333333333333334),
        "qn_95": np.float64(0.7333333333333334),
        "qn_05": np.float64(0.7333333333333334),
        "n": 3,
        "group_sample_sizes": {"group": "Bird", "count": 3},
    }


def test_calculate_rli_for_valid_input_with_null_dd_values():
    df = pl.DataFrame({"group": ["Bird", "Bird", "Bird"], "weights": [1, 2, None]})

    result = calculate_rli_for(df, number_of_repetitions=2)

    assert result in [
        {
            "rli": np.float64(0.7333333333333334),
            "qn_95": np.float64(0.7333333333333334),
            "qn_05": np.float64(0.7333333333333334),
            "n": 2,
            "group_sample_sizes": {"group": "Bird", "count": 3},
        },
        {
            "rli": np.float64(0.6666666666666667),
            "qn_95": np.float64(0.6666666666666667),
            "qn_05": np.float64(0.6666666666666667),
            "n": 2,
            "group_sample_sizes": {"group": "Bird", "count": 3},
        },
        {
            "rli": np.float64(0.7000000000000001),
            "qn_95": np.float64(0.7300000000000001),
            "qn_05": np.float64(0.67),
            "n": 2,
            "group_sample_sizes": {"group": "Bird", "count": 3},
        },
    ]


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
