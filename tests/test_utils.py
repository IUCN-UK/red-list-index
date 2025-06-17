import pytest
import polars as pl
import numpy as np
import random

from red_list_index.utils import validate_input_dataframe
from red_list_index.utils import validate_input_dataframe_weights
from red_list_index.utils import validate_categories
from red_list_index.utils import replace_data_deficient_rows
from red_list_index.utils import add_weights_column
from red_list_index.utils import calculate_rli_for
from red_list_index.utils import interpolate_rli_for_missing_years
from red_list_index.utils import calculate_aggregate_from

from red_list_index.constants import RED_LIST_CATEGORY_WEIGHTS


def test_validate_input_dataframe_weights_empty_dataframe():
    df = pl.DataFrame({"weights": ["a", "b", "c"]})
    with pytest.raises(TypeError, match="'weights' column must be of integer type"):
        validate_input_dataframe_weights(df)


def test_validate_input_dataframe_weights_missing_weights_column():
    df = pl.DataFrame({"other_column": [1, 2, 3]})
    with pytest.raises(ValueError, match="Missing 'weights' column in DataFrame."):
        validate_input_dataframe_weights(df)


def test_validate_input_dataframe_weights_invalid_weights_dtype():
    df = pl.DataFrame({"weights": ["a", "b", "c"]})
    with pytest.raises(TypeError, match="'weights' column must be of integer type"):
        validate_input_dataframe_weights(df)


def test_validate_input_dataframe_weights_max_weight_exceeds_limit():
    df = pl.DataFrame({"weights": [1, 2, 6, None]})
    with pytest.raises(ValueError, match="Maximum value in 'weights' column"):
        validate_input_dataframe_weights(df)


def test_validate_input_dataframe_weights_negative_weights():
    df = pl.DataFrame({"weights": [-1, 2, 3, None]})
    with pytest.raises(ValueError, match="Minimum value in 'weights' column"):
        validate_input_dataframe_weights(df)


def test_validate_input_dataframe_weights_invalid_dataframe_type():
    df = {"weights": [1, 2, 3, None]}
    with pytest.raises(TypeError, match="Expected df to be a polars DataFrame"):
        validate_input_dataframe_weights(df)


def test_validate_categories_all_valid():
    categories = ["LC", "EN", "VU"]
    valid_categories = ["LC", "EN", "VU", "CR", "EX"]
    result = validate_categories(categories, valid_categories)
    assert result == []


def test_validate_categories_some_invalid():
    categories = ["LC", "EN", "INVALID"]
    valid_categories = ["LC", "EN", "VU", "CR", "EX"]
    result = validate_categories(categories, valid_categories)
    assert result == ["INVALID"]


def test_validate_categories_all_invalid():
    categories = ["INVALID1", "INVALID2"]
    valid_categories = ["LC", "EN", "VU", "CR", "EX"]
    result = validate_categories(categories, valid_categories)
    assert result == ["INVALID1", "INVALID2"]


def test_validate_categories_empty_categories():
    categories = []
    valid_categories = ["LC", "EN", "VU", "CR", "EX"]
    result = validate_categories(categories, valid_categories)
    assert result == []


def test_validate_categories_empty_valid_categories():
    categories = ["LC", "EN", "VU"]
    valid_categories = []
    result = validate_categories(categories, valid_categories)
    assert result == ["LC", "EN", "VU"]


def test_validate_categories_type_error():
    with pytest.raises(TypeError, match="categories must be a list"):
        validate_categories("not_a_list", ["LC", "EN"])


def test_validate_categories_categories_type_error():
    with pytest.raises(TypeError, match="valid_categories must be a list"):
        validate_categories(["LC", "EN"], "not_a_list")


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


def test_validate_input_dataframe_invalid_type():
    df = {"column1": [1, 2, 3]}
    with pytest.raises(TypeError, match="Expected df to be a polars DataFrame"):
        validate_input_dataframe(df)


def test_validate_input_dataframe_missing_columns():
    df = pl.DataFrame({"column1": [1, 2, 3]})
    with pytest.raises(
        ValueError,
        match=r"Missing required column\(s\): group, id, red_list_category, weights, year",
    ):
        validate_input_dataframe(df)


def test_validate_input_dataframe_invalid_dtype():
    df = pl.DataFrame(
        {
            "id": [1, 2, 3],
            "group": ["A", "B", "C"],
            "red_list_category": ["LC", "EN", "VU"],
            "weights": [1.5, 2.5, 3.5],  # Invalid weights dtype, needs to be Int64
            "year": [2020, 2021, 2022],
        }
    )
    with pytest.raises(
        ValueError,
        match=r"Validation errors:\nColumn 'weights' must be Int64, got Float64",
    ):
        validate_input_dataframe(df)


def test_validate_input_dataframe_null_id_values():
    df = pl.DataFrame(
        {
            "id": [1, None, 3],
            "group": ["A", "B", "C"],
            "red_list_category": ["LC", "EN", "VU"],
            "weights": [1, 2, 3],
            "year": [2020, 2021, 2022],
        }
    )
    with pytest.raises(
        ValueError, match=r"Validation errors:\nColumn 'id' contains 1 null value\(s\)"
    ):
        validate_input_dataframe(df)


def test_validate_input_dataframe_null_group_values():
    df = pl.DataFrame(
        {
            "id": [1, 2, 3],
            "group": ["A", "B", None],
            "red_list_category": ["LC", "EN", "VU"],
            "weights": [1, 2, 3],
            "year": [2020, 2021, 2022],
        }
    )
    with pytest.raises(
        ValueError,
        match=r"Validation errors:\nColumn 'group' contains 1 null value\(s\)",
    ):
        validate_input_dataframe(df)


def test_validate_input_dataframe_null_red_list_category_values():
    df = pl.DataFrame(
        {
            "id": [1, 2, 3],
            "group": ["A", "B", "C"],
            "red_list_category": ["LC", "EN", None],
            "weights": [1, 2, 3],
            "year": [2020, 2021, 2022],
        }
    )
    with pytest.raises(
        ValueError,
        match=r"Validation errors:\nColumn 'red_list_category' contains 1 null value\(s\)",
    ):
        validate_input_dataframe(df)


def test_validate_input_dataframe_null_red_list_year_values():
    df = pl.DataFrame(
        {
            "id": [1, 2, 3],
            "group": ["A", "B", "C"],
            "red_list_category": ["LC", "EN", "EX"],
            "weights": [1, 2, 3],
            "year": [2020, 2021, None],
        }
    )
    with pytest.raises(
        ValueError,
        match=r"Validation errors:\nColumn 'year' contains 1 null value\(s\)",
    ):
        validate_input_dataframe(df)


def test_validate_input_dataframe_invalid_red_list_category_values():
    df = pl.DataFrame(
        {
            "id": [1, 2, 3],
            "group": ["A", "B", "C"],
            "red_list_category": [
                "LC",
                "INVALID",
                "VU",
            ],  # Invalid Red List category value
            "weights": [1, 2, 3],
            "year": [2020, 2021, 2022],
        }
    )
    with pytest.raises(
        ValueError,
        match=r"Validation errors:\nColumn 'red_list_category' has invalid value\(s\) \['INVALID'\]; allowed: dict_keys\(\['LC', 'NT', 'VU', 'EN', 'CR', 'RE', 'CR\(PE\)', 'CR\(PEW\)', 'EW', 'EX', 'DD'\]\)",
    ):
        validate_input_dataframe(df)


def test_add_weights_column_valid_input():
    df = pl.DataFrame(
        {
            "red_list_category": ["LC", "EN", "VU", "CR", "EX"],
        }
    )
    expected_weights = [
        RED_LIST_CATEGORY_WEIGHTS[category] for category in df["red_list_category"]
    ]
    result = add_weights_column(df)
    assert "weights" in result.columns
    assert result["weights"].to_list() == expected_weights


def test_add_weights_column_invalid_category():
    df = pl.DataFrame(
        {
            "red_list_category": ["LC", "INVALID_B", "INVALID_A", "VU"],
        }
    )
    with pytest.raises(
        ValueError,
        match=r"Invalid value found in 'red_list_category' column: \['INVALID_A', 'INVALID_B'\]",
    ):
        add_weights_column(df)


def test_add_weights_column_empty_dataframe():
    df = pl.DataFrame({"red_list_category": []})
    with pytest.raises(
        ValueError, match="Input DataFrame has an empty 'red_list_category' column"
    ):
        add_weights_column(df)


def test_add_weights_column_missing_red_list_category_column():
    df = pl.DataFrame({"other_column": [1, 2, 3]})
    with pytest.raises(
        ValueError, match="Input DataFrame must contain a 'red_list_category' column"
    ):
        add_weights_column(df)


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
