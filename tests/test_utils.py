import pytest
import polars as pl
import random

from red_list_index.utils import validate_input_dataframe
from red_list_index.utils import validate_input_dataframe_weights
from red_list_index.utils import validate_categories
from red_list_index.utils import replace_data_deficient_rows
from red_list_index.utils import add_weights_column
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
