import pytest
import polars as pl

from red_list_index.utils import validate_input_dataframe
from red_list_index.utils import validate_categories
from red_list_index.utils import replace_data_deficient_rows


def test_validate_input_dataframe_empty_dataframe():
    df = pl.DataFrame({"weights": ["a", "b", "c"]})
    with pytest.raises(TypeError, match="'weights' column must be of integer type"):
        validate_input_dataframe(df, weight_of_extinct=5)


def test_validate_input_dataframe_missing_weights_column():
    df = pl.DataFrame({"other_column": [1, 2, 3]})
    with pytest.raises(ValueError, match="Missing 'weights' column in DataFrame."):
        validate_input_dataframe(df, weight_of_extinct=5)


def test_validate_input_dataframe_invalid_weights_dtype():
    df = pl.DataFrame({"weights": ["a", "b", "c"]})
    with pytest.raises(TypeError, match="'weights' column must be of integer type"):
        validate_input_dataframe(df, weight_of_extinct=5)


def test_validate_input_dataframe_max_weight_exceeds_limit():
    df = pl.DataFrame({"weights": [1, 2, 6, None]})
    with pytest.raises(ValueError, match="Maximum value in 'weights' column"):
        validate_input_dataframe(df, weight_of_extinct=5)


def test_validate_input_dataframe_negative_weights():
    df = pl.DataFrame({"weights": [-1, 2, 3, None]})
    with pytest.raises(ValueError, match="Minimum value in 'weights' column"):
        validate_input_dataframe(df, weight_of_extinct=5)


def test_validate_input_dataframe_invalid_dataframe_type():
    df = {"weights": [1, 2, 3, None]}
    with pytest.raises(TypeError, match="Expected df to be a polars DataFrame"):
        validate_input_dataframe(df, weight_of_extinct=5)


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
    df = pl.DataFrame({"weights": [1, 2, 3, None, None]})
    result = replace_data_deficient_rows(df, weight_of_extinct=5)
    assert len(result) == 5
    assert all(isinstance(weight, int) for weight in result)


def test_replace_data_deficient_rows_no_null_weights():
    df = pl.DataFrame({"weights": [1, 2, 3, 4, 5]})
    result = replace_data_deficient_rows(df, weight_of_extinct=5)
    assert result == [1, 2, 3, 4, 5]
