import pytest

from red_list_index.utils import validate_categories


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


def test_categories_type_error():
    with pytest.raises(TypeError, match="categories must be a list"):
        validate_categories("not_a_list", ["LC", "EN"])


def test_valid_categories_type_error():
    with pytest.raises(TypeError, match="valid_categories must be a list"):
        validate_categories(["LC", "EN"], "not_a_list")
