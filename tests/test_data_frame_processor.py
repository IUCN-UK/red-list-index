import pytest
import polars as pl
from red_list_index.data_frame_processor import DataFrameProcessor
from red_list_index.constants import RED_LIST_CATEGORY_WEIGHTS
from tempfile import NamedTemporaryFile


def create_test_csv(data):
    """Helper function to create a temporary CSV file for testing."""
    temp_file = NamedTemporaryFile(delete=False, suffix=".csv")
    df = pl.DataFrame(data)
    df.write_csv(temp_file.name)
    return temp_file.name


def test_valid_data_frame():
    """Test that a valid DataFrame is processed correctly."""
    data = {
        "id": [1, 2, 3],
        "red_list_category": ["LC", "VU", "EN"],
        "year": [2020, 2021, 2022],
        "group": ["Mammals", "Birds", "Reptiles"],
    }
    input_file = create_test_csv(data)
    processor = DataFrameProcessor(input_file)

    assert "weights" in processor.df.columns
    expected_weights = [
        RED_LIST_CATEGORY_WEIGHTS[cat] for cat in data["red_list_category"]
    ]
    assert processor.df["weights"].to_list() == expected_weights


def test_missing_required_columns():
    """Test that missing required columns raise a ValueError."""
    data = {
        "id": [1, 2, 3],
        "year": [2020, 2021, 2022],
        "group": ["Mammals", "Birds", "Reptiles"],
    }
    input_file = create_test_csv(data)

    with pytest.raises(
        ValueError, match=r"Missing required column\(s\): red_list_category"
    ):
        DataFrameProcessor(input_file)


def test_invalid_schema():
    """Test that invalid column types raise a ValueError."""
    data = {
        "id": ["1a", "2a", "3a"],  # Invalid type (should be Int64)
        "red_list_category": ["LC", "VU", "EN"],
        "year": [2020, 2021, 2022],
        "group": ["Mammals", "Birds", "Reptiles"],
    }
    input_file = create_test_csv(data)

    with pytest.raises(
        ValueError, match=r"Validation errors:\nColumn 'id' must be Int64, got String"
    ):
        DataFrameProcessor(input_file)


def test_null_values():
    """Test that null values in required columns raise a ValueError."""
    data = {
        "id": [1, None, 3],
        "red_list_category": ["LC", "VU", "EN"],
        "year": [2020, 2021, 2022],
        "group": ["Mammals", "Birds", "Reptiles"],
    }
    input_file = create_test_csv(data)

    with pytest.raises(
        ValueError, match=r"Validation errors:\nColumn 'id' contains 1 null value\(s\)"
    ):
        DataFrameProcessor(input_file)


def test_invalid_red_list_category():
    """Test that invalid red_list_category values raise a ValueError."""
    data = {
        "id": [1, 2, 3],
        "red_list_category": ["INVALID", "VU", "EN"],
        "year": [2020, 2021, 2022],
        "group": ["Mammals", "Birds", "Reptiles"],
    }
    input_file = create_test_csv(data)

    with pytest.raises(
        ValueError,
        match=r"Validation errors:\nColumn 'red_list_category' has invalid value\(s\) \['INVALID'\]; allowed: dict_keys\(\['LC', 'NT', 'VU', 'EN', 'CR', 'RE', 'CR\(PE\)', 'CR\(PEW\)', 'EW', 'EX', 'DD'\]\)",
    ):
        DataFrameProcessor(input_file)


def test_polars_shape_error():
    """Test that an empty red_list_category column raises a ShapeError."""
    data = {
        "id": [1, 2, 3],
        "red_list_category": [],
        "year": [2020, 2021, 2022],
        "group": ["Mammals", "Birds", "Reptiles"],
    }
    with pytest.raises(
        pl.exceptions.ShapeError,
        match=r"could not create a new DataFrame: height of column 'red_list_category' \(0\) does not match height of column 'id' \(3\)",
    ):
        pl.DataFrame(data)
