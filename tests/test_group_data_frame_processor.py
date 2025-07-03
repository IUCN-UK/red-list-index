import csv
import pytest
from red_list_index.group_data_frame_processor import GroupDataFrameProcessor
from tempfile import NamedTemporaryFile
from red_list_index.constants import RED_LIST_CATEGORY_WEIGHTS


def create_test_csv(data):
    """Helper function to create a temporary CSV file for testing."""
    temp_file = NamedTemporaryFile(delete=False, suffix=".csv", mode="w", newline="")
    writer = csv.DictWriter(temp_file, fieldnames=list(data.keys()))
    writer.writeheader()
    # Convert column-wise dict of lists to row-wise dicts
    rows = zip(*data.values())
    for row in rows:
        writer.writerow(dict(zip(data.keys(), row)))
    temp_file.close()
    return temp_file.name


def test_valid_data_frame():
    """Test that a valid DataFrame is processed correctly."""
    data = {
        "sis_taxon_id": [1, 2, 3],
        "red_list_category": ["LC", "VU", "EN"],
        "year": [2020, 2021, 2022],
        "taxonomic_group": ["Mammals", "Birds", "Reptiles"],
    }
    input_file = create_test_csv(data)
    processor = GroupDataFrameProcessor(input_file)

    assert "weights" in processor.df.columns
    expected_weights = [
        RED_LIST_CATEGORY_WEIGHTS[cat] for cat in data["red_list_category"]
    ]
    assert processor.df["weights"].to_list() == expected_weights


def test_missing_required_columns():
    """Test that missing required columns raise a ValueError."""
    data = {
        "sis_taxon_id": [1, 2, 3],
        "year": [2020, 2021, 2022],
        "taxonomic_group": ["Mammals", "Birds", "Reptiles"],
    }
    input_file = create_test_csv(data)

    with pytest.raises(
        ValueError, match=r"Missing required column\(s\): red_list_category"
    ):
        GroupDataFrameProcessor(input_file)


def test_invalid_schema():
    """Test that invalid column types raise a ValueError."""
    data = {
        "sis_taxon_id": ["1a", "2a", "3a"],  # Invalid type (should be Int64)
        "red_list_category": ["LC", "VU", "EN"],
        "year": [2020, 2021, 2022],
        "taxonomic_group": ["Mammals", "Birds", "Reptiles"],
    }
    input_file = create_test_csv(data)

    with pytest.raises(
        ValueError,
        match=r"Validation errors:\nColumn 'sis_taxon_id' must be Int64, got String",
    ):
        GroupDataFrameProcessor(input_file)


def test_null_values():
    """Test that null values in required columns raise a ValueError."""
    data = {
        "sis_taxon_id": [1, None, 3],
        "red_list_category": ["LC", "VU", "EN"],
        "year": [2020, 2021, 2022],
        "taxonomic_group": ["Mammals", "Birds", "Reptiles"],
    }
    input_file = create_test_csv(data)

    with pytest.raises(
        ValueError,
        match=r"Validation errors:\nColumn 'sis_taxon_id' contains 1 null value\(s\)",
    ):
        GroupDataFrameProcessor(input_file)


def test_invalid_red_list_category():
    """Test that invalid red_list_category values raise a ValueError."""
    data = {
        "sis_taxon_id": [1, 2, 3],
        "red_list_category": ["INVALID", "VU", "EN"],
        "year": [2020, 2021, 2022],
        "taxonomic_group": ["Mammals", "Birds", "Reptiles"],
    }
    input_file = create_test_csv(data)

    with pytest.raises(
        ValueError,
        match=r"Validation errors:\nColumn 'red_list_category' has invalid value\(s\) \['INVALID'\]; allowed: dict_keys\(\['LC', 'NT', 'VU', 'EN', 'CR', 'RE', 'CR\(PE\)', 'CR\(PEW\)', 'EW', 'EX', 'DD'\]\)",
    ):
        GroupDataFrameProcessor(input_file)


def test_null_red_list_category_error():
    """Test that an empty red_list_category column raises a ShapeError."""
    data = {
        "sis_taxon_id": [1, 2, 3],
        "red_list_category": [None, None, None],  # All None values
        "year": [2020, 2021, 2022],
        "taxonomic_group": ["Mammals", "Birds", "Reptiles"],
    }
    input_file = create_test_csv(data)
    with pytest.raises(
        ValueError, match=r"Column 'red_list_category' contains 3 null value\(s\)"
    ):
        GroupDataFrameProcessor(input_file)
