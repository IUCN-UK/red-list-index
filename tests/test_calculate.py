from red_list_index.calculate import Calculate
import polars as pl

RED_LIST_CATEGORY_WEIGHTS = {
    "LC": 0,
    "NT": 1,
    "VU": 2,
    "EN": 3,
    "CR": 4,
    "RE": 5,
    "CR(PE)": 5,
    "CR(PEW)": 5,
    "EW": 5,
    "EX": 5,
    "DD": None,
}

CSV_PATH = "tests/fixtures/species_red_list_category_list.csv"


def test_calculate_red_list_index_basic():
    calc = Calculate([1, 2, 3, 4, 5])
    result = calc.red_list_index()
    assert result == 0.4


def test_calculate_red_list_index_birds_2024():
    weighted_df = get_weighted_red_list(taxonomic_group="Bird", year=2024)
    calc = Calculate(weighted_df["weights"].to_list())
    result = calc.red_list_index()
    # Note: This RLI value will differ from the final calculation for 2024 Birds,
    # since all 'DD' entries are removed here and, unlike in script execution,
    # they are not replaced with sampled values.
    assert result == 0.9043291207313794


def test_init_validation_with_null_value():
    try:
        Calculate([1, None, 3])
        assert False, "Should raise an ValueError for a null value in category_weights"
    except ValueError as e:
        assert str(e) == "Null value found at index 1 in category_weights."


def test_init_validation_with_negative_value():
    try:
        Calculate([1, -2, 3])
        assert False, (
            "Should raise a ValueError for a negative value in category_weights"
        )
    except ValueError as e:
        assert str(e) == "Negative value found at index 1: -2"


def test_init_validation_with_value_greater_than_ex():
    try:
        Calculate(
            [1, 2, 6]
        )  # We'll need to update this id RED_LIST_CATEGORY_WEIGHTS["EX"] changes to a value greater than 5
        assert False, (
            "Should raise a ValueError for a value greater than EX in category_weights"
        )
    except ValueError as e:
        assert "Value greater than EX found at index 2" in str(e)


def test_init_validation_with_empty_weights():
    try:
        Calculate([])
        assert False, "Should raise a ValueError for empty category_weights"
    except ValueError as e:
        assert str(e) == "category_weights cannot be empty."


def test_init_validation_with_non_integer_value():
    try:
        Calculate([1, "two", 3])
        assert False, (
            "Should raise a ValueError for a non-integer value in category_weights"
        )
    except ValueError as e:
        assert "Non-integer value found at index 1" in str(e)


def add_weight_column(df: pl.DataFrame) -> pl.DataFrame:
    """Adds a weights column based on red_list_category."""
    return df.with_columns(
        pl.col("red_list_category")
        .replace(RED_LIST_CATEGORY_WEIGHTS)
        .cast(pl.Int64)
        .alias("weights")
    )


def get_weighted_red_list(
    csv_path: str = CSV_PATH, taxonomic_group: str = "Bird", year: int = 2024
) -> pl.DataFrame:
    """Reads CSV, filters by taxonomic_group & year, adds weights, and drops nulls."""
    df = pl.read_csv(csv_path)
    filtered = df.filter(
        (pl.col("year") == year) & (pl.col("taxonomic_group") == taxonomic_group)
    )
    weighted = add_weight_column(filtered)
    return weighted.drop_nulls(subset=["weights"])
