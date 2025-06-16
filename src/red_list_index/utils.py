import numpy as np
import polars as pl
from .constants import INPUT_DATA_FRAME_SCHEMA
from .constants import RED_LIST_CATEGORY_WEIGHTS


def validate_input_dataframe(df: pl.DataFrame):
    """
    Validate that the provided input DataFrame conforms to the expected schema.

    Checks performed:
    1. Type: `df` must be a `polars.DataFrame`.
    2. Presence: All columns in `INPUT_DATA_FRAME_SCHEMA` must exist.
    3. Dtype: Each column must match its specified Polars dtype.
    4. Not-null: Columns marked `not_null=True` must contain no nulls.
    5. Allowed values: Columns with an `allowed` list must only contain those values.

    Raises:
        TypeError: If `df` is not a `polars.DataFrame`.
        ValueError: If any required column is missing, has the wrong dtype,
                    contains nulls where not permitted, or contains disallowed values.
    """
    if not isinstance(df, pl.DataFrame):
        raise TypeError(
            f"Expected df to be a polars DataFrame, got {type(df).__name__}"
        )

    missing = set(INPUT_DATA_FRAME_SCHEMA) - set(df.columns)
    if missing:
        raise ValueError(f"Missing required column(s): {', '.join(sorted(missing))}")

    schema = df.schema
    errors = []

    for col, spec in INPUT_DATA_FRAME_SCHEMA.items():
        actual_dtype = schema[col]
        if actual_dtype != spec["dtype"]:
            errors.append(
                f"Column '{col}' must be {spec['dtype']!r}, got {actual_dtype!r}"
            )

        if spec.get("not_null", False):
            nulls = df[col].null_count()
            if nulls > 0:
                errors.append(f"Column '{col}' contains {nulls} null value(s)")

        if "allowed" in spec:
            bad = df.select(pl.col(col)).filter(~pl.col(col).is_in(spec["allowed"]))
            if bad.height > 0:
                bad_vals = bad.unique().to_series().to_list()
                errors.append(
                    f"Column '{col}' has invalid value(s) {bad_vals}; "
                    f"allowed: {spec['allowed']}"
                )

    if errors:
        raise ValueError("Validation errors:\n" + "\n".join(errors))


def validate_input_dataframe_weights(df: pl.DataFrame):
    """
    Validate the input Polars DataFrame is suitable for Red List calculations.

    Args:
        df (pl.DataFrame): Input DataFrame expected to contain a 'weights' column of integer type (pl.Int64).

    Raises:
        TypeError: If df is not a Polars DataFrame or if the 'weights' column is not of integer type (pl.Int64).
        ValueError: If the 'weights' column is missing, if any non-null weight is negative,
                    or if any non-null weight exceeds weight_of_extinct.
        TODO: Add "id","red_list_category","year", and "group" column presence and type checks.

    Example:
        >>> import polars as pl
        >>> df = pl.DataFrame({'weights': [1, 2, 5, None]})
        >>> validate_input_dataframe(df)
        # No exception raised

        >>> df_invalid = pl.DataFrame({'weights': [1, 2, 7]})
        >>> validate_input_dataframe(df_invalid)
        ValueError: Maximum value in 'weights' column (7) is greater than weight_of_extinct (5).
    """

    if not isinstance(df, pl.DataFrame):
        raise TypeError(
            f"Expected df to be a polars DataFrame, got {type(df).__name__}"
        )

    if "weights" not in df.columns:
        raise ValueError("Missing 'weights' column in DataFrame.")

    if not df.schema["weights"] == pl.Int64:
        raise TypeError(
            f"'weights' column must be of integer type, got {df.schema['weights']}."
        )

    # Check the maximum value in 'weights' column is not greater than weight_of_extinct
    # Only consider non-null (non-DD) weights for the checks
    weight_of_extinct = RED_LIST_CATEGORY_WEIGHTS["EX"]
    weights_non_null = df.filter(pl.col("weights").is_not_null())["weights"]
    if len(weights_non_null) > 0:
        max_weight = weights_non_null.max()
        min_weight = weights_non_null.min()
        if max_weight > weight_of_extinct:
            raise ValueError(
                f"Maximum value in 'weights' column ({max_weight}) "
                f"is greater than weight_of_extinct ({weight_of_extinct})."
            )
        if min_weight < 0:
            raise ValueError(
                f"Minimum value in 'weights' column ({min_weight}) is less than zero."
            )


def validate_categories(categories, valid_categories):
    """
    Validate that all Red List categories in the input list are present in the supplied list of valid Red List categories.

    Args:
        categories (list): List of Red List categories to validate.
        valid_categories (list): List of valid Red List categories.

    Returns:
        list: Empty list if all Red List categories are valid, list of invalid Red List categories otherwise.

    Example:
        >>> categories = ["LC", "EN", "VU"]
        >>> valid_categories = ["LC", "EN", "VU", "CR", "EX"]
        >>> validate_categories(categories, valid_categories)
        []

        >>> categories = ["LC", "EN", "INVALID"]
        >>> validate_categories(categories, valid_categories)
        ['INVALID']
    """
    if not isinstance(categories, list):
        raise TypeError("categories must be a list")
    if not isinstance(valid_categories, list):
        raise TypeError("valid_categories must be a list")

    invalid_categories = [item for item in categories if item not in valid_categories]

    return invalid_categories


def replace_data_deficient_rows(df: pl.DataFrame):
    """
    Replace Data Deficient (DD) weights in the DataFrame with randomly sampled valid weights.

    Args:
        df (pl.DataFrame): A Polars DataFrame containing a 'weights' column, where some rows may have null (DD) values.

    Returns:
        list: A combined list containing all valid (non-null) weights and randomly sampled imputed weights for DD rows.

    Example:
        >>> import polars as pl, numpy as np
        >>> df = pl.DataFrame({'weights': [1, 2, None, 3, None]})
        >>> replace_data_deficient_rows(df)
        [1, 2, 3, 2, 1]  # Example output - imputed DD values are randomly sampled from valid weights
    """

    valid_weights = df.filter(pl.col("weights").is_not_null())["weights"].to_numpy()

    data_deficient_count = df.filter(pl.col("weights").is_null()).height

    random_weights = np.random.choice(
        valid_weights, size=data_deficient_count, replace=True
    )

    return valid_weights.tolist() + random_weights.tolist()


def add_weights_column(df: pl.DataFrame) -> pl.DataFrame:
    """
    Adds a 'weights' column to the DataFrame by mapping values from the Red List category weight dictionary.

    Args:
        df (pl.DataFrame): The input Polars DataFrame.

    Returns:
        pl.DataFrame: The modified DataFrame with a new 'weights' column.
    """
    if "red_list_category" not in df.columns:
        raise ValueError("Input DataFrame must contain a 'red_list_category' column")

    found_categories = set(df["red_list_category"].unique().to_list())

    if not found_categories:
        raise ValueError("Input DataFrame has an empty 'red_list_category' column")

    valid_categories = set(RED_LIST_CATEGORY_WEIGHTS.keys())
    invalid_categories = found_categories - valid_categories

    if invalid_categories:
        raise ValueError(
            f"Invalid value found in 'red_list_category' column: {sorted(invalid_categories)}"
        )

    return df.with_columns(
        pl.col("red_list_category")
        .replace(RED_LIST_CATEGORY_WEIGHTS)
        .cast(pl.Int64)
        .alias("weights")
    )
