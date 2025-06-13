import numpy as np
import polars as pl


def validate_input_dataframe(df: pl.DataFrame, weight_of_extinct=5):
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
    # Only consider non-null weights for the checks
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
        bool: True if all Red List categories are valid, False otherwise.

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


def replace_data_deficient_rows(df: pl.DataFrame, weight_of_extinct=5):
    valid_weights = df.filter(pl.col("weights").is_not_null())["weights"].to_numpy()

    data_deficient_count = df.filter(pl.col("weights").is_null()).height

    random_weights = np.random.choice(
        valid_weights, size=data_deficient_count, replace=True
    )

    return valid_weights.tolist() + random_weights.tolist()
