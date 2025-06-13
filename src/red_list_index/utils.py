import numpy as np
import polars as pl


def validate_input_dataframe(df: pl.DataFrame, weight_of_extinct=5):
    """
    Validate the input Polars DataFrame is suitable for Red List calculations.

    Args:
        df (pl.DataFrame): Input DataFrame expected to contain a 'weights' column of integer type (pl.Int64).
        weight_of_extinct (int, optional): Maximum allowed value in the 'weights' column (default is 5).

    Raises:
        TypeError: If df is not a Polars DataFrame or if the 'weights' column is not of integer type (pl.Int64).
        ValueError: If the 'weights' column is missing, if any non-null weight is negative,
                    or if any non-null weight exceeds weight_of_extinct.
        TODO: Add "id","red_list_category","year", and "group" column presence and type checks.
        
    Example:
        >>> import polars as pl
        >>> df = pl.DataFrame({'weights': [1, 2, 5, None]})
        >>> validate_input_dataframe(df, weight_of_extinct=5)
        # No exception raised

        >>> df_bad = pl.DataFrame({'weights': [1, 2, 7]})
        >>> validate_input_dataframe(df_bad, weight_of_extinct=5)
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
    """
    Replace Data Deficient (DD) weights in the DataFrame with randomly sampled valid weights.

    Args:
        df (pl.DataFrame): A Polars DataFrame containing a 'weights' column, where some rows may have null (DD) values.
        weight_of_extinct (int, optional): Placeholder argument (default is 5).

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
