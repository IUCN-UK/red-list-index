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
