from .constants import RED_LIST_CATEGORY_WEIGHTS


class Calculate:
    """
    Calculator for the Red List Index.

    Args:
        category_weights (list of numbers): List of weights for categories, e.g. [1, 2, 3, 4, 5].

    Example:
        >>> calculate = Calculate([1, 2, 3, 4, 5])
        >>> calculate.red_list_index()
        0.4
    """

    def __init__(self, category_weights):
        self.weight_of_extinct = RED_LIST_CATEGORY_WEIGHTS["EX"]
        _validate_category_weights(category_weights, self.weight_of_extinct)

        self.category_weights = category_weights

    def red_list_index(self):
        sum_of_weights = sum(self.category_weights)

        calculated_red_list_index = 1 - (
            sum_of_weights / (self.weight_of_extinct * len(self.category_weights))
        )

        return calculated_red_list_index


def _validate_category_weights(category_weights, weight_of_extinct):
    # Validate category_weights input
    if not category_weights or len(category_weights) == 0:
        raise ValueError("category_weights cannot be empty.")

    for index, weight in enumerate(category_weights):
        if weight is None:
            raise ValueError(f"Null value found at index {index} in category_weights.")
        if not isinstance(weight, int):
            raise ValueError(
                f"Non-integer value found at index {index}: {weight} ({type(weight).__name__})"
            )
        if weight < 0:
            raise ValueError(f"Negative value found at index {index}: {weight}")
        if weight > weight_of_extinct:
            raise ValueError(
                f"Value greater than EX found at index {index}: {weight} > {weight_of_extinct}"
            )
    return True
