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
        self.category_weights = category_weights

    def red_list_index(self):
        weight_of_extinct = RED_LIST_CATEGORY_WEIGHTS["EX"]

        sum_of_weights = sum(self.category_weights)

        calculated_red_list_index = 1 - (
            sum_of_weights / (weight_of_extinct * len(self.category_weights))
        )

        return calculated_red_list_index
