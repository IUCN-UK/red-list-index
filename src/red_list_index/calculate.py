class Calculate:
    """
    Calculator for the Red List Index.

    Args:
        category_weights (list of numbers): List of weights for categories, e.g. [1, 2, 3, 4, 5].
        weight_of_extinct (int, optional): Weight for EX (Extinct) category. Defaults to 5.

    Example:
        >>> calculate = Calculate([1, 2, 3, 4, 5])
        >>> calculate.red_list_index()
        0.4
    """

    def __init__(self, category_weights, weight_of_extinct=5):
        self.category_weights = category_weights
        self.weight_of_extinct = weight_of_extinct

    def red_list_index(self):
        sum_of_weights = sum(self.category_weights)

        calculated_red_list_index = 1 - (
            sum_of_weights / (self.weight_of_extinct * len(self.category_weights))
        )

        return calculated_red_list_index
