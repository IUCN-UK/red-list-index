from dataclasses import dataclass, field
from typing import List
from .constants import RED_LIST_CATEGORY_WEIGHTS


@dataclass
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

    category_weights: List[int]
    weight_of_extinct: int = field(
        init=False
    )  # Not included as a parameter to the generated __init__ constructor but instead set inside the class in __post_init__

    def red_list_index(self):
        sum_of_weights = sum(self.category_weights)
        calculated_red_list_index = 1 - (
            sum_of_weights / (self.weight_of_extinct * len(self.category_weights))
        )
        return calculated_red_list_index

    def __post_init__(self):
        # __post_init__ is called automatically after the dataclass __init__ method.
        # Here, we set weight_of_extinct from constants (since it depends on external data and should not be set by the user),
        # and perform input validation on category_weights to ensure correctness before calculations.

        self.weight_of_extinct = RED_LIST_CATEGORY_WEIGHTS["EX"]
        if not self.category_weights:
            raise ValueError("category_weights cannot be empty.")
        for index, weight in enumerate(self.category_weights):
            if weight is None:
                raise ValueError(
                    f"Null value found at index {index} in category_weights."
                )
            if not isinstance(weight, int):
                raise ValueError(
                    f"Non-integer value found at index {index}: {weight} ({type(weight).__name__})"
                )
            if weight < 0:
                raise ValueError(f"Negative value found at index {index}: {weight}")
            if weight > self.weight_of_extinct:
                raise ValueError(
                    f"Value greater than EX found at index {index}: {weight} > {self.weight_of_extinct}"
                )
        return True
