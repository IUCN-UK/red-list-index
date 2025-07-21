from red_list_index.calculate import Calculate


def test_red_list_index_basic():
    calc = Calculate([1, 2, 3, 4, 5])
    result = calc.red_list_index()
    assert result == 0.4


def test_init_validation_with_valid_weights():
    calc = Calculate([1, 2, 3, 4, 5])
    assert calc.category_weights == [1, 2, 3, 4, 5]


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
