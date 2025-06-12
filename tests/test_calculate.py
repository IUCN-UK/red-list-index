from red_list_index.calculate import Calculate


def test_red_list_index_basic():
    calc = Calculate([1, 2, 3, 4, 5])
    result = calc.red_list_index()
    assert result == 0.4


def test_red_list_index_custom_weight_of_extinct():
    calc = Calculate([1, 2, 3, 4, 5], weight_of_extinct=10)
    # Expected: 1 - (15 / (10 * 5)) = 1 - (15 / 50) = 1 - 0.3 = 0.7
    result = calc.red_list_index()
    assert result == 0.7


def test_red_list_index_empty_weights():
    calc = Calculate([])
    try:
        calc.red_list_index()
        assert False, "Should raise ZeroDivisionError for empty list"
    except ZeroDivisionError:
        pass
