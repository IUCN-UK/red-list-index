from red_list_index.calculate import Calculate


def test_red_list_index_basic():
    calc = Calculate([1, 2, 3, 4, 5])
    result = calc.red_list_index()
    assert result == 0.4


def test_red_list_index_empty_weights():
    calc = Calculate([])
    try:
        calc.red_list_index()
        assert False, "Should raise ZeroDivisionError for empty list"
    except ZeroDivisionError:
        pass
