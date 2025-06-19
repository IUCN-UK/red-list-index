import polars as pl

from red_list_index.utils import calculate_aggregate_from


def test_calculate_aggregate_from_valid_input():
    df_rli_extrapolated_data = pl.DataFrame(
        {
            "year": [2000, 2000, 2001, 2001, 2002],
            "rli": [0.5, 0.6, 0.7, 0.8, 0.9],
            "group": ["Bird", "Mammal", "Bird", "Mammal", "Bird"],
        }
    )
    expected = pl.DataFrame(
        {
            "year": [2000, 2001, 2002],
            "group": ["Aggregate", "Aggregate", "Aggregate"],
            "rli": [0.55, 0.75, 0.9],
            "qn_95": [None, None, None],
            "qn_05": [None, None, None],
            "n": [None, None, None],
            "group_sample_sizes": [None, None, None],
        }
    )
    result = calculate_aggregate_from(df_rli_extrapolated_data)
    assert result.shape == expected.shape, (
        f"Expected shape {expected.shape}, but got {result.shape}"
    )
    assert result["year"].to_list() == expected["year"].to_list()
    assert result["group"].to_list() == expected["group"].to_list()
    assert result["rli"].to_list() == expected["rli"].to_list()
    assert result["qn_95"].to_list() == expected["qn_95"].to_list()
    assert result["qn_05"].to_list() == expected["qn_05"].to_list()
    assert result["n"].to_list() == expected["n"].to_list()
    assert (
        result["group_sample_sizes"].to_list()
        == expected["group_sample_sizes"].to_list()
    )
