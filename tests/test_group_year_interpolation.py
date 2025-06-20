import polars as pl

from red_list_index.group_year_interpolation import GroupYearInterpolation


def test_interpolate_rli_for_missing_years_valid_input():
    rli_df = pl.DataFrame(
        {
            "year": [2000, 2002, 2004, 2001, 2003],
            "taxonomic_group": ["Bird", "Bird", "Bird", "Mammal", "Mammal"],
            "rli": [0.5, 0.6, 0.7, 0.8, 0.9],
            "qn_05": [0.4, 0.5, 0.6, 0.7, 0.8],
            "qn_95": [0.6, 0.7, 0.8, 0.9, 1.0],
            "n": [1, 2, 3, 4, 5],
            "taxonomic_group_sample_sizes": [
                [{"group": "Bird", "count": 20}],
                [{"group": "Bird", "count": 30}],
                [{"group": "Bird", "count": 40}],
                [{"group": "Mammal", "count": 50}],
                [{"group": "Mammal", "count": 60}],
            ],
        }
    )

    expected = pl.DataFrame(
        {
            "year": [2000, 2001, 2002, 2003, 2004, 2001, 2002, 2003],
            "taxonomic_group": [
                "Bird",
                "Bird",
                "Bird",
                "Bird",
                "Bird",
                "Mammal",
                "Mammal",
                "Mammal",
            ],
            "rli": [
                0.5,
                0.55,
                0.6,
                0.6499999999999999,
                0.7,
                0.8,
                0.8500000000000001,
                0.9,
            ],
            "qn_05": [0.4, 0.45, 0.5, 0.55, 0.6, 0.7, 0.75, 0.8],
            "qn_95": [0.6, 0.6499999999999999, 0.7, 0.75, 0.8, 0.9, 0.95, 1.0],
            "n": [1, 1, 2, 2, 3, 4, 4, 5],
            "taxonomic_group_sample_sizes": [
                [{"group": "Bird", "count": 20}],
                [{"group": "Bird", "count": 20}],
                [{"group": "Bird", "count": 30}],
                [{"group": "Bird", "count": 30}],
                [{"group": "Bird", "count": 40}],
                [{"group": "Mammal", "count": 50}],
                [{"group": "Mammal", "count": 50}],
                [{"group": "Mammal", "count": 60}],
            ],
        }
    )

    result = GroupYearInterpolation.interpolate_rli_for_missing_years(rli_df).sort(
        ["taxonomic_group", "year"]
    )
    # result = interpolate_rli_for_missing_years(rli_df).sort(["group", "year"])

    assert result.shape == expected.shape, (
        f"Expected shape {expected.shape}, but got {result.shape}"
    )
    assert result["year"].to_list() == expected["year"].to_list()
    assert result["taxonomic_group"].to_list() == expected["taxonomic_group"].to_list()
    assert result["rli"].to_list() == expected["rli"].to_list()
    assert result["qn_05"].to_list() == expected["qn_05"].to_list()
    assert result["qn_95"].to_list() == expected["qn_95"].to_list()
    assert result["n"].to_list() == expected["n"].to_list()
    assert (
        result["taxonomic_group_sample_sizes"].to_list()
        == expected["taxonomic_group_sample_sizes"].to_list()
    )
