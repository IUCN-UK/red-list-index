import polars as pl


# Weights for each Red List category
RED_LIST_CATEGORY_WEIGHTS = {
    "LC": 0,  # Least Concern
    "NT": 1,  # Near Threatened
    "VU": 2,  # Vulnerable
    "EN": 3,  # Endangered
    "CR": 4,  # Critically Endangered
    "RE": 5,  # Regionally Extinct
    "CR(PE)": 5,  # Critically Endangered (Possibly Extinct)
    "CR(PEW)": 5,  # Critically Endangered (Possibly Extinct in the Wild)
    "EW": 5,  # Extinct in the Wild
    "EX": 5,  # Extinct
    "DD": None,  # Data Deficient
}

INPUT_DATA_FRAME_SCHEMA = {
    "id": {"dtype": pl.Int64, "not_null": True},
    "red_list_category": {
        "dtype": pl.Utf8,
        "not_null": True,
        "allowed": ["EN", "PE", "RS"],
    },
    "year": {"dtype": pl.Int64, "not_null": True},
    "group": {"dtype": pl.Utf8, "not_null": True},
    "weights": {"dtype": pl.Int64, "not_null": False},
}
