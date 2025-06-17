import polars as pl
from .constants import RED_LIST_CATEGORY_WEIGHTS


class DataFrameProcessor:
    INPUT_DATA_FRAME_SCHEMA = {
        "id": {"dtype": pl.Int64, "not_null": True},
        "red_list_category": {
            "dtype": pl.Utf8,
            "not_null": True,
            "allowed": RED_LIST_CATEGORY_WEIGHTS.keys(),
        },
        "year": {"dtype": pl.Int64, "not_null": True},
        "group": {"dtype": pl.Utf8, "not_null": True}
    }

    def __init__(self, input_file):
        self.df = pl.read_csv(input_file)
        self._validate_required_columns()
        self._validate_schema()
        self._validate_categories()
        self._add_weights_column()

    def _validate_required_columns(self):
        missing = set(self.INPUT_DATA_FRAME_SCHEMA) - set(self.df.columns)
        if missing:
            raise ValueError(
                f"Missing required column(s): {', '.join(sorted(missing))}"
            )


    def _validate_schema(self):
        schema = self.df.schema
        errors = []

        for col, spec in self.INPUT_DATA_FRAME_SCHEMA.items():
            actual_dtype = schema[col]
            if actual_dtype != spec["dtype"]:
                errors.append(
                    f"Column '{col}' must be {spec['dtype']!r}, got {actual_dtype!r}"
                )

            if spec.get("not_null", False):
                nulls = self.df[col].null_count()
                if nulls > 0:
                    errors.append(f"Column '{col}' contains {nulls} null value(s)")

            if "allowed" in spec:
                bad = self.df.select(pl.col(col)).filter(
                    ~pl.col(col).is_in(spec["allowed"])
                )
                if bad.height > 0:
                    bad_vals = bad.unique().to_series().to_list()
                    errors.append(
                        f"Column '{col}' has invalid value(s) {bad_vals}; "
                        f"allowed: {spec['allowed']}"
                    )

        if errors:
            raise ValueError("Validation errors:\n" + "\n".join(errors))

    def _validate_categories(self):
        found_categories = set(self.df["red_list_category"].unique().to_list())
        if not found_categories:
            raise ValueError("Input DataFrame has an empty 'red_list_category' column")

        valid_categories = set(RED_LIST_CATEGORY_WEIGHTS.keys())
        invalid_categories = found_categories - valid_categories

        if invalid_categories:
            raise ValueError(
                f"Invalid value found in 'red_list_category' column: {sorted(invalid_categories)}"
            )

    def _add_weights_column(self):
        self.df = self.df.with_columns(
            pl.col("red_list_category")
            .replace(RED_LIST_CATEGORY_WEIGHTS)
            .cast(pl.Int64)
            .alias("weights")
        )
