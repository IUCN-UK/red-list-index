# /// script
# requires-python = ">=3.12"
# dependencies = ['polars','numpy']
# ///

import argparse
import polars as pl
import sys
from pathlib import Path

# Add 'src' directory to the module search path
sys.path.append(str(Path(__file__).resolve().parent / "src"))

from red_list_index.utils import (
    validate_input_dataframe,
    add_weights_column,
    build_global_red_list_indices,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Calculate Global Red List Index from input CSV"
    )
    parser.add_argument("input_csv", help="Path to the input CSV file")
    parser.add_argument("output_csv", help="Path to save the output CSV file")
    parser.add_argument(
        "--number_of_repetitions",
        type=int,
        default=1000,
        help="Number of repetitions (default: 1000)",
    )

    args = parser.parse_args()

    input_file = args.input_csv
    output_file = args.output_csv
    number_of_repetitions = args.number_of_repetitions

    try:
        df = pl.read_csv(input_file)
        print(f"[✓] Reading from: {input_file}")
    except Exception as e:
        print(f"[✗] Reading from: {input_file} - {e}")
        sys.exit(1)

    try:
        df = add_weights_column(df)
        print("[✓] Adding 'weights' column to DataFrame")
    except Exception as e:
        print(f"[✗] Adding 'weights' column to DataFrame - {e}")
        sys.exit(1)

    try:
        validate_input_dataframe(df)
        print("[✓] Validating input DataFrame")
    except Exception as e:
        print(f"[✗] Validating input DataFrame - {e}")
        sys.exit(1)

    print(
        f"\r[-] Building Global Red List Index DataFrame (number of repetitions: {number_of_repetitions})",
        end="",
        flush=True,
    )
    try:
        rli_df = build_global_red_list_indices(df, number_of_repetitions)
        print(
            f"\r[✓] Building Global Red List Index DataFrame (number of repetitions: {number_of_repetitions}){' ' * 10}"
        )
    except Exception as e:
        print(
            f"\r[✗] Building Global Red List Index DataFrame (number of repetitions: {number_of_repetitions}) - {e}{' ' * 10}"
        )
        sys.exit(1)

    try:
        # Ensure 'group_sample_sizes' is cast to string for CSV output
        rli_df = rli_df.with_columns(pl.col("group_sample_sizes").cast(pl.Utf8))
        rli_df.write_csv(output_file)

        print(f"[✓] Saving results to: {output_file}")
    except Exception as e:
        print(f"[✗] Saving results to: {output_file} - {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
