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

from red_list_index.calculate import Calculate
from red_list_index.utils import validate_input_dataframe, add_weights_column
from red_list_index.constants import RED_LIST_CATEGORY_WEIGHTS


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Calculate Global Red List Index from input CSV"
    )
    parser.add_argument("input_csv", help="Path to the input CSV file")
    parser.add_argument("output_csv", help="Path to save the output CSV file")

    args = parser.parse_args()

    input_file = args.input_csv
    output_file = args.output_csv

    try:
        df = pl.read_csv(input_file)
        print(f"[✓] Reading from: {input_file}")
    except Exception as e:
        print(f"[✗] Reading from: {input_file} - {e}")
        sys.exit(1)

    try:
        df = add_weights_column(df)
        print(f"[✓] Adding 'weights' column to DataFrame")
    except Exception as e:
        print(f"[✗] Adding 'weights' column to DataFrame - {e}")
        sys.exit(1)

    try:
        validate_input_dataframe(df)
        print(f"[✓] Validating input DataFrame")
    except Exception as e:
        print(f"[✗] Validating input DataFrame - {e}")
        sys.exit(1)

    try:
        # TODO: Save CSV code to go here
        print(f"[✓] Saving results to: {output_file}")
    except Exception as e:
        print(f"[✗] Saving results to: {output_file} - {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
