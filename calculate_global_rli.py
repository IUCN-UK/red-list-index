# /// script
# requires-python = ">=3.12"
# dependencies = []
# ///

import argparse

def main() -> None:
    parser = argparse.ArgumentParser(description="Calculate Global Red List Index from input CSV")
    parser.add_argument("input_csv", help="Path to the input CSV file")
    parser.add_argument("output_csv", help="Path to save the output CSV file")
    
    args = parser.parse_args()

    input_file = args.input_csv
    output_file = args.output_csv

    print(f"Reading from: {input_file}")
    print(f"Saving results to: {output_file}")


if __name__ == "__main__":
    main()
