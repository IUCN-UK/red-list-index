# /// script
# requires-python = ">=3.12"
# dependencies = ['polars','numpy','seaborn','matplotlib','pyarrow']
# ///

import argparse
import polars as pl
import sys

from pathlib import Path

# Add 'src' directory to the module search path
sys.path.append(str(Path(__file__).resolve().parent / "src"))

from red_list_index.data_frame_processor import DataFrameProcessor
from red_list_index.calculate_groups import CalculateGroups
from red_list_index.plot import Plot
from red_list_index.group_year_interpolation import GroupYearInterpolation
from red_list_index.group_year_extrapolation import GroupYearExtrapolation
from red_list_index.group_year_aggregate import GroupYearAggreagate


def limit_number_of_repetitions(value):
    ivalue = int(value)
    if ivalue > 10000:
        raise argparse.ArgumentTypeError(
            "number_of_repetitions must be no more than 10000"
        )
    if ivalue < 1:
        raise argparse.ArgumentTypeError("number_of_repetitions must be at least 1")
    return ivalue


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Calculate Global Red List Index from input CSV"
    )
    parser.add_argument("input_csv", help="Path to the input CSV file")
    parser.add_argument("output_csv", help="Path to save the output CSV file")
    parser.add_argument(
        "--number_of_repetitions",
        type=limit_number_of_repetitions,
        default=1000,
        help="Number of repetitions (default: 1000, maximum: 10000, minimum: 1)",
    )

    args = parser.parse_args()

    input_file = args.input_csv
    output_file = args.output_csv
    number_of_repetitions = args.number_of_repetitions

    try:
        df = DataFrameProcessor(input_file).df

        print(f"[✓] Processing and validating dataframe for: {input_file}")
    except Exception as e:
        print(f"[✗] Processing and validating dataframe for: {input_file} - {e}")
        sys.exit(1)

    print(
        f"[-] Building Global Red List Index DataFrame (number of repetitions: {number_of_repetitions})",
        end="\r",
        flush=True,
    )
    try:
        rli_df = CalculateGroups(df, number_of_repetitions).df
        print(
            f"[✓] Building Global Red List Index DataFrame (number of repetitions: {number_of_repetitions})"
        )
    except Exception as e:
        print(
            f"\r[✗] Building Global Red List Index DataFrame (number of repetitions: {number_of_repetitions}) - {e}{' ' * 10}"
        )
        sys.exit(1)

    try:
        rli_df = GroupYearInterpolation.interpolate_rli_for_missing_years(rli_df)

        print("[✓] Interpolating RLI for missing years")
    except Exception as e:
        print(f"[✗] Interpolating RLI for missing years - {e}")
        sys.exit(1)

    try:
        rli_df_extrapolated = GroupYearExtrapolation.extrapolate_trends_for(rli_df)

        print("[✓] Exrapolating RLI to extend years")
    except Exception as e:
        print(f"[✗] Exrapolating RLI to extend years - {e}")
        sys.exit(1)

    try:
        rli_df_aggregated = GroupYearAggreagate.calculate_aggregate_from(
            rli_df_extrapolated
        )

        rli_df = pl.concat([rli_df, rli_df_aggregated], how="vertical")

        print("[✓] Aggregate RLI")
    except Exception as e:
        print(f"[✗] Aggregate RLI - {e}")
        sys.exit(1)

    try:
        # Ensure 'group_sample_sizes' is cast to string for CSV output
        rli_df = rli_df.with_columns(
            pl.col("taxonomic_group_sample_sizes").cast(pl.Utf8)
        )
        rli_df.write_csv(output_file)

        print(f"[✓] Saving results to: {output_file}")
    except Exception as e:
        print(f"[✗] Saving results to: {output_file} - {e}")
        sys.exit(1)

    try:
        plot = Plot(rli_df)
        plot.global_rli(output_file.replace(".csv", ".png"))
        print(f"[✓] Saving plot to: {output_file.replace('.csv', '.png')}")
    except Exception as e:
        print(f"[✗] Saving plot to: {output_file.replace('.csv', '.png')} - {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
