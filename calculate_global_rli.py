# /// script
# requires-python = ">=3.12"
# dependencies = ['polars','numpy','seaborn','matplotlib','pyarrow']
# ///


"""
Script to calculate Global Red List Index from input CSV.
"""

import argparse
import logging
import tomllib
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
from red_list_index.group_year_aggregate import GroupYearAggregate


def get_project_version() -> str:
    pyproject_path = Path(__file__).parent / "pyproject.toml"
    with pyproject_path.open("rb") as f:
        data = tomllib.load(f)
    return data["project"]["version"]


def limit_number_of_repetitions(value: str) -> int:
    ivalue = int(value)
    if ivalue > 10000 or ivalue < 1:
        raise argparse.ArgumentTypeError("number_of_repetitions must be 1–10000")
    return ivalue


def main() -> int:
    """
    Main entry point for the script.
    """
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
    parser.add_argument(
        "--plot", action="store_true", default=False, help="Save output plot as PNG"
    )
    parser.add_argument(
        "--verbose", action="store_true", default=False, help="Enable verbose logging"
    )

    parser.add_argument(
        "--version", action="version", version=f"%(prog)s {get_project_version()}"
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s: %(message)s",
    )

    input_file = args.input_csv
    output_file = args.output_csv
    number_of_repetitions = args.number_of_repetitions

    try:
        df = DataFrameProcessor(input_file).df
        logging.info(f"Processing and validating dataframe for: {input_file}")
        rli_df = CalculateGroups(df, number_of_repetitions).df
        logging.info(
            f"Building Global Red List Index DataFrame (repetitions: {number_of_repetitions})"
        )
        rli_df = GroupYearInterpolation.interpolate_rli_for_missing_years(rli_df)
        logging.info("Interpolating RLI for missing years")
        rli_df_extrapolated = GroupYearExtrapolation.extrapolate_trends_for(rli_df)
        logging.info("Extrapolating RLI to extend years")
        rli_df_aggregated = GroupYearAggregate.calculate_aggregate_from(
            rli_df_extrapolated
        )
        rli_df = pl.concat([rli_df, rli_df_aggregated], how="vertical")
        logging.info("Aggregated RLI")
        rli_df = rli_df.with_columns(
            pl.col("taxonomic_group_sample_sizes").cast(pl.Utf8)
        )
        Path(output_file).parent.mkdir(parents=True, exist_ok=True)
        rli_df.write_csv(output_file)
        logging.info(f"Saved results to: {output_file}")
        if args.plot:
            plot = Plot(rli_df)
            plot.global_rli(output_file.replace(".csv", ".png"))
            logging.info(f"Saved plot to: {output_file.replace('.csv', '.png')}")
        return 0
    except Exception as e:
        logging.error(f"Error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
