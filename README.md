# The IUCN Red List Index

The International Union for Conservation of Nature’s Red List of Threatened Species has evolved to become the world’s most comprehensive information source on the global conservation status of animal, fungi and plant species. It's a critical indicator of the health of the world’s biodiversity.

The IUCN Red List Index (RLI) shows trends in overall extinction risk for species, and is used by governments to track their progress towards targets for reducing biodiversity loss.

## Installation

## Running the Global Red List Index Calculation Script

To calculate Red List Index values over time for each comprehensive group using a CSV file, run the included `calculate_global_rli.py` script from your terminal. 

You must provide an input CSV file containing the following columns:

*id:* The species identifier. Must be an integer with no null values.

*red_list_category:* The conservation status. Must be a non-null string and one of the following values: LC, NT, VU, EN, CR, RE, CR(PE), CR(PEW), EW, EX, or DD.

*year:* The year. Must be an integer with no null values.

*group:* The comprehensive group. Must be a non-null string.


The `calculate_global_rli.py` script allows you to compute the Global Red List Index (RLI) from your input data in CSV format. This script processes your species data and outputs the calculated RLI values to a specified CSV file.

### Basic Usage

To run the script, use the following command:

```bash
uv run calculate_global_rli.py <input_csv> <output_csv>
```

- `<input_csv>`: Path to your input CSV file with species data (refer to the documentation above for the required format).
- `<output_csv>`: Path where the output CSV with RLI results will be saved.

### Optional Arguments

- `-h`, `--help`: Show the help message and exit.
- `--number_of_repetitions NUMBER_OF_REPETITIONS`: Specify the number of repetitions for the calculation (default is 1000).

### Example

```bash
uv run calculate_global_rli.py species_data.csv rli_output.csv --number_of_repetitions 1000
```

This command will calculate the Global Red List Index using the data from `species_data.csv`, save the results to `rli_output.csv`, and perform 1000 repetitions of the calculation to acount for resampling of Data Deficient (DD) rows.  The script displays output upon the completion of each stage (see example below).

For example, running this for the `species_red_list_category_list.csv` file found in the test fixtures directory:
```
➜ uv run calculate_global_rli.py ./tests/fixtures/species_red_list_category_list.csv rli_output.csv --number_of_repetitions 1000
[✓] Reading from: ./tests/fixtures/species_red_list_category_list.csv
[✓] Adding 'weights' column to DataFrame
[✓] Validating input DataFrame
[✓] Building Global Red List Index DataFrame (number of repetitions: 1000)          
[✓] Interpolate RLI for missing years
[✓] Aggregate RLI
[✓] Saving results to: output.csv
[✓] Saving plot to: output.png
```

## Standalone RLI calculator use

The `red_list_index` package also supports calculating the Red List Index for a single custom set of weights.

To get started, open a Python REPL using a tool like `uv`:

```bash
uv run python
```

Then, initialize the package and perform a Red List Index calculation:

```python
>>> import red_list_index
>>> from red_list_index.calculate import Calculate
>>> calculate = Calculate([1, 2, 3, 4, 5])
>>> calculate.red_list_index()
0.4
```

## Development

Run tests with 
```
uv run pytest
```

Run the Ruff formatter on all directories and files
```
uv run ruff format
```

Run Ruff check
```
uv run ruff check
```
