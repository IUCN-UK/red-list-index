# The IUCN Red List Index

The International Union for Conservation of Nature’s Red List of Threatened Species has evolved to become the world’s most comprehensive information source on the global conservation status of animal, fungi and plant species. It's a critical indicator of the health of the world’s biodiversity.

The IUCN Red List Index (RLI) shows trends in overall extinction risk for species, and is used by governments to track their progress towards targets for reducing biodiversity loss.

## Installation

## Example Usage

The `red_list_index` package contains a number of functions to allow efficient
calculations of a Red List Index for groups and dissagregations. 

Open a Python REPL with e.g. `uv`:
```
uv run python
```

Initialize the package and run a Red List Index calculation
```
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