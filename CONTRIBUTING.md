# Contributing

Thanks for your interest in improving the Free Global Ticker Database!

## How to contribute

1. Fork the repository
2. Create a feature branch (`git checkout -b feat/my-change`)
3. Make your changes
4. Run the quality tests: `python -m pytest tests/ -q`
5. If you changed data or the build script, rebuild: `python scripts/rebuild_dataset.py`
6. Verify the rebuild is clean: `git diff data/`
7. Commit and open a Pull Request

## Adding new tickers or aliases

- Add entries to the source CSV files in `data/`
- Run `python scripts/rebuild_dataset.py` to regenerate all output formats
- All quality tests must pass before merging

## Reporting issues

If you find incorrect data (wrong ISIN, misclassified sector, bad alias), please open an issue with:
- The ticker symbol
- What is wrong
- What the correct value should be (with a source if possible)

## Code style

- Python 3.10+
- Keep runtime dependencies limited to `pandas`, `pyarrow`, `pytest`, and `requests`
- Keep the dataset build and review scripts dependency-light and easy to trace
