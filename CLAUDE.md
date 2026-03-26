# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

This project uses [uv](https://docs.astral.sh/uv/) for dependency management (Python 3.12).

```bash
# Install dependencies
uv sync

# Install with optional cloud storage extras
uv sync --extra gcs    # Google Cloud Storage
uv sync --extra s3     # AWS S3
uv sync --extra all    # Both

# Run the pipeline
uv run python main.py
# or via the installed script entry point:
uv run pipeline
```

## Configuration

`config.json` defines source systems and the destination. Credentials use `${ENV_VAR}` placeholders that are resolved from environment variables at runtime.

Required env vars for Kaggle: `KAGGLE_USERNAME`, `KAGGLE_KEY`.

Destination `type` can be `"local"` (default), `"gcs"`, or `"s3"`.

## Architecture

The pipeline has three layers:

1. **`parsers.py`** — Source-system abstraction. `BaseParser` defines the contract (`list_files()` + `download_file()`). Concrete parsers register themselves in `PARSER_REGISTRY` keyed by the config `"type"` string. `get_parser()` looks up and instantiates the right class. Currently only `KaggleParser` exists, which talks to the Kaggle REST API v1 using HTTP Basic Auth.

2. **`pipeline.py`** — Transport layer. `DataPipeline` combines any `BaseParser` with a destination writer (`LocalWriter`, `GCSWriter`, `S3Writer`). `make_writer()` constructs the correct writer from `dest_config`. The pipeline calls `list_files()`, iterates results, calls `download_file()` per file, then writes bytes to the destination.

3. **`main.py`** — Orchestration. Loads and resolves `config.json`, then for each `source_systems` entry instantiates the appropriate parser and runs a `DataPipeline`.

### Adding a new source system

1. Subclass `BaseParser` in `parsers.py`, implement `list_files()` and `download_file()`.
2. Add an entry to `PARSER_REGISTRY`: `"type_string": MyParser`.
3. Add credentials and source entries to `config.json`.
