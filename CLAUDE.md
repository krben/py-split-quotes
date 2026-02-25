# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Quote Splitter Service — a Python service that reads quote JSON files from Azure Blob Storage, extracts configured objects (e.g., `QuoteCharges`) into separate files, and archives the originals. Runs as a Docker container.

## Commands

```bash
# Run locally
python app/app.py

# Install dependencies
pip install -r requirements/requirements.txt

# Build Docker image
docker build -t quotessplitter .

# Run Docker container
docker run quotessplitter
```

## Architecture

Single-file application (`app/app.py`) with these key classes:

- **`AzureDataLake`** — Core logic. Connects to Azure Blob Storage, reads quotes from `files/sbt/quotes/`, splits each quote by extracting objects defined in `config.json`, uploads split files to subfolders (`{ObjectName}/`), and moves originals to `Original/`. Uses `ThreadPoolExecutor` with 4 workers. Generates a dummy charge when `QuoteCharges` is empty.
- **`AzureDataFactory`** — Triggers Azure Data Factory pipelines via REST API.
- **`LoggerAPI`** — Wraps Azure Application Insights logging via opencensus.
- **`CustomError`** — Exception class carrying the failed function name.

## Configuration

- `app/config.py` — Loads all credentials/settings from environment variables (`.env` file via `python-dotenv`).
- `app/config.json` — Defines `key_field` (default: `"QuoteId"`) and `extract_objects` array (objects to extract from each quote).
- `app/.env` — Azure credentials and connection settings (not committed).

## Key Details

- Python 3.12, timezone set to `Europe/Stockholm` in Docker.
- The `app/quotes/` directory contains local sample data organized by object type (`QuoteCharges/`, `QuoteGroups/`, `QuoteItems/`).
- Blob path convention: `files/sbt/quotes/{ObjectName}/{timestamp}_{quoteId}_{ObjectName}.json`.
- Blobs in `Archive/` and `Original/` folders are skipped during processing.
- Files with more than 2 underscore-separated parts in the filename are treated as already split and skipped.
