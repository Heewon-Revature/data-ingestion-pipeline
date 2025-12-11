# Data Ingestion Pipeline

A Python ETL pipeline that fetches book data from Open Library API, cleans and validates it, and loads into PostgreSQL.

## Project Structure

```
ingestion/
├── src/
│   ├── config.py      # Configuration loader
│   ├── readers/       # API reader
│   ├── clean.py       # Data cleaning
│   ├── validate.py    # Data validation
│   ├── load.py        # Database loader
│   └── main.py        # Pipeline orchestration
├── config/
│   └── sources.yml    # Source definitions
└── requirements.txt
```

## Setup

```bash
pip install -r requirements.txt
```

## Run

```bash
cd src
python main.py --init-db --config ../config/sources.yml
python main.py --config ../config/sources.yml
```

## Features

- **Extract**: Fetches from 10 Open Library subjects
- **Clean**: Flattens lists, strips whitespace, normalizes nulls, removes duplicates
- **Validate**: Type casting, rule-based validation (NOT NULL, comparisons, len())
- **Load**: Upserts to PostgreSQL, saves rejects separately
