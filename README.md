# Data Ingestion Subsystem

A Python ETL pipeline for ingesting data from REST APIs into PostgreSQL.

## Overview

This project reads data from the [Open Library API](https://openlibrary.org/developers/api), validates and cleans it, and loads it into PostgreSQL staging tables.

## Project Structure

```
ingestion/
├── src/
│   ├── config.py       # Configuration loading
│   ├── readers/        # API reader
│   ├── validate.py     # Data validation
│   ├── clean.py        # Data cleaning
│   ├── load.py         # Database loader
│   ├── logger.py       # Logging
│   └── main.py         # Pipeline orchestration
├── config/
│   └── sources.yml     # API source definitions
├── requirements.txt
└── .env.example
```

## Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Configure database
cp .env.example .env
# Edit .env with your PostgreSQL credentials

# Initialize database tables
cd src
python main.py --init-db --config ../config/sources.yml

# Run ingestion
python main.py --config ../config/sources.yml
```

## API Sources

| Source | Query | Records |
|--------|-------|---------|
| `openlibrary_python_books` | Python programming | 100 |
| `openlibrary_scifi_books` | Science fiction | 100 |
| `openlibrary_classics` | Classic literature | 100 |

## Data Fields

| Field | Type | Description |
|-------|------|-------------|
| key | str | Open Library ID (primary key) |
| title | str | Book title |
| subtitle | str | Book subtitle |
| author_name | str | Authors (comma-separated) |
| first_publish_year | int | Year first published |
| edition_count | int | Number of editions |
| language | str | Languages (comma-separated) |
| publisher | str | Publishers (comma-separated) |
| publish_date | str | Publication dates |
| isbn | str | ISBN numbers (comma-separated) |
| number_of_pages_median | int | Typical page count |
| ratings_average | float | Average rating |
| ratings_count | int | Number of ratings |
| already_read_count | int | Users who finished it |
| subject | str | Subjects (comma-separated) |
| has_fulltext | bool | Full text available |
