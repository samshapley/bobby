# UK Police Data Extraction and SQL Query Tool

This package provides two scripts for extracting data from the UK Police API and running SQL queries against it:

1. `data_pull_script.py` - Extracts the most recent data from the UK Police API and stores it in CSV files and an SQLite database
2. `sql_query_tool.py` - Provides a command-line interface to run SQL queries against the SQLite database

## Prerequisites

Make sure you have the following installed:

- Python 3.7 or newer
- Required Python packages (install with `pip install -r requirements.txt`):
  - requests
  - pandas
  - sqlite3
  - tabulate

Additionally, you need the UK Police API Extractor library that's already in this repository.

## Installation

1. Clone this repository and navigate to the directory:

```bash
git clone <repository_url>
cd <repository_directory>
```

2. Install the required dependencies:

```bash
pip install -r requirements.txt
```

3. Make the scripts executable (Linux/Mac):

```bash
chmod +x data_pull_script.py sql_query_tool.py
```

## Data Extraction

### Extracting Latest Police Data

To pull the most recent data from the UK Police API:

```bash
python data_pull_script.py
```

By default, this will:

1. Create necessary directories (`csv_data`, `db_data`, and `logs`)
2. Extract the latest available data from the UK Police API for:
   - Crime data for major UK cities
   - Police force information and senior officers
   - Neighborhood data for various police forces
   - Stop and search data for various police forces
3. Save the data as CSV files in the `csv_data` directory
4. Create an SQLite database at `db_data/police_data.db`

### Important Notes

- The UK Police API provides data on a monthly basis, so "latest data" actually means the most recent available month.
- The API may have rate limits, so the extraction could take some time to complete.
- If extraction for certain endpoints fails, the script will log the errors and continue with other extractions.

### Customizing Data Extraction

You can customize the extraction by providing command-line arguments:

```bash
python data_pull_script.py --csv-dir custom_csv_directory --db-path custom_db_path.db
```

## Running SQL Queries

The `sql_query_tool.py` script provides several ways to interact with the extracted data.

### Interactive Mode

The easiest way to explore the data is using the interactive mode:

```bash
python sql_query_tool.py interactive
```

This will open an interactive session where you can:
- Type `tables;` to list all tables in the database
- Type `describe TABLE_NAME;` to get information about a specific table
- Type `help;` to see available commands
- Type any SQL query ending with a semicolon to execute it

### Listing Tables

To list all tables in the database:

```bash
python sql_query_tool.py list-tables
```

Example output:
```
+------------------------+-----------+---------+--------------------------------------------+
| Table Name             | Row Count | Columns | Column Names                               |
|------------------------+-----------+---------+--------------------------------------------|
| crimes_london_2025_02  | 3245      | 12      | category, location_type, location_lati... |
| police_forces_20250426 | 45        | 3       | id, name, description, ...                |
| ...                    | ...       | ...     | ...                                        |
+------------------------+-----------+---------+--------------------------------------------+
```

### Describing Tables

To get detailed information about a specific table:

```bash
python sql_query_tool.py describe crimes_london_2025_02
```

This will show:
- Column names, types, and constraints
- Sample data (first 5 rows)
- Total number of rows

### Running SQL Queries

You can run SQL queries in several ways:

1. Directly from the command line:

```bash
python sql_query_tool.py query --query "SELECT category, COUNT(*) as count FROM crimes_london_2025_02 GROUP BY category ORDER BY count DESC;"
```

2. From a file containing the SQL query:

```bash
python sql_query_tool.py query --query-file my_query.sql
```

3. Limit the number of results:

```bash
python sql_query_tool.py query --query "SELECT * FROM crimes_london_2025_02;" --limit 10
```

4. Save the results to a file:

```bash
python sql_query_tool.py query --query "SELECT * FROM crimes_london_2025_02;" --output results.csv --format csv
```

Supported output formats are `csv` (default), `json`, and `excel`.

## Example SQL Queries

Here are some example SQL queries to get you started:

### Crime Counts by Category

```sql
SELECT category, COUNT(*) as count 
FROM crimes_london_2025_02 
GROUP BY category 
ORDER BY count DESC;
```

### Comparison of Crime Types Across Cities

```sql
SELECT 
    'london' as city, 
    category, 
    COUNT(*) as count 
FROM 
    crimes_london_2025_02 
GROUP BY 
    category
UNION ALL
SELECT 
    'manchester' as city, 
    category, 
    COUNT(*) as count 
FROM 
    crimes_manchester_2025_02 
GROUP BY 
    category
ORDER BY 
    city, 
    count DESC;
```

### Join Forces and Their Senior Officers

```sql
SELECT 
    f.name as force_name, 
    o.name as officer_name, 
    o.rank 
FROM 
    police_forces_20250426 f
JOIN 
    senior_officers_20250426 o
ON 
    f.id = o.force_id;
```

## Troubleshooting

- If you encounter errors during data extraction, check the `data_pull.log` file for details.
- If the database doesn't exist, run the `data_pull_script.py` script first.
- For SQL query errors, check the query syntax or try describing the table first to verify column names.

## Limitations

- The UK Police API provides data on a monthly basis, not daily or hourly.
- Some endpoints might return empty results for certain areas or time periods.
- The API has rate limits, so excessive querying might lead to temporary blocks.