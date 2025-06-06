# Bobby: UK Police Data SQL Agent

Bobby is an intelligent command-line agent for exploring, analyzing, and reporting on UK Police data. It combines a conversational AI (powered by Claude) with a rich SQLite database of UK police data, letting you ask questions in plain English and get answers, insights, and even roast-worthy stats about police departments.

## What is Bobby?

Bobby is your data detective. It pulls the latest UK Police data (crimes, outcomes, forces, neighborhoods, stop & search, and more), loads it into a local SQLite database, and lets you query it using natural language. Bobby writes and runs SQL for you, explains its reasoning, and presents results in a beautiful, interactive CLI.

**Key Features:**
- Conversational interface: Ask questions, get answers, see the agent's thought process.
- Automatic SQL: Bobby writes and executes SQL queries for you.
- Data coverage: Crimes, outcomes, police forces, neighborhoods, stop & search, and more.
- Animated, colorful CLI with progress bars, syntax highlighting, and fun police-themed effects.
- Extensible: Add new data sources, reports, or agent skills.
- **New:** Consolidated database schema for simplified querying across cities and dates.

---

## Quickstart

### 1. Install Requirements

```bash
pip install -r requirements.txt
```

You'll also need an [Anthropic Claude API key](https://console.anthropic.com/) for the conversational AI. Set it as an environment variable:

```bash
export ANTHROPIC_API_KEY=your_api_key_here
```

### 2. Pull the Latest Data

Run the data pull script to fetch the latest UK Police data and build the SQLite database:

```bash
python config_data_pull.py --config extraction_config.json
```

This will:
- Download and save all relevant data as CSVs in `csv_data/`
- Build a SQLite database at `db_data/police_data.db` using the consolidated schema
- Add appropriate metadata for simplified queries

### 3. Run Bobby's CLI

Start the interactive agent:

```bash
python bobby_cli.py --db-path db_data/police_data.db --interactive
```

Or ask a one-off question:

```bash
python bobby_cli.py --db-path db_data/police_data.db --question "Which city had the most violent crimes last month?"
```

---

## What Can I Ask Bobby?

- "Show me the top 5 crime categories in London for 2023-01."
- "Which police force had the most stop and searches in Manchester?"
- "List all neighborhoods in the West Midlands force."
- "How many crimes had no outcome in Liverpool last month?"
- "Who are the senior officers for the Met Police?"

Bobby will:
- Analyze your question
- Write and run the necessary SQL
- Show you the results, with explanations and context

---

## Database Schema

Bobby now uses a consolidated database schema that simplifies queries across cities and dates:

### Core Tables

- **crimes** - Contains all crime data with city, date, and location type columns
- **outcomes** - All outcome data with city and date columns
- **stops** - All stop and search data with city, force_id, and stop type columns

### Reference and Entity Tables

- **police_forces** - List of all police forces
- **senior_officers** - Information about senior officers with force_id
- **neighborhoods** - All neighborhoods with force_id
- **neighborhood_boundaries** - Boundary points with force_id and neighborhood_id
- **neighborhood_teams** - Team members with force_id and neighborhood_id
- **neighborhood_events** - Events with force_id and neighborhood_id
- **neighborhood_priorities** - Priorities with force_id and neighborhood_id
- **crime_categories** - Reference data for crime categories
- **data_updates** - Information about data currency

### Example Queries

With the consolidated schema, you can easily query across different cities and dates:

```sql
-- Get crime counts by category for London
SELECT category, COUNT(*) as count 
FROM crimes 
WHERE city = 'london' AND data_date = '2023-01'
GROUP BY category 
ORDER BY count DESC;

-- Compare violent crime across cities
SELECT city, COUNT(*) as violent_crimes
FROM crimes
WHERE category = 'violent-crime' AND data_date = '2023-01'
GROUP BY city 
ORDER BY violent_crimes DESC;

-- Find stops with no location for a specific force
SELECT * FROM stops
WHERE force_id = 'metropolitan' AND stop_type = 'no_location'
LIMIT 10;
```

---

## Data Sources

Bobby uses the [UK Police Data API](https://data.police.uk/docs/) and covers:

- Street-level crimes & outcomes
- Police forces & senior officers
- Neighborhoods & boundaries
- Stop and search data

All data is stored locally in a SQLite database for fast querying.

---

## Developer Guide

- `config_data_pull.py`: Extracts data from the UK Police API using configuration
- `update_database.py`: Builds the SQLite database with the consolidated schema
- `bobby_core.py`: Core logic for the agent (SQL, Claude API, tool calls, etc).
- `bobby_cli.py`: The interactive, animated command-line interface.
- `schema/consolidated_schema.sql`: The consolidated database schema definition
- `police_api_extractor/`: Library for extracting and flattening UK Police API data.

### Configuration Options

The `extraction_config.json` file includes options for the consolidated schema:

```json
"extraction": {
    "use_consolidated_schema": true,
    "schema_path": "schema/consolidated_schema.sql"
}
```

### Running Tests

```bash
python test_consolidated_schema.py  # Test the consolidated schema
python -m unittest discover tests   # Run all unit tests
```

---

Built by Sam Shapley. Powered by [Anthropic Claude](https://www.anthropic.com/) and the [UK Police Data API](https://data.police.uk/).

## Contributing

Pull requests and issues welcome! See [CONTRIBUTING.md](CONTRIBUTING.md) for details.