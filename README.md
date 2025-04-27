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

---

## Quickstart

### 1. Install Requirements

```bash
pip install -r requirements.txt
```

You’ll also need an [Anthropic Claude API key](https://console.anthropic.com/) for the conversational AI. Set it as an environment variable:

```bash
export ANTHROPIC_API_KEY=your_api_key_here
```

### 2. Pull the Latest Data

Run the data pull script to fetch the latest UK Police data and build the SQLite database:

```bash
python data_pull_script.py --csv-dir csv_data --db-path db_data/police_data.db
```

This will:
- Download and save all relevant data as CSVs in `csv_data/`
- Build a SQLite database at `db_data/police_data.db`

### 3. Run Bobby’s CLI

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

## Data Sources

Bobby uses the [UK Police Data API](https://data.police.uk/docs/) and covers:

- Street-level crimes & outcomes
- Police forces & senior officers
- Neighborhoods & boundaries
- Stop and search data

All data is stored locally in a SQLite database for fast querying.

---

## Developer Guide

- `data_pull_script.py`: Extracts data from the UK Police API and builds the SQLite database.
- `bobby_core.py`: Core logic for the agent (SQL, Claude API, tool calls, etc).
- `bobby_cli.py`: The interactive, animated command-line interface.
- `police_api_extractor/`: Library for extracting and flattening UK Police API data.

### Running Tests

```bash
python -m unittest discover tests
```

---

Built by Sam Shapley. Powered by [Anthropic Claude](https://www.anthropic.com/) and the [UK Police Data API](https://data.police.uk/).

## Contributing

Pull requests and issues welcome! See [CONTRIBUTING.md](CONTRIBUTING.md) for details.