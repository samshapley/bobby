#!/usr/bin/env python3
"""
UK Police Data Extractor and SQL Query Tool

This script provides a command-line interface to extract data from the UK Police API
and query it using SQL. It combines functionality from both data_pull_script.py and
sql_query_tool.py into a single convenient script.
"""

import os
import sys
import logging
import argparse
import sqlite3
import pandas as pd
import re
from datetime import datetime, timedelta
from tabulate import tabulate

# Import the police API extractor library
from police_api_extractor import (
    UKPoliceAPIClient,
    CrimeExtractor,
    NeighborhoodExtractor,
    StopsExtractor,
    ForceExtractor
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("extract_police_data.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("extract_police_data")

def ensure_directories(dirs=None):
    """Create necessary directories for data storage."""
    if dirs is None:
        dirs = ["csv_data", "db_data", "logs"]
    
    for directory in dirs:
        os.makedirs(directory, exist_ok=True)
        logger.info(f"Ensured directory exists: {directory}")

def get_latest_available_date(client):
    """Get the latest available date for the crime data."""
    try:
        availability = client.check_availability()
        
        if isinstance(availability, dict) and "date" in availability:
            dates = availability["date"]
            if dates:
                return dates[0]  # First date is most recent
        elif isinstance(availability, list):
            if availability:
                if isinstance(availability[0], dict) and "date" in availability[0]:
                    return availability[0]["date"]
                elif isinstance(availability[0], str):
                    return availability[0]
                    
        # If we can't determine the latest date, use a fallback
        logger.warning("Could not determine latest available date, using fallback")
        # Get current date and go back to the previous month
        today = datetime.now()
        previous_month = today.replace(day=1) - timedelta(days=1)
        return previous_month.strftime("%Y-%m")
    except Exception as e:
        logger.error(f"Error getting latest available date: {e}")
        # Fallback to previous month
        today = datetime.now()
        previous_month = today.replace(day=1) - timedelta(days=1)
        return previous_month.strftime("%Y-%m")

def extract_crime_data(client, latest_date, output_dir="csv_data", cities=None):
    """Extract crime data for major cities."""
    logger.info(f"Extracting crime data for date: {latest_date}")
    
    crime_extractor = CrimeExtractor(client=client)
    
    # List of major UK cities with their approximate coordinates
    if cities is None:
        cities = [
            {"name": "london", "lat": 51.5074, "lng": -0.1278},
            {"name": "manchester", "lat": 53.4808, "lng": -2.2426},
            {"name": "birmingham", "lat": 52.4862, "lng": -1.8904},
            {"name": "leeds", "lat": 53.8008, "lng": -1.5491},
            {"name": "liverpool", "lat": 53.4084, "lng": -2.9916}
        ]
    
    filepaths = []
    
    # Get crime data for each city
    for city in cities:
        try:
            logger.info(f"Extracting crime data for {city['name']}")
            
            # Extract street-level crimes
            _, filepath = crime_extractor.extract_street_crimes_to_csv(
                lat=city["lat"],
                lng=city["lng"],
                date=latest_date,
                output_dir=output_dir,
                filename=f"crimes_{city['name']}_{latest_date}"
            )
            if filepath:
                filepaths.append(filepath)
                logger.info(f"Successfully extracted crime data for {city['name']}")
            
            # Extract street-level outcomes
            _, filepath = crime_extractor.extract_street_outcomes_to_csv(
                lat=city["lat"],
                lng=city["lng"],
                date=latest_date,
                output_dir=output_dir,
                filename=f"outcomes_{city['name']}_{latest_date}"
            )
            if filepath:
                filepaths.append(filepath)
                logger.info(f"Successfully extracted outcome data for {city['name']}")
            
        except Exception as e:
            logger.error(f"Error extracting crime data for {city['name']}: {e}")
    
    # Get crime categories
    try:
        categories = crime_extractor.get_crime_categories(date=latest_date)
        categories_filepath = crime_extractor.save_to_csv(
            data=categories,
            filename=f"crime_categories_{latest_date}",
            output_dir=output_dir
        )
        if categories_filepath:
            filepaths.append(categories_filepath)
    except Exception as e:
        logger.error(f"Error extracting crime categories: {e}")
    
    return filepaths

def extract_force_data(client, output_dir="csv_data", limit=5):
    """Extract police force data, limiting detailed extraction to top N forces."""
    logger.info("Extracting police force data")
    
    force_extractor = ForceExtractor(client=client)
    filepaths = []
    
    try:
        # Get list of all police forces
        data, filepath = force_extractor.extract_forces_to_csv(output_dir=output_dir)
        if filepath:
            filepaths.append(filepath)
            logger.info(f"Extracted data for {len(data)} police forces")
        
        # Get detailed information for a limited number of forces
        force_sample = data[:limit] if limit else data
        for force in force_sample:
            try:
                force_id = force.get("id")
                if force_id:
                    # Extract force details
                    _, details_filepath = force_extractor.extract_force_details_to_csv(
                        force_id=force_id, 
                        output_dir=output_dir
                    )
                    if details_filepath:
                        filepaths.append(details_filepath)
                    
                    # Extract senior officers
                    _, officers_filepath = force_extractor.extract_senior_officers_to_csv(
                        force_id=force_id, 
                        output_dir=output_dir
                    )
                    if officers_filepath:
                        filepaths.append(officers_filepath)
            except Exception as e:
                logger.error(f"Error extracting data for force '{force_id}': {e}")
    except Exception as e:
        logger.error(f"Error extracting force data: {e}")
    
    return filepaths

def extract_neighborhood_data(client, output_dir="csv_data", force_limit=3, neighborhood_limit=2):
    """Extract neighborhood data, limiting extraction to manageable numbers."""
    logger.info("Extracting neighborhood data")
    
    neighborhood_extractor = NeighborhoodExtractor(client=client)
    filepaths = []
    
    try:
        # Get list of all police forces
        forces = neighborhood_extractor.get_forces()
        force_sample = forces[:force_limit] if force_limit else forces
        
        # Get neighborhoods for each force
        for force in force_sample:
            try:
                force_id = force.get("id")
                if force_id:
                    # Extract all neighborhoods for this force
                    data, filepath = neighborhood_extractor.extract_all_neighborhoods_to_csv(
                        force_id=force_id,
                        output_dir=output_dir
                    )
                    if filepath:
                        filepaths.append(filepath)
                        logger.info(f"Extracted {len(data)} neighborhoods for force '{force_id}'")
                    
                    # For a limited number of neighborhoods of each force, get additional details
                    neighborhoods = data[:neighborhood_limit] if neighborhood_limit else data
                    for neighborhood in neighborhoods:
                        try:
                            neighborhood_id = neighborhood.get("id")
                            if neighborhood_id:
                                # Get neighborhood details
                                details = neighborhood_extractor.get_neighborhood_details(
                                    force_id=force_id,
                                    neighborhood_id=neighborhood_id
                                )
                                details_filepath = neighborhood_extractor.save_to_csv(
                                    data=details,
                                    filename=f"neighborhood_details_{force_id}_{neighborhood_id}",
                                    output_dir=output_dir
                                )
                                if details_filepath:
                                    filepaths.append(details_filepath)
                                
                                # Get neighborhood boundary
                                try:
                                    boundary = neighborhood_extractor.get_neighborhood_boundary(
                                        force_id=force_id,
                                        neighborhood_id=neighborhood_id
                                    )
                                    boundary_filepath = neighborhood_extractor.save_to_csv(
                                        data=boundary,
                                        filename=f"neighborhood_boundary_{force_id}_{neighborhood_id}",
                                        output_dir=output_dir
                                    )
                                    if boundary_filepath:
                                        filepaths.append(boundary_filepath)
                                except Exception as e:
                                    logger.error(f"Error getting boundary for neighborhood '{neighborhood_id}': {e}")
                        except Exception as e:
                            logger.error(f"Error extracting details for neighborhood '{neighborhood_id}': {e}")
            except Exception as e:
                logger.error(f"Error extracting neighborhoods for force '{force_id}': {e}")
    except Exception as e:
        logger.error(f"Error extracting neighborhood data: {e}")
    
    return filepaths

def extract_stops_data(client, latest_date, output_dir="csv_data", force_limit=3):
    """Extract stop and search data, limiting extraction to manageable numbers."""
    logger.info(f"Extracting stop and search data for date: {latest_date}")
    
    stops_extractor = StopsExtractor(client=client)
    filepaths = []
    
    try:
        # Get list of all police forces
        forces = stops_extractor.get_forces()
        force_sample = forces[:force_limit] if force_limit else forces
        
        # Get stop and search data for each force
        for force in force_sample:
            try:
                force_id = force.get("id")
                if force_id:
                    # Extract stop and search data for this force
                    data, filepath = stops_extractor.extract_stops_by_force_to_csv(
                        force_id=force_id,
                        date=latest_date,
                        output_dir=output_dir,
                        filename=f"stops_{force_id}_{latest_date}"
                    )
                    if filepath:
                        filepaths.append(filepath)
                        logger.info(f"Extracted {len(data)} stop and searches for force '{force_id}'")
                    
                    # Extract stops with no location for this force
                    try:
                        data, filepath = stops_extractor.extract_stops_by_force_to_csv(
                            force_id=force_id,
                            date=latest_date,
                            output_dir=output_dir,
                            filename=f"stops_no_location_{force_id}_{latest_date}"
                        )
                        if filepath:
                            filepaths.append(filepath)
                    except Exception as e:
                        logger.error(f"Error extracting stops with no location for force '{force_id}': {e}")
            except Exception as e:
                logger.error(f"Error extracting stops for force '{force_id}': {e}")
    except Exception as e:
        logger.error(f"Error extracting stop and search data: {e}")
    
    return filepaths

def extract_all_data(output_dir="csv_data", db_path="db_data/police_data.db"):
    """Extract all available data from the UK Police API."""
    logger.info("Starting extraction of all data")
    
    # Create a client with extended timeout
    client = UKPoliceAPIClient(timeout=120)
    
    # Get the latest available date
    latest_date = get_latest_available_date(client)
    logger.info(f"Latest available date: {latest_date}")
    
    # Extract all data types
    all_filepaths = []
    
    # Extract crime data
    crime_filepaths = extract_crime_data(client, latest_date, output_dir)
    all_filepaths.extend(crime_filepaths)
    logger.info(f"Extracted {len(crime_filepaths)} crime data files")
    
    # Extract force data
    force_filepaths = extract_force_data(client, output_dir)
    all_filepaths.extend(force_filepaths)
    logger.info(f"Extracted {len(force_filepaths)} force data files")
    
    # Extract neighborhood data
    neighborhood_filepaths = extract_neighborhood_data(client, output_dir)
    all_filepaths.extend(neighborhood_filepaths)
    logger.info(f"Extracted {len(neighborhood_filepaths)} neighborhood data files")
    
    # Extract stop and search data
    stops_filepaths = extract_stops_data(client, latest_date, output_dir)
    all_filepaths.extend(stops_filepaths)
    logger.info(f"Extracted {len(stops_filepaths)} stop and search data files")
    
    logger.info(f"Total extracted files: {len(all_filepaths)}")
    
    # Create SQLite database
    create_sqlite_database(all_filepaths, db_path)
    
    return all_filepaths, db_path

def create_sqlite_database(csv_filepaths, db_path="db_data/police_data.db"):
    """Create an SQLite database from the extracted CSV files."""
    logger.info(f"Creating SQLite database at {db_path}")
    
    # Make sure the directory exists
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    # Create or connect to the SQLite database
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Process each CSV file
        for filepath in csv_filepaths:
            try:
                # Skip if filepath is empty
                if not filepath:
                    continue
                
                # Extract table name from the filepath
                filename = os.path.basename(filepath)
                base_name = os.path.splitext(filename)[0]
                
                # Clean up table name (remove special characters and spaces)
                table_name = base_name.replace('-', '_').replace(' ', '_').lower()
                
                # Read the CSV file
                logger.info(f"Importing {filepath} into table {table_name}")
                df = pd.read_csv(filepath)
                
                # Write to SQLite
                df.to_sql(table_name, conn, if_exists="replace", index=False)
                logger.info(f"Successfully imported {len(df)} rows into {table_name}")
            except Exception as e:
                logger.error(f"Error importing {filepath}: {e}")
        
        # Close the connection
        conn.close()
        logger.info("Database creation completed")
        return True
    except Exception as e:
        logger.error(f"Error creating database: {e}")
        return False

def list_tables(db_path):
    """List all tables in the SQLite database."""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Query to get all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        if not tables:
            print("No tables found in the database.")
            return None
        
        # Get count of rows in each table
        table_info = []
        for table in tables:
            table_name = table[0]
            cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
            row_count = cursor.fetchone()[0]
            
            # Get column names
            cursor.execute(f"PRAGMA table_info({table_name});")
            columns = cursor.fetchall()
            column_names = [col[1] for col in columns]
            
            table_info.append({
                "Table Name": table_name,
                "Row Count": row_count,
                "Columns": len(column_names),
                "Column Names": ", ".join(column_names[:5]) + ("..." if len(column_names) > 5 else "")
            })
        
        conn.close()
        return table_info
    except Exception as e:
        logger.error(f"Error listing tables: {e}")
        return None

def describe_table(db_path, table_name):
    """Describe a specific table in the SQLite database."""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Check if table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table_name,))
        if not cursor.fetchone():
            print(f"Table '{table_name}' not found in the database.")
            return None
        
        # Get column information
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = cursor.fetchall()
        
        column_info = []
        for col in columns:
            column_info.append({
                "Column ID": col[0],
                "Name": col[1],
                "Type": col[2],
                "Not Null": "Yes" if col[3] else "No",
                "Default Value": col[4] or "None",
                "Primary Key": "Yes" if col[5] else "No"
            })
        
        # Get sample data
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 5;")
        sample_data = cursor.fetchall()
        
        # Get row count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
        row_count = cursor.fetchone()[0]
        
        conn.close()
        return {
            "table_name": table_name,
            "columns": column_info,
            "sample_data": sample_data,
            "column_names": [col[1] for col in columns],
            "row_count": row_count
        }
    except Exception as e:
        logger.error(f"Error describing table '{table_name}': {e}")
        return None

def run_query(db_path, query, limit=None):
    """Run a SQL query against the SQLite database."""
    try:
        conn = sqlite3.connect(db_path)
        
        # Apply LIMIT clause if specified and not already in the query
        if limit is not None and not re.search(r'\bLIMIT\s+\d+\b', query, re.IGNORECASE):
            query = query.rstrip(';')
            query = f"{query} LIMIT {limit};"
        
        # Execute query and load results into a pandas DataFrame
        results = pd.read_sql_query(query, conn)
        conn.close()
        
        return results
    except Exception as e:
        logger.error(f"Error running query: {e}")
        return None

def save_results(results, output_file, format="csv"):
    """Save query results to a file."""
    try:
        if format == "csv":
            results.to_csv(output_file, index=False)
        elif format == "json":
            results.to_json(output_file, orient="records", lines=True)
        elif format == "excel":
            results.to_excel(output_file, index=False)
        else:
            logger.error(f"Unsupported format: {format}")
            return False
        
        logger.info(f"Results saved to {output_file}")
        return True
    except Exception as e:
        logger.error(f"Error saving results: {e}")
        return False

def interactive_mode(db_path):
    """Run in interactive mode, allowing multiple queries."""
    print("\n--- UK Police Data SQL Query Tool (Interactive Mode) ---")
    print("Type 'exit', 'quit', or press Ctrl+D to exit.")
    print("Type 'tables' to list all tables.")
    print("Type 'describe TABLE_NAME' to get table information.")
    print("Type 'help' for more commands.\n")
    
    history = []
    
    while True:
        try:
            # Get user input (support multi-line SQL)
            lines = []
            prompt = "sql> "
            
            while True:
                try:
                    line = input(prompt)
                    
                    # Check for exit commands
                    if not lines and line.lower() in ("exit", "quit"):
                        return
                    
                    # Add to lines
                    lines.append(line)
                    
                    # Check if the query is complete
                    if line.strip().endswith(";"):
                        break
                    
                    # Change the prompt for continuation lines
                    prompt = "...> "
                except EOFError:
                    return
            
            # Join lines to form the query
            query_input = " ".join(lines).strip()
            
            # Process special commands
            if query_input.lower() == "tables;":
                table_info = list_tables(db_path)
                if table_info:
                    print(tabulate(table_info, headers="keys", tablefmt="psql"))
                continue
            
            if query_input.lower().startswith("describe ") and query_input.endswith(";"):
                table_name = query_input[9:-1].strip()
                table_data = describe_table(db_path, table_name)
                if table_data:
                    print(f"\nTable: {table_data['table_name']} ({table_data['row_count']} rows)")
                    print("\nColumns:")
                    print(tabulate(table_data["columns"], headers="keys", tablefmt="psql"))
                    
                    if table_data["sample_data"]:
                        print("\nSample Data (5 rows):")
                        print(tabulate(
                            table_data["sample_data"],
                            headers=table_data["column_names"],
                            tablefmt="psql"
                        ))
                continue
            
            if query_input.lower() == "help;":
                print("\nAvailable Commands:")
                print("  tables;                - List all tables in the database")
                print("  describe TABLE_NAME;   - Show detailed information about a table")
                print("  help;                  - Show this help message")
                print("  exit; or quit;         - Exit the interactive mode")
                print("\nAny other input will be treated as a SQL query.")
                print("Multi-line queries are supported - continue typing until you end with a semicolon ';'")
                continue
            
            # Execute the query if it's not a special command
            results = run_query(db_path, query_input)
            if results is not None:
                # Add to history
                history.append(query_input)
                
                print(f"\nResults ({len(results)} rows):")
                
                # For large results, only show a portion
                if len(results) > 20:
                    print(tabulate(results.head(20), headers="keys", tablefmt="psql"))
                    print(f"... {len(results) - 20} more rows ...")
                else:
                    print(tabulate(results, headers="keys", tablefmt="psql"))
                
                # Ask if user wants to save results
                save_choice = input("\nSave results? [y/N]: ").lower()
                if save_choice.startswith("y"):
                    format_choice = input("Format [csv/json/excel] (default: csv): ").lower()
                    if not format_choice:
                        format_choice = "csv"
                    
                    if format_choice not in ["csv", "json", "excel"]:
                        print(f"Unsupported format: {format_choice}. Using CSV instead.")
                        format_choice = "csv"
                    
                    filename = input(f"Output filename (default: query_results.{format_choice}): ")
                    if not filename:
                        filename = f"query_results.{format_choice}"
                    
                    if save_results(results, filename, format_choice):
                        print(f"Results saved to {filename}")
                    else:
                        print("Failed to save results.")
        except KeyboardInterrupt:
            print("\nOperation cancelled by user.")
            continue
        except Exception as e:
            print(f"Error: {e}")

def main():
    """Main function for the script."""
    parser = argparse.ArgumentParser(description="Extract data from the UK Police API and run SQL queries against it.")
    
    # Create subparsers for different commands
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    # Extract command
    extract_parser = subparsers.add_parser("extract", help="Extract data from the UK Police API")
    extract_parser.add_argument("--csv-dir", default="csv_data", help="Directory to store CSV files")
    extract_parser.add_argument("--db-path", default="db_data/police_data.db", help="Path for the SQLite database")
    
    # Query-related commands
    query_parent_parser = argparse.ArgumentParser(add_help=False)
    query_parent_parser.add_argument("--db-path", default="db_data/police_data.db", help="Path to the SQLite database")
    
    # List tables command
    list_parser = subparsers.add_parser("list-tables", parents=[query_parent_parser], help="List all tables in the database")
    
    # Describe table command
    describe_parser = subparsers.add_parser("describe", parents=[query_parent_parser], help="Describe a specific table")
    describe_parser.add_argument("table_name", help="Name of the table to describe")
    
    # Query command
    query_parser = subparsers.add_parser("query", parents=[query_parent_parser], help="Run a SQL query")
    query_parser.add_argument("--query", help="SQL query to run")
    query_parser.add_argument("--query-file", help="File containing SQL query to run")
    query_parser.add_argument("--output", help="Output file for query results")
    query_parser.add_argument("--format", choices=["csv", "json", "excel"], default="csv", help="Output format")
    query_parser.add_argument("--limit", type=int, help="Limit the number of results returned")
    
    # Interactive mode
    interactive_parser = subparsers.add_parser("interactive", parents=[query_parent_parser], help="Run in interactive mode")
    
    args = parser.parse_args()
    
    # Process commands
    if args.command == "extract":
        ensure_directories([args.csv_dir, os.path.dirname(args.db_path), "logs"])
        extract_all_data(args.csv_dir, args.db_path)
        print(f"Data extraction complete. CSV files stored in {args.csv_dir} and database created at {args.db_path}")
        
    elif args.command in ["list-tables", "describe", "query", "interactive"]:
        # Check if the database exists
        if not os.path.exists(args.db_path):
            logger.error(f"Database not found at {args.db_path}. Run 'extract' command first.")
            return 1
        
        if args.command == "list-tables":
            table_info = list_tables(args.db_path)
            if table_info:
                print(tabulate(table_info, headers="keys", tablefmt="psql"))
        
        elif args.command == "describe":
            table_data = describe_table(args.db_path, args.table_name)
            if table_data:
                print(f"\nTable: {table_data['table_name']} ({table_data['row_count']} rows)")
                print("\nColumns:")
                print(tabulate(table_data["columns"], headers="keys", tablefmt="psql"))
                
                if table_data["sample_data"]:
                    print("\nSample Data (5 rows):")
                    print(tabulate(
                        table_data["sample_data"],
                        headers=table_data["column_names"],
                        tablefmt="psql"
                    ))
        
        elif args.command == "query":
            # Get the query from either --query or --query-file
            query = None
            if args.query:
                query = args.query
            elif args.query_file:
                try:
                    with open(args.query_file, "r") as f:
                        query = f.read()
                except Exception as e:
                    logger.error(f"Error reading query file: {e}")
                    return 1
            else:
                logger.error("Either --query or --query-file must be specified")
                return 1
            
            # Run the query
            results = run_query(args.db_path, query, args.limit)
            if results is not None:
                # Print results
                print(f"Results ({len(results)} rows):")
                if len(results) > 20:
                    print(tabulate(results.head(20), headers="keys", tablefmt="psql"))
                    print(f"... {len(results) - 20} more rows ...")
                else:
                    print(tabulate(results, headers="keys", tablefmt="psql"))
                
                # Save results if output file is specified
                if args.output:
                    if save_results(results, args.output, args.format):
                        print(f"Results saved to {args.output}")
            else:
                return 1
        
        elif args.command == "interactive":
            interactive_mode(args.db_path)
    
    else:
        parser.print_help()
    
    return 0

if __name__ == "__main__":
    sys.exit(main())