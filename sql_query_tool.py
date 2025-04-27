#!/usr/bin/env python3
"""
SQL Query Tool for UK Police API Data

This script provides a command-line interface to run SQL queries against the
SQLite database created by the data_pull_script.py script.
"""

import os
import sys
import argparse
import sqlite3
import pandas as pd
import logging
import re
from tabulate import tabulate

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("query_tool.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("query_tool")

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
    
    # Check if using consolidated schema
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='crimes'")
    using_consolidated = cursor.fetchone() is not None
    conn.close()
    
    if using_consolidated:
        print("Using consolidated schema. Examples:")
        print("  - SELECT * FROM crimes WHERE city='london' AND data_date='2023-01' LIMIT 5;")
        print("  - SELECT category, COUNT(*) FROM crimes WHERE city='manchester' GROUP BY category;")
        print("  - SELECT * FROM stops WHERE force_id='metropolitan' AND data_date='2023-01';")
    else:
        print("Using original schema with separate tables for each city/date.")
        print("  - SELECT * FROM crimes_london_2023_01 LIMIT 5;")
    
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
                
                if using_consolidated:
                    print("\nConsolidated Schema Examples:")
                    print("  SELECT * FROM crimes WHERE city='london' AND data_date='2023-01' LIMIT 5;")
                    print("  SELECT category, COUNT(*) as count FROM crimes WHERE city='manchester' GROUP BY category;")
                    print("  SELECT officer_defined_ethnicity, COUNT(*) as count FROM stops")
                    print("    WHERE force_id='metropolitan' GROUP BY officer_defined_ethnicity;")
                    print("  SELECT n.name, COUNT(p.id) FROM neighborhoods n")
                    print("    JOIN neighborhood_priorities p ON n.force_id=p.force_id AND n.neighborhood_id=p.neighborhood_id")
                    print("    WHERE n.force_id='metropolitan' GROUP BY n.name;")
                else:
                    print("\nOriginal Schema Examples:")
                    print("  SELECT * FROM crimes_london_2023_01 LIMIT 5;")
                    print("  SELECT category, COUNT(*) as count FROM crimes_manchester_2023_01 GROUP BY category;")
                    print("  SELECT * FROM stops_metropolitan_2023_01 LIMIT 5;")
                
                print("\nMulti-line queries are supported - continue typing until you end with a semicolon ';'")
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
    """Main function to run the SQL query tool."""
    parser = argparse.ArgumentParser(description="Run SQL queries against the UK Police data SQLite database.")
    parser.add_argument("--db-path", default="db_data/police_data.db", help="Path to the SQLite database")
    
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    # List tables command
    list_parser = subparsers.add_parser("list-tables", help="List all tables in the database")
    
    # Describe table command
    describe_parser = subparsers.add_parser("describe", help="Describe a specific table")
    describe_parser.add_argument("table_name", help="Name of the table to describe")
    
    # Query command
    query_parser = subparsers.add_parser("query", help="Run a SQL query")
    query_parser.add_argument("--query", help="SQL query to run")
    query_parser.add_argument("--query-file", help="File containing SQL query to run")
    query_parser.add_argument("--output", help="Output file for query results")
    query_parser.add_argument("--format", choices=["csv", "json", "excel"], default="csv", help="Output format")
    query_parser.add_argument("--limit", type=int, help="Limit the number of results returned")
    
    # Interactive mode
    interactive_parser = subparsers.add_parser("interactive", help="Run in interactive mode")
    
    args = parser.parse_args()
    
    # Check if the database exists
    if not os.path.exists(args.db_path):
        logger.error(f"Database not found at {args.db_path}. Run data_pull_script.py first.")
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
    
    elif args.command == "interactive" or not args.command:
        interactive_mode(args.db_path)
    
    else:
        parser.print_help()
    
    return 0

if __name__ == "__main__":
    sys.exit(main())