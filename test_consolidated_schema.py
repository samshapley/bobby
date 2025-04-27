#!/usr/bin/env python3
"""
Test Script for Consolidated Schema Implementation

This script tests the consolidated schema implementation by extracting a small
amount of data and creating a database using the new schema.
"""

import os
import sys
import logging
import sqlite3
import argparse
from datetime import datetime

# Import the necessary modules
from update_database import create_sqlite_database, ensure_directories
from data_extractors.crime_extraction import extract_crime_data
from data_extractors.force_extraction import extract_force_data 
from data_extractors.stop_search_extraction import extract_stop_search_data
from data_extractors.neighborhood_extraction import extract_neighborhood_data
from police_api_extractor import UKPoliceAPIClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("test_schema.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("test_schema")

def main(args):
    """Run a test of the consolidated schema."""
    logger.info("Starting test of consolidated schema implementation")
    
    # Create necessary directories
    ensure_directories([args.csv_dir, os.path.dirname(args.db_path), "test_logs"])
    
    # Create API client
    client = UKPoliceAPIClient(timeout=args.timeout)
    
    # Extract a small amount of test data
    all_filepaths = []
    
    # Get the latest date
    logger.info("Getting the latest available date")
    try:
        availability = client.check_availability()
        latest_date = None
        
        if isinstance(availability, dict) and "date" in availability:
            dates = availability["date"]
            if dates:
                latest_date = dates[0]  # First date is most recent
        elif isinstance(availability, list):
            if availability and isinstance(availability[0], dict) and "date" in availability[0]:
                latest_date = availability[0]["date"]
            elif availability and isinstance(availability[0], str):
                latest_date = availability[0]
                
        if not latest_date:
            # Fallback to a default date
            latest_date = "2023-01"
            logger.warning(f"Could not determine latest date, using fallback: {latest_date}")
        
        logger.info(f"Using date: {latest_date}")
    except Exception as e:
        logger.error(f"Error getting latest date: {e}")
        latest_date = "2023-01"
        logger.info(f"Using fallback date: {latest_date}")
    
    # Define a single test city for limited data extraction
    test_cities = [
        {"name": "london", "lat": 51.5074, "lng": -0.1278}
    ]
    
    # Extract crime data with minimal scope
    logger.info("Extracting crime data")
    crime_filepaths = extract_crime_data(
        client,
        latest_date,
        output_dir=args.csv_dir,
        cities=test_cities,
        collect_no_location=True,
        collect_at_location=True
    )
    all_filepaths.extend(crime_filepaths)
    logger.info(f"Extracted {len(crime_filepaths)} crime-related files")
    
    # Extract force data (minimal scope)
    logger.info("Extracting force data")
    force_filepaths = extract_force_data(
        client,
        output_dir=args.csv_dir
    )
    all_filepaths.extend(force_filepaths)
    logger.info(f"Extracted {len(force_filepaths)} force-related files")
    
    # Extract stop and search data with minimal scope
    logger.info("Extracting stop and search data")
    stops_filepaths = extract_stop_search_data(
        client,
        latest_date,
        output_dir=args.csv_dir,
        cities=test_cities,
        collect_no_location=True,
        collect_by_area=True,
        collect_at_location=True
    )
    all_filepaths.extend(stops_filepaths)
    logger.info(f"Extracted {len(stops_filepaths)} stop and search files")
    
    # Extract neighborhood data with minimal scope
    logger.info("Extracting neighborhood data")
    neighborhood_filepaths = extract_neighborhood_data(
        client,
        output_dir=args.csv_dir,
        neighborhood_depth=1,  # Just one neighborhood per force
        collect_boundaries=True,
        collect_teams=True,
        collect_events=True,
        collect_priorities=True
    )
    all_filepaths.extend(neighborhood_filepaths)
    logger.info(f"Extracted {len(neighborhood_filepaths)} neighborhood files")
    
    # Create the database with consolidated schema
    logger.info("Creating database with consolidated schema")
    create_sqlite_database(
        all_filepaths,
        db_path=args.db_path,
        replace_existing=args.replace_db,
        schema_path=args.schema_path,
        use_consolidated_schema=True
    )
    
    # Verify the database was created with the consolidated schema
    logger.info("Verifying database with consolidated schema")
    try:
        conn = sqlite3.connect(args.db_path)
        cursor = conn.cursor()
        
        # Check for consolidated tables
        consolidated_tables = [
            "crimes", "outcomes", "stops", "police_forces", "senior_officers",
            "neighborhoods", "neighborhood_boundaries", "neighborhood_teams",
            "neighborhood_events", "neighborhood_priorities", "crime_categories",
            "data_updates"
        ]
        
        for table in consolidated_tables:
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'")
            if cursor.fetchone():
                logger.info(f"✅ Table '{table}' exists in database")
            else:
                logger.warning(f"❌ Table '{table}' does NOT exist in database")
        
        # Run some test queries to verify data was imported correctly
        logger.info("Running test queries")
        
        # Test crimes table
        try:
            cursor.execute("SELECT COUNT(*) FROM crimes")
            count = cursor.fetchone()[0]
            logger.info(f"Crimes table has {count} rows")
            
            cursor.execute("SELECT city, data_date, COUNT(*) FROM crimes GROUP BY city, data_date")
            for row in cursor.fetchall():
                logger.info(f"- {row[0]} ({row[1]}): {row[2]} crimes")
        except Exception as e:
            logger.error(f"Error querying crimes table: {e}")
        
        # Test stops table
        try:
            cursor.execute("SELECT COUNT(*) FROM stops")
            count = cursor.fetchone()[0]
            logger.info(f"Stops table has {count} rows")
            
            cursor.execute("SELECT stop_type, COUNT(*) FROM stops GROUP BY stop_type")
            for row in cursor.fetchall():
                logger.info(f"- {row[0]}: {row[1]} stops")
        except Exception as e:
            logger.error(f"Error querying stops table: {e}")
        
        # Test neighborhoods table
        try:
            cursor.execute("SELECT COUNT(*) FROM neighborhoods")
            count = cursor.fetchone()[0]
            logger.info(f"Neighborhoods table has {count} rows")
            
            cursor.execute("SELECT force_id, COUNT(*) FROM neighborhoods GROUP BY force_id")
            for row in cursor.fetchall():
                logger.info(f"- Force '{row[0]}': {row[1]} neighborhoods")
        except Exception as e:
            logger.error(f"Error querying neighborhoods table: {e}")
        
        cursor.close()
        conn.close()
        
        logger.info("Test completed successfully!")
        
    except Exception as e:
        logger.error(f"Error verifying database: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test the consolidated schema implementation")
    parser.add_argument("--csv-dir", default="test_csv_data", help="Directory for CSV files")
    parser.add_argument("--db-path", default="test_db_data/test_consolidated.db", help="Path for the SQLite database")
    parser.add_argument("--timeout", type=int, default=120, help="API request timeout in seconds")
    parser.add_argument("--replace-db", action="store_true", help="Replace existing database if it exists")
    parser.add_argument("--schema-path", default="schema/consolidated_schema.sql", help="Path to the schema SQL file")
    
    args = parser.parse_args()
    main(args)