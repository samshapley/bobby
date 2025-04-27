#!/usr/bin/env python3
"""
Narrow Test Script for Bobby Consolidated Schema

This script creates sample data and verifies that the data is properly imported 
into the consolidated database schema. Unlike the full test script, this doesn't 
rely on API calls but instead creates test data directly.
"""

import os
import sys
import logging
import sqlite3
import pandas as pd
import argparse
from datetime import datetime
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("narrow_test.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("narrow_test")

def ensure_directories(dirs):
    """Create necessary directories if they don't exist."""
    for directory in dirs:
        os.makedirs(directory, exist_ok=True)
        logger.info(f"Ensured directory exists: {directory}")

def create_test_data(test_dir):
    """Create test data files with sample data."""
    test_files = []
    
    # 1. Create a sample crimes file
    crimes_data = pd.DataFrame({
        "category": ["violent-crime", "burglary", "theft", "anti-social-behaviour", "robbery"],
        "location_latitude": [51.5074, 51.5080, 51.5060, 51.5090, 51.5100],
        "location_longitude": [-0.1278, -0.1280, -0.1270, -0.1290, -0.1310],
        "location_street_id": [123, 124, 125, 126, 127],
        "location_street_name": ["Oxford St", "Regent St", "Bond St", "Baker St", "Piccadilly"],
        "month": ["2023-01", "2023-01", "2023-01", "2023-01", "2023-01"],
        "outcome_status": ["Under investigation", "Complete", "Under investigation", None, "Complete"],
        "outcome_status_category": ["awaiting-court-result", "status-update-unavailable", "awaiting-court-result", None, "status-update-unavailable"],
        "outcome_status_date": ["2023-02", "2023-03", "2023-02", None, "2023-03"],
        "persistent_id": ["abc123", "def456", "ghi789", "jkl012", "mno345"]
    })
    crimes_file = os.path.join(test_dir, "crimes_london_2023-01.csv")
    crimes_data.to_csv(crimes_file, index=False)
    test_files.append(crimes_file)
    logger.info(f"Created test crimes file with {len(crimes_data)} records")
    
    # 2. Create a sample outcomes file
    outcomes_data = pd.DataFrame({
        "category_code": ["charged", "no-further-action", "under-investigation", "charged", "no-further-action"],
        "category_name": ["Offender charged", "No further action", "Under investigation", "Offender charged", "No further action"],
        "crime_category": ["violent-crime", "burglary", "theft", "robbery", "anti-social-behaviour"],
        "crime_location_street_id": [123, 124, 125, 127, 126],
        "crime_location_street_name": ["Oxford St", "Regent St", "Bond St", "Piccadilly", "Baker St"],
        "crime_month": ["2023-01", "2023-01", "2023-01", "2023-01", "2023-01"],
        "date": ["2023-02-15", "2023-02-20", "2023-02-25", "2023-03-01", "2023-03-05"],
        "person_id": None,
        "crime_id": ["abc123", "def456", "ghi789", "mno345", "jkl012"]
    })
    outcomes_file = os.path.join(test_dir, "outcomes_london_2023-01.csv")
    outcomes_data.to_csv(outcomes_file, index=False)
    test_files.append(outcomes_file)
    logger.info(f"Created test outcomes file with {len(outcomes_data)} records")
    
    # 3. Create a sample stops file
    stops_data = pd.DataFrame({
        "age_range": ["18-24", "25-34", "over 34", "18-24", "under 18"],
        "gender": ["Male", "Female", "Male", "Female", "Male"],
        "officer_defined_ethnicity": ["White", "Black", "Asian", "White", "Black"],
        "self_defined_ethnicity": ["White - British", "Black - African", "Asian - Indian", "White - Other", "Black - Caribbean"],
        "object_of_search": ["Controlled drugs", "Stolen goods", "Weapons", "Controlled drugs", "Controlled drugs"],
        "datetime": ["2023-01-01T12:30:00", "2023-01-02T15:45:00", "2023-01-03T18:20:00", "2023-01-04T09:15:00", "2023-01-05T22:10:00"],
        "location_latitude": [51.5074, 51.5080, 51.5060, 51.5090, 51.5100],
        "location_longitude": [-0.1278, -0.1280, -0.1270, -0.1290, -0.1310],
        "location_street_id": [123, 124, 125, 126, 127],
        "location_street_name": ["Oxford St", "Regent St", "Bond St", "Baker St", "Piccadilly"],
        "outcome": ["Arrest", "No further action", "Warning", "Community resolution", "Caution"],
        "outcome_linked_to_object_of_search": [True, False, True, True, False],
        "removal_of_more_than_outer_clothing": [False, False, True, False, False],
        "type": ["Person search", "Person search", "Person and Vehicle search", "Person search", "Person search"]
    })
    stops_file = os.path.join(test_dir, "stops_metropolitan_2023-01.csv")
    stops_data.to_csv(stops_file, index=False)
    test_files.append(stops_file)
    logger.info(f"Created test stops file with {len(stops_data)} records")
    
    # 4. Create a sample police forces file
    forces_data = pd.DataFrame({
        "id": ["metropolitan", "west-midlands", "greater-manchester"],
        "name": ["Metropolitan Police", "West Midlands Police", "Greater Manchester Police"],
        "description": ["London police force", "Midlands police force", "Manchester police force"],
        "telephone": ["101", "101", "101"],
        "url": ["https://met.police.uk", "https://west-midlands.police.uk", "https://gmp.police.uk"],
        "engagement_methods": [
            "[{'type': 'facebook'}]",
            "[{'type': 'twitter'}]",
            "[{'type': 'youtube'}]"
        ]
    })
    forces_file = os.path.join(test_dir, "police_forces.csv")
    forces_data.to_csv(forces_file, index=False)
    test_files.append(forces_file)
    logger.info(f"Created test police forces file with {len(forces_data)} records")
    
    # 5. Create a sample senior officers file
    officers_data = pd.DataFrame({
        "name": ["John Smith", "Jane Doe", "Robert Brown"],
        "rank": ["Chief Constable", "Deputy Chief Constable", "Assistant Chief Constable"],
        "bio": ["Career police officer", "Specialist in crime prevention", "Community policing expert"],
        "contact_details": ["john.smith@met.police.uk", "jane.doe@met.police.uk", "robert.brown@met.police.uk"]
    })
    officers_file = os.path.join(test_dir, "senior_officers_metropolitan.csv")
    officers_data.to_csv(officers_file, index=False)
    test_files.append(officers_file)
    logger.info(f"Created test senior officers file with {len(officers_data)} records")
    
    # 6. Create a sample neighborhoods file
    neighborhoods_data = pd.DataFrame({
        "id": ["EW1001", "EW1002", "EW1003"],
        "name": ["Westminster", "Camden", "Islington"],
        "description": ["Central London borough", "North London borough", "North London borough"],
        "url": ["https://met.police.uk/westminster", "https://met.police.uk/camden", "https://met.police.uk/islington"],
        "population": [250000, 220000, 180000]
    })
    neighborhoods_file = os.path.join(test_dir, "neighborhoods_metropolitan.csv")
    neighborhoods_data.to_csv(neighborhoods_file, index=False)
    test_files.append(neighborhoods_file)
    logger.info(f"Created test neighborhoods file with {len(neighborhoods_data)} records")
    
    # 7. Create a sample crime categories file
    categories_data = pd.DataFrame({
        "url": ["all-crime", "anti-social-behaviour", "burglary", "robbery", "violent-crime", "theft"],
        "name": ["All crime", "Anti-social behaviour", "Burglary", "Robbery", "Violent crime", "Theft"]
    })
    categories_file = os.path.join(test_dir, "crime_categories_2023-01.csv")
    categories_data.to_csv(categories_file, index=False)
    test_files.append(categories_file)
    logger.info(f"Created test crime categories file with {len(categories_data)} records")
    
    return test_files

def create_test_database(test_files, db_path, schema_path):
    """Create a test database with the consolidated schema."""
    # Import the necessary function directly from update_database.py
    from update_database import create_sqlite_database
    
    # Create the database
    logger.info(f"Creating test database at {db_path} using schema at {schema_path}")
    create_sqlite_database(
        csv_filepaths=test_files,
        db_path=db_path,
        replace_existing=True,
        schema_path=schema_path,
        use_consolidated_schema=True
    )
    
    logger.info("Test database creation completed")

def verify_database(db_path):
    """Verify the test database by running queries."""
    logger.info(f"Verifying test database at {db_path}")
    
    try:
        # Connect to the database
        conn = sqlite3.connect(db_path)
        
        # Create a cursor
        cursor = conn.cursor()
        
        # Check for tables
        tables_to_check = [
            "crimes", "outcomes", "stops", "police_forces", "senior_officers",
            "neighborhoods", "crime_categories"
        ]
        
        # Check each table for row count
        for table in tables_to_check:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                row_count = cursor.fetchone()[0]
                logger.info(f"Table {table} has {row_count} rows")
                
                if row_count == 0:
                    logger.warning(f"Table {table} has 0 rows - data not imported correctly")
            except sqlite3.Error as e:
                logger.error(f"Error checking table {table}: {e}")
        
        # Run a few sample queries
        test_queries = [
            # Basic count by category
            {
                "name": "Crime count by category",
                "query": """
                    SELECT category, COUNT(*) as count 
                    FROM crimes 
                    WHERE city='london' AND data_date='2023-01'
                    GROUP BY category
                """
            },
            # Join with outcomes
            {
                "name": "Crimes with outcomes",
                "query": """
                    SELECT c.category, o.category_name as outcome, COUNT(*) as count
                    FROM crimes c
                    LEFT JOIN outcomes o ON c.persistent_id = o.crime_id
                    WHERE c.city='london' AND c.data_date='2023-01'
                    GROUP BY c.category, o.category_name
                """
            },
            # Stops query
            {
                "name": "Stops by object of search",
                "query": """
                    SELECT object_of_search, COUNT(*) as count
                    FROM stops
                    WHERE force_id='metropolitan' AND data_date='2023-01'
                    GROUP BY object_of_search
                """
            }
        ]
        
        # Run each query
        for test in test_queries:
            try:
                logger.info(f"Running test query: {test['name']}")
                cursor.execute(test['query'])
                results = cursor.fetchall()
                
                if results:
                    for row in results:
                        logger.info(f"  - Result: {row}")
                else:
                    logger.warning(f"  - No results returned for query: {test['name']}")
            except sqlite3.Error as e:
                logger.error(f"Error running query '{test['name']}': {e}")
        
        # Close the connection
        conn.close()
        
        logger.info("Database verification completed")
        
    except sqlite3.Error as e:
        logger.error(f"Database verification failed: {e}")

def main():
    """Run the narrow test."""
    parser = argparse.ArgumentParser(description="Narrow test for the consolidated schema")
    parser.add_argument("--test-dir", default="narrow_test_data", help="Directory for test CSV files")
    parser.add_argument("--db-path", default="narrow_test_db/narrow_test.db", help="Path for the test SQLite database")
    parser.add_argument("--schema-path", default="bobby/schema/consolidated_schema.sql", help="Path to the schema SQL file")
    
    args = parser.parse_args()
    
    # Create necessary directories
    ensure_directories([args.test_dir, os.path.dirname(args.db_path)])
    
    # Create test data
    logger.info("Creating test data")
    test_files = create_test_data(args.test_dir)
    
    # Create test database
    logger.info("Creating test database")
    create_test_database(test_files, args.db_path, args.schema_path)
    
    # Verify database
    logger.info("Verifying database")
    verify_database(args.db_path)

if __name__ == "__main__":
    main()