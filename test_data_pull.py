#!/usr/bin/env python3
"""
Test version of the Data Pull Script for UK Police API

This script extracts only the crime data for London from the UK Police API
and creates a SQLite database for testing purposes.
"""

import os
import sys
import logging
import sqlite3
import pandas as pd
from datetime import datetime

# Import the police API extractor library
from police_api_extractor import UKPoliceAPIClient, CrimeExtractor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("test_data_pull")

def ensure_directories():
    """Create necessary directories for data storage."""
    directories = ["test_csv_data", "test_db_data"]
    for directory in directories:
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
        
        # Fall back to current month-1
        return datetime.now().strftime("%Y-%m")
    except Exception as e:
        logger.error(f"Error getting latest available date: {e}")
        # Fall back to current month-1
        return datetime.now().strftime("%Y-%m")

def extract_london_crime_data(client, latest_date, output_dir="test_csv_data"):
    """Extract crime data for London."""
    logger.info(f"Extracting crime data for London for date: {latest_date}")
    
    crime_extractor = CrimeExtractor(client=client)
    
    # London coordinates
    lat = 51.5074
    lng = -0.1278
    
    try:
        # Extract street-level crimes
        _, filepath = crime_extractor.extract_street_crimes_to_csv(
            lat=lat,
            lng=lng,
            date=latest_date,
            output_dir=output_dir,
            filename=f"test_crimes_london_{latest_date}"
        )
        
        # Get crime categories
        categories = crime_extractor.get_crime_categories(date=latest_date)
        categories_filepath = crime_extractor.save_to_csv(
            data=categories,
            filename=f"test_crime_categories_{latest_date}",
            output_dir=output_dir
        )
        
        return [filepath, categories_filepath]
    except Exception as e:
        logger.error(f"Error extracting crime data for London: {e}")
        return []

def create_sqlite_database(csv_filepaths, db_path="test_db_data/test_police_data.db"):
    """Create an SQLite database from the extracted CSV files."""
    logger.info(f"Creating SQLite database at {db_path}")
    
    # Create or connect to the SQLite database
    try:
        conn = sqlite3.connect(db_path)
        
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
    except Exception as e:
        logger.error(f"Error creating database: {e}")

def main():
    """Main function to extract data and create SQLite database."""
    # Create necessary directories
    ensure_directories()
    
    # Create a client with reasonable timeout
    client = UKPoliceAPIClient(timeout=60)
    
    # Get the latest available date
    latest_date = get_latest_available_date(client)
    logger.info(f"Latest available date: {latest_date}")
    
    # Extract London crime data
    csv_filepaths = extract_london_crime_data(client, latest_date)
    
    if csv_filepaths:
        # Create SQLite database
        create_sqlite_database(csv_filepaths)
        logger.info("Test data pull and database creation completed")
    else:
        logger.error("No data was extracted, cannot create database")

if __name__ == "__main__":
    main()