#!/usr/bin/env python3
"""
Data Pull Script for UK Police API

This script extracts the most recent data from the UK Police API for all core data types:
- Crimes
- Neighborhood information
- Stop and searches
- Force data

It stores the data in CSV files and creates a SQLite database for SQL querying.
"""

import os
import sys
import logging
import sqlite3
import pandas as pd
import argparse
from datetime import datetime, timedelta

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
        logging.FileHandler("data_pull.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("data_pull")

def ensure_directories():
    """Create necessary directories for data storage."""
    directories = ["csv_data", "db_data", "logs"]
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

def extract_crime_data(client, latest_date, output_dir="csv_data"):
    """Extract crime data for major cities."""
    logger.info(f"Extracting crime data for date: {latest_date}")
    
    crime_extractor = CrimeExtractor(client=client)
    
    # List of major UK cities with their approximate coordinates
    # These are just examples, you can expand or modify as needed
    cities = [
        {"name": "london", "lat": 51.5074, "lng": -0.1278},
        {"name": "manchester", "lat": 53.4808, "lng": -2.2426},
        {"name": "birmingham", "lat": 52.4862, "lng": -1.8904},
        {"name": "leeds", "lat": 53.8008, "lng": -1.5491},
        {"name": "glasgow", "lat": 55.8642, "lng": -4.2518},
        {"name": "liverpool", "lat": 53.4084, "lng": -2.9916},
        {"name": "newcastle", "lat": 54.9783, "lng": -1.6178},
        {"name": "cardiff", "lat": 51.4816, "lng": -3.1791}
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

def extract_force_data(client, output_dir="csv_data"):
    """Extract police force data."""
    logger.info("Extracting police force data")
    
    force_extractor = ForceExtractor(client=client)
    filepaths = []
    
    try:
        # Get list of all police forces
        data, filepath = force_extractor.extract_forces_to_csv(output_dir=output_dir)
        if filepath:
            filepaths.append(filepath)
            logger.info(f"Extracted data for {len(data)} police forces")
        
        # Get detailed information for each force
        for force in data:
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

def extract_neighborhood_data(client, output_dir="csv_data"):
    """Extract neighborhood data."""
    logger.info("Extracting neighborhood data")
    
    neighborhood_extractor = NeighborhoodExtractor(client=client)
    filepaths = []
    
    try:
        # Get list of all police forces
        forces = neighborhood_extractor.get_forces()
        
        # Get neighborhoods for each force
        for force in forces:
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
                    
                    # For the first 2 neighborhoods of each force, get additional details
                    # This is to avoid excessive API calls since there can be many neighborhoods
                    neighborhoods = data[:2]
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
                        except Exception as e:
                            logger.error(f"Error extracting details for neighborhood '{neighborhood_id}': {e}")
            except Exception as e:
                logger.error(f"Error extracting neighborhoods for force '{force_id}': {e}")
    except Exception as e:
        logger.error(f"Error extracting neighborhood data: {e}")
    
    return filepaths

def extract_stops_data(client, latest_date, output_dir="csv_data"):
    """Extract stop and search data."""
    logger.info(f"Extracting stop and search data for date: {latest_date}")
    
    stops_extractor = StopsExtractor(client=client)
    filepaths = []
    
    try:
        # Get list of all police forces
        forces = stops_extractor.get_forces()
        
        # Get stop and search data for each force
        for force in forces:
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
            except Exception as e:
                logger.error(f"Error extracting stops for force '{force_id}': {e}")
    except Exception as e:
        logger.error(f"Error extracting stop and search data: {e}")
    
    return filepaths

def extract_all_data(output_dir="csv_data"):
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
    return all_filepaths

def create_sqlite_database(csv_filepaths, db_path="db_data/police_data.db"):
    """Create an SQLite database from the extracted CSV files."""
    logger.info(f"Creating SQLite database at {db_path}")
    
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
    except Exception as e:
        logger.error(f"Error creating database: {e}")

def main():
    """Main function to extract data and create SQLite database."""
    # Parse command line arguments (if needed)
    parser = argparse.ArgumentParser(description="Extract recent data from UK Police API and create a SQLite database")
    parser.add_argument("--csv-dir", default="csv_data", help="Directory to store CSV files")
    parser.add_argument("--db-path", default="db_data/police_data.db", help="Path for the SQLite database")
    args = parser.parse_args()
    
    # Create necessary directories
    ensure_directories()
    
    # Extract all data
    logger.info("Starting data extraction")
    csv_filepaths = extract_all_data(output_dir=args.csv_dir)
    
    # Create SQLite database
    logger.info("Creating SQLite database")
    create_sqlite_database(csv_filepaths, db_path=args.db_path)
    
    logger.info("Data pull and database creation completed")

if __name__ == "__main__":
    main()
