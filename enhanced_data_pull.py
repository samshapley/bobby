#!/usr/bin/env python3
"""
Enhanced Data Pull Script for UK Police API

This script provides a comprehensive data extraction tool for UK Police data.
It uses modular extractors to pull different types of data and provides
configurable options for the extraction process.

Features:
- Modular extraction of different data types (crimes, forces, neighborhoods, stops)
- Configurable depth of data collection
- Detailed logging
- SQLite database creation
- City-specific data collection
"""

import os
import sys
import logging
import sqlite3
import pandas as pd
import argparse
from datetime import datetime, timedelta
import json
from typing import List, Dict, Any, Optional

# Import the police API extractor library
from police_api_extractor import UKPoliceAPIClient

# Import the data extractors
from data_extractors import (
    extract_crime_data,
    extract_force_data,
    extract_neighborhood_data,
    extract_stop_search_data
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

def ensure_directories(dirs):
    """Create necessary directories for data storage."""
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

def get_available_dates(client, num_months=3):
    """Get a list of available dates for the crime data."""
    try:
        availability = client.check_availability()
        dates = []
        
        if isinstance(availability, dict) and "date" in availability:
            dates = availability["date"]
        elif isinstance(availability, list):
            if availability and isinstance(availability[0], dict) and "date" in availability[0]:
                dates = [item["date"] for item in availability]
            elif availability and isinstance(availability[0], str):
                dates = availability
                
        return dates[:num_months] if dates and num_months > 0 else []
    except Exception as e:
        logger.error(f"Error getting available dates: {e}")
        return []

def define_cities():
    """Define a list of major UK cities with their coordinates."""
    return [
        {"name": "london", "lat": 51.5074, "lng": -0.1278},
        {"name": "manchester", "lat": 53.4808, "lng": -2.2426},
        {"name": "birmingham", "lat": 52.4862, "lng": -1.8904},
        {"name": "leeds", "lat": 53.8008, "lng": -1.5491},
        {"name": "glasgow", "lat": 55.8642, "lng": -4.2518},
        {"name": "liverpool", "lat": 53.4084, "lng": -2.9916},
        {"name": "newcastle", "lat": 54.9783, "lng": -1.6178},
        {"name": "cardiff", "lat": 51.4816, "lng": -3.1791}
    ]

def filter_cities(cities, city_filter=None):
    """Filter the list of cities based on the provided filter."""
    if not city_filter:
        return cities
        
    filtered_cities = []
    city_names = [c.strip().lower() for c in city_filter.split(',')]
    
    for city in cities:
        if city["name"].lower() in city_names:
            filtered_cities.append(city)
            
    if not filtered_cities and cities:
        logger.warning(f"No cities matched the filter '{city_filter}'. Using all cities.")
        return cities
        
    return filtered_cities

def extract_all_data(args):
    """Extract all available data from the UK Police API based on provided arguments."""
    logger.info("Starting extraction of all data with enhanced options")
    
    # Create a client with extended timeout
    client = UKPoliceAPIClient(timeout=args.timeout)
    
    # Get the latest available date
    latest_date = get_latest_available_date(client)
    logger.info(f"Latest available date: {latest_date}")
    
    # Get historical dates if requested
    dates = [latest_date]
    if args.historical > 1:
        all_dates = get_available_dates(client, args.historical)
        dates = all_dates if all_dates else dates
        logger.info(f"Using {len(dates)} historical dates: {', '.join(dates)}")
    
    # Define and filter cities
    cities = define_cities()
    cities = filter_cities(cities, args.cities)
    logger.info(f"Processing data for {len(cities)} cities")
    
    # Extract all data types
    all_filepaths = []

    # For each date, extract the data
    for date in dates:
        # Extract crime data if requested
        if args.extract_crimes:
            logger.info(f"Extracting crime data for date {date}")
            crime_filepaths = extract_crime_data(
                client, 
                date, 
                output_dir=args.csv_dir,
                cities=cities,
                collect_no_location=args.crimes_no_location,
                collect_at_location=args.crimes_at_location,
                collect_outcomes=True
            )
            all_filepaths.extend(crime_filepaths)
            logger.info(f"Extracted {len(crime_filepaths)} crime data files")
        
        # Extract stop and search data if requested
        if args.extract_stops:
            logger.info(f"Extracting stop and search data for date {date}")
            stops_filepaths = extract_stop_search_data(
                client, 
                date, 
                output_dir=args.csv_dir,
                cities=cities,
                collect_no_location=args.stops_no_location,
                collect_by_area=args.stops_by_area,
                collect_at_location=args.stops_at_location
            )
            all_filepaths.extend(stops_filepaths)
            logger.info(f"Extracted {len(stops_filepaths)} stop and search data files")
    
    # Extract force data (not date-specific)
    if args.extract_forces:
        logger.info("Extracting police force data")
        force_filepaths = extract_force_data(
            client, 
            output_dir=args.csv_dir
        )
        all_filepaths.extend(force_filepaths)
        logger.info(f"Extracted {len(force_filepaths)} force data files")
    
    # Extract neighborhood data (not date-specific)
    if args.extract_neighborhoods:
        logger.info("Extracting neighborhood data")
        neighborhood_filepaths = extract_neighborhood_data(
            client, 
            output_dir=args.csv_dir,
            neighborhood_depth=args.neighborhood_depth,
            collect_boundaries=args.neighborhood_boundaries,
            collect_teams=args.neighborhood_teams,
            collect_events=args.neighborhood_events,
            collect_priorities=args.neighborhood_priorities
        )
        all_filepaths.extend(neighborhood_filepaths)
        logger.info(f"Extracted {len(neighborhood_filepaths)} neighborhood data files")
    
    logger.info(f"Total extracted files: {len(all_filepaths)}")
    
    # Save metadata if requested
    if args.save_metadata:
        metadata = {
            "extraction_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "dates_extracted": dates,
            "cities_processed": [city["name"] for city in cities],
            "files_extracted": len(all_filepaths),
            "extraction_options": vars(args)
        }
        
        metadata_file = os.path.join(args.csv_dir, "extraction_metadata.json")
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=4)
            
        logger.info(f"Saved extraction metadata to {metadata_file}")
    
    return all_filepaths

def create_sqlite_database(csv_filepaths, db_path="db_data/police_data.db", replace_existing=False):
    """Create an SQLite database from the extracted CSV files."""
    logger.info(f"Creating SQLite database at {db_path}")
    
    # Check if database exists and whether to replace it
    if os.path.exists(db_path) and not replace_existing:
        logger.info(f"Database already exists at {db_path} and replace_existing is False. Skipping database creation.")
        return
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    # Create or connect to the SQLite database
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Process each CSV file
        for filepath in csv_filepaths:
            try:
                # Skip if filepath is empty
                if not filepath or not os.path.exists(filepath):
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
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Enhanced tool to extract data from UK Police API and create a SQLite database",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    # Basic options
    parser.add_argument("--csv-dir", default="csv_data", help="Directory to store CSV files")
    parser.add_argument("--db-path", default="db_data/police_data.db", help="Path for the SQLite database")
    parser.add_argument("--timeout", type=int, default=120, help="API request timeout in seconds")
    parser.add_argument("--replace-db", action="store_true", help="Replace existing database if it exists")
    parser.add_argument("--save-metadata", action="store_true", help="Save extraction metadata as JSON")
    
    # Data selection options
    parser.add_argument("--cities", default=None, help="Comma-separated list of cities to include (e.g., 'london,manchester')")
    parser.add_argument("--historical", type=int, default=1, help="Number of months to collect historical data for (1 means latest only)")
    
    # Data type selection
    data_type_group = parser.add_argument_group('Data Types')
    data_type_group.add_argument("--extract-crimes", action="store_true", default=True, help="Extract crime data")
    data_type_group.add_argument("--extract-forces", action="store_true", default=True, help="Extract police force data")
    data_type_group.add_argument("--extract-neighborhoods", action="store_true", default=True, help="Extract neighborhood data")
    data_type_group.add_argument("--extract-stops", action="store_true", default=True, help="Extract stop and search data")
    data_type_group.add_argument("--extract-all", action="store_true", help="Extract all data types and enable all collection options")
    
    # Crime data options
    crime_group = parser.add_argument_group('Crime Data Options')
    crime_group.add_argument("--crimes-no-location", action="store_true", help="Collect crimes with no location data")
    crime_group.add_argument("--crimes-at-location", action="store_true", help="Collect crimes at specific locations")
    
    # Neighborhood data options
    neighborhood_group = parser.add_argument_group('Neighborhood Data Options')
    neighborhood_group.add_argument("--neighborhood-depth", type=int, default=2, help="Number of neighborhoods per force to collect detailed data for (0 for all)")
    neighborhood_group.add_argument("--neighborhood-boundaries", action="store_true", help="Collect neighborhood boundaries")
    neighborhood_group.add_argument("--neighborhood-teams", action="store_true", help="Collect neighborhood teams")
    neighborhood_group.add_argument("--neighborhood-events", action="store_true", help="Collect neighborhood events")
    neighborhood_group.add_argument("--neighborhood-priorities", action="store_true", help="Collect neighborhood priorities")
    
    # Stop and search data options
    stops_group = parser.add_argument_group('Stop and Search Data Options')
    stops_group.add_argument("--stops-no-location", action="store_true", help="Collect stops with no location data")
    stops_group.add_argument("--stops-by-area", action="store_true", help="Collect stops by area")
    stops_group.add_argument("--stops-at-location", action="store_true", help="Collect stops at specific locations")
    
    args = parser.parse_args()
    
    # If extract_all is specified, enable all data types and collection options
    if args.extract_all:
        args.extract_crimes = True
        args.extract_forces = True
        args.extract_neighborhoods = True
        args.extract_stops = True
        args.crimes_no_location = True
        args.crimes_at_location = True
        args.neighborhood_boundaries = True
        args.neighborhood_teams = True
        args.neighborhood_events = True
        args.neighborhood_priorities = True
        args.stops_no_location = True
        args.stops_by_area = True
        args.stops_at_location = True
    
    # Create necessary directories
    ensure_directories([args.csv_dir, os.path.dirname(args.db_path), "logs"])
    
    # Extract all data
    logger.info("Starting data extraction with enhanced options")
    csv_filepaths = extract_all_data(args)
    
    # Create SQLite database
    if csv_filepaths:
        logger.info("Creating SQLite database")
        create_sqlite_database(csv_filepaths, db_path=args.db_path, replace_existing=args.replace_db)
        
        logger.info("Data pull and database creation completed")
    else:
        logger.warning("No data files were extracted. Database creation skipped.")

if __name__ == "__main__":
    main()