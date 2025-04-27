#!/usr/bin/env python3
"""
Configuration-Based Data Pull Script for UK Police API

This script uses a JSON configuration file to drive the data extraction process.
It provides a more flexible way to extract data without having to modify code.
"""

import os
import sys
import logging
import json
import argparse
from datetime import datetime
from typing import Dict, Any

# Import the enhanced data pull script
from enhanced_data_pull import (
    extract_all_data,
    create_sqlite_database,
    ensure_directories,
    get_latest_available_date
)

# Import the police API extractor library
from police_api_extractor import UKPoliceAPIClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("data_pull.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("config_data_pull")

def load_config(config_file="extraction_config.json"):
    """Load the configuration from a JSON file."""
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
        return config
    except Exception as e:
        logger.error(f"Error loading configuration: {e}")
        return None

def create_args_from_config(config):
    """Create an arguments object from the configuration."""
    class Args:
        pass
    
    args = Args()
    
    # Basic options
    args.csv_dir = config.get("extraction", {}).get("csv_dir", "csv_data")
    args.db_path = config.get("extraction", {}).get("db_path", "db_data/police_data.db")
    args.timeout = config.get("api", {}).get("timeout", 120)
    args.replace_db = config.get("extraction", {}).get("replace_db", False)
    args.save_metadata = config.get("extraction", {}).get("save_metadata", False)
    
    # Historical data
    args.historical = config.get("extraction", {}).get("historical_months", 1)
    
    # Cities
    cities_config = config.get("cities", [])
    args.cities = None  # Will use all cities defined in the extractors
    
    # If cities are defined in the config, create a comma-separated list
    if cities_config:
        city_names = [city.get("name") for city in cities_config if city.get("name")]
        if city_names:
            args.cities = ",".join(city_names)
    
    # Data types
    crime_config = config.get("data_types", {}).get("crimes", {})
    force_config = config.get("data_types", {}).get("forces", {})
    neighborhood_config = config.get("data_types", {}).get("neighborhoods", {})
    stops_config = config.get("data_types", {}).get("stops", {})
    
    # Extract flags
    args.extract_crimes = crime_config.get("extract", True)
    args.extract_forces = force_config.get("extract", True)
    args.extract_neighborhoods = neighborhood_config.get("extract", True)
    args.extract_stops = stops_config.get("extract", True)
    args.extract_all = False  # This is a CLI-only option
    
    # Crime options
    args.crimes_no_location = crime_config.get("no_location", False)
    args.crimes_at_location = crime_config.get("at_location", False)
    
    # Neighborhood options
    args.neighborhood_depth = neighborhood_config.get("depth", 2)
    args.neighborhood_boundaries = neighborhood_config.get("boundaries", False)
    args.neighborhood_teams = neighborhood_config.get("teams", False)
    args.neighborhood_events = neighborhood_config.get("events", False)
    args.neighborhood_priorities = neighborhood_config.get("priorities", False)
    
    # Stops options
    args.stops_no_location = stops_config.get("no_location", False)
    args.stops_by_area = stops_config.get("by_area", False)
    args.stops_at_location = stops_config.get("at_location", False)
    
    return args

def main():
    """Main function to extract data using the configuration file."""
    parser = argparse.ArgumentParser(description="Configuration-based data extraction for UK Police API")
    parser.add_argument("--config", default="config/extraction_config.json", help="Path to configuration file")
    parser.add_argument("--print-config", action="store_true", help="Print the current configuration and exit")
    parser.add_argument("--disable-extraction", action="store_true", help="Disable data extraction (useful with --print-config)")
    
    args = parser.parse_args()
    
    # Load configuration
    config = load_config(args.config)
    if not config:
        logger.error("Failed to load configuration. Exiting.")
        return 1
    
    # Print configuration if requested
    if args.print_config:
        print(json.dumps(config, indent=4))
        if args.disable_extraction:
            return 0
    
    # Create args object from configuration
    extraction_args = create_args_from_config(config)
    
    # Create necessary directories
    ensure_directories([extraction_args.csv_dir, os.path.dirname(extraction_args.db_path), "logs"])
    
    # Extract all data
    logger.info("Starting data extraction with configuration-based options")
    csv_filepaths = extract_all_data(extraction_args)
    
    # Create SQLite database
    if csv_filepaths:
        logger.info("Creating SQLite database")
        create_sqlite_database(csv_filepaths, db_path=extraction_args.db_path, replace_existing=extraction_args.replace_db)
        
        logger.info("Data pull and database creation completed")
    else:
        logger.warning("No data files were extracted. Database creation skipped.")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
