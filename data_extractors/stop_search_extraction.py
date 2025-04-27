#!/usr/bin/env python3
"""
Stop and Search Data Extraction Module for UK Police API

This module handles the extraction of all stop and search related data from the UK Police API:
- Stops by area
- Stops at specific locations
- Stops with no location
- Stops by force
"""

import os
import logging
from datetime import datetime
from typing import List, Dict, Tuple, Optional

from police_api_extractor import StopsExtractor, UKPoliceAPIClient

# Configure logging
logger = logging.getLogger('data_pull.stops')

def extract_stop_search_data(
    client: UKPoliceAPIClient,
    latest_date: str,
    output_dir: str = "csv_data",
    cities: Optional[List[Dict]] = None,
    collect_no_location: bool = False,
    collect_by_area: bool = False,
    collect_at_location: bool = False
) -> List[str]:
    """
    Extract stop and search data from the UK Police API.
    
    Args:
        client: UK Police API client instance
        latest_date: Latest available date (YYYY-MM format)
        output_dir: Directory to save CSV files
        cities: List of cities with coordinates (defaults to standard UK cities if None)
        collect_no_location: Whether to collect stops with no location data
        collect_by_area: Whether to collect stops by area (using city coordinates)
        collect_at_location: Whether to collect stops at specific locations
        
    Returns:
        List of filepaths to created CSV files
    """
    logger.info(f"Extracting stop and search data for date: {latest_date}")
    
    # Use default cities if none provided
    if cities is None and (collect_by_area or collect_at_location):
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
    
    stops_extractor = StopsExtractor(client=client)
    filepaths = []
    
    # Extract stops by force for all forces
    try:
        # Get list of all police forces
        forces = client._make_request("forces")
        
        if forces:
            for force in forces:
                try:
                    force_id = force.get("id")
                    if force_id:
                        data, filepath = stops_extractor.extract_stops_by_force_to_csv(
                            force_id=force_id,
                            date=latest_date,
                            output_dir=output_dir
                        )
                        if filepath:
                            filepaths.append(filepath)
                            logger.info(f"Extracted {len(data)} stop and searches for force '{force_id}'")
                            
                            # Extract stops with no location if requested
                            if collect_no_location:
                                try:
                                    no_loc_data = stops_extractor.get_stops_no_location(
                                        force_id=force_id,
                                        date=latest_date
                                    )
                                    if no_loc_data:
                                        no_loc_filepath = stops_extractor.save_to_csv(
                                            data=no_loc_data,
                                            filename=f"stops_no_location_{force_id}_{latest_date}",
                                            output_dir=output_dir
                                        )
                                        if no_loc_filepath:
                                            filepaths.append(no_loc_filepath)
                                            logger.info(f"Extracted {len(no_loc_data)} stops with no location for force '{force_id}'")
                                except Exception as e:
                                    logger.error(f"Error extracting stops with no location for force '{force_id}': {e}")
                except Exception as e:
                    logger.error(f"Error extracting stops for force '{force_id}': {e}")
    except Exception as e:
        logger.error(f"Error extracting stop and search data by force: {e}")
    
    # Extract stops by area if requested
    if collect_by_area and cities:
        logger.info("Extracting stops by area")
        for city in cities:
            try:
                data, filepath = stops_extractor.extract_stops_by_area_to_csv(
                    lat=city["lat"],
                    lng=city["lng"],
                    date=latest_date,
                    output_dir=output_dir,
                    filename=f"stops_area_{city['name']}_{latest_date}"
                )
                if filepath:
                    filepaths.append(filepath)
                    logger.info(f"Extracted {len(data)} stops by area for {city['name']}")
            except Exception as e:
                logger.error(f"Error extracting stops by area for {city['name']}: {e}")
    
    # Extract stops at specific locations if requested
    if collect_at_location and cities:
        logger.info("Extracting stops at specific locations")
        # This is a placeholder for extracting stops at specific locations
        # In a real implementation, you would need to define specific locations of interest
        
        # Example implementation for key locations (city centers):
        for city in cities:
            try:
                # For demonstration, we'll use the city center coordinates
                # In a real application, you might want to use more specific locations
                location_id = f"{city['lat']},{city['lng']}"
                
                data = stops_extractor.get_stops_at_location(
                    location_id=location_id,
                    date=latest_date
                )
                
                if data:
                    filepath = stops_extractor.save_to_csv(
                        data=data,
                        filename=f"stops_at_location_{city['name']}_{latest_date}",
                        output_dir=output_dir
                    )
                    if filepath:
                        filepaths.append(filepath)
                        logger.info(f"Extracted {len(data)} stops at location for {city['name']}")
            except Exception as e:
                logger.error(f"Error extracting stops at location for {city['name']}: {e}")
    
    logger.info(f"Stop and search data extraction completed. Extracted {len(filepaths)} files.")
    return filepaths

if __name__ == "__main__":
    # This allows the module to be run standalone for testing
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create directories
    os.makedirs("csv_data", exist_ok=True)
    os.makedirs("logs", exist_ok=True)
    
    # Extract data
    client = UKPoliceAPIClient(timeout=120)
    
    # Get latest date (you would typically get this from the API)
    latest_date = "2023-03"  # Example date, replace with actual API call
    
    extract_stop_search_data(
        client,
        latest_date,
        collect_no_location=True,
        collect_by_area=True,
        collect_at_location=True
    )