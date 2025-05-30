#!/usr/bin/env python3
"""
Crime Data Extraction Module for UK Police API

This module handles the extraction of all crime-related data from the UK Police API:
- Street-level crimes
- Crime outcomes
- Crimes at specific locations
- Crimes with no location
- Crime categories
"""

import os
import logging
from datetime import datetime
from typing import List, Dict, Tuple, Optional

from police_api_extractor import CrimeExtractor, UKPoliceAPIClient

# Configure logging
logger = logging.getLogger('data_pull.crimes')

def extract_crime_data(
    client: UKPoliceAPIClient, 
    latest_date: str, 
    output_dir: str = "csv_data",
    cities: Optional[List[Dict]] = None,
    collect_no_location: bool = False,
    collect_at_location: bool = False,
    collect_outcomes: bool = True
) -> List[str]:
    """
    Extract crime data for specified cities and options.
    
    Args:
        client: UK Police API client instance
        latest_date: Latest available date (YYYY-MM format)
        output_dir: Directory to save CSV files
        cities: List of cities with coordinates (defaults to standard UK cities if None)
        collect_no_location: Whether to collect crimes with no location data
        collect_at_location: Whether to collect crimes at specific locations
        collect_outcomes: Whether to collect crime outcomes
        
    Returns:
        List of filepaths to created CSV files
    """
    logger.info(f"Extracting crime data for date: {latest_date}")
    
    # Use default cities if none provided
    if cities is None:
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
    
    crime_extractor = CrimeExtractor(client=client)
    filepaths = []
    
    # Extract crime data for each city
    for city in cities:
        try:
            logger.info(f"Extracting street-level crimes for {city['name']}")
            
            # Extract street-level crimes
            crimes_data, temp_filepath = crime_extractor.extract_street_crimes_to_csv(
                lat=city["lat"],
                lng=city["lng"],
                date=latest_date,
                output_dir=output_dir,
                filename=f"crimes_{city['name']}_{latest_date}"
            )
            
            # We keep the original file for backward compatibility, but also
            # create an enhanced version with additional metadata for the consolidated schema
            if crimes_data:
                # Add metadata to the dataframe
                for crime in crimes_data:
                    crime['city'] = city['name']
                    crime['data_date'] = latest_date
                    crime['location_type'] = 'street'
                
                # Save enhanced version
                enhanced_filepath = crime_extractor.save_to_csv(
                    data=crimes_data,
                    filename=f"crimes_{city['name']}_{latest_date}",
                    output_dir=output_dir
                )
                
                if enhanced_filepath:
                    filepaths.append(enhanced_filepath)
                    logger.info(f"Successfully extracted street-level crimes for {city['name']} with metadata")
            elif temp_filepath:
                filepaths.append(temp_filepath)
                logger.info(f"Added original street-level crimes file for {city['name']}")
            
            # Extract street-level outcomes if requested
            if collect_outcomes:
                logger.info(f"Extracting street-level outcomes for {city['name']}")
                outcomes_data, temp_outcomes_filepath = crime_extractor.extract_street_outcomes_to_csv(
                    lat=city["lat"],
                    lng=city["lng"],
                    date=latest_date,
                    output_dir=output_dir,
                    filename=f"outcomes_{city['name']}_{latest_date}"
                )
                
                # Similar to crimes, add metadata for consolidated schema
                if outcomes_data:
                    # Add metadata to the dataframe
                    for outcome in outcomes_data:
                        outcome['city'] = city['name']
                        outcome['data_date'] = latest_date
                    
                    # Save enhanced version
                    enhanced_outcomes_filepath = crime_extractor.save_to_csv(
                        data=outcomes_data,
                        filename=f"outcomes_{city['name']}_{latest_date}",
                        output_dir=output_dir
                    )
                    
                    if enhanced_outcomes_filepath:
                        filepaths.append(enhanced_outcomes_filepath)
                        logger.info(f"Successfully extracted street-level outcomes for {city['name']} with metadata")
                elif temp_outcomes_filepath:
                    filepaths.append(temp_outcomes_filepath)
                    logger.info(f"Added original street-level outcomes file for {city['name']}")
            
        except Exception as e:
            logger.error(f"Error extracting crime data for {city['name']}: {e}")
    
    # Extract crimes with no location if requested
    if collect_no_location:
        logger.info("Extracting crimes with no location data")
        try:
            # Get list of all police forces
            forces = client._make_request("forces")
            
            for force in forces:
                force_id = force.get("id")
                if force_id:
                    try:
                        no_loc_data, temp_filepath = crime_extractor.extract_crimes_no_location_to_csv(
                            force_id=force_id,
                            date=latest_date,
                            output_dir=output_dir
                        )
                        
                        # Add metadata for consolidated schema
                        if no_loc_data:
                            # Add metadata to the dataframe
                            for crime in no_loc_data:
                                crime['force_id'] = force_id
                                crime['data_date'] = latest_date
                                crime['location_type'] = 'none'
                            
                            # Save enhanced version
                            enhanced_filepath = crime_extractor.save_to_csv(
                                data=no_loc_data,
                                filename=f"crimes_no_location_{force_id}_{latest_date}",
                                output_dir=output_dir
                            )
                            
                            if enhanced_filepath:
                                filepaths.append(enhanced_filepath)
                                logger.info(f"Extracted {len(no_loc_data)} crimes with no location for {force_id} with metadata")
                        elif temp_filepath:
                            filepaths.append(temp_filepath)
                            logger.info(f"Added original no-location crimes file for {force_id}")
                    except Exception as e:
                        logger.error(f"Error extracting crimes with no location for force '{force_id}': {e}")
        except Exception as e:
            logger.error(f"Error extracting crimes with no location: {e}")
    
    # Extract crimes at specific locations if requested
    if collect_at_location and cities:
        logger.info("Extracting crimes at specific locations")
        # This is a placeholder for extracting crimes at specific locations
        # In a real implementation, you would need to define specific locations of interest
        
        # Example implementation for city centers:
        for city in cities:
            try:
                at_loc_data = crime_extractor.get_crimes_at_location(
                    lat=city["lat"],
                    lng=city["lng"],
                    date=latest_date
                )
                
                if at_loc_data:
                    # Add metadata for consolidated schema
                    for crime in at_loc_data:
                        crime['city'] = city['name']
                        crime['data_date'] = latest_date
                        crime['location_type'] = 'specific'
                    
                    filepath = crime_extractor.save_to_csv(
                        data=at_loc_data,
                        filename=f"crimes_at_location_{city['name']}_{latest_date}",
                        output_dir=output_dir
                    )
                    if filepath:
                        filepaths.append(filepath)
                        logger.info(f"Extracted {len(at_loc_data)} crimes at location for {city['name']} with metadata")
            except Exception as e:
                logger.error(f"Error extracting crimes at location for {city['name']}: {e}")
    
    # Get crime categories
    try:
        logger.info("Extracting crime categories")
        categories = crime_extractor.get_crime_categories(date=latest_date)
        categories_filepath = crime_extractor.save_to_csv(
            data=categories,
            filename=f"crime_categories_{latest_date}",
            output_dir=output_dir
        )
        if categories_filepath:
            filepaths.append(categories_filepath)
            logger.info(f"Extracted {len(categories)} crime categories")
    except Exception as e:
        logger.error(f"Error extracting crime categories: {e}")
    
    # Get last updated date information
    try:
        logger.info("Extracting last updated date information")
        last_updated = crime_extractor.get_last_updated()
        if last_updated:
            # Add data_type field for consolidated schema
            if isinstance(last_updated, dict):
                last_updated['data_type'] = 'crime'
                last_updated['data_date'] = latest_date
            
            last_updated_filepath = crime_extractor.save_to_csv(
                data=[last_updated] if not isinstance(last_updated, list) else last_updated,
                filename=f"crime_last_updated_{latest_date}",
                output_dir=output_dir
            )
            
            if last_updated_filepath:
                filepaths.append(last_updated_filepath)
                logger.info("Extracted last updated date information with metadata")
    except Exception as e:
        logger.error(f"Error extracting last updated date information: {e}")
    
    logger.info(f"Crime data extraction completed. Extracted {len(filepaths)} files.")
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
    
    extract_crime_data(client, latest_date, collect_no_location=True, collect_at_location=True)