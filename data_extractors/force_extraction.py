#!/usr/bin/env python3
"""
Force Data Extraction Module for UK Police API

This module handles the extraction of all force-related data from the UK Police API:
- List of police forces
- Force details
- Senior officers
"""

import os
import logging
from datetime import datetime
from typing import List, Dict, Tuple, Optional

from police_api_extractor import ForceExtractor, UKPoliceAPIClient

# Configure logging
logger = logging.getLogger('data_pull.forces')

def extract_force_data(
    client: UKPoliceAPIClient,
    output_dir: str = "csv_data"
) -> List[str]:
    """
    Extract police force data from the UK Police API.
    
    Args:
        client: UK Police API client instance
        output_dir: Directory to save CSV files
        
    Returns:
        List of filepaths to created CSV files
    """
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
                        logger.info(f"Extracted details for force '{force_id}'")
                    
                    # Extract senior officers
                    _, officers_filepath = force_extractor.extract_senior_officers_to_csv(
                        force_id=force_id, 
                        output_dir=output_dir
                    )
                    if officers_filepath:
                        filepaths.append(officers_filepath)
                        logger.info(f"Extracted senior officers for force '{force_id}'")
            except Exception as e:
                logger.error(f"Error extracting data for force '{force_id}': {e}")
    except Exception as e:
        logger.error(f"Error extracting force data: {e}")
    
    logger.info(f"Force data extraction completed. Extracted {len(filepaths)} files.")
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
    extract_force_data(client)