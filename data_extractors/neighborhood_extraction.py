#!/usr/bin/env python3
"""
Neighborhood Data Extraction Module for UK Police API

This module handles the extraction of all neighborhood-related data from the UK Police API:
- Neighborhood lists by force
- Neighborhood details
- Neighborhood boundaries
- Neighborhood teams
- Neighborhood events
- Neighborhood priorities
"""

import os
import logging
from datetime import datetime
from typing import List, Dict, Tuple, Optional

from police_api_extractor import NeighborhoodExtractor, UKPoliceAPIClient

# Configure logging
logger = logging.getLogger('data_pull.neighborhoods')

def extract_neighborhood_data(
    client: UKPoliceAPIClient,
    output_dir: str = "csv_data",
    neighborhood_depth: int = 2,
    collect_boundaries: bool = False,
    collect_teams: bool = False,
    collect_events: bool = False,
    collect_priorities: bool = False
) -> List[str]:
    """
    Extract neighborhood data from the UK Police API.
    
    Args:
        client: UK Police API client instance
        output_dir: Directory to save CSV files
        neighborhood_depth: Number of neighborhoods per force to collect detailed data for
                           (set to 0 for all, but be cautious of API rate limits)
        collect_boundaries: Whether to collect neighborhood boundaries
        collect_teams: Whether to collect neighborhood team members
        collect_events: Whether to collect neighborhood events
        collect_priorities: Whether to collect neighborhood priorities
        
    Returns:
        List of filepaths to created CSV files
    """
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
                    
                    # For neighborhoods up to neighborhood_depth, get additional details
                    # If neighborhood_depth is 0, get all neighborhoods
                    neighborhoods_to_process = data if neighborhood_depth == 0 else data[:neighborhood_depth]
                    logger.info(f"Processing {len(neighborhoods_to_process)} neighborhoods in detail for force '{force_id}'")
                    
                    for neighborhood in neighborhoods_to_process:
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
                                    logger.info(f"Extracted details for neighborhood '{neighborhood_id}'")
                                
                                # Get neighborhood boundary if requested
                                if collect_boundaries:
                                    try:
                                        boundary = neighborhood_extractor.get_neighborhood_boundary(
                                            force_id=force_id,
                                            neighborhood_id=neighborhood_id
                                        )
                                        boundary_filepath = neighborhood_extractor.save_to_csv(
                                            data=boundary,
                                            filename=f"neighborhood_boundary_{force_id}_{neighborhood_id}",
                                            output_dir=output_dir
                                        )
                                        if boundary_filepath:
                                            filepaths.append(boundary_filepath)
                                            logger.info(f"Extracted boundary for neighborhood '{neighborhood_id}'")
                                    except Exception as e:
                                        logger.error(f"Error extracting boundary for neighborhood '{neighborhood_id}': {e}")
                                
                                # Get neighborhood team if requested
                                if collect_teams:
                                    try:
                                        team = neighborhood_extractor.get_neighborhood_team(
                                            force_id=force_id,
                                            neighborhood_id=neighborhood_id
                                        )
                                        team_filepath = neighborhood_extractor.save_to_csv(
                                            data=team,
                                            filename=f"neighborhood_team_{force_id}_{neighborhood_id}",
                                            output_dir=output_dir
                                        )
                                        if team_filepath:
                                            filepaths.append(team_filepath)
                                            logger.info(f"Extracted team for neighborhood '{neighborhood_id}'")
                                    except Exception as e:
                                        logger.error(f"Error extracting team for neighborhood '{neighborhood_id}': {e}")
                                
                                # Get neighborhood events if requested
                                if collect_events:
                                    try:
                                        events = neighborhood_extractor.get_neighborhood_events(
                                            force_id=force_id,
                                            neighborhood_id=neighborhood_id
                                        )
                                        events_filepath = neighborhood_extractor.save_to_csv(
                                            data=events,
                                            filename=f"neighborhood_events_{force_id}_{neighborhood_id}",
                                            output_dir=output_dir
                                        )
                                        if events_filepath:
                                            filepaths.append(events_filepath)
                                            logger.info(f"Extracted events for neighborhood '{neighborhood_id}'")
                                    except Exception as e:
                                        logger.error(f"Error extracting events for neighborhood '{neighborhood_id}': {e}")
                                
                                # Get neighborhood priorities if requested
                                if collect_priorities:
                                    try:
                                        priorities = neighborhood_extractor.get_neighborhood_priorities(
                                            force_id=force_id,
                                            neighborhood_id=neighborhood_id
                                        )
                                        priorities_filepath = neighborhood_extractor.save_to_csv(
                                            data=priorities,
                                            filename=f"neighborhood_priorities_{force_id}_{neighborhood_id}",
                                            output_dir=output_dir
                                        )
                                        if priorities_filepath:
                                            filepaths.append(priorities_filepath)
                                            logger.info(f"Extracted priorities for neighborhood '{neighborhood_id}'")
                                    except Exception as e:
                                        logger.error(f"Error extracting priorities for neighborhood '{neighborhood_id}': {e}")
                                
                        except Exception as e:
                            logger.error(f"Error extracting details for neighborhood '{neighborhood_id}': {e}")
            except Exception as e:
                logger.error(f"Error extracting neighborhoods for force '{force_id}': {e}")
    except Exception as e:
        logger.error(f"Error extracting neighborhood data: {e}")
    
    logger.info(f"Neighborhood data extraction completed. Extracted {len(filepaths)} files.")
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
    extract_neighborhood_data(
        client,
        neighborhood_depth=2,
        collect_boundaries=True,
        collect_teams=True,
        collect_events=True,
        collect_priorities=True
    )