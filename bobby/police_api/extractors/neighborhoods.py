import csv
import os
from datetime import datetime
from typing import Dict, List, Optional, Union, Any, Tuple
import logging

from ..client import UKPoliceAPIClient

logger = logging.getLogger('police_api.neighborhoods')

class NeighborhoodExtractor:
    """
    Extractor for neighborhood-related endpoints in the UK Police API.
    
    This class provides methods to extract neighborhood data and save it to CSV files.
    """
    
    def __init__(self, client: Optional[UKPoliceAPIClient] = None):
        """
        Initialize the neighborhood extractor.
        
        Args:
            client: UK Police API client instance (creates a new one if not provided)
        """
        self.client = client or UKPoliceAPIClient()
    
    def get_forces(self) -> List[Dict]:
        """
        Get a list of all police forces.
        
        Returns:
            List of police forces
        """
        logger.info("Retrieving list of police forces")
        return self.client._make_request("forces") or []
    
    def get_neighborhoods(self, force_id: str) -> List[Dict]:
        """
        Get a list of neighborhoods for a specific force.
        
        Args:
            force_id: Police force identifier
            
        Returns:
            List of neighborhoods
        """
        logger.info(f"Retrieving neighborhoods for force: {force_id}")
        return self.client._make_request(f"{force_id}/neighbourhoods") or []
    
    def get_neighborhood_details(self, force_id: str, neighborhood_id: str) -> Dict:
        """
        Get details for a specific neighborhood.
        
        Args:
            force_id: Police force identifier
            neighborhood_id: Neighborhood identifier
            
        Returns:
            Neighborhood details
        """
        logger.info(f"Retrieving details for neighborhood: {force_id}/{neighborhood_id}")
        return self.client._make_request(f"{force_id}/neighbourhoods/{neighborhood_id}")
    
    def get_neighborhood_boundary(self, force_id: str, neighborhood_id: str) -> List[Dict]:
        """
        Get boundary data for a specific neighborhood.
        
        Args:
            force_id: Police force identifier
            neighborhood_id: Neighborhood identifier
            
        Returns:
            List of boundary coordinates
        """
        logger.info(f"Retrieving boundary for neighborhood: {force_id}/{neighborhood_id}")
        return self.client._make_request(f"{force_id}/neighbourhoods/{neighborhood_id}/boundary") or []
    
    def get_neighborhood_team(self, force_id: str, neighborhood_id: str) -> List[Dict]:
        """
        Get team members for a specific neighborhood.
        
        Args:
            force_id: Police force identifier
            neighborhood_id: Neighborhood identifier
            
        Returns:
            List of team members
        """
        logger.info(f"Retrieving team for neighborhood: {force_id}/{neighborhood_id}")
        return self.client._make_request(f"{force_id}/neighbourhoods/{neighborhood_id}/people") or []
    
    def get_neighborhood_events(self, force_id: str, neighborhood_id: str) -> List[Dict]:
        """
        Get events for a specific neighborhood.
        
        Args:
            force_id: Police force identifier
            neighborhood_id: Neighborhood identifier
            
        Returns:
            List of events
        """
        logger.info(f"Retrieving events for neighborhood: {force_id}/{neighborhood_id}")
        return self.client._make_request(f"{force_id}/neighbourhoods/{neighborhood_id}/events") or []
    
    def get_neighborhood_priorities(self, force_id: str, neighborhood_id: str) -> List[Dict]:
        """
        Get priorities for a specific neighborhood.
        
        Args:
            force_id: Police force identifier
            neighborhood_id: Neighborhood identifier
            
        Returns:
            List of priorities
        """
        logger.info(f"Retrieving priorities for neighborhood: {force_id}/{neighborhood_id}")
        return self.client._make_request(f"{force_id}/neighbourhoods/{neighborhood_id}/priorities") or []
    
    def locate_neighborhood(self, lat: float, lng: float) -> Dict:
        """
        Locate the neighborhood containing a particular point.
        
        Args:
            lat: Latitude
            lng: Longitude
            
        Returns:
            Neighborhood containing the point
        """
        params = {
            "q": f"{lat},{lng}"
        }
        logger.info(f"Locating neighborhood for coordinates: {lat},{lng}")
        return self.client._make_request("locate-neighbourhood", params=params)
    
    def save_to_csv(
        self,
        data: Union[List[Dict], Dict],
        filename: str,
        output_dir: str = "output",
        flatten: bool = True,
        append: bool = False
    ) -> str:
        """
        Save neighborhood data to a CSV file.
        
        Args:
            data: Neighborhood data to save
            filename: Name of the CSV file
            output_dir: Directory to save the file in
            flatten: Whether to flatten nested dictionaries
            append: Whether to append to an existing file
            
        Returns:
            Path to the saved CSV file
        """
        if not data:
            logger.warning("No data to save")
            return ""
        
        # Convert single dict to list for consistent processing
        if isinstance(data, dict):
            data = [data]
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Add timestamp to filename if not appending
        if not append:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{filename}_{timestamp}.csv"
        
        filepath = os.path.join(output_dir, filename)
        
        # Get all possible keys from the data
        fieldnames = set()
        flattened_data = []
        
        for item in data:
            if flatten:
                # Flatten nested dictionaries
                flat_item = self._flatten_dict(item)
            else:
                flat_item = item
                
            flattened_data.append(flat_item)
            fieldnames.update(flat_item.keys())
            
        # Sort fieldnames for consistent output
        fieldnames = sorted(fieldnames)
        
        # Write to CSV
        mode = 'a' if append else 'w'
        with open(filepath, mode, newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            # Write header if not appending or file is empty
            if not append or os.path.getsize(filepath) == 0:
                writer.writeheader()
                
            writer.writerows(flattened_data)
            
        logger.info(f"Saved {len(data)} records to {filepath}")
        return filepath
    
    def _flatten_dict(self, d: Dict, parent_key: str = '', sep: str = '_') -> Dict:
        """
        Flatten nested dictionaries for CSV output.
        
        Args:
            d: Dictionary to flatten
            parent_key: Parent key for nested dictionaries
            sep: Separator for nested keys
            
        Returns:
            Flattened dictionary
        """
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep).items())
            else:
                items.append((new_key, v))
                
        return dict(items)
        
    def extract_all_neighborhoods_to_csv(
        self,
        force_id: str,
        output_dir: str = "output"
    ) -> Tuple[List[Dict], str]:
        """
        Extract all neighborhoods for a force and save to CSV.
        
        Args:
            force_id: Police force identifier
            output_dir: Directory to save the file in
            
        Returns:
            Tuple of (data, filepath)
        """
        data = self.get_neighborhoods(force_id)
        filename = f"neighborhoods_{force_id}"
        filepath = self.save_to_csv(data, filename, output_dir)
        return data, filepath