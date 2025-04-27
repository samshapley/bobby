import csv
import os
from datetime import datetime
from typing import Dict, List, Optional, Union, Any, Tuple
import logging

from ..client import UKPoliceAPIClient

logger = logging.getLogger('police_api.stops')

class StopsExtractor:
    """
    Extractor for stop and search related endpoints in the UK Police API.
    
    This class provides methods to extract stop and search data and save it to CSV files.
    """
    
    def __init__(self, client: Optional[UKPoliceAPIClient] = None):
        """
        Initialize the stops extractor.
        
        Args:
            client: UK Police API client instance (creates a new one if not provided)
        """
        self.client = client or UKPoliceAPIClient()
    
    def get_stops_by_area(
        self,
        lat: Optional[float] = None,
        lng: Optional[float] = None,
        poly: Optional[str] = None,
        date: Optional[str] = None
    ) -> List[Dict]:
        """
        Get stop and searches within a 1 mile radius of a location or within a custom area.
        
        Args:
            lat: Latitude of the requested area
            lng: Longitude of the requested area
            poly: Custom area as lat/lng pairs (e.g., "52.268,0.543:52.794,0.238:52.130,0.478")
            date: Month in YYYY-MM format (defaults to latest month)
            
        Returns:
            List of stop and searches
            
        Raises:
            ValueError: If neither point (lat/lng) nor poly is provided
        """
        # Input validation
        if (lat is None or lng is None) and poly is None:
            raise ValueError("Either lat/lng pair or poly must be provided")
            
        if lat is not None and lng is not None and poly is not None:
            logger.warning("Both lat/lng and poly provided; using poly")
            
        # Prepare request parameters
        params = {}
        if date:
            params["date"] = date
            
        if poly is not None:
            params["poly"] = poly
        else:
            params["lat"] = lat
            params["lng"] = lng
            
        logger.info(f"Retrieving stop and searches by area with params: {params}")
        return self.client._make_request("stops-street", params=params) or []
    
    def get_stops_at_location(
        self,
        location_id: str,
        date: Optional[str] = None
    ) -> List[Dict]:
        """
        Get stop and searches at a specific location.
        
        Args:
            location_id: Location identifier
            date: Month in YYYY-MM format (defaults to latest month)
            
        Returns:
            List of stop and searches at the specified location
        """
        params = {}
        if date:
            params["date"] = date
            
        logger.info(f"Retrieving stop and searches at location: {location_id}")
        return self.client._make_request(f"stops-at-location?location_id={location_id}", params=params) or []
    
    def get_stops_no_location(
        self,
        force_id: str,
        date: Optional[str] = None
    ) -> List[Dict]:
        """
        Get stop and searches with no location information.
        
        Args:
            force_id: Police force identifier
            date: Month in YYYY-MM format (defaults to latest month)
            
        Returns:
            List of stop and searches with no location information
        """
        params = {
            "force": force_id
        }
        
        if date:
            params["date"] = date
            
        logger.info(f"Retrieving stop and searches with no location for force: {force_id}")
        return self.client._make_request("stops-no-location", params=params) or []
    
    def get_stops_by_force(
        self,
        force_id: str,
        date: Optional[str] = None
    ) -> List[Dict]:
        """
        Get stop and searches for a specific police force.
        
        Args:
            force_id: Police force identifier
            date: Month in YYYY-MM format (defaults to latest month)
            
        Returns:
            List of stop and searches for the specified force
        """
        params = {}
        if date:
            params["date"] = date
            
        logger.info(f"Retrieving stop and searches for force: {force_id}")
        return self.client._make_request(f"stops-force/{force_id}", params=params) or []
    
    def save_to_csv(
        self,
        data: List[Dict],
        filename: str,
        output_dir: str = "output",
        flatten: bool = True,
        append: bool = False
    ) -> str:
        """
        Save stop and search data to a CSV file.
        
        Args:
            data: Stop and search data to save
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
    
    def extract_stops_by_area_to_csv(
        self,
        lat: Optional[float] = None,
        lng: Optional[float] = None,
        poly: Optional[str] = None,
        date: Optional[str] = None,
        filename: Optional[str] = None,
        output_dir: str = "output"
    ) -> Tuple[List[Dict], str]:
        """
        Extract stop and searches by area and save to CSV.
        
        Args:
            lat: Latitude of the requested area
            lng: Longitude of the requested area
            poly: Custom area as lat/lng pairs
            date: Month in YYYY-MM format
            filename: Name of the output file (auto-generated if not provided)
            output_dir: Directory to save the file in
            
        Returns:
            Tuple of (data, filepath)
        """
        data = self.get_stops_by_area(lat, lng, poly, date)
        
        if not filename:
            location = poly or f"{lat}_{lng}"
            date_part = date or "latest"
            filename = f"stops_area_{date_part}_{location}".replace(":", "_").replace(",", "_")
            
        filepath = self.save_to_csv(data, filename, output_dir)
        return data, filepath
    
    def extract_stops_by_force_to_csv(
        self,
        force_id: str,
        date: Optional[str] = None,
        filename: Optional[str] = None,
        output_dir: str = "output"
    ) -> Tuple[List[Dict], str]:
        """
        Extract stop and searches by force and save to CSV.
        
        Args:
            force_id: Police force identifier
            date: Month in YYYY-MM format
            filename: Name of the output file (auto-generated if not provided)
            output_dir: Directory to save the file in
            
        Returns:
            Tuple of (data, filepath)
        """
        data = self.get_stops_by_force(force_id, date)
        
        if not filename:
            date_part = date or "latest"
            filename = f"stops_force_{force_id}_{date_part}"
            
        filepath = self.save_to_csv(data, filename, output_dir)
        return data, filepath