import csv
import os
from datetime import datetime
from typing import Dict, List, Optional, Union, Any, Tuple
import logging

from ..client import UKPoliceAPIClient

logger = logging.getLogger('police_api.crimes')

class CrimeExtractor:
    """
    Extractor for crime-related endpoints in the UK Police API.
    
    This class provides methods to extract crime data and save it to CSV files.
    """
    
    def __init__(self, client: Optional[UKPoliceAPIClient] = None):
        """
        Initialize the crime extractor.
        
        Args:
            client: UK Police API client instance (creates a new one if not provided)
        """
        self.client = client or UKPoliceAPIClient()
    
    def get_street_crimes(
        self,
        lat: Optional[float] = None,
        lng: Optional[float] = None,
        date: Optional[str] = None,
        poly: Optional[str] = None,
        category: str = "all-crime"
    ) -> List[Dict]:
        """
        Get street-level crimes within a 1 mile radius of a point or within a custom area.
        
        Args:
            lat: Latitude of the requested crime area
            lng: Longitude of the requested crime area
            date: Month in YYYY-MM format (defaults to latest month)
            poly: Custom area as lat/lng pairs (e.g., "52.268,0.543:52.794,0.238:52.130,0.478")
            category: Crime category (defaults to "all-crime")
            
        Returns:
            List of crime incidents
            
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
            
        endpoint = f"crimes-street/{category}"
        logger.info(f"Retrieving street crimes: {endpoint} with params: {params}")
        
        return self.client._make_request(endpoint, params=params) or []
    
    def get_crimes_at_location(
        self,
        lat: float,
        lng: float,
        date: Optional[str] = None,
        category: Optional[str] = None
    ) -> List[Dict]:
        """
        Get crimes at a particular location.
        
        Args:
            lat: Latitude of the location
            lng: Longitude of the location
            date: Month in YYYY-MM format (defaults to latest month)
            category: Optional crime category to filter
            
        Returns:
            List of crimes at the location
        """
        params = {
            "lat": lat,
            "lng": lng
        }
        
        if date:
            params["date"] = date
            
        if category:
            params["category"] = category
            
        logger.info(f"Retrieving crimes at location: lat={lat}, lng={lng}")
        
        return self.client._make_request("crimes-at-location", params=params) or []
    
    def get_street_outcomes(
        self,
        lat: Optional[float] = None,
        lng: Optional[float] = None,
        date: Optional[str] = None,
        poly: Optional[str] = None
    ) -> List[Dict]:
        """
        Get street-level outcomes within a 1 mile radius of a point or within a custom area.
        
        Args:
            lat: Latitude of the requested area
            lng: Longitude of the requested area
            date: Month in YYYY-MM format (defaults to latest month)
            poly: Custom area as lat/lng pairs (e.g., "52.268,0.543:52.794,0.238:52.130,0.478")
            
        Returns:
            List of crime outcomes
            
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
            
        logger.info(f"Retrieving street outcomes with params: {params}")
        
        return self.client._make_request("outcomes-at-location", params=params) or []

    def get_crimes_no_location(
        self,
        force_id: str,
        date: Optional[str] = None,
        category: Optional[str] = None
    ) -> List[Dict]:
        """
        Get crimes with no location information.
        
        Args:
            force_id: Police force identifier
            date: Month in YYYY-MM format (defaults to latest month)
            category: Optional crime category to filter
            
        Returns:
            List of crimes with no location information
        """
        params = {
            "force": force_id
        }
        
        if date:
            params["date"] = date
            
        if category:
            params["category"] = category
            
        logger.info(f"Retrieving crimes with no location for force: {force_id}")
        
        return self.client._make_request("crimes-no-location", params=params) or []
    
    def get_last_updated(self) -> Dict:
        """
        Get the date when crime data was last updated.
        
        Returns:
            Dictionary with last updated date information
        """
        logger.info("Retrieving last updated date for crime data")
        
        return self.client._make_request("crime-last-updated") or {}
    
    def get_outcomes_for_crime(self, persistent_id: str) -> List[Dict]:
        """
        Get outcomes for a specific crime.
        
        Args:
            persistent_id: The persistent ID of the crime
            
        Returns:
            List of outcomes for the specified crime
        """
        logger.info(f"Retrieving outcomes for crime: {persistent_id}")
        
        return self.client._make_request(f"outcomes-for-crime/{persistent_id}") or []
    
    def get_crime_categories(self, date: Optional[str] = None) -> List[Dict]:
        """
        Get a list of valid crime categories for a given date.
        
        Args:
            date: Month in YYYY-MM format (defaults to latest month)
            
        Returns:
            List of crime categories
        """
        params = {}
        if date:
            params["date"] = date
            
        logger.info(f"Retrieving crime categories for date: {date}")
        
        return self.client._make_request("crime-categories", params=params) or []
        
    def save_to_csv(
        self,
        data: List[Dict],
        filename: str,
        output_dir: str = "output",
        flatten: bool = True,
        append: bool = False
    ) -> str:
        """
        Save crime data to a CSV file.
        
        Args:
            data: Crime data to save
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

    def extract_street_crimes_to_csv(
        self,
        lat: Optional[float] = None,
        lng: Optional[float] = None,
        poly: Optional[str] = None,
        date: Optional[str] = None,
        category: str = "all-crime",
        filename: Optional[str] = None,
        output_dir: str = "output"
    ) -> Tuple[List[Dict], str]:
        """
        Extract street-level crimes and save to CSV.
        
        This is a convenience method that combines get_street_crimes and save_to_csv.
        
        Args:
            lat: Latitude of the requested crime area
            lng: Longitude of the requested crime area
            poly: Custom area as lat/lng pairs
            date: Month in YYYY-MM format
            category: Crime category
            filename: Name of the output file (auto-generated if not provided)
            output_dir: Directory to save the file in
            
        Returns:
            Tuple of (data, filepath)
        """
        data = self.get_street_crimes(lat, lng, date, poly, category)
        
        if not filename:
            location = poly or f"{lat}_{lng}"
            date_part = date or "latest"
            filename = f"street_crimes_{category}_{date_part}_{location}".replace(":", "_").replace(",", "_")
            
        filepath = self.save_to_csv(data, filename, output_dir)
        return data, filepath
        
    def extract_street_outcomes_to_csv(
        self,
        lat: Optional[float] = None,
        lng: Optional[float] = None,
        poly: Optional[str] = None,
        date: Optional[str] = None,
        filename: Optional[str] = None,
        output_dir: str = "output"
    ) -> Tuple[List[Dict], str]:
        """
        Extract street-level outcomes and save to CSV.
        
        This is a convenience method that combines get_street_outcomes and save_to_csv.
        
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
        data = self.get_street_outcomes(lat, lng, date, poly)
        
        if not filename:
            location = poly or f"{lat}_{lng}"
            date_part = date or "latest"
            filename = f"street_outcomes_{date_part}_{location}".replace(":", "_").replace(",", "_")
            
        filepath = self.save_to_csv(data, filename, output_dir)
        return data, filepath
        
    def extract_crimes_no_location_to_csv(
        self,
        force_id: str,
        date: Optional[str] = None,
        category: Optional[str] = None,
        filename: Optional[str] = None,
        output_dir: str = "output"
    ) -> Tuple[List[Dict], str]:
        """
        Extract crimes with no location and save to CSV.
        
        This is a convenience method that combines get_crimes_no_location and save_to_csv.
        
        Args:
            force_id: Police force identifier
            date: Month in YYYY-MM format
            category: Optional crime category to filter
            filename: Name of the output file (auto-generated if not provided)
            output_dir: Directory to save the file in
            
        Returns:
            Tuple of (data, filepath)
        """
        data = self.get_crimes_no_location(force_id, date, category)
        
        if not filename:
            category_part = category or "all-crime"
            date_part = date or "latest"
            filename = f"crimes_no_location_{force_id}_{category_part}_{date_part}"
            
        filepath = self.save_to_csv(data, filename, output_dir)
        return data, filepath