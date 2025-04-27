import csv
import os
from datetime import datetime
from typing import Dict, List, Optional, Union, Any, Tuple
import logging

from ..client import UKPoliceAPIClient

logger = logging.getLogger('police_api.forces')

class ForceExtractor:
    """
    Extractor for force-related endpoints in the UK Police API.
    
    This class provides methods to extract police force data and save it to CSV files.
    """
    
    def __init__(self, client: Optional[UKPoliceAPIClient] = None):
        """
        Initialize the force extractor.
        
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
    
    def get_force_details(self, force_id: str) -> Dict:
        """
        Get details for a specific police force.
        
        Args:
            force_id: Police force identifier
            
        Returns:
            Details of the specified police force
        """
        logger.info(f"Retrieving details for force: {force_id}")
        return self.client._make_request(f"forces/{force_id}")
    
    def get_force_senior_officers(self, force_id: str) -> List[Dict]:
        """
        Get senior officers for a specific police force.
        
        Args:
            force_id: Police force identifier
            
        Returns:
            List of senior officers for the specified force
        """
        logger.info(f"Retrieving senior officers for force: {force_id}")
        return self.client._make_request(f"forces/{force_id}/people") or []
    
    def save_to_csv(
        self,
        data: Union[List[Dict], Dict],
        filename: str,
        output_dir: str = "output",
        flatten: bool = True,
        append: bool = False
    ) -> str:
        """
        Save police force data to a CSV file.
        
        Args:
            data: Force data to save
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
    
    def extract_forces_to_csv(
        self,
        output_dir: str = "output"
    ) -> Tuple[List[Dict], str]:
        """
        Extract all police forces and save to CSV.
        
        Args:
            output_dir: Directory to save the file in
            
        Returns:
            Tuple of (data, filepath)
        """
        data = self.get_forces()
        filename = "police_forces"
        filepath = self.save_to_csv(data, filename, output_dir)
        return data, filepath
    
    def extract_force_details_to_csv(
        self,
        force_id: str,
        output_dir: str = "output"
    ) -> Tuple[Dict, str]:
        """
        Extract details for a specific police force and save to CSV.
        
        Args:
            force_id: Police force identifier
            output_dir: Directory to save the file in
            
        Returns:
            Tuple of (data, filepath)
        """
        data = self.get_force_details(force_id)
        filename = f"force_details_{force_id}"
        filepath = self.save_to_csv(data, filename, output_dir)
        return data, filepath
    
    def extract_senior_officers_to_csv(
        self,
        force_id: str,
        output_dir: str = "output"
    ) -> Tuple[List[Dict], str]:
        """
        Extract senior officers for a specific police force and save to CSV.
        
        Args:
            force_id: Police force identifier
            output_dir: Directory to save the file in
            
        Returns:
            Tuple of (data, filepath)
        """
        data = self.get_force_senior_officers(force_id)
        filename = f"senior_officers_{force_id}"
        filepath = self.save_to_csv(data, filename, output_dir)
        return data, filepath