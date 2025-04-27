"""
Bobby: A Python toolkit for accessing and analyzing UK Police Data.
"""

__version__ = "0.1.0"

# Import key functionality to make it available at the top level
from bobby.police_api.client import UKPoliceAPIClient
from bobby.data_extractors.stop_search_extraction import extract_stop_search_data
from bobby.data_extractors.crime_extraction import extract_crime_data
from bobby.data_extractors.force_extraction import extract_force_data
from bobby.data_extractors.neighborhood_extraction import extract_neighborhood_data

# Export key classes and functions for easier imports
__all__ = [
    "UKPoliceAPIClient",
    "extract_stop_search_data",
    "extract_crime_data",
    "extract_force_data",
    "extract_neighborhood_data",
]