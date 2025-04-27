"""
UK Police Data Extractors Package

This package provides modular extractors for different types of UK Police data:
- Crime data (street-level crimes, outcomes, etc.)
- Force data (police forces, details, senior officers)
- Neighborhood data (neighborhoods, details, boundaries, etc.)
- Stop and search data (stops by area, by force, etc.)
"""

from .crime_extraction import extract_crime_data
from .force_extraction import extract_force_data
from .neighborhood_extraction import extract_neighborhood_data
from .stop_search_extraction import extract_stop_search_data

__all__ = [
    'extract_crime_data',
    'extract_force_data',
    'extract_neighborhood_data',
    'extract_stop_search_data'
]