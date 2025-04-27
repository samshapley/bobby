"""
Example script demonstrating how to use the UK Police API extractor library
to fetch and save street-level crime data.
"""

import os
import sys
import logging
from datetime import datetime

# Add parent directory to path to import the library
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from police_api_extractor import CrimeExtractor, UKPoliceAPIClient

def main():
    # Configure logging to display info messages
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create a client with longer timeout
    client = UKPoliceAPIClient(timeout=60)
    
    # Create a crime extractor using our client
    extractor = CrimeExtractor(client=client)
    
    # Get available dates
    available_dates = client.check_availability()
    print("Available dates for crime data:")
    
    # Handle the response based on its type
    if isinstance(available_dates, dict):
        # If it's a dictionary with a "date" key
        date_list = available_dates.get("date", [])
        for date in date_list:
            print(f"  - {date}")
        most_recent_date = date_list[0] if date_list else ""
    elif isinstance(available_dates, list):
        # If it's a list (direct list of dates)
        for date in available_dates:
            if isinstance(date, dict) and "date" in date:
                print(f"  - {date['date']}")
            else:
                print(f"  - {date}")
        most_recent_date = available_dates[0]["date"] if available_dates and isinstance(available_dates[0], dict) and "date" in available_dates[0] else (available_dates[0] if available_dates else "")
    else:
        print("  No date information available")
        most_recent_date = ""
    
    print(f"\nUsing most recent date: {most_recent_date}")
    
    print("\n--- Example 1: Street-level crimes by coordinates ---")
    # Coordinates for Central London (Trafalgar Square)
    lat = 51.508039
    lng = -0.128069
    
    # Get street-level crimes
    crimes = extractor.get_street_crimes(
        lat=lat,
        lng=lng,
        date=most_recent_date
    )
    
    print(f"Found {len(crimes)} crimes near coordinates ({lat}, {lng}) in {most_recent_date}")
    
    # Count crimes by category
    categories = {}
    for crime in crimes:
        category = crime.get("category")
        categories[category] = categories.get(category, 0) + 1
    
    print("\nCrime categories:")
    for category, count in sorted(categories.items(), key=lambda x: x[1], reverse=True):
        print(f"  - {category}: {count}")
    
    # Save to CSV
    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)
    
    data, filepath = extractor.extract_street_crimes_to_csv(
        lat=lat,
        lng=lng,
        date=most_recent_date,
        output_dir=output_dir
    )
    
    print(f"\nSaved {len(data)} records to {filepath}")
    
    print("\n--- Example 2: Street-level crimes within a custom polygon ---")
    # Define a polygon covering part of central London
    # Westminster - Southwark - Tower Bridge area
    poly = "51.501,-0.142:51.507,-0.090:51.488,-0.080:51.483,-0.132:51.501,-0.142"
    
    # Get street-level crimes within the polygon
    crimes = extractor.get_street_crimes(
        poly=poly,
        date=most_recent_date
    )
    
    print(f"Found {len(crimes)} crimes within the defined polygon in {most_recent_date}")
    
    # Count crimes by category
    categories = {}
    for crime in crimes:
        category = crime.get("category")
        categories[category] = categories.get(category, 0) + 1
    
    print("\nCrime categories:")
    for category, count in sorted(categories.items(), key=lambda x: x[1], reverse=True):
        print(f"  - {category}: {count}")
    
    # Save to CSV
    data, filepath = extractor.extract_street_crimes_to_csv(
        poly=poly,
        date=most_recent_date,
        output_dir=output_dir
    )
    
    print(f"\nSaved {len(data)} records to {filepath}")
    
    print("\n--- Example 3: Getting crime categories ---")
    # Get crime categories
    categories = extractor.get_crime_categories()
    
    print("Available crime categories:")
    for category in categories:
        print(f"  - {category.get('url')}: {category.get('name')}")
        
    print("\nDone!")

if __name__ == "__main__":
    main()