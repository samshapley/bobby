"""
Example script demonstrating how to use the UK Police API extractor library
to fetch and save neighborhood data.
"""

import os
import sys
import logging
import random

# Add parent directory to path to import the library
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from police_api_extractor import NeighborhoodExtractor, UKPoliceAPIClient

def main():
    # Configure logging to display info messages
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create a client with longer timeout
    client = UKPoliceAPIClient(timeout=60)
    
    # Create a neighborhood extractor using our client
    extractor = NeighborhoodExtractor(client=client)
    
    # Set up output directory
    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)
    
    print("--- Example 1: Get list of forces ---")
    forces = extractor.get_forces()
    print(f"Found {len(forces)} police forces")
    
    # Print first few forces
    print("\nSample forces:")
    for force in forces[:5]:
        print(f"  - {force.get('id')}: {force.get('name')}")
    
    # Choose a force to work with
    # Choosing leicestershire as it appears to work well with the API
    selected_force = "leicestershire"
    print(f"\nSelected force: {selected_force}")
    
    print("\n--- Example 2: Get neighborhoods for a force ---")
    try:
        neighborhoods = extractor.get_neighborhoods(selected_force)
        print(f"Found {len(neighborhoods)} neighborhoods for {selected_force}")
        
        # Print first few neighborhoods
        print("\nSample neighborhoods:")
        for neighborhood in neighborhoods[:5]:
            print(f"  - {neighborhood.get('id')}: {neighborhood.get('name')}")
        
        # Save neighborhoods to CSV
        data, filepath = extractor.extract_all_neighborhoods_to_csv(
            force_id=selected_force,
            output_dir=output_dir
        )
        print(f"\nSaved {len(data)} neighborhoods to {filepath}")
        
        # Choose a neighborhood to work with
        if neighborhoods:
            selected_neighborhood = neighborhoods[0]['id']
            print(f"\nSelected neighborhood: {selected_neighborhood}")
            
            print("\n--- Example 3: Get neighborhood details ---")
            try:
                details = extractor.get_neighborhood_details(selected_force, selected_neighborhood)
                
                print("Neighborhood details:")
                print(f"  - Name: {details.get('name')}")
                print(f"  - Description: {details.get('description', 'N/A')}")
                print(f"  - Population: {details.get('population', 'N/A')}")
            except Exception as e:
                print(f"Error getting neighborhood details: {str(e)}")
            
            print("\n--- Example 4: Get neighborhood boundary ---")
            try:
                boundary = extractor.get_neighborhood_boundary(selected_force, selected_neighborhood)
                
                print(f"Found {len(boundary)} boundary points")
                if boundary:
                    print("\nSample boundary points:")
                    for point in boundary[:3]:
                        print(f"  - Latitude: {point.get('latitude')}, Longitude: {point.get('longitude')}")
            except Exception as e:
                print(f"Error getting neighborhood boundary: {str(e)}")
            
            print("\n--- Example 5: Get neighborhood team ---")
            try:
                team = extractor.get_neighborhood_team(selected_force, selected_neighborhood)
                
                print(f"Found {len(team)} team members")
                if team:
                    print("\nTeam members:")
                    for member in team:
                        print(f"  - {member.get('rank', '')} {member.get('name', 'Unknown')}")
            except Exception as e:
                print(f"Error getting neighborhood team: {str(e)}")
            
            print("\n--- Example 6: Get neighborhood priorities ---")
            try:
                priorities = extractor.get_neighborhood_priorities(selected_force, selected_neighborhood)
                
                print(f"Found {len(priorities)} priorities")
                if priorities:
                    print("\nPriorities:")
                    for priority in priorities:
                        print(f"  - {priority.get('issue', 'Unknown issue')}")
                        print(f"    Action: {priority.get('action', 'No action specified')}")
            except Exception as e:
                print(f"Error getting neighborhood priorities: {str(e)}")
    except Exception as e:
        print(f"Error getting neighborhoods: {str(e)}")
    
    print("\n--- Example 7: Locate neighborhood by coordinates ---")
    # Coordinates for a location in London
    lat = 52.629729
    lng = -1.131592  # Leicester coordinates
    
    try:
        locate_result = extractor.locate_neighborhood(lat, lng)
        if locate_result:
            print(f"Location ({lat}, {lng}) is in:")
            print(f"  - Force: {locate_result.get('force', 'Unknown')}")
            print(f"  - Neighborhood: {locate_result.get('neighbourhood', 'Unknown')}")
        else:
            print(f"No neighborhood found for coordinates ({lat}, {lng})")
    except Exception as e:
        print(f"Error locating neighborhood: {str(e)}")
    
    print("\nDone!")

if __name__ == "__main__":
    main()