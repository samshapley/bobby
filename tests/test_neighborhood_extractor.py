import unittest
from unittest.mock import patch, Mock, mock_open
import os
import json
from police_api_extractor.extractors.neighborhoods import NeighborhoodExtractor

class TestNeighborhoodExtractor(unittest.TestCase):
    """
    Tests for the NeighborhoodExtractor class.
    """
    
    def setUp(self):
        """Set up test environment before each test."""
        self.mock_client = Mock()
        self.extractor = NeighborhoodExtractor(client=self.mock_client)
        
        # Sample neighborhoods data for testing
        self.sample_neighborhoods = [
            {
                "id": "NC04",
                "name": "City Centre"
            },
            {
                "id": "NC66",
                "name": "Cultural Quarter"
            },
            {
                "id": "NC67",
                "name": "Riverside"
            }
        ]
        
        # Sample neighborhood details for testing
        self.sample_neighborhood_details = {
            "description": "The City Centre neighborhood covers the central commercial district",
            "population": "15000",
            "url_force": "https://www.leicestershire.police.uk/",
            "contact_details": {
                "email": "centralleicester@police.uk",
                "telephone": "101",
                "web": "https://www.leicestershire.police.uk/local-policing/city-centre"
            },
            "id": "NC04",
            "name": "City Centre",
            "superintendent": "John Smith"
        }
        
        # Sample neighborhood boundary data for testing
        self.sample_boundary = [
            {"latitude": "52.6394", "longitude": "-1.13119"},
            {"latitude": "52.6391", "longitude": "-1.13140"},
            {"latitude": "52.6389", "longitude": "-1.13182"}
        ]
        
        # Sample neighborhood team data for testing
        self.sample_team = [
            {
                "name": "Jane Doe",
                "rank": "Sergeant",
                "bio": "Jane Doe is a Sergeant in the City Centre neighborhood team",
                "contact_details": {
                    "email": "jane.doe@police.uk",
                    "telephone": "101 ext 3456"
                }
            },
            {
                "name": "John Smith",
                "rank": "Police Constable",
                "bio": "John Smith is a Police Constable in the City Centre neighborhood team",
                "contact_details": {
                    "email": "john.smith@police.uk",
                    "telephone": "101 ext 3457"
                }
            }
        ]
        
        # Sample neighborhood events data for testing
        self.sample_events = [
            {
                "title": "Community Meeting",
                "description": "Monthly community meeting to discuss local issues",
                "address": "City Centre Community Centre, Main Street",
                "type": "Meeting",
                "start_date": "2023-03-15T18:00:00",
                "end_date": "2023-03-15T20:00:00"
            },
            {
                "title": "Street Surgery",
                "description": "Officers will be available to discuss concerns",
                "address": "High Street, City Centre",
                "type": "Surgery",
                "start_date": "2023-03-20T10:00:00",
                "end_date": "2023-03-20T12:00:00"
            }
        ]
        
        # Sample neighborhood priorities data for testing
        self.sample_priorities = [
            {
                "issue": "Anti-social behavior",
                "action": "Increased patrols and engagement with local youth groups",
                "object-of-interest": "City Centre parks and public spaces",
                "last-updated": "2023-03-01"
            },
            {
                "issue": "Shoplifting",
                "action": "Working with local businesses to improve security",
                "object-of-interest": "Shopping district",
                "last-updated": "2023-03-05"
            }
        ]
        
        # Sample locate neighborhood response for testing
        self.sample_locate_response = {
            "force": "leicestershire",
            "neighbourhood": "NC66"
        }
    
    def tearDown(self):
        """Clean up after each test."""
        pass
    
    def test_get_forces(self):
        """Test get_forces method."""
        # Sample forces data for testing
        sample_forces = [
            {"id": "leicestershire", "name": "Leicestershire Police"},
            {"id": "metropolitan", "name": "Metropolitan Police"},
            {"id": "west-midlands", "name": "West Midlands Police"}
        ]
        
        # Setup mock
        self.mock_client._make_request.return_value = sample_forces
        
        # Call the method
        result = self.extractor.get_forces()
        
        # Assertions
        self.mock_client._make_request.assert_called_once_with("forces")
        self.assertEqual(result, sample_forces)
    
    def test_get_forces_empty_response(self):
        """Test get_forces method with empty response."""
        # Setup mock
        self.mock_client._make_request.return_value = None
        
        # Call the method
        result = self.extractor.get_forces()
        
        # Assertions
        self.mock_client._make_request.assert_called_once_with("forces")
        self.assertEqual(result, [])
    
    def test_get_neighborhoods(self):
        """Test get_neighborhoods method."""
        # Setup mock
        self.mock_client._make_request.return_value = self.sample_neighborhoods
        
        # Call the method
        result = self.extractor.get_neighborhoods("leicestershire")
        
        # Assertions
        self.mock_client._make_request.assert_called_once_with("leicestershire/neighbourhoods")
        self.assertEqual(result, self.sample_neighborhoods)
    
    def test_get_neighborhoods_empty_response(self):
        """Test get_neighborhoods method with empty response."""
        # Setup mock
        self.mock_client._make_request.return_value = None
        
        # Call the method
        result = self.extractor.get_neighborhoods("leicestershire")
        
        # Assertions
        self.mock_client._make_request.assert_called_once_with("leicestershire/neighbourhoods")
        self.assertEqual(result, [])
    
    def test_get_neighborhood_details(self):
        """Test get_neighborhood_details method."""
        # Setup mock
        self.mock_client._make_request.return_value = self.sample_neighborhood_details
        
        # Call the method
        result = self.extractor.get_neighborhood_details("leicestershire", "NC04")
        
        # Assertions
        self.mock_client._make_request.assert_called_once_with("leicestershire/neighbourhoods/NC04")
        self.assertEqual(result, self.sample_neighborhood_details)
    
    def test_get_neighborhood_boundary(self):
        """Test get_neighborhood_boundary method."""
        # Setup mock
        self.mock_client._make_request.return_value = self.sample_boundary
        
        # Call the method
        result = self.extractor.get_neighborhood_boundary("leicestershire", "NC04")
        
        # Assertions
        self.mock_client._make_request.assert_called_once_with("leicestershire/neighbourhoods/NC04/boundary")
        self.assertEqual(result, self.sample_boundary)
    
    def test_get_neighborhood_boundary_empty_response(self):
        """Test get_neighborhood_boundary method with empty response."""
        # Setup mock
        self.mock_client._make_request.return_value = None
        
        # Call the method
        result = self.extractor.get_neighborhood_boundary("leicestershire", "NC04")
        
        # Assertions
        self.mock_client._make_request.assert_called_once_with("leicestershire/neighbourhoods/NC04/boundary")
        self.assertEqual(result, [])
    
    def test_get_neighborhood_team(self):
        """Test get_neighborhood_team method."""
        # Setup mock
        self.mock_client._make_request.return_value = self.sample_team
        
        # Call the method
        result = self.extractor.get_neighborhood_team("leicestershire", "NC04")
        
        # Assertions
        self.mock_client._make_request.assert_called_once_with("leicestershire/neighbourhoods/NC04/people")
        self.assertEqual(result, self.sample_team)
    
    def test_get_neighborhood_team_empty_response(self):
        """Test get_neighborhood_team method with empty response."""
        # Setup mock
        self.mock_client._make_request.return_value = None
        
        # Call the method
        result = self.extractor.get_neighborhood_team("leicestershire", "NC04")
        
        # Assertions
        self.mock_client._make_request.assert_called_once_with("leicestershire/neighbourhoods/NC04/people")
        self.assertEqual(result, [])
    
    def test_get_neighborhood_events(self):
        """Test get_neighborhood_events method."""
        # Setup mock
        self.mock_client._make_request.return_value = self.sample_events
        
        # Call the method
        result = self.extractor.get_neighborhood_events("leicestershire", "NC04")
        
        # Assertions
        self.mock_client._make_request.assert_called_once_with("leicestershire/neighbourhoods/NC04/events")
        self.assertEqual(result, self.sample_events)
    
    def test_get_neighborhood_events_empty_response(self):
        """Test get_neighborhood_events method with empty response."""
        # Setup mock
        self.mock_client._make_request.return_value = None
        
        # Call the method
        result = self.extractor.get_neighborhood_events("leicestershire", "NC04")
        
        # Assertions
        self.mock_client._make_request.assert_called_once_with("leicestershire/neighbourhoods/NC04/events")
        self.assertEqual(result, [])
    
    def test_get_neighborhood_priorities(self):
        """Test get_neighborhood_priorities method."""
        # Setup mock
        self.mock_client._make_request.return_value = self.sample_priorities
        
        # Call the method
        result = self.extractor.get_neighborhood_priorities("leicestershire", "NC04")
        
        # Assertions
        self.mock_client._make_request.assert_called_once_with("leicestershire/neighbourhoods/NC04/priorities")
        self.assertEqual(result, self.sample_priorities)
    
    def test_get_neighborhood_priorities_empty_response(self):
        """Test get_neighborhood_priorities method with empty response."""
        # Setup mock
        self.mock_client._make_request.return_value = None
        
        # Call the method
        result = self.extractor.get_neighborhood_priorities("leicestershire", "NC04")
        
        # Assertions
        self.mock_client._make_request.assert_called_once_with("leicestershire/neighbourhoods/NC04/priorities")
        self.assertEqual(result, [])
    
    def test_locate_neighborhood(self):
        """Test locate_neighborhood method."""
        # Setup mock
        self.mock_client._make_request.return_value = self.sample_locate_response
        
        # Call the method
        result = self.extractor.locate_neighborhood(52.6394, -1.13119)
        
        # Assertions
        self.mock_client._make_request.assert_called_once_with("locate-neighbourhood", params={"q": "52.6394,-1.13119"})
        self.assertEqual(result, self.sample_locate_response)
    
    @patch('os.path.getsize')
    @patch('builtins.open', new_callable=mock_open)
    @patch('os.makedirs')
    def test_save_to_csv_dict_input(self, mock_makedirs, mock_file_open, mock_getsize):
        """Test save_to_csv method with dictionary input."""
        # Setup mocks
        mock_getsize.return_value = 0
        
        # Call the method
        result = self.extractor.save_to_csv(
            data=self.sample_neighborhood_details,
            filename="test_neighborhood_details",
            output_dir="test_output"
        )
        
        # Assertions
        mock_makedirs.assert_called_once_with("test_output", exist_ok=True)
        mock_file_open.assert_called_once()
        self.assertTrue(result.startswith("test_output/test_neighborhood_details_"))
        self.assertTrue(result.endswith(".csv"))
    
    @patch('os.path.getsize')
    @patch('builtins.open', new_callable=mock_open)
    @patch('os.makedirs')
    def test_save_to_csv_list_input(self, mock_makedirs, mock_file_open, mock_getsize):
        """Test save_to_csv method with list input."""
        # Setup mocks
        mock_getsize.return_value = 0
        
        # Call the method
        result = self.extractor.save_to_csv(
            data=self.sample_neighborhoods,
            filename="test_neighborhoods",
            output_dir="test_output"
        )
        
        # Assertions
        mock_makedirs.assert_called_once_with("test_output", exist_ok=True)
        mock_file_open.assert_called_once()
        self.assertTrue(result.startswith("test_output/test_neighborhoods_"))
        self.assertTrue(result.endswith(".csv"))
    
    def test_flatten_dict(self):
        """Test _flatten_dict method."""
        # Test data
        nested_dict = {
            "id": "NC04",
            "name": "City Centre",
            "contact_details": {
                "email": "centralleicester@police.uk",
                "telephone": "101",
                "social": {
                    "twitter": "@LeicesterPolice",
                    "facebook": "LeicesterPolice"
                }
            }
        }
        
        # Call the method
        result = self.extractor._flatten_dict(nested_dict)
        
        # Assertions
        expected_result = {
            "id": "NC04",
            "name": "City Centre",
            "contact_details_email": "centralleicester@police.uk",
            "contact_details_telephone": "101",
            "contact_details_social_twitter": "@LeicesterPolice",
            "contact_details_social_facebook": "LeicesterPolice"
        }
        self.assertEqual(result, expected_result)
    
    @patch('police_api_extractor.extractors.neighborhoods.NeighborhoodExtractor.get_neighborhoods')
    @patch('police_api_extractor.extractors.neighborhoods.NeighborhoodExtractor.save_to_csv')
    def test_extract_all_neighborhoods_to_csv(self, mock_save_to_csv, mock_get_neighborhoods):
        """Test extract_all_neighborhoods_to_csv method."""
        # Setup mocks
        mock_get_neighborhoods.return_value = self.sample_neighborhoods
        mock_save_to_csv.return_value = "test_output/neighborhoods_leicestershire_20210120_120000.csv"
        
        # Call the method
        data, filepath = self.extractor.extract_all_neighborhoods_to_csv(
            force_id="leicestershire",
            output_dir="test_output"
        )
        
        # Assertions
        mock_get_neighborhoods.assert_called_once_with("leicestershire")
        mock_save_to_csv.assert_called_once_with(self.sample_neighborhoods, "neighborhoods_leicestershire", "test_output")
        self.assertEqual(data, self.sample_neighborhoods)
        self.assertEqual(filepath, "test_output/neighborhoods_leicestershire_20210120_120000.csv")

if __name__ == "__main__":
    unittest.main()