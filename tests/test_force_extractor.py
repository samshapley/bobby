import unittest
from unittest.mock import patch, Mock, mock_open
import os
import json
import tempfile
import csv
from police_api_extractor.extractors.forces import ForceExtractor

class TestForceExtractor(unittest.TestCase):
    """
    Tests for the ForceExtractor class.
    """
    
    def setUp(self):
        """Set up test environment before each test."""
        self.mock_client = Mock()
        self.extractor = ForceExtractor(client=self.mock_client)
        
        # Sample force data for testing
        self.sample_forces = [
            {
                "id": "avon-and-somerset",
                "name": "Avon and Somerset Constabulary"
            },
            {
                "id": "bedfordshire",
                "name": "Bedfordshire Police"
            },
            {
                "id": "cambridgeshire",
                "name": "Cambridgeshire Constabulary"
            }
        ]
        
        # Sample force details for testing
        self.sample_force_details = {
            "description": "Avon and Somerset Constabulary is the territorial police force responsible for law enforcement in the county of Somerset and the now-defunct county of Avon",
            "url": "https://www.avonandsomerset.police.uk/",
            "engagement_methods": [
                {
                    "url": "https://www.facebook.com/avonandsomersetpolice",
                    "description": "Facebook",
                    "title": "Facebook"
                },
                {
                    "url": "https://www.twitter.com/aspolice",
                    "description": "Twitter",
                    "title": "Twitter"
                }
            ],
            "telephone": "101",
            "id": "avon-and-somerset",
            "name": "Avon and Somerset Constabulary"
        }
        
        # Sample senior officers data for testing
        self.sample_senior_officers = [
            {
                "name": "John Smith",
                "rank": "Chief Constable",
                "bio": "John Smith is the Chief Constable of Avon and Somerset Constabulary",
                "contact_details": {}
            },
            {
                "name": "Jane Doe",
                "rank": "Deputy Chief Constable",
                "bio": "Jane Doe is the Deputy Chief Constable of Avon and Somerset Constabulary",
                "contact_details": {
                    "twitter": "@janedoe",
                    "email": "jane.doe@police.uk"
                }
            }
        ]
    
    def tearDown(self):
        """Clean up after each test."""
        pass
    
    def test_get_forces(self):
        """Test get_forces method."""
        # Setup mock
        self.mock_client._make_request.return_value = self.sample_forces
        
        # Call the method
        result = self.extractor.get_forces()
        
        # Assertions
        self.mock_client._make_request.assert_called_once_with("forces")
        self.assertEqual(result, self.sample_forces)
    
    def test_get_forces_empty_response(self):
        """Test get_forces method with empty response."""
        # Setup mock
        self.mock_client._make_request.return_value = None
        
        # Call the method
        result = self.extractor.get_forces()
        
        # Assertions
        self.mock_client._make_request.assert_called_once_with("forces")
        self.assertEqual(result, [])
    
    def test_get_force_details(self):
        """Test get_force_details method."""
        # Setup mock
        self.mock_client._make_request.return_value = self.sample_force_details
        
        # Call the method
        result = self.extractor.get_force_details("avon-and-somerset")
        
        # Assertions
        self.mock_client._make_request.assert_called_once_with("forces/avon-and-somerset")
        self.assertEqual(result, self.sample_force_details)
    
    def test_get_force_senior_officers(self):
        """Test get_force_senior_officers method."""
        # Setup mock
        self.mock_client._make_request.return_value = self.sample_senior_officers
        
        # Call the method
        result = self.extractor.get_force_senior_officers("avon-and-somerset")
        
        # Assertions
        self.mock_client._make_request.assert_called_once_with("forces/avon-and-somerset/people")
        self.assertEqual(result, self.sample_senior_officers)
    
    def test_get_force_senior_officers_empty_response(self):
        """Test get_force_senior_officers method with empty response."""
        # Setup mock
        self.mock_client._make_request.return_value = None
        
        # Call the method
        result = self.extractor.get_force_senior_officers("avon-and-somerset")
        
        # Assertions
        self.mock_client._make_request.assert_called_once_with("forces/avon-and-somerset/people")
        self.assertEqual(result, [])
    
    @patch('os.path.getsize')
    @patch('builtins.open', new_callable=mock_open)
    @patch('os.makedirs')
    def test_save_to_csv_dict_input(self, mock_makedirs, mock_file_open, mock_getsize):
        """Test save_to_csv method with dictionary input."""
        # Setup mocks
        mock_getsize.return_value = 0
        
        # Call the method
        result = self.extractor.save_to_csv(
            data=self.sample_force_details,
            filename="test_force_details",
            output_dir="test_output"
        )
        
        # Assertions
        mock_makedirs.assert_called_once_with("test_output", exist_ok=True)
        mock_file_open.assert_called_once()
        self.assertTrue(result.startswith("test_output/test_force_details_"))
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
            data=self.sample_forces,
            filename="test_forces",
            output_dir="test_output"
        )
        
        # Assertions
        mock_makedirs.assert_called_once_with("test_output", exist_ok=True)
        mock_file_open.assert_called_once()
        self.assertTrue(result.startswith("test_output/test_forces_"))
        self.assertTrue(result.endswith(".csv"))
    
    def test_flatten_dict(self):
        """Test _flatten_dict method."""
        # Test data
        nested_dict = {
            "id": "avon-and-somerset",
            "name": "Avon and Somerset Constabulary",
            "contact_details": {
                "telephone": "101",
                "website": "https://www.avonandsomerset.police.uk/",
                "social": {
                    "twitter": "@aspolice",
                    "facebook": "avonandsomersetpolice"
                }
            }
        }
        
        # Call the method
        result = self.extractor._flatten_dict(nested_dict)
        
        # Assertions
        expected_result = {
            "id": "avon-and-somerset",
            "name": "Avon and Somerset Constabulary",
            "contact_details_telephone": "101",
            "contact_details_website": "https://www.avonandsomerset.police.uk/",
            "contact_details_social_twitter": "@aspolice",
            "contact_details_social_facebook": "avonandsomersetpolice"
        }
        self.assertEqual(result, expected_result)
    
    @patch('police_api_extractor.extractors.forces.ForceExtractor.get_forces')
    @patch('police_api_extractor.extractors.forces.ForceExtractor.save_to_csv')
    def test_extract_forces_to_csv(self, mock_save_to_csv, mock_get_forces):
        """Test extract_forces_to_csv method."""
        # Setup mocks
        mock_get_forces.return_value = self.sample_forces
        mock_save_to_csv.return_value = "test_output/police_forces_20210120_120000.csv"
        
        # Call the method
        data, filepath = self.extractor.extract_forces_to_csv(output_dir="test_output")
        
        # Assertions
        mock_get_forces.assert_called_once()
        mock_save_to_csv.assert_called_once_with(self.sample_forces, "police_forces", "test_output")
        self.assertEqual(data, self.sample_forces)
        self.assertEqual(filepath, "test_output/police_forces_20210120_120000.csv")
    
    @patch('police_api_extractor.extractors.forces.ForceExtractor.get_force_details')
    @patch('police_api_extractor.extractors.forces.ForceExtractor.save_to_csv')
    def test_extract_force_details_to_csv(self, mock_save_to_csv, mock_get_force_details):
        """Test extract_force_details_to_csv method."""
        # Setup mocks
        mock_get_force_details.return_value = self.sample_force_details
        mock_save_to_csv.return_value = "test_output/force_details_avon-and-somerset_20210120_120000.csv"
        
        # Call the method
        data, filepath = self.extractor.extract_force_details_to_csv(
            force_id="avon-and-somerset",
            output_dir="test_output"
        )
        
        # Assertions
        mock_get_force_details.assert_called_once_with("avon-and-somerset")
        mock_save_to_csv.assert_called_once_with(
            self.sample_force_details, 
            "force_details_avon-and-somerset", 
            "test_output"
        )
        self.assertEqual(data, self.sample_force_details)
        self.assertEqual(filepath, "test_output/force_details_avon-and-somerset_20210120_120000.csv")
    
    @patch('police_api_extractor.extractors.forces.ForceExtractor.get_force_senior_officers')
    @patch('police_api_extractor.extractors.forces.ForceExtractor.save_to_csv')
    def test_extract_senior_officers_to_csv(self, mock_save_to_csv, mock_get_force_senior_officers):
        """Test extract_senior_officers_to_csv method."""
        # Setup mocks
        mock_get_force_senior_officers.return_value = self.sample_senior_officers
        mock_save_to_csv.return_value = "test_output/senior_officers_avon-and-somerset_20210120_120000.csv"
        
        # Call the method
        data, filepath = self.extractor.extract_senior_officers_to_csv(
            force_id="avon-and-somerset",
            output_dir="test_output"
        )
        
        # Assertions
        mock_get_force_senior_officers.assert_called_once_with("avon-and-somerset")
        mock_save_to_csv.assert_called_once_with(
            self.sample_senior_officers, 
            "senior_officers_avon-and-somerset", 
            "test_output"
        )
        self.assertEqual(data, self.sample_senior_officers)
        self.assertEqual(filepath, "test_output/senior_officers_avon-and-somerset_20210120_120000.csv")

if __name__ == "__main__":
    unittest.main()