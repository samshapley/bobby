import unittest
from unittest.mock import patch, Mock, mock_open
import os
import json
import tempfile
import csv
from police_api_extractor.extractors.crimes import CrimeExtractor

class TestCrimeExtractor(unittest.TestCase):
    """
    Tests for the CrimeExtractor class.
    """
    
    def setUp(self):
        """Set up test environment before each test."""
        self.mock_client = Mock()
        self.extractor = CrimeExtractor(client=self.mock_client)
        
        # Sample crime data for testing
        self.sample_crimes = [
            {
                "category": "anti-social-behaviour",
                "location_type": "Force",
                "location": {
                    "latitude": "52.6394",
                    "longitude": "-1.13119",
                    "street": {
                        "id": 884343,
                        "name": "On or near Shopping Area"
                    }
                },
                "context": "",
                "outcome_status": None,
                "persistent_id": "a8df1b5aa9518c5fb3fc7f97d57e845b8a4b7a01e7bd6b175a7f699060af6c8c",
                "id": 78212911,
                "location_subtype": "",
                "month": "2021-01"
            },
            {
                "category": "burglary",
                "location_type": "Force",
                "location": {
                    "latitude": "52.6389",
                    "longitude": "-1.1312",
                    "street": {
                        "id": 884343,
                        "name": "On or near Shopping Area"
                    }
                },
                "context": "",
                "outcome_status": {
                    "category": "under-investigation",
                    "date": "2021-01"
                },
                "persistent_id": "68c99ad0c82e3ee93c871c9ec7e8365fa4e645de6926f8e0a6d3c382b5eaef5e",
                "id": 78213024,
                "location_subtype": "",
                "month": "2021-01"
            }
        ]
        
        # Sample crime outcomes for testing
        self.sample_outcomes = [
            {
                "category": {
                    "code": "no-further-action",
                    "name": "Investigation complete; no suspect identified"
                },
                "date": "2021-01",
                "person_id": None,
                "crime": {
                    "category": "anti-social-behaviour",
                    "persistent_id": "a8df1b5aa9518c5fb3fc7f97d57e845b8a4b7a01e7bd6b175a7f699060af6c8c",
                    "location_type": "Force",
                    "location": {
                        "latitude": "52.6394",
                        "longitude": "-1.13119",
                        "street": {
                            "id": 884343,
                            "name": "On or near Shopping Area"
                        }
                    },
                    "context": "",
                    "id": 78212911,
                    "location_subtype": "",
                    "month": "2021-01"
                }
            }
        ]
        
        # Sample crime categories for testing
        self.sample_categories = [
            {"url": "all-crime", "name": "All crime"},
            {"url": "anti-social-behaviour", "name": "Anti-social behaviour"},
            {"url": "bicycle-theft", "name": "Bicycle theft"}
        ]
        
        # Sample crime with no location data for testing
        self.sample_crimes_no_location = [
            {
                "category": "anti-social-behaviour",
                "persistent_id": "a8df1b5aa9518c5fb3fc7f97d57e845b8a4b7a01e7bd6b175a7f699060af6c8c",
                "id": 78212911,
                "context": "",
                "month": "2021-01"
            }
        ]
        
        # Sample last updated data for testing
        self.sample_last_updated = {
            "date": "2023-03-15T12:00:00Z"
        }
    
    def tearDown(self):
        """Clean up after each test."""
        pass
    
    def test_get_street_crimes_point(self):
        """Test get_street_crimes with point parameters."""
        # Setup mock
        self.mock_client._make_request.return_value = self.sample_crimes
        
        # Call the method
        result = self.extractor.get_street_crimes(
            lat=52.6394,
            lng=-1.13119,
            date="2021-01"
        )
        
        # Assertions
        self.mock_client._make_request.assert_called_once_with(
            "crimes-street/all-crime",
            params={"lat": 52.6394, "lng": -1.13119, "date": "2021-01"}
        )
        self.assertEqual(result, self.sample_crimes)
    
    def test_get_street_crimes_poly(self):
        """Test get_street_crimes with polygon parameters."""
        # Setup mock
        self.mock_client._make_request.return_value = self.sample_crimes
        
        # Call the method
        result = self.extractor.get_street_crimes(
            poly="52.268,0.543:52.794,0.238:52.130,0.478",
            date="2021-01"
        )
        
        # Assertions
        self.mock_client._make_request.assert_called_once_with(
            "crimes-street/all-crime",
            params={"poly": "52.268,0.543:52.794,0.238:52.130,0.478", "date": "2021-01"}
        )
        self.assertEqual(result, self.sample_crimes)
    
    def test_get_street_crimes_validation_error(self):
        """Test get_street_crimes with missing required parameters."""
        # Call the method and check exception
        with self.assertRaises(ValueError):
            self.extractor.get_street_crimes()
        
        # Assertions
        self.mock_client._make_request.assert_not_called()
    
    def test_get_crimes_at_location(self):
        """Test get_crimes_at_location method."""
        # Setup mock
        self.mock_client._make_request.return_value = self.sample_crimes
        
        # Call the method
        result = self.extractor.get_crimes_at_location(
            lat=52.6394,
            lng=-1.13119,
            date="2021-01",
            category="all-crime"
        )
        
        # Assertions
        self.mock_client._make_request.assert_called_once_with(
            "crimes-at-location",
            params={"lat": 52.6394, "lng": -1.13119, "date": "2021-01", "category": "all-crime"}
        )
        self.assertEqual(result, self.sample_crimes)
    
    def test_get_crime_categories(self):
        """Test get_crime_categories method."""
        # Setup mock response
        self.mock_client._make_request.return_value = self.sample_categories
        
        # Call the method
        result = self.extractor.get_crime_categories(date="2021-01")
        
        # Assertions
        self.mock_client._make_request.assert_called_once_with(
            "crime-categories",
            params={"date": "2021-01"}
        )
        self.assertEqual(result, self.sample_categories)
    
    def test_get_street_outcomes(self):
        """Test get_street_outcomes method with point parameters."""
        # Setup mock
        self.mock_client._make_request.return_value = self.sample_outcomes
        
        # Call the method
        result = self.extractor.get_street_outcomes(
            lat=52.6394,
            lng=-1.13119,
            date="2021-01"
        )
        
        # Assertions
        self.mock_client._make_request.assert_called_once_with(
            "outcomes-at-location",
            params={"lat": 52.6394, "lng": -1.13119, "date": "2021-01"}
        )
        self.assertEqual(result, self.sample_outcomes)
    
    def test_get_street_outcomes_with_poly(self):
        """Test get_street_outcomes method with polygon parameters."""
        # Setup mock
        self.mock_client._make_request.return_value = self.sample_outcomes
        
        # Call the method
        result = self.extractor.get_street_outcomes(
            poly="52.268,0.543:52.794,0.238:52.130,0.478",
            date="2021-01"
        )
        
        # Assertions
        self.mock_client._make_request.assert_called_once_with(
            "outcomes-at-location",
            params={"poly": "52.268,0.543:52.794,0.238:52.130,0.478", "date": "2021-01"}
        )
        self.assertEqual(result, self.sample_outcomes)
    
    def test_get_street_outcomes_validation_error(self):
        """Test get_street_outcomes with missing required parameters."""
        # Call the method and check exception
        with self.assertRaises(ValueError):
            self.extractor.get_street_outcomes()
        
        # Assertions
        self.mock_client._make_request.assert_not_called()
    
    def test_get_crimes_no_location(self):
        """Test get_crimes_no_location method."""
        # Setup mock
        self.mock_client._make_request.return_value = self.sample_crimes_no_location
        
        # Call the method
        result = self.extractor.get_crimes_no_location(
            force_id="leicestershire",
            date="2021-01",
            category="all-crime"
        )
        
        # Assertions
        self.mock_client._make_request.assert_called_once_with(
            "crimes-no-location",
            params={"force": "leicestershire", "date": "2021-01", "category": "all-crime"}
        )
        self.assertEqual(result, self.sample_crimes_no_location)
    
    def test_get_last_updated(self):
        """Test get_last_updated method."""
        # Setup mock
        self.mock_client._make_request.return_value = self.sample_last_updated
        
        # Call the method
        result = self.extractor.get_last_updated()
        
        # Assertions
        self.mock_client._make_request.assert_called_once_with("crime-last-updated")
        self.assertEqual(result, self.sample_last_updated)
    
    def test_get_outcomes_for_crime(self):
        """Test get_outcomes_for_crime method."""
        # Setup mock
        persistent_id = "a8df1b5aa9518c5fb3fc7f97d57e845b8a4b7a01e7bd6b175a7f699060af6c8c"
        self.mock_client._make_request.return_value = self.sample_outcomes
        
        # Call the method
        result = self.extractor.get_outcomes_for_crime(persistent_id)
        
        # Assertions
        self.mock_client._make_request.assert_called_once_with(f"outcomes-for-crime/{persistent_id}")
        self.assertEqual(result, self.sample_outcomes)
    
    @patch('os.path.getsize')
    @patch('builtins.open', new_callable=mock_open)
    @patch('os.makedirs')
    def test_save_to_csv(self, mock_makedirs, mock_file_open, mock_getsize):
        """Test save_to_csv method."""
        # Setup mocks
        mock_getsize.return_value = 0
        
        # Call the method
        result = self.extractor.save_to_csv(
            data=self.sample_crimes,
            filename="test_crimes",
            output_dir="test_output"
        )
        
        # Assertions
        mock_makedirs.assert_called_once_with("test_output", exist_ok=True)
        mock_file_open.assert_called_once()
        self.assertTrue(result.startswith("test_output/test_crimes_"))
        self.assertTrue(result.endswith(".csv"))

    def test_flatten_dict(self):
        """Test _flatten_dict method."""
        # Test data
        nested_dict = {
            "id": 123,
            "location": {
                "latitude": "52.6394",
                "longitude": "-1.13119",
                "street": {
                    "id": 884343,
                    "name": "On or near Shopping Area"
                }
            }
        }
        
        # Call the method
        result = self.extractor._flatten_dict(nested_dict)
        
        # Assertions
        expected_result = {
            "id": 123,
            "location_latitude": "52.6394",
            "location_longitude": "-1.13119",
            "location_street_id": 884343,
            "location_street_name": "On or near Shopping Area"
        }
        self.assertEqual(result, expected_result)
    
    @patch('police_api_extractor.extractors.crimes.CrimeExtractor.get_street_crimes')
    @patch('police_api_extractor.extractors.crimes.CrimeExtractor.save_to_csv')
    def test_extract_street_crimes_to_csv(self, mock_save_to_csv, mock_get_street_crimes):
        """Test extract_street_crimes_to_csv method."""
        # Setup mocks
        mock_get_street_crimes.return_value = self.sample_crimes
        mock_save_to_csv.return_value = "test_output/street_crimes_all-crime_2021-01_52.6394_-1.13119_20210120_120000.csv"
        
        # Call the method
        data, filepath = self.extractor.extract_street_crimes_to_csv(
            lat=52.6394,
            lng=-1.13119,
            date="2021-01"
        )
        
        # Assertions
        mock_get_street_crimes.assert_called_once_with(52.6394, -1.13119, "2021-01", None, "all-crime")
        self.assertEqual(data, self.sample_crimes)
        self.assertEqual(filepath, "test_output/street_crimes_all-crime_2021-01_52.6394_-1.13119_20210120_120000.csv")

    @patch('police_api_extractor.extractors.crimes.CrimeExtractor.get_street_outcomes')
    @patch('police_api_extractor.extractors.crimes.CrimeExtractor.save_to_csv')
    def test_extract_street_outcomes_to_csv(self, mock_save_to_csv, mock_get_street_outcomes):
        """Test extract_street_outcomes_to_csv method."""
        # Setup mocks
        mock_get_street_outcomes.return_value = self.sample_outcomes
        mock_save_to_csv.return_value = "test_output/street_outcomes_2021-01_52.6394_-1.13119_20210120_120000.csv"
        
        # Call the method
        data, filepath = self.extractor.extract_street_outcomes_to_csv(
            lat=52.6394,
            lng=-1.13119,
            date="2021-01"
        )
        
        # Assertions
        mock_get_street_outcomes.assert_called_once_with(52.6394, -1.13119, "2021-01", None)
        self.assertEqual(data, self.sample_outcomes)
        self.assertEqual(filepath, "test_output/street_outcomes_2021-01_52.6394_-1.13119_20210120_120000.csv")

    @patch('police_api_extractor.extractors.crimes.CrimeExtractor.get_crimes_no_location')
    @patch('police_api_extractor.extractors.crimes.CrimeExtractor.save_to_csv')
    def test_extract_crimes_no_location_to_csv(self, mock_save_to_csv, mock_get_crimes_no_location):
        """Test extract_crimes_no_location_to_csv method."""
        # Setup mocks
        mock_get_crimes_no_location.return_value = self.sample_crimes_no_location
        mock_save_to_csv.return_value = "test_output/crimes_no_location_leicestershire_all-crime_2021-01_20210120_120000.csv"
        
        # Call the method
        data, filepath = self.extractor.extract_crimes_no_location_to_csv(
            force_id="leicestershire",
            date="2021-01",
            category="all-crime"
        )
        
        # Assertions
        mock_get_crimes_no_location.assert_called_once_with("leicestershire", "2021-01", "all-crime")
        self.assertEqual(data, self.sample_crimes_no_location)
        self.assertEqual(filepath, "test_output/crimes_no_location_leicestershire_all-crime_2021-01_20210120_120000.csv")

if __name__ == "__main__":
    unittest.main()