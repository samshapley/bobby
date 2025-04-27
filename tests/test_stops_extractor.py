import unittest
from unittest.mock import patch, Mock, mock_open
import os
import json
from bobby.police_api.extractors.stops import StopsExtractor

class TestStopsExtractor(unittest.TestCase):
    """
    Tests for the StopsExtractor class.
    """
    
    def setUp(self):
        """Set up test environment before each test."""
        self.mock_client = Mock()
        self.extractor = StopsExtractor(client=self.mock_client)
        
        # Sample stops data for testing
        self.sample_stops = [
            {
                "age_range": "18-24",
                "outcome": "A no further action disposal",
                "involved_person": True,
                "self_defined_ethnicity": "White - English/Welsh/Scottish/Northern Irish/British",
                "gender": "Male",
                "legislation": "Misuse of Drugs Act 1971 (section 23)",
                "outcome_linked_to_object_of_search": False,
                "datetime": "2021-01-01T12:00:00+00:00",
                "removal_of_more_than_outer_clothing": False,
                "operation": None,
                "officer_defined_ethnicity": "White",
                "type": "Person search",
                "operation_name": None,
                "object_of_search": "Controlled drugs",
                "location": {
                    "latitude": "52.6394",
                    "longitude": "-1.13119",
                    "street": {
                        "id": 884343,
                        "name": "On or near High Street"
                    }
                }
            },
            {
                "age_range": "25-34",
                "outcome": "Arrest",
                "involved_person": True,
                "self_defined_ethnicity": "White - English/Welsh/Scottish/Northern Irish/British",
                "gender": "Male",
                "legislation": "Police and Criminal Evidence Act 1984 (section 1)",
                "outcome_linked_to_object_of_search": True,
                "datetime": "2021-01-02T15:30:00+00:00",
                "removal_of_more_than_outer_clothing": False,
                "operation": None,
                "officer_defined_ethnicity": "White",
                "type": "Person search",
                "operation_name": None,
                "object_of_search": "Offensive weapons",
                "location": {
                    "latitude": "52.6391",
                    "longitude": "-1.13140",
                    "street": {
                        "id": 884344,
                        "name": "On or near Market Street"
                    }
                }
            }
        ]
        
        # Sample stops at location data for testing
        self.sample_stops_at_location = [
            {
                "age_range": "18-24",
                "outcome": "A no further action disposal",
                "involved_person": True,
                "self_defined_ethnicity": "White - English/Welsh/Scottish/Northern Irish/British",
                "gender": "Male",
                "legislation": "Misuse of Drugs Act 1971 (section 23)",
                "outcome_linked_to_object_of_search": False,
                "datetime": "2021-01-01T12:00:00+00:00",
                "removal_of_more_than_outer_clothing": False,
                "operation": None,
                "officer_defined_ethnicity": "White",
                "type": "Person search",
                "operation_name": None,
                "object_of_search": "Controlled drugs",
                "location": {
                    "latitude": "52.6394",
                    "longitude": "-1.13119",
                    "street": {
                        "id": 884343,
                        "name": "On or near High Street"
                    }
                }
            }
        ]
        
        # Sample stops no location data for testing
        self.sample_stops_no_location = [
            {
                "age_range": "25-34",
                "outcome": "Arrest",
                "involved_person": True,
                "self_defined_ethnicity": "White - English/Welsh/Scottish/Northern Irish/British",
                "gender": "Male",
                "legislation": "Police and Criminal Evidence Act 1984 (section 1)",
                "outcome_linked_to_object_of_search": True,
                "datetime": "2021-01-02T15:30:00+00:00",
                "removal_of_more_than_outer_clothing": False,
                "operation": None,
                "officer_defined_ethnicity": "White",
                "type": "Person search",
                "operation_name": None,
                "object_of_search": "Offensive weapons"
            }
        ]
    
    def tearDown(self):
        """Clean up after each test."""
        pass
    
    def test_get_stops_by_area_with_point(self):
        """Test get_stops_by_area method with point parameters."""
        # Setup mock
        self.mock_client._make_request.return_value = self.sample_stops
        
        # Call the method
        result = self.extractor.get_stops_by_area(
            lat=52.6394,
            lng=-1.13119,
            date="2021-01"
        )
        
        # Assertions
        self.mock_client._make_request.assert_called_once_with(
            "stops-street",
            params={"lat": 52.6394, "lng": -1.13119, "date": "2021-01"}
        )
        self.assertEqual(result, self.sample_stops)
    
    def test_get_stops_by_area_with_poly(self):
        """Test get_stops_by_area method with polygon parameters."""
        # Setup mock
        self.mock_client._make_request.return_value = self.sample_stops
        
        # Call the method
        result = self.extractor.get_stops_by_area(
            poly="52.268,0.543:52.794,0.238:52.130,0.478",
            date="2021-01"
        )
        
        # Assertions
        self.mock_client._make_request.assert_called_once_with(
            "stops-street",
            params={"poly": "52.268,0.543:52.794,0.238:52.130,0.478", "date": "2021-01"}
        )
        self.assertEqual(result, self.sample_stops)
    
    def test_get_stops_by_area_both_point_and_poly(self):
        """Test get_stops_by_area method with both point and polygon parameters."""
        # Setup mock
        self.mock_client._make_request.return_value = self.sample_stops
        
        # Call the method
        result = self.extractor.get_stops_by_area(
            lat=52.6394,
            lng=-1.13119,
            poly="52.268,0.543:52.794,0.238:52.130,0.478",
            date="2021-01"
        )
        
        # Assertions
        self.mock_client._make_request.assert_called_once_with(
            "stops-street",
            params={"poly": "52.268,0.543:52.794,0.238:52.130,0.478", "date": "2021-01"}
        )
        self.assertEqual(result, self.sample_stops)
    
    def test_get_stops_by_area_validation_error(self):
        """Test get_stops_by_area method with missing required parameters."""
        # Call the method and check exception
        with self.assertRaises(ValueError):
            self.extractor.get_stops_by_area()
        
        # Assertions
        self.mock_client._make_request.assert_not_called()
    
    def test_get_stops_by_area_empty_response(self):
        """Test get_stops_by_area method with empty response."""
        # Setup mock
        self.mock_client._make_request.return_value = None
        
        # Call the method
        result = self.extractor.get_stops_by_area(
            lat=52.6394,
            lng=-1.13119,
            date="2021-01"
        )
        
        # Assertions
        self.mock_client._make_request.assert_called_once()
        self.assertEqual(result, [])
    
    def test_get_stops_at_location(self):
        """Test get_stops_at_location method."""
        # Setup mock
        self.mock_client._make_request.return_value = self.sample_stops_at_location
        
        # Call the method
        result = self.extractor.get_stops_at_location(
            location_id="884343",
            date="2021-01"
        )
        
        # Assertions
        self.mock_client._make_request.assert_called_once_with(
            "stops-at-location?location_id=884343",
            params={"date": "2021-01"}
        )
        self.assertEqual(result, self.sample_stops_at_location)
    
    def test_get_stops_at_location_empty_response(self):
        """Test get_stops_at_location method with empty response."""
        # Setup mock
        self.mock_client._make_request.return_value = None
        
        # Call the method
        result = self.extractor.get_stops_at_location(
            location_id="884343",
            date="2021-01"
        )
        
        # Assertions
        self.mock_client._make_request.assert_called_once()
        self.assertEqual(result, [])
    
    def test_get_stops_no_location(self):
        """Test get_stops_no_location method."""
        # Setup mock
        self.mock_client._make_request.return_value = self.sample_stops_no_location
        
        # Call the method
        result = self.extractor.get_stops_no_location(
            force_id="leicestershire",
            date="2021-01"
        )
        
        # Assertions
        self.mock_client._make_request.assert_called_once_with(
            "stops-no-location",
            params={"force": "leicestershire", "date": "2021-01"}
        )
        self.assertEqual(result, self.sample_stops_no_location)
    
    def test_get_stops_no_location_empty_response(self):
        """Test get_stops_no_location method with empty response."""
        # Setup mock
        self.mock_client._make_request.return_value = None
        
        # Call the method
        result = self.extractor.get_stops_no_location(
            force_id="leicestershire",
            date="2021-01"
        )
        
        # Assertions
        self.mock_client._make_request.assert_called_once()
        self.assertEqual(result, [])
    
    def test_get_stops_by_force(self):
        """Test get_stops_by_force method."""
        # Setup mock
        self.mock_client._make_request.return_value = self.sample_stops
        
        # Call the method
        result = self.extractor.get_stops_by_force(
            force_id="leicestershire",
            date="2021-01"
        )
        
        # Assertions
        self.mock_client._make_request.assert_called_once_with(
            "stops-force/leicestershire",
            params={"date": "2021-01"}
        )
        self.assertEqual(result, self.sample_stops)
    
    def test_get_stops_by_force_empty_response(self):
        """Test get_stops_by_force method with empty response."""
        # Setup mock
        self.mock_client._make_request.return_value = None
        
        # Call the method
        result = self.extractor.get_stops_by_force(
            force_id="leicestershire",
            date="2021-01"
        )
        
        # Assertions
        self.mock_client._make_request.assert_called_once()
        self.assertEqual(result, [])
    
    @patch('os.path.getsize')
    @patch('builtins.open', new_callable=mock_open)
    @patch('os.makedirs')
    def test_save_to_csv(self, mock_makedirs, mock_file_open, mock_getsize):
        """Test save_to_csv method."""
        # Setup mocks
        mock_getsize.return_value = 0
        
        # Call the method
        result = self.extractor.save_to_csv(
            data=self.sample_stops,
            filename="test_stops",
            output_dir="test_output"
        )
        
        # Assertions
        mock_makedirs.assert_called_once_with("test_output", exist_ok=True)
        mock_file_open.assert_called_once()
        self.assertTrue(result.startswith("test_output/test_stops_"))
        self.assertTrue(result.endswith(".csv"))
    
    def test_save_to_csv_empty_data(self):
        """Test save_to_csv method with empty data."""
        # Call the method
        result = self.extractor.save_to_csv(
            data=[],
            filename="test_stops",
            output_dir="test_output"
        )
        
        # Assertions
        self.assertEqual(result, "")
    
    def test_flatten_dict(self):
        """Test _flatten_dict method."""
        # Test data
        nested_dict = {
            "age_range": "18-24",
            "outcome": "A no further action disposal",
            "gender": "Male",
            "location": {
                "latitude": "52.6394",
                "longitude": "-1.13119",
                "street": {
                    "id": 884343,
                    "name": "On or near High Street"
                }
            }
        }
        
        # Call the method
        result = self.extractor._flatten_dict(nested_dict)
        
        # Assertions
        expected_result = {
            "age_range": "18-24",
            "outcome": "A no further action disposal",
            "gender": "Male",
            "location_latitude": "52.6394",
            "location_longitude": "-1.13119",
            "location_street_id": 884343,
            "location_street_name": "On or near High Street"
        }
        self.assertEqual(result, expected_result)
    
    @patch('police_api_extractor.extractors.stops.StopsExtractor.get_stops_by_area')
    @patch('police_api_extractor.extractors.stops.StopsExtractor.save_to_csv')
    def test_extract_stops_by_area_to_csv_with_point(self, mock_save_to_csv, mock_get_stops_by_area):
        """Test extract_stops_by_area_to_csv method with point parameters."""
        # Setup mocks
        mock_get_stops_by_area.return_value = self.sample_stops
        mock_save_to_csv.return_value = "test_output/stops_area_2021-01_52.6394_-1.13119_20210120_120000.csv"
        
        # Call the method
        data, filepath = self.extractor.extract_stops_by_area_to_csv(
            lat=52.6394,
            lng=-1.13119,
            date="2021-01",
            output_dir="test_output"
        )
        
        # Assertions
        mock_get_stops_by_area.assert_called_once_with(52.6394, -1.13119, None, "2021-01")
        self.assertEqual(data, self.sample_stops)
        self.assertEqual(filepath, "test_output/stops_area_2021-01_52.6394_-1.13119_20210120_120000.csv")
    
    @patch('police_api_extractor.extractors.stops.StopsExtractor.get_stops_by_area')
    @patch('police_api_extractor.extractors.stops.StopsExtractor.save_to_csv')
    def test_extract_stops_by_area_to_csv_with_poly(self, mock_save_to_csv, mock_get_stops_by_area):
        """Test extract_stops_by_area_to_csv method with polygon parameters."""
        # Setup mocks
        mock_get_stops_by_area.return_value = self.sample_stops
        mock_save_to_csv.return_value = "test_output/stops_area_2021-01_52.268_0.543_52.794_0.238_52.130_0.478_20210120_120000.csv"
        
        # Call the method
        data, filepath = self.extractor.extract_stops_by_area_to_csv(
            poly="52.268,0.543:52.794,0.238:52.130,0.478",
            date="2021-01",
            output_dir="test_output"
        )
        
        # Assertions
        mock_get_stops_by_area.assert_called_once_with(None, None, "52.268,0.543:52.794,0.238:52.130,0.478", "2021-01")
        self.assertEqual(data, self.sample_stops)
        self.assertEqual(filepath, "test_output/stops_area_2021-01_52.268_0.543_52.794_0.238_52.130_0.478_20210120_120000.csv")
    
    @patch('police_api_extractor.extractors.stops.StopsExtractor.get_stops_by_force')
    @patch('police_api_extractor.extractors.stops.StopsExtractor.save_to_csv')
    def test_extract_stops_by_force_to_csv(self, mock_save_to_csv, mock_get_stops_by_force):
        """Test extract_stops_by_force_to_csv method."""
        # Setup mocks
        mock_get_stops_by_force.return_value = self.sample_stops
        mock_save_to_csv.return_value = "test_output/stops_force_leicestershire_2021-01_20210120_120000.csv"
        
        # Call the method
        data, filepath = self.extractor.extract_stops_by_force_to_csv(
            force_id="leicestershire",
            date="2021-01",
            output_dir="test_output"
        )
        
        # Assertions
        mock_get_stops_by_force.assert_called_once_with("leicestershire", "2021-01")
        self.assertEqual(data, self.sample_stops)
        self.assertEqual(filepath, "test_output/stops_force_leicestershire_2021-01_20210120_120000.csv")

if __name__ == "__main__":
    unittest.main()