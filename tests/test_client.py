import unittest
from unittest.mock import patch, Mock, mock_open
import requests
import json
from bobby.police_api.client import UKPoliceAPIClient

class TestUKPoliceAPIClient(unittest.TestCase):
    """
    Tests for the UKPoliceAPIClient class.
    """
    
    def setUp(self):
        """Set up test environment before each test."""
        self.client = UKPoliceAPIClient()
    
    def tearDown(self):
        """Clean up after each test."""
        pass
    
    @patch('requests.get')
    def test_make_request_get_success(self, mock_get):
        """Test successful GET request."""
        # Setup mock response
        mock_response = Mock()
        mock_response.text = json.dumps({"test": "data"})
        mock_response.json.return_value = {"test": "data"}
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        # Call the method
        result = self.client._make_request("test-endpoint", method="GET", params={"param1": "value1"})
        
        # Assertions
        mock_get.assert_called_once_with(
            "https://data.police.uk/api/test-endpoint",
            params={"param1": "value1"},
            timeout=30
        )
        self.assertEqual(result, {"test": "data"})
    
    @patch('requests.post')
    def test_make_request_post_success(self, mock_post):
        """Test successful POST request."""
        # Setup mock response
        mock_response = Mock()
        mock_response.text = json.dumps({"test": "data"})
        mock_response.json.return_value = {"test": "data"}
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response
        
        # Call the method
        result = self.client._make_request(
            "test-endpoint", 
            method="POST", 
            params={"param1": "value1"}, 
            data={"data1": "value1"}
        )
        
        # Assertions
        mock_post.assert_called_once_with(
            "https://data.police.uk/api/test-endpoint",
            params={"param1": "value1"},
            json={"data1": "value1"},
            timeout=30
        )
        self.assertEqual(result, {"test": "data"})
    
    @patch('requests.get')
    def test_make_request_http_error(self, mock_get):
        """Test handling of HTTP errors."""
        # Setup mock response with proper HTTP error response
        mock_response = Mock()
        mock_response.status_code = 404
        http_error = requests.exceptions.HTTPError()
        http_error.response = mock_response  # Set the response attribute
        mock_response.raise_for_status.side_effect = http_error
        mock_get.return_value = mock_response
        
        # Call the method and check exception
        with self.assertRaises(requests.exceptions.HTTPError):
            self.client._make_request("test-endpoint")
        
        # Assertions
        mock_get.assert_called_once()
    
    @patch('requests.get')
    def test_make_request_rate_limit_error(self, mock_get):
        """Test handling of rate limit errors."""
        # Setup mock response
        mock_response = Mock()
        http_error = requests.exceptions.HTTPError()
        mock_response.status_code = 429
        http_error.response = mock_response
        mock_response.raise_for_status.side_effect = http_error
        mock_get.return_value = mock_response
        
        # Call the method and check exception
        with self.assertRaises(requests.exceptions.HTTPError):
            self.client._make_request("test-endpoint")
        
        # Assertions
        mock_get.assert_called_once()
    
    @patch('requests.get')
    def test_make_request_connection_error(self, mock_get):
        """Test handling of connection errors."""
        # Setup mock
        mock_get.side_effect = requests.exceptions.ConnectionError()
        
        # Call the method and check exception
        with self.assertRaises(requests.exceptions.ConnectionError):
            self.client._make_request("test-endpoint")
        
        # Assertions
        mock_get.assert_called_once()
    
    @patch('requests.get')
    def test_make_request_json_error(self, mock_get):
        """Test handling of JSON parse errors."""
        # Setup mock response
        mock_response = Mock()
        mock_response.text = "Not JSON"
        mock_response.json.side_effect = ValueError()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        # Call the method and check exception
        with self.assertRaises(ValueError):
            self.client._make_request("test-endpoint")
        
        # Assertions
        mock_get.assert_called_once()
    
    @patch('requests.get')
    def test_make_request_empty_response(self, mock_get):
        """Test handling of empty responses."""
        # Setup mock response
        mock_response = Mock()
        mock_response.text = ""
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        # Call the method
        result = self.client._make_request("test-endpoint")
        
        # Assertions
        self.assertIsNone(result)
        mock_get.assert_called_once()
    
    @patch('police_api_extractor.client.UKPoliceAPIClient._make_request')
    def test_check_availability(self, mock_make_request):
        """Test check_availability method."""
        # Setup mock
        mock_make_request.return_value = {"date": ["2023-01", "2023-02", "2023-03"]}
        
        # Call the method
        result = self.client.check_availability()
        
        # Assertions
        mock_make_request.assert_called_once_with("crimes-street-dates")
        self.assertEqual(result, {"date": ["2023-01", "2023-02", "2023-03"]})

if __name__ == "__main__":
    unittest.main()