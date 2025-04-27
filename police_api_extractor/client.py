import requests
from typing import Dict, List, Optional, Union, Any
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('police_api')

class UKPoliceAPIClient:
    """
    Base client for the UK Police Data API.
    
    This class provides the foundation for all API interactions, handling HTTP requests,
    error management, and response parsing.
    """
    
    BASE_URL = "https://data.police.uk/api"
    
    def __init__(self, timeout: int = 30):
        """
        Initialize the UK Police API client.
        
        Args:
            timeout: Request timeout in seconds
        """
        self.timeout = timeout
        logger.info("Initializing UK Police API client")
    
    def _make_request(
        self, 
        endpoint: str, 
        method: str = "GET", 
        params: Optional[Dict] = None,
        data: Optional[Dict] = None
    ) -> Union[List[Dict], Dict, None]:
        """
        Make a request to the UK Police API.
        
        Args:
            endpoint: API endpoint (without base URL)
            method: HTTP method (GET or POST)
            params: URL parameters for the request
            data: JSON data for POST requests
            
        Returns:
            Parsed JSON response
            
        Raises:
            requests.exceptions.RequestException: If the request fails
        """
        url = f"{self.BASE_URL}/{endpoint.lstrip('/')}"
        logger.debug(f"Making {method} request to {url}")
        
        try:
            if method.upper() == "GET":
                response = requests.get(url, params=params, timeout=self.timeout)
            elif method.upper() == "POST":
                response = requests.post(url, params=params, json=data, timeout=self.timeout)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            # Raise an exception for 4XX and 5XX status codes
            response.raise_for_status()
            
            # Check if response is empty
            if not response.text.strip():
                logger.warning(f"Empty response received from {url}")
                return None
                
            # Parse JSON response
            return response.json()
            
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error: {e}")
            if e.response.status_code == 429:
                logger.error("Rate limit exceeded")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise
        except ValueError as e:
            logger.error(f"Error parsing response: {e}")
            raise
            
    def check_availability(self) -> Dict[str, List[str]]:
        """
        Check what date ranges are available for crime data.
        
        Returns:
            Dictionary with available date ranges
        """
        return self._make_request("crimes-street-dates")