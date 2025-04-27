# UK Police Data API Extractor

A Python library for extracting data from the UK Police Data API and saving it to CSV files.

## Features

- Extract data from all UK Police Data API endpoints
- Save data to CSV files
- Flatten nested JSON structures for easier analysis
- Comprehensive logging
- Full test coverage
- Clear separation of concerns

## Installation

Clone this repository:

```bash
git clone <repository_url>
cd uk-police-api-extractor
```

## Usage

### Basic Example

```python
from police_api_extractor import CrimeExtractor

# Create a crime extractor
extractor = CrimeExtractor()

# Extract street-level crimes for a specific location
data, filepath = extractor.extract_street_crimes_to_csv(
    lat=51.5074,  # London
    lng=-0.1278,
    date="2023-01",  # Optional, defaults to latest month
    output_dir="output"  # Optional, defaults to "output"
)

print(f"Saved {len(data)} records to {filepath}")
```

### Available Extractors

The library includes extractors for different categories of data:

- `CrimeExtractor`: For crime-related endpoints
- `NeighborhoodExtractor`: For neighborhood-related endpoints
- `StopsExtractor`: For stop and search related endpoints
- `ForceExtractor`: For police force related endpoints

### More Examples

Check the `examples/` directory for more detailed examples:

- `street_crime_example.py`: Shows how to extract street-level crime data
- `neighborhood_example.py`: Shows how to extract neighborhood data

## API Endpoints Covered

### Crime-related Endpoints

- Street level crimes
- Street level outcomes
- Crimes at location
- Crimes with no location
- Crime categories
- Outcomes for a specific crime

### Neighborhood-related Endpoints

- List of neighborhoods
- Specific neighborhood details
- Neighborhood boundary
- Neighborhood team
- Neighborhood events
- Neighborhood priorities
- Locate neighborhood

### Stop and Search-related Endpoints

- Stop and searches by area
- Stop and searches by location
- Stop and searches with no location
- Stop and searches by force

### Force-related Endpoints

- List of forces
- Specific force details
- Force senior officers

## Development

### Running Tests

```bash
python -m unittest discover tests
```

## License

MIT