-- Bobby System Consolidated Database Schema
-- This schema replaces the previous approach of multiple tables with city and date in table names
-- with a consolidated approach using fewer tables with appropriate columns.

-- Drop existing tables if needed (commented out for safety - uncomment when ready to apply)
-- DROP TABLE IF EXISTS crimes;
-- DROP TABLE IF EXISTS outcomes;
-- DROP TABLE IF EXISTS stops;
-- DROP TABLE IF EXISTS senior_officers;
-- DROP TABLE IF EXISTS neighborhoods;
-- DROP TABLE IF EXISTS neighborhood_boundaries;
-- DROP TABLE IF EXISTS neighborhood_teams;
-- DROP TABLE IF EXISTS neighborhood_events;
-- DROP TABLE IF EXISTS neighborhood_priorities;
-- DROP TABLE IF EXISTS data_updates;

-- Core Tables

-- Crimes table - consolidates all crime data with city and date as columns
CREATE TABLE IF NOT EXISTS crimes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    city VARCHAR(50) NOT NULL,
    data_date VARCHAR(7) NOT NULL, -- Format: YYYY-MM
    force_id VARCHAR(50),
    category VARCHAR(100),
    location_type VARCHAR(20), -- 'street', 'specific', 'none'
    location_latitude FLOAT,
    location_longitude FLOAT,
    location_street_id INTEGER,
    location_street_name VARCHAR(255),
    context TEXT,
    outcome_status VARCHAR(100),
    outcome_status_category VARCHAR(100),
    outcome_status_date VARCHAR(10),
    persistent_id VARCHAR(100),
    month VARCHAR(7),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Outcomes table - consolidates all outcome data
CREATE TABLE IF NOT EXISTS outcomes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    city VARCHAR(50) NOT NULL,
    data_date VARCHAR(7) NOT NULL,
    force_id VARCHAR(50),
    category_code VARCHAR(50),
    category_name VARCHAR(100),
    crime_category VARCHAR(100),
    crime_location_street_id INTEGER,
    crime_location_street_name VARCHAR(255),
    crime_month VARCHAR(7),
    date VARCHAR(10),
    person_id VARCHAR(100),
    crime_id VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Stops table - consolidates all stop and search data
CREATE TABLE IF NOT EXISTS stops (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    city VARCHAR(50),
    force_id VARCHAR(50) NOT NULL,
    data_date VARCHAR(7) NOT NULL,
    stop_type VARCHAR(20) NOT NULL, -- 'standard', 'area', 'location', 'no_location'
    age_range VARCHAR(20),
    gender VARCHAR(20),
    ethnicity VARCHAR(50),
    officer_defined_ethnicity VARCHAR(50),
    self_defined_ethnicity VARCHAR(50),
    object_of_search VARCHAR(100),
    datetime VARCHAR(30),
    location_latitude FLOAT,
    location_longitude FLOAT,
    location_street_id INTEGER,
    location_street_name VARCHAR(255),
    outcome VARCHAR(100),
    outcome_linked_to_object_of_search BOOLEAN,
    removal_of_more_than_outer_clothing BOOLEAN,
    operation_name VARCHAR(100),
    type VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Reference and Entity Tables

-- Police forces table (reference table)
CREATE TABLE IF NOT EXISTS police_forces (
    id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    url VARCHAR(255),
    telephone VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Senior officers table
CREATE TABLE IF NOT EXISTS senior_officers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    force_id VARCHAR(50) NOT NULL,
    name VARCHAR(100) NOT NULL,
    rank VARCHAR(100),
    bio TEXT,
    contact_details TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (force_id) REFERENCES police_forces(id)
);

-- Neighborhoods table
CREATE TABLE IF NOT EXISTS neighborhoods (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    neighborhood_id VARCHAR(50) NOT NULL,
    force_id VARCHAR(50) NOT NULL,
    name VARCHAR(100),
    description TEXT,
    url VARCHAR(255),
    population INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (force_id) REFERENCES police_forces(id),
    UNIQUE(neighborhood_id, force_id)
);

-- Neighborhood boundaries table
CREATE TABLE IF NOT EXISTS neighborhood_boundaries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    neighborhood_id VARCHAR(50) NOT NULL,
    force_id VARCHAR(50) NOT NULL,
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL,
    sequence INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (force_id) REFERENCES police_forces(id),
    FOREIGN KEY (neighborhood_id, force_id) REFERENCES neighborhoods(neighborhood_id, force_id)
);

-- Neighborhood teams table
CREATE TABLE IF NOT EXISTS neighborhood_teams (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    neighborhood_id VARCHAR(50) NOT NULL,
    force_id VARCHAR(50) NOT NULL,
    name VARCHAR(100),
    rank VARCHAR(100),
    bio TEXT,
    contact_details TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (force_id) REFERENCES police_forces(id),
    FOREIGN KEY (neighborhood_id, force_id) REFERENCES neighborhoods(neighborhood_id, force_id)
);

-- Neighborhood events table
CREATE TABLE IF NOT EXISTS neighborhood_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    neighborhood_id VARCHAR(50) NOT NULL,
    force_id VARCHAR(50) NOT NULL,
    title VARCHAR(255) NOT NULL,
    description TEXT,
    address TEXT,
    start_date VARCHAR(30),
    end_date VARCHAR(30),
    type VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (force_id) REFERENCES police_forces(id),
    FOREIGN KEY (neighborhood_id, force_id) REFERENCES neighborhoods(neighborhood_id, force_id)
);

-- Neighborhood priorities table
CREATE TABLE IF NOT EXISTS neighborhood_priorities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    neighborhood_id VARCHAR(50) NOT NULL,
    force_id VARCHAR(50) NOT NULL,
    issue TEXT NOT NULL,
    action TEXT,
    issue_date VARCHAR(30),
    action_date VARCHAR(30),
    status VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (force_id) REFERENCES police_forces(id),
    FOREIGN KEY (neighborhood_id, force_id) REFERENCES neighborhoods(neighborhood_id, force_id)
);

-- Crime categories table (reference table)
CREATE TABLE IF NOT EXISTS crime_categories (
    name VARCHAR(100) PRIMARY KEY,
    url VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Data updates table (for tracking data currency)
CREATE TABLE IF NOT EXISTS data_updates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    data_type VARCHAR(50) NOT NULL,
    data_date VARCHAR(7) NOT NULL,
    update_date TIMESTAMP NOT NULL,
    details TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create Indexes for optimized queries

-- Crimes indexes
CREATE INDEX IF NOT EXISTS idx_crimes_city_date ON crimes(city, data_date);
CREATE INDEX IF NOT EXISTS idx_crimes_force_date ON crimes(force_id, data_date);
CREATE INDEX IF NOT EXISTS idx_crimes_category ON crimes(category);
CREATE INDEX IF NOT EXISTS idx_crimes_location ON crimes(location_latitude, location_longitude);

-- Outcomes indexes
CREATE INDEX IF NOT EXISTS idx_outcomes_city_date ON outcomes(city, data_date);
CREATE INDEX IF NOT EXISTS idx_outcomes_force_date ON outcomes(force_id, data_date);
CREATE INDEX IF NOT EXISTS idx_outcomes_category ON outcomes(category_name);

-- Stops indexes
CREATE INDEX IF NOT EXISTS idx_stops_force_date ON stops(force_id, data_date);
CREATE INDEX IF NOT EXISTS idx_stops_city_date ON stops(city, data_date);
CREATE INDEX IF NOT EXISTS idx_stops_type ON stops(stop_type);
CREATE INDEX IF NOT EXISTS idx_stops_location ON stops(location_latitude, location_longitude);

-- Neighborhood-related indexes
CREATE INDEX IF NOT EXISTS idx_neighborhoods ON neighborhoods(force_id, neighborhood_id);
CREATE INDEX IF NOT EXISTS idx_neighborhood_teams ON neighborhood_teams(force_id, neighborhood_id);
CREATE INDEX IF NOT EXISTS idx_neighborhood_events ON neighborhood_events(force_id, neighborhood_id);
CREATE INDEX IF NOT EXISTS idx_neighborhood_priorities ON neighborhood_priorities(force_id, neighborhood_id);
CREATE INDEX IF NOT EXISTS idx_neighborhood_boundaries ON neighborhood_boundaries(force_id, neighborhood_id);