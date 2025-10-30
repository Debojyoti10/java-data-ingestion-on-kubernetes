-- Weather Data Pipeline Database Schema
-- This script creates the necessary tables for the weather data ingestion pipeline

-- Create database (run this manually in PostgreSQL first)
-- CREATE DATABASE weather_db;

-- Use the weather_db database
-- \c weather_db;

-- Create the weather_data table
CREATE TABLE IF NOT EXISTS weather_data (
    id SERIAL PRIMARY KEY,
    city VARCHAR(100) NOT NULL,
    country VARCHAR(10),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    temperature DECIMAL(5, 2),
    feels_like DECIMAL(5, 2),
    temp_min DECIMAL(5, 2),
    temp_max DECIMAL(5, 2),
    humidity INTEGER CHECK (humidity >= 0 AND humidity <= 100),
    pressure INTEGER,
    sea_level INTEGER,
    ground_level INTEGER,
    wind_speed DECIMAL(5, 2),
    wind_direction INTEGER CHECK (wind_direction >= 0 AND wind_direction <= 360),
    wind_gust DECIMAL(5, 2),
    weather_main VARCHAR(50),
    weather_description VARCHAR(200),
    cloudiness INTEGER CHECK (cloudiness >= 0 AND cloudiness <= 100),
    visibility INTEGER,
    uv_index DECIMAL(3, 1),
    sunrise_time TIMESTAMP,
    sunset_time TIMESTAMP,
    api_timestamp TIMESTAMP,
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Spark-transformed fields
    temperature_category VARCHAR(20),
    heat_index DECIMAL(5, 2),
    wind_category VARCHAR(20),
    comfort_level VARCHAR(20),
    data_quality_score INTEGER,
    processing_timestamp TIMESTAMP,
    temperature_fahrenheit DECIMAL(5, 2)
);

-- Add columns for Spark transformations if they don't exist (for existing tables)
DO $$ 
BEGIN
    -- Add temperature_category column if it doesn't exist
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'weather_data' AND column_name = 'temperature_category') THEN
        ALTER TABLE weather_data ADD COLUMN temperature_category VARCHAR(20);
    END IF;
    
    -- Add heat_index column if it doesn't exist
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'weather_data' AND column_name = 'heat_index') THEN
        ALTER TABLE weather_data ADD COLUMN heat_index DECIMAL(5, 2);
    END IF;
    
    -- Add wind_category column if it doesn't exist
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'weather_data' AND column_name = 'wind_category') THEN
        ALTER TABLE weather_data ADD COLUMN wind_category VARCHAR(20);
    END IF;
    
    -- Add comfort_level column if it doesn't exist
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'weather_data' AND column_name = 'comfort_level') THEN
        ALTER TABLE weather_data ADD COLUMN comfort_level VARCHAR(20);
    END IF;
    
    -- Add data_quality_score column if it doesn't exist
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'weather_data' AND column_name = 'data_quality_score') THEN
        ALTER TABLE weather_data ADD COLUMN data_quality_score INTEGER;
    END IF;
    
    -- Add processing_timestamp column if it doesn't exist
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'weather_data' AND column_name = 'processing_timestamp') THEN
        ALTER TABLE weather_data ADD COLUMN processing_timestamp TIMESTAMP;
    END IF;
    
    -- Add temperature_fahrenheit column if it doesn't exist
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'weather_data' AND column_name = 'temperature_fahrenheit') THEN
        ALTER TABLE weather_data ADD COLUMN temperature_fahrenheit DECIMAL(5, 2);
    END IF;
END $$;

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_weather_city ON weather_data(city);
CREATE INDEX IF NOT EXISTS idx_weather_country ON weather_data(country);
CREATE INDEX IF NOT EXISTS idx_weather_recorded_at ON weather_data(recorded_at);
CREATE INDEX IF NOT EXISTS idx_weather_api_timestamp ON weather_data(api_timestamp);

-- Create a summary table for aggregated data
CREATE TABLE IF NOT EXISTS weather_summary (
    id SERIAL PRIMARY KEY,
    city VARCHAR(100) NOT NULL,
    date DATE NOT NULL,
    avg_temperature DECIMAL(5, 2),
    max_temperature DECIMAL(5, 2),
    min_temperature DECIMAL(5, 2),
    avg_humidity DECIMAL(5, 2),
    avg_pressure DECIMAL(7, 2),
    total_records INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(city, date)
);

-- Create index on summary table
CREATE INDEX IF NOT EXISTS idx_weather_summary_city_date ON weather_summary(city, date);

-- Insert some sample cities for reference (optional)
CREATE TABLE IF NOT EXISTS cities (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    country VARCHAR(10),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample cities
INSERT INTO cities (name, country, latitude, longitude) VALUES
('London', 'GB', 51.5074, -0.1278),
('New York', 'US', 40.7128, -74.0060),
('Tokyo', 'JP', 35.6762, 139.6503),
('Paris', 'FR', 48.8566, 2.3522),
('Sydney', 'AU', -33.8688, 151.2093),
('Mumbai', 'IN', 19.0760, 72.8777),
('Berlin', 'DE', 52.5200, 13.4050),
('Toronto', 'CA', 43.6532, -79.3832),
('Singapore', 'SG', 1.3521, 103.8198),
('Dubai', 'AE', 25.2048, 55.2708)
ON CONFLICT (name) DO NOTHING;