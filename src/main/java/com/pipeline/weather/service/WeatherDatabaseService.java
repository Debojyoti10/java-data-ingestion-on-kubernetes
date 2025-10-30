package com.pipeline.weather.service;

import com.pipeline.weather.config.ConfigManager;
import com.pipeline.weather.model.WeatherData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Database service for managing weather data persistence
 * Handles all CRUD operations for weather data in PostgreSQL
 */
public class WeatherDatabaseService {
    
    private static final Logger logger = LoggerFactory.getLogger(WeatherDatabaseService.class);
    
    private final ConfigManager configManager;
    
    public WeatherDatabaseService() {
        this.configManager = ConfigManager.getInstance();
        initializeDatabase();
    }
    
    /**
     * Initializes the database connection and creates tables if they don't exist
     */
    private void initializeDatabase() {
        try {
            // Load PostgreSQL driver
            Class.forName(configManager.getDatabaseDriver());
            
            // Test connection
            try (Connection conn = getConnection()) {
                logger.info("Database connection established successfully"); //First Connection Database Initialization
                createTablesIfNotExist(conn);
            }
            
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("PostgreSQL driver not found", e);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to initialize database", e);
        }
    }
    
    /**
     * Creates database tables if they don't exist
     */
    private void createTablesIfNotExist(Connection conn) throws SQLException {
        String createWeatherTable = """
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
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """;
        
        String createIndexes = """
            CREATE INDEX IF NOT EXISTS idx_weather_city ON weather_data(city);
            CREATE INDEX IF NOT EXISTS idx_weather_country ON weather_data(country);
            CREATE INDEX IF NOT EXISTS idx_weather_recorded_at ON weather_data(recorded_at);
            CREATE INDEX IF NOT EXISTS idx_weather_api_timestamp ON weather_data(api_timestamp);
            """;
        
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(createWeatherTable);
            stmt.execute(createIndexes);
            logger.info("Database tables and indexes created/verified successfully");
        }
    }
    
    /**
     * Inserts weather data into the database
     * @param weatherData the weather data to insert
     * @return the generated ID of the inserted record
     * @throws SQLException if insertion fails
     */
    public Long insertWeatherData(WeatherData weatherData) throws SQLException {
        String insertSQL = """
            INSERT INTO weather_data 
            (city, country, latitude, longitude, temperature, feels_like, temp_min, temp_max,
             humidity, pressure, sea_level, ground_level, wind_speed, wind_direction, wind_gust,
             weather_main, weather_description, cloudiness, visibility, uv_index,
             sunrise_time, sunset_time, api_timestamp, recorded_at,
             temperature_category, heat_index, wind_category, comfort_level, 
             data_quality_score, processing_timestamp, temperature_fahrenheit) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;
        
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(insertSQL, Statement.RETURN_GENERATED_KEYS)) { // Data insertion Connection
            
            setWeatherDataParameters(stmt, weatherData);
            
            int affectedRows = stmt.executeUpdate();
            
            if (affectedRows > 0) {
                try (ResultSet generatedKeys = stmt.getGeneratedKeys()) {
                    if (generatedKeys.next()) {
                        Long id = generatedKeys.getLong(1);
                        weatherData.setId(id);
                        logger.debug("Weather data inserted successfully for {} with ID: {}", 
                            weatherData.getCity(), id);
                        return id;
                    }
                }
            }
            
            throw new SQLException("Failed to insert weather data, no ID obtained");
            
        } catch (SQLException e) {
            logger.error("Failed to insert weather data for city: {}", weatherData.getCity(), e);
            throw e;
        }
    }
    
    /**
     * Inserts multiple weather data records in batch
     * @param weatherDataList list of weather data to insert
     * @return number of successfully inserted records
     */
    public int insertWeatherDataBatch(List<WeatherData> weatherDataList) {
        if (weatherDataList == null || weatherDataList.isEmpty()) {
            return 0;
        }
        
        String insertSQL = """
            INSERT INTO weather_data 
            (city, country, latitude, longitude, temperature, feels_like, temp_min, temp_max,
             humidity, pressure, sea_level, ground_level, wind_speed, wind_direction, wind_gust,
             weather_main, weather_description, cloudiness, visibility, uv_index,
             sunrise_time, sunset_time, api_timestamp, recorded_at,
             temperature_category, heat_index, wind_category, comfort_level, 
             data_quality_score, processing_timestamp, temperature_fahrenheit) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;
        
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(insertSQL)) { // Batch Insertion Connection
            
            conn.setAutoCommit(false);
            int successCount = 0;
            
            for (WeatherData weatherData : weatherDataList) {
                try {
                    setWeatherDataParameters(stmt, weatherData);
                    stmt.addBatch();
                    successCount++;
                } catch (Exception e) {
                    logger.warn("Skipping invalid weather data for city: {}", weatherData.getCity(), e);
                }
            }
            
            int[] results = stmt.executeBatch();
            conn.commit();
            
            logger.info("Batch insert completed. {} out of {} records inserted successfully", 
                successCount, weatherDataList.size());
            
            return successCount;
            
        } catch (SQLException e) {
            logger.error("Failed to insert weather data batch", e);
            return 0;
        }
    }
    
    /**
     * Sets parameters for weather data in prepared statement
     */
    private void setWeatherDataParameters(PreparedStatement stmt, WeatherData data) throws SQLException {
        stmt.setString(1, data.getCity());
        stmt.setString(2, data.getCountry());
        
        // Handle nullable BigDecimal fields
        if (data.getLatitude() != null) {
            stmt.setBigDecimal(3, data.getLatitude());
        } else {
            stmt.setNull(3, Types.DECIMAL);
        }
        
        if (data.getLongitude() != null) {
            stmt.setBigDecimal(4, data.getLongitude());
        } else {
            stmt.setNull(4, Types.DECIMAL);
        }
        
        if (data.getTemperature() != null) {
            stmt.setBigDecimal(5, data.getTemperature());
        } else {
            stmt.setNull(5, Types.DECIMAL);
        }
        
        if (data.getFeelsLike() != null) {
            stmt.setBigDecimal(6, data.getFeelsLike());
        } else {
            stmt.setNull(6, Types.DECIMAL);
        }
        
        if (data.getTempMin() != null) {
            stmt.setBigDecimal(7, data.getTempMin());
        } else {
            stmt.setNull(7, Types.DECIMAL);
        }
        
        if (data.getTempMax() != null) {
            stmt.setBigDecimal(8, data.getTempMax());
        } else {
            stmt.setNull(8, Types.DECIMAL);
        }
        
        // Handle nullable Integer fields
        if (data.getHumidity() != null) {
            stmt.setInt(9, data.getHumidity());
        } else {
            stmt.setNull(9, Types.INTEGER);
        }
        
        if (data.getPressure() != null) {
            stmt.setInt(10, data.getPressure());
        } else {
            stmt.setNull(10, Types.INTEGER);
        }
        
        if (data.getSeaLevel() != null) {
            stmt.setInt(11, data.getSeaLevel());
        } else {
            stmt.setNull(11, Types.INTEGER);
        }
        
        if (data.getGroundLevel() != null) {
            stmt.setInt(12, data.getGroundLevel());
        } else {
            stmt.setNull(12, Types.INTEGER);
        }
        
        if (data.getWindSpeed() != null) {
            stmt.setBigDecimal(13, data.getWindSpeed());
        } else {
            stmt.setNull(13, Types.DECIMAL);
        }
        
        if (data.getWindDirection() != null) {
            stmt.setInt(14, data.getWindDirection());
        } else {
            stmt.setNull(14, Types.INTEGER);
        }
        
        if (data.getWindGust() != null) {
            stmt.setBigDecimal(15, data.getWindGust());
        } else {
            stmt.setNull(15, Types.DECIMAL);
        }
        
        stmt.setString(16, data.getWeatherMain());
        stmt.setString(17, data.getWeatherDescription());
        
        if (data.getCloudiness() != null) {
            stmt.setInt(18, data.getCloudiness());
        } else {
            stmt.setNull(18, Types.INTEGER);
        }
        
        if (data.getVisibility() != null) {
            stmt.setInt(19, data.getVisibility());
        } else {
            stmt.setNull(19, Types.INTEGER);
        }
        
        if (data.getUvIndex() != null) {
            stmt.setBigDecimal(20, data.getUvIndex());
        } else {
            stmt.setNull(20, Types.DECIMAL);
        }
        
        // Handle timestamp fields
        if (data.getSunriseTime() != null) {
            stmt.setTimestamp(21, Timestamp.valueOf(data.getSunriseTime()));
        } else {
            stmt.setNull(21, Types.TIMESTAMP);
        }
        
        if (data.getSunsetTime() != null) {
            stmt.setTimestamp(22, Timestamp.valueOf(data.getSunsetTime()));
        } else {
            stmt.setNull(22, Types.TIMESTAMP);
        }
        
        if (data.getApiTimestamp() != null) {
            stmt.setTimestamp(23, Timestamp.valueOf(data.getApiTimestamp()));
        } else {
            stmt.setNull(23, Types.TIMESTAMP);
        }
        
        if (data.getRecordedAt() != null) {
            stmt.setTimestamp(24, Timestamp.valueOf(data.getRecordedAt()));
        } else {
            stmt.setTimestamp(24, Timestamp.valueOf(LocalDateTime.now()));
        }
        
        // Set Spark-transformed fields parameters (25-31)
        stmt.setString(25, data.getTemperatureCategory());
        
        if (data.getHeatIndex() != null) {
            stmt.setBigDecimal(26, data.getHeatIndex());
        } else {
            stmt.setNull(26, Types.DECIMAL);
        }
        
        stmt.setString(27, data.getWindCategory());
        stmt.setString(28, data.getComfortLevel());
        
        if (data.getDataQualityScore() != null) {
            stmt.setInt(29, data.getDataQualityScore());
        } else {
            stmt.setNull(29, Types.INTEGER);
        }
        
        if (data.getProcessingTimestamp() != null) {
            stmt.setTimestamp(30, Timestamp.valueOf(data.getProcessingTimestamp()));
        } else {
            stmt.setNull(30, Types.TIMESTAMP);
        }
        
        if (data.getTemperatureFahrenheit() != null) {
            stmt.setBigDecimal(31, data.getTemperatureFahrenheit());
        } else {
            stmt.setNull(31, Types.DECIMAL);
        }
    }
    
    /**
     * Retrieves weather data by city
     * @param city the city name
     * @param limit maximum number of records to return
     * @return list of weather data
     */
    public List<WeatherData> getWeatherDataByCity(String city, int limit) throws SQLException {
        String selectSQL = """
            SELECT * FROM weather_data 
            WHERE city = ? 
            ORDER BY recorded_at DESC 
            LIMIT ?
            """;
        
        List<WeatherData> weatherDataList = new ArrayList<>();
        
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(selectSQL)) { // Data Retrieval Connection
            
            stmt.setString(1, city);
            stmt.setInt(2, limit);
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    WeatherData weatherData = mapResultSetToWeatherData(rs);
                    weatherDataList.add(weatherData);
                }
            }
            
            logger.debug("Retrieved {} weather records for city: {}", weatherDataList.size(), city);
            
        } catch (SQLException e) {
            logger.error("Failed to retrieve weather data for city: {}", city, e);
            throw e;
        }
        
        return weatherDataList;
    }
    
    /**
     * Maps ResultSet to WeatherData object
     */
    private WeatherData mapResultSetToWeatherData(ResultSet rs) throws SQLException {
        WeatherData data = new WeatherData();
        
        data.setId(rs.getLong("id"));
        data.setCity(rs.getString("city"));
        data.setCountry(rs.getString("country"));
        data.setLatitude(rs.getBigDecimal("latitude"));
        data.setLongitude(rs.getBigDecimal("longitude"));
        data.setTemperature(rs.getBigDecimal("temperature"));
        data.setFeelsLike(rs.getBigDecimal("feels_like"));
        data.setTempMin(rs.getBigDecimal("temp_min"));
        data.setTempMax(rs.getBigDecimal("temp_max"));
        
        // Handle nullable integers
        int humidity = rs.getInt("humidity");
        data.setHumidity(rs.wasNull() ? null : humidity);
        
        int pressure = rs.getInt("pressure");
        data.setPressure(rs.wasNull() ? null : pressure);
        
        int seaLevel = rs.getInt("sea_level");
        data.setSeaLevel(rs.wasNull() ? null : seaLevel);
        
        int groundLevel = rs.getInt("ground_level");
        data.setGroundLevel(rs.wasNull() ? null : groundLevel);
        
        data.setWindSpeed(rs.getBigDecimal("wind_speed"));
        
        int windDirection = rs.getInt("wind_direction");
        data.setWindDirection(rs.wasNull() ? null : windDirection);
        
        data.setWindGust(rs.getBigDecimal("wind_gust"));
        data.setWeatherMain(rs.getString("weather_main"));
        data.setWeatherDescription(rs.getString("weather_description"));
        
        int cloudiness = rs.getInt("cloudiness");
        data.setCloudiness(rs.wasNull() ? null : cloudiness);
        
        int visibility = rs.getInt("visibility");
        data.setVisibility(rs.wasNull() ? null : visibility);
        
        data.setUvIndex(rs.getBigDecimal("uv_index"));
        
        // Handle timestamps
        Timestamp sunriseTs = rs.getTimestamp("sunrise_time");
        data.setSunriseTime(sunriseTs != null ? sunriseTs.toLocalDateTime() : null);
        
        Timestamp sunsetTs = rs.getTimestamp("sunset_time");
        data.setSunsetTime(sunsetTs != null ? sunsetTs.toLocalDateTime() : null);
        
        Timestamp apiTs = rs.getTimestamp("api_timestamp");
        data.setApiTimestamp(apiTs != null ? apiTs.toLocalDateTime() : null);
        
        Timestamp recordedTs = rs.getTimestamp("recorded_at");
        data.setRecordedAt(recordedTs != null ? recordedTs.toLocalDateTime() : null);
        
        Timestamp createdTs = rs.getTimestamp("created_at");
        data.setCreatedAt(createdTs != null ? createdTs.toLocalDateTime() : null);
        
        return data;
    }
    
    /**
     * Gets total count of weather records
     * @return total number of records
     */
    public long getTotalRecordCount() throws SQLException {
        String countSQL = "SELECT COUNT(*) FROM weather_data";
        
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement(); //Count Query Connection
             ResultSet rs = stmt.executeQuery(countSQL)) {
            
            if (rs.next()) {
                return rs.getLong(1);
            }
            return 0;
            
        } catch (SQLException e) {
            logger.error("Failed to get total record count", e);
            throw e;
        }
    }
    
    /**
     * Gets count of weather records by city
     * @param city the city name
     * @return number of records for the city
     */
    public long getRecordCountByCity(String city) throws SQLException {
        String countSQL = "SELECT COUNT(*) FROM weather_data WHERE city = ?";
        
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(countSQL)) { //City Count Connection
            
            stmt.setString(1, city);
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getLong(1);
                }
                return 0;
            }
            
        } catch (SQLException e) {
            logger.error("Failed to get record count for city: {}", city, e);
            throw e;
        }
    }
    
    /**
     * Tests database connectivity
     * @return true if connection is successful
     */
    public boolean testConnection() {
        try (Connection conn = getConnection()) { // Connects Here
            return conn.isValid(5);
        } catch (SQLException e) {
            logger.error("Database connection test failed", e);
            return false;
        }
    }
    
    /**
     * Gets a database connection
     * @return database connection
     * @throws SQLException if connection fails
     */
    private Connection getConnection() throws SQLException {
        // Direct check for DATABASE_URL environment variable (Kubernetes override)
        String databaseUrl = System.getenv("DATABASE_URL");
        System.out.println("DEBUG: DATABASE_URL from env: " + databaseUrl);
        logger.info("DEBUG: DATABASE_URL from env: {}", databaseUrl);
        
        if (databaseUrl != null && !databaseUrl.isEmpty()) {
            System.out.println("DEBUG: Using DATABASE_URL from environment: " + databaseUrl);
            logger.info("Using DATABASE_URL from environment: {}", databaseUrl);
            return DriverManager.getConnection(
                databaseUrl,
                configManager.getDatabaseUsername(),
                configManager.getDatabasePassword()
            );
        }
        
        // Fall back to ConfigManager
        String configUrl = configManager.getDatabaseUrl();
        System.out.println("DEBUG: Falling back to ConfigManager URL: " + configUrl);
        logger.info("Using database URL from ConfigManager: {}", configUrl);
        return DriverManager.getConnection(
            configUrl, //Database URL
            configManager.getDatabaseUsername(), //Postgres Username
            configManager.getDatabasePassword() //Postgres Password
        );
    }
}