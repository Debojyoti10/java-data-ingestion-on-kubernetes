package com.pipeline.weather.config;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * Configuration manager for the weather data pipeline
 * Handles loading and accessing configuration properties
 */
public class ConfigManager {
    
    private static final Logger logger = LoggerFactory.getLogger(ConfigManager.class);
    private static ConfigManager instance;
    private Configuration config;
    
    private ConfigManager() {
        loadConfiguration();
    }
    
    public static ConfigManager getInstance() {
        if (instance == null) {
            synchronized (ConfigManager.class) {
                if (instance == null) {
                    instance = new ConfigManager();
                }
            }
        }
        return instance;
    }
    
    private void loadConfiguration() {
        try {
            // Check for profile-specific configuration first
            String profile = System.getProperty("app.profile", "default");
            String configFile = "application.properties";
            
            if (!"default".equals(profile)) {
                configFile = "application-" + profile + ".properties";
            }
            
            Parameters params = new Parameters();
            FileBasedConfigurationBuilder<PropertiesConfiguration> builder =
                new FileBasedConfigurationBuilder<>(PropertiesConfiguration.class)
                    .configure(params.properties()
                        .setFileName(configFile));
            
            config = builder.getConfiguration();
            logger.info("Configuration loaded successfully from: {}", configFile);
            
        } catch (ConfigurationException e) {
            logger.warn("Failed to load profile-specific configuration, falling back to default: {}", e.getMessage());
            // Fallback to default configuration
            try {
                Parameters params = new Parameters();
                FileBasedConfigurationBuilder<PropertiesConfiguration> builder =
                    new FileBasedConfigurationBuilder<>(PropertiesConfiguration.class)
                        .configure(params.properties()
                            .setFileName("application.properties"));
                
                config = builder.getConfiguration();
                logger.info("Default configuration loaded successfully");
            } catch (ConfigurationException fallbackException) {
                logger.error("Failed to load default configuration: {}", fallbackException.getMessage());
                throw new RuntimeException("Configuration loading failed", fallbackException);
            }
        }
    }
    
    // Database configuration methods
    public String getDatabaseUrl() {
        // Prioritize properties file configuration
        String configUrl = config.getString("db.url", null);
        if (configUrl != null && !configUrl.isEmpty()) {
            logger.info("Using database URL from properties file: {}", configUrl);
            return configUrl;
        }
        
        // Check for direct DATABASE_URL environment variable (fallback for containers)
        String databaseUrl = System.getenv("DATABASE_URL");
        if (databaseUrl != null && !databaseUrl.isEmpty()) {
            logger.info("Using database URL from DATABASE_URL environment variable: {}", databaseUrl);
            return databaseUrl;
        }
        
        // Check individual environment variables (fallback for Kubernetes)
        String dbHost = System.getenv("DB_HOST");
        String dbPort = System.getenv("DB_PORT");
        String dbName = System.getenv("DB_NAME");
        
        if (dbHost != null && dbPort != null && dbName != null) {
            String envUrl = String.format("jdbc:postgresql://%s:%s/%s", dbHost, dbPort, dbName);
            logger.info("Using database URL from environment variables: {}", envUrl);
            return envUrl;
        }
        
        // If we reach here, configuration is missing
        logger.error("No database configuration found!");
        logger.error("Please configure db.url in application.properties or set environment variables:");
        logger.error("Required: DB_HOST, DB_PORT, DB_NAME or DATABASE_URL");
        throw new IllegalStateException("Database configuration not found. Please configure db.url in properties file or set environment variables");
    }
    
    public String getDatabaseUsername() {
        // Prioritize properties file
        String configUser = config.getString("db.username", null);
        if (configUser != null && !configUser.isEmpty()) {
            return configUser;
        }
        
        // Fall back to environment variable
        String dbUser = System.getenv("DB_USER");
        if (dbUser != null && !dbUser.isEmpty()) {
            return dbUser;
        }
        
        // Default fallback
        return "postgres";
    }
    
    public String getDatabasePassword() {
        // Prioritize properties file
        String configPassword = config.getString("db.password", null);
        if (configPassword != null && !configPassword.isEmpty()) {
            return configPassword;
        }
        
        // Fall back to environment variable
        String dbPassword = System.getenv("DB_PASSWORD");
        if (dbPassword != null && !dbPassword.isEmpty()) {
            return dbPassword;
        }
        
        // Default fallback
        return "password";
    }
    
    public String getDatabaseDriver() {
        return config.getString("db.driver", "org.postgresql.Driver");
    }
    
    // Weather API configuration methods
    public String getWeatherApiBaseUrl() {
        return config.getString("weather.api.base.url", "https://api.openweathermap.org/data/2.5");
    }
    
    public String getWeatherApiKey() {
        // Check both possible environment variable names
        String apiKey = System.getenv("WEATHER_API_KEY");
        if (apiKey == null || apiKey.isEmpty()) {
            apiKey = System.getenv("OPENWEATHER_API_KEY");
        }
        if (apiKey == null || apiKey.isEmpty()) {
            apiKey = config.getString("weather.api.key");
        }
        
        if (apiKey == null || apiKey.isEmpty()) {
            throw new IllegalStateException("OpenWeatherMap API key not found. " +
                "Please set WEATHER_API_KEY or OPENWEATHER_API_KEY environment variable or weather.api.key in properties file");
        }
        
        return apiKey;
    }
    
    public String getWeatherApiUnits() {
        return config.getString("weather.api.units", "metric");
    }
    
    // Application configuration methods
    public List<String> getCities() {
        String citiesStr = config.getString("app.cities", "London,Delhi,Tokyo,Paris,Sydney");
        return Arrays.asList(citiesStr.split(","));
    }
    
    public int getFetchIntervalSeconds() {
        return config.getInt("app.fetch.interval.seconds", 60);
    }
    
    public int getMaxRetries() {
        return config.getInt("app.max.retries", 3);
    }
    
    // Logging configuration
    public String getLoggingLevel() {
        return config.getString("logging.level", "INFO");
    }
    
    public String getLoggingPattern() {
        return config.getString("logging.pattern", "%d{yyyy-MM-dd HH:mm:ss} [%level] %logger{36} - %msg%n");
    }
    
    // Utility methods
    public String getProperty(String key) {
        return config.getString(key);
    }
    
    public String getProperty(String key, String defaultValue) {
        return config.getString(key, defaultValue);
    }
    
    public int getIntProperty(String key, int defaultValue) {
        return config.getInt(key, defaultValue);
    }
    
    public boolean getBooleanProperty(String key, boolean defaultValue) {
        return config.getBoolean(key, defaultValue);
    }
    
    // Method to build complete API URLs
    public String getCurrentWeatherUrl(String city) {
        return String.format("%s/weather?q=%s&appid=%s&units=%s",
                getWeatherApiBaseUrl(), city, getWeatherApiKey(), getWeatherApiUnits());
    }
    
    public String getForecastUrl(String city) {
        return String.format("%s/forecast?q=%s&appid=%s&units=%s",
                getWeatherApiBaseUrl(), city, getWeatherApiKey(), getWeatherApiUnits());
    }
    
    public String getCurrentWeatherByCoordinatesUrl(double lat, double lon) {
        return String.format("%s/weather?lat=%f&lon=%f&appid=%s&units=%s",
                getWeatherApiBaseUrl(), lat, lon, getWeatherApiKey(), getWeatherApiUnits());
    }
    
    public String getUVIndexUrl(double lat, double lon) {
        return String.format("%s/uvi?lat=%f&lon=%f&appid=%s",
                getWeatherApiBaseUrl(), lat, lon, getWeatherApiKey());
    }
    
    /**
     * Validates that all required configuration is present
     * @throws IllegalStateException if required configuration is missing
     */
    public void validateConfiguration() {
        try {
            getDatabaseUrl();
            getDatabaseUsername();
            getDatabasePassword();
            getWeatherApiKey();
            getWeatherApiBaseUrl();
            
            logger.info("Configuration validation successful");
            logger.info("Database URL: {}", getDatabaseUrl());
            logger.info("Weather API Base URL: {}", getWeatherApiBaseUrl());
            logger.info("Cities to fetch: {}", getCities());
            logger.info("Fetch interval: {} seconds", getFetchIntervalSeconds());
            
        } catch (Exception e) {
            logger.error("Configuration validation failed: {}", e.getMessage());
            throw new IllegalStateException("Invalid configuration", e);
        }
    }
}