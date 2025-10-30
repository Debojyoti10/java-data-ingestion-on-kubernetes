package com.pipeline.weather;

import com.fasterxml.jackson.databind.JsonNode;
import com.pipeline.weather.config.ConfigManager;
import com.pipeline.weather.model.WeatherData;
import com.pipeline.weather.service.WeatherApiService;
import com.pipeline.weather.service.WeatherDatabaseService;
import com.pipeline.weather.service.WeatherSparkService;
import com.pipeline.weather.service.MockSparkTransformationService;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Main application class for the Weather Data Ingestion Pipeline
 * Orchestrates the fetching of weather data from OpenWeatherMap API and storing it in PostgreSQL
 */
public class WeatherPipelineApplication {
    
    private static final Logger logger = LoggerFactory.getLogger(WeatherPipelineApplication.class);
    
    private final ConfigManager configManager;
    private final WeatherApiService apiService;
    private final WeatherDatabaseService databaseService;
    private final WeatherSparkService sparkService;
    private final ExecutorService executorService;
    
    public WeatherPipelineApplication() {
        this.configManager = ConfigManager.getInstance();
        this.apiService = new WeatherApiService();
        this.databaseService = new WeatherDatabaseService();
        this.sparkService = new WeatherSparkService();
        this.executorService = Executors.newFixedThreadPool(5);
        
        logger.info("Weather Data Pipeline Application with Spark integration initialized");
    }
    
    public static void main(String[] args) {
        logger.info("Starting Weather Data Ingestion Pipeline...");
        
        WeatherPipelineApplication app = null;
        try {
            app = new WeatherPipelineApplication();
            
            // Validate configuration
            app.configManager.validateConfiguration();
            
            // Test connections
            if (!app.testConnections()) {
                logger.error("Connection tests failed. Exiting...");
                System.exit(1);
            }
            
            // Run the pipeline based on command line arguments
            if (args.length > 0) {
                switch (args[0].toLowerCase()) {
                    case "single":
                        app.runSingleFetch();
                        break;
                    case "legacy":
                        app.runLegacyMode();
                        break;
                    case "spark":
                        app.runSparkMode();
                        break;
                    case "continuous":
                        app.runContinuous();
                        break;
                    case "test":
                        app.runTest();
                        break;
                    default:
                        app.showUsage();
                        break;
                }
            } else {
                // Default: run legacy mode without Spark
                app.runLegacyMode();
            }
            
        } catch (Exception e) {
            logger.error("Application failed: {}", e.getMessage(), e);
            System.exit(1);
        } finally {
            if (app != null) {
                app.shutdown();
            }
        }
        
        logger.info("Weather Data Pipeline completed successfully");
    }
    
    /**
     * Runs a single fetch cycle for all configured cities with Spark transformations
     */
    public void runSingleFetch() {
        logger.info("Running single fetch cycle with Apache Spark transformations...");
        
        List<String> cities = configManager.getCities();
        logger.info("Fetching weather data for {} cities: {}", cities.size(), cities);
        
        try {
            // Initialize Spark if not already done
            if (!sparkService.isSparkInitialized()) {
                sparkService.initializeSpark();
            }
            
            // Step 1: Fetch weather data from API for all cities
            List<WeatherData> rawWeatherData = fetchWeatherDataFromAPI(cities);
            
            if (rawWeatherData.isEmpty()) {
                logger.warn("No weather data fetched from API");
                return;
            }
            
            logger.info("Successfully fetched {} weather records from API", rawWeatherData.size());
            
            // Step 2: Process data using Spark transformations
            logger.info("Starting Spark data transformations...");
            Dataset<Row> transformedDataset = sparkService.processWeatherData(rawWeatherData);
            
            // Step 3: Perform analytics (optional)
            sparkService.performAnalytics(transformedDataset);
            
            // Step 4: Convert back to WeatherData objects for database storage
            List<WeatherData> processedData = sparkService.convertToWeatherDataList(transformedDataset);
            
            // Step 5: Store transformed data in PostgreSQL
            logger.info("Storing {} transformed records in PostgreSQL...", processedData.size());
            int successCount = 0;
            int errorCount = 0;
            
            for (WeatherData weatherData : processedData) {
                try {
                    Long id = databaseService.insertWeatherData(weatherData);
                    weatherData.setId(id);
                    successCount++;
                    logger.debug("Stored weather data with ID: {} for city: {}", id, weatherData.getCity());
                } catch (Exception e) {
                    errorCount++;
                    logger.error("Failed to store weather data for city '{}': {}", 
                        weatherData.getCity(), e.getMessage());
                }
            }
            
            logger.info("Spark-enhanced fetch cycle completed. Success: {}, Errors: {}", successCount, errorCount);
            
            // Display summary
            displaySummary();
            
        } catch (Exception e) {
            logger.error("Error in Spark-enhanced fetch cycle: {}", e.getMessage(), e);
        }
    }
    
    /**
     * Legacy mode without Spark - direct API to PostgreSQL
     */
    public void runLegacyMode() {
        logger.info("Running legacy mode without Spark...");
        
        List<String> cities = configManager.getCities();
        logger.info("Fetching weather data for {} cities: {}", cities.size(), cities);
        
        int successCount = 0;
        int errorCount = 0;
        
        for (String city : cities) {
            try {
                WeatherData weatherData = fetchAndProcessWeatherData(city.trim());
                if (weatherData != null) {
                    successCount++;
                    logger.info("Successfully processed weather data for: {}", city);
                } else {
                    errorCount++;
                    logger.warn("Failed to process weather data for: {}", city);
                }
                
                // Rate limiting to respect API limits (free tier: 60 calls/minute)
                Thread.sleep(1000);
                
            } catch (Exception e) {
                errorCount++;
                logger.error("Error processing weather data for city '{}': {}", city, e.getMessage());
            }
        }
        
        logger.info("Legacy mode completed. Success: {}, Errors: {}", successCount, errorCount);
        
        // Display summary
        displaySummary();
    }
    
    /**
     * Dedicated Spark mode with enhanced transformations and analytics
     */
    public void runSparkMode() {
        logger.info("Running dedicated Spark mode with advanced analytics...");
        
        try {
            // Initialize Spark
            sparkService.initializeSpark();
            
            List<String> cities = configManager.getCities();
            logger.info("Processing weather data for {} cities using Apache Spark", cities.size());
            
            // Fetch data from API
            List<WeatherData> rawWeatherData = fetchWeatherDataFromAPI(cities);
            
            if (rawWeatherData.isEmpty()) {
                logger.warn("No weather data fetched from API");
                return;
            }
            
            logger.info("Successfully fetched {} weather records from API", rawWeatherData.size());
            
            // Advanced Spark processing with multiple transformations
            Dataset<Row> transformedDataset = sparkService.processWeatherData(rawWeatherData);
            
            // Perform comprehensive analytics
            logger.info("Performing comprehensive weather analytics...");
            sparkService.performAnalytics(transformedDataset);
            
            // Show detailed transformation results
            logger.info("Displaying transformed data sample:");
            transformedDataset.select(
                "city_name", "temperature", "temperature_category", 
                "heat_index", "comfort_level", "wind_category", 
                "data_quality_score"
            ).show(20, false);
            
            // Store processed data
            List<WeatherData> processedData = sparkService.convertToWeatherDataList(transformedDataset);
            
            logger.info("Storing {} enhanced records in PostgreSQL...", processedData.size());
            int successCount = 0;
            
            for (WeatherData weatherData : processedData) {
                try {
                    Long id = databaseService.insertWeatherData(weatherData);
                    weatherData.setId(id);
                    successCount++;
                } catch (Exception e) {
                    logger.error("Failed to store weather data for city '{}': {}", 
                        weatherData.getCity(), e.getMessage());
                }
            }
            
            logger.info("Spark mode completed successfully. {} records processed and stored", successCount);
            
            // Display enhanced summary
            displaySparkSummary(transformedDataset);
            
        } catch (Exception e) {
            logger.error("Error in Spark mode: {}", e.getMessage(), e);
        }
    }
    
    /**
     * Runs continuous fetching at configured intervals
     */
    public void runContinuous() {
        logger.info("Running continuous fetch mode...");
        int intervalSeconds = configManager.getFetchIntervalSeconds();
        logger.info("Fetch interval: {} seconds", intervalSeconds);
        
        int cycleCount = 0;
        
        while (true) {
            try {
                cycleCount++;
                logger.info("Starting fetch cycle #{}", cycleCount);
                
                runSingleFetch();
                
                logger.info("Cycle #{} completed. Waiting {} seconds for next cycle...", 
                    cycleCount, intervalSeconds);
                Thread.sleep(intervalSeconds * 1000L);
                
            } catch (InterruptedException e) {
                logger.info("Continuous fetch interrupted. Shutting down...");
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("Error in continuous fetch cycle #{}: {}", cycleCount, e.getMessage());
                try {
                    Thread.sleep(intervalSeconds * 1000L);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }
    
    /**
     * Runs parallel fetch for better performance
     */
    public void runParallelFetch() {
        logger.info("Running parallel fetch...");
        
        List<String> cities = configManager.getCities();
        
        List<CompletableFuture<WeatherData>> futures = cities.stream()
            .map(city -> CompletableFuture
                .supplyAsync(() -> fetchAndProcessWeatherData(city.trim()), executorService)
                .exceptionally(ex -> {
                    logger.error("Parallel fetch failed for city '{}': {}", city, ex.getMessage());
                    return null;
                }))
            .collect(Collectors.toList());
        
        // Wait for all to complete
        CompletableFuture<Void> allFutures = CompletableFuture.allOf(
            futures.toArray(new CompletableFuture[0]));
        
        try {
            allFutures.get(60, TimeUnit.SECONDS);
            
            long successCount = futures.stream()
                .map(CompletableFuture::join)
                .filter(data -> data != null)
                .count();
            
            logger.info("Parallel fetch completed. {} out of {} cities processed successfully", 
                successCount, cities.size());
            
        } catch (Exception e) {
            logger.error("Parallel fetch failed: {}", e.getMessage());
        }
    }
    
    /**
     * Fetches weather data from API for all cities (for Spark processing)
     * @param cities list of city names
     * @return List of WeatherData objects
     */
    private List<WeatherData> fetchWeatherDataFromAPI(List<String> cities) {
        List<WeatherData> weatherDataList = new java.util.ArrayList<>();
        
        for (String city : cities) {
            try {
                logger.debug("Fetching weather data for: {}", city);
                
                // Fetch data from API - Use forecast API for richer data
                JsonNode apiResponse = apiService.getWeatherForecast(city.trim());
                
                // Extract first forecast entry for current conditions
                WeatherData weatherData = WeatherData.fromForecastResponse(apiResponse);
                
                // Enhance with UV Index if coordinates are available
                if (weatherData.getLatitude() != null && weatherData.getLongitude() != null) {
                    try {
                        JsonNode uvResponse = apiService.getUVIndex(
                            weatherData.getLatitude().doubleValue(), 
                            weatherData.getLongitude().doubleValue()
                        );
                        weatherData.enhanceWithUVIndex(uvResponse);
                        logger.debug("Enhanced {} with UV index data", city);
                    } catch (Exception uvE) {
                        logger.warn("Could not fetch UV index for {}: {}", city, uvE.getMessage());
                    }
                }
                
                weatherDataList.add(weatherData);
                
                logger.debug("Successfully fetched weather data for: {}", city);
                
                // Rate limiting to respect API limits (free tier: 60 calls/minute)
                Thread.sleep(1000);
                
            } catch (Exception e) {
                logger.error("Failed to fetch weather data for city '{}': {}", city, e.getMessage());
                // Continue with other cities even if one fails
            }
        }
        
        return weatherDataList;
    }
    
    /**
     * Fetches and processes weather data for a single city (legacy method for backward compatibility)
     * @param city the city name
     * @return WeatherData object if successful, null otherwise
     */
    private WeatherData fetchAndProcessWeatherData(String city) {
        try {
            logger.debug("Fetching weather data for: {}", city);
            
            // Fetch data from API - Use forecast API for richer data
            JsonNode apiResponse = apiService.getWeatherForecast(city);
            
            // Convert to WeatherData model
            WeatherData weatherData = WeatherData.fromForecastResponse(apiResponse);
            
            // Enhance with UV Index if coordinates are available
            if (weatherData.getLatitude() != null && weatherData.getLongitude() != null) {
                try {
                    JsonNode uvResponse = apiService.getUVIndex(
                        weatherData.getLatitude().doubleValue(), 
                        weatherData.getLongitude().doubleValue()
                    );
                    weatherData.enhanceWithUVIndex(uvResponse);
                    logger.debug("Enhanced {} with UV index data", city);
                } catch (Exception uvE) {
                    logger.warn("Could not fetch UV index for {}: {}", city, uvE.getMessage());
                }
            }
            
            // Store in database
            Long id = databaseService.insertWeatherData(weatherData);
            weatherData.setId(id);
            
            logger.debug("Weather data stored with ID: {} for city: {}", id, city);
            
            return weatherData;
            
        } catch (Exception e) {
            logger.error("Failed to fetch and process weather data for city '{}': {}", city, e.getMessage());
            return null;
        }
    }
    
    /**
     * Tests all connections and configurations
     */
    public boolean testConnections() {
        logger.info("Testing connections...");
        
        boolean apiTest = false;
        boolean dbTest = false;
        
        try {
            // Test API connection
            logger.info("Testing OpenWeatherMap API connection...");
            apiTest = apiService.testApiConnection();
            if (apiTest) {
                logger.info("✓ API connection successful");
            } else {
                logger.error("✗ API connection failed");
            }
            
        } catch (Exception e) {
            logger.error("✗ API connection test failed: {}", e.getMessage());
        }
        
        try {
            // Test database connection
            logger.info("Testing PostgreSQL database connection...");
            dbTest = databaseService.testConnection();
            if (dbTest) {
                logger.info("✓ Database connection successful");
            } else {
                logger.error("✗ Database connection failed");
            }
            
        } catch (Exception e) {
            logger.error("✗ Database connection test failed: {}", e.getMessage());
        }
        
        return apiTest && dbTest;
    }
    
    /**
     * Runs test mode with sample data
     */
    public void runTest() {
        logger.info("Running test mode...");
        
        try {
            // Test with a single city
            String testCity = "London";
            logger.info("Testing with city: {}", testCity);
            
            WeatherData result = fetchAndProcessWeatherData(testCity);
            
            if (result != null) {
                logger.info("✓ Test successful!");
                logger.info("Test result: {}", result);
                
                // Verify data in database
                List<WeatherData> savedData = databaseService.getWeatherDataByCity(testCity, 1);
                if (!savedData.isEmpty()) {
                    logger.info("✓ Data successfully saved and retrieved from database");
                } else {
                    logger.warn("⚠ Data not found in database");
                }
                
            } else {
                logger.error("✗ Test failed");
            }
            
        } catch (Exception e) {
            logger.error("Test mode failed: {}", e.getMessage(), e);
        }
    }
    
    /**
     * Displays summary statistics
     */
    private void displaySummary() {
        try {
            long totalRecords = databaseService.getTotalRecordCount();
            logger.info("Database Summary:");
            logger.info("Total weather records: {}", totalRecords);
            
            List<String> cities = configManager.getCities();
            for (String city : cities) {
                long cityRecords = databaseService.getRecordCountByCity(city.trim());
                logger.info("Records for {}: {}", city.trim(), cityRecords);
            }
            
        } catch (Exception e) {
            logger.error("Failed to display summary: {}", e.getMessage());
        }
    }
    
    /**
     * Displays enhanced summary with Spark analytics
     */
    private void displaySparkSummary(Dataset<Row> dataset) {
        try {
            logger.info("=== Spark Processing Summary ===");
            
            // Basic statistics
            long recordCount = dataset.count();
            logger.info("Total processed records: {}", recordCount);
            
            // Temperature distribution
            logger.info("\nTemperature Distribution:");
            dataset.groupBy("temperature_category")
                .count()
                .orderBy("temperature_category")
                .show();
            
            // Comfort level analysis
            logger.info("\nComfort Level Analysis:");
            dataset.groupBy("comfort_level")
                .count()
                .withColumn("percentage", 
                    org.apache.spark.sql.functions.round(
                        org.apache.spark.sql.functions.col("count").multiply(100.0)
                            .divide(org.apache.spark.sql.functions.lit(recordCount)), 2))
                .show();
            
            // Wind analysis
            logger.info("\nWind Category Distribution:");
            dataset.groupBy("wind_category")
                .count()
                .orderBy("wind_category")
                .show();
            
            // Data quality metrics
            logger.info("\nData Quality Metrics:");
            dataset.agg(
                org.apache.spark.sql.functions.avg("data_quality_score").alias("avg_quality_score"),
                org.apache.spark.sql.functions.min("data_quality_score").alias("min_quality_score"),
                org.apache.spark.sql.functions.max("data_quality_score").alias("max_quality_score")
            ).show();
            
            logger.info("=== End Spark Summary ===\n");
            
        } catch (Exception e) {
            logger.error("Failed to display Spark summary: {}", e.getMessage());
        }
    }
    
    /**
     * Shows usage information
     */
    private void showUsage() {
        System.out.println("\nWeather Data Pipeline with Apache Spark Usage:");
        System.out.println("java -jar weather-data-pipeline-1.0.0.jar [mode]");
        System.out.println("\nModes:");
        System.out.println("  single     - Run single fetch cycle with Spark transformations (default)");
        System.out.println("  spark      - Run dedicated Spark mode with advanced analytics");
        System.out.println("  continuous - Run continuous fetching at configured intervals");
        System.out.println("  test       - Run test mode with sample data");
        System.out.println("\nFeatures:");
        System.out.println("  ✓ Apache Spark data transformations");
        System.out.println("  ✓ Advanced weather analytics");
        System.out.println("  ✓ Data quality scoring");
        System.out.println("  ✓ Temperature categorization");
        System.out.println("  ✓ Comfort level analysis");
        System.out.println("  ✓ Heat index calculations");
        System.out.println("\nEnvironment Variables:");
        System.out.println("  OPENWEATHER_API_KEY - Your OpenWeatherMap API key (required)");
        System.out.println("\nConfiguration:");
        System.out.println("  Edit src/main/resources/application.properties for database and API settings");
    }
    
    /**
     * Shuts down the application gracefully
     */
    public void shutdown() {
        logger.info("Shutting down Weather Pipeline Application...");
        
        try {
            // Shutdown Spark first
            if (sparkService != null) {
                logger.info("Shutting down Spark services...");
                sparkService.shutdown();
            }
            
            if (executorService != null && !executorService.isShutdown()) {
                executorService.shutdown();
                if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
                    logger.warn("Executor service did not terminate gracefully, forcing shutdown");
                    executorService.shutdownNow();
                }
            }
            
            if (apiService != null) {
                apiService.shutdown();
            }
            
            logger.info("Application shutdown completed");
            
        } catch (Exception e) {
            logger.error("Error during shutdown: {}", e.getMessage());
        }
    }
}