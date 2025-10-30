package com.pipeline.weather.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pipeline.weather.config.ConfigManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Service class for fetching weather data from OpenWeatherMap API
 */
public class WeatherApiService {
    
    private static final Logger logger = LoggerFactory.getLogger(WeatherApiService.class);
    
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final ConfigManager configManager;
    
    public WeatherApiService() {
        this.httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(30))
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();
        
        this.objectMapper = new ObjectMapper();
        this.configManager = ConfigManager.getInstance();
    }
    
    /**
     * Fetches current weather data for a specific city
     * @param city the city name
     * @return JsonNode containing weather data
     * @throws Exception if API call fails
     */
    public JsonNode getCurrentWeather(String city) throws Exception {
        String url = configManager.getCurrentWeatherUrl(city);
        logger.info("Fetching current weather for city: {}", city);
        
        return makeApiCall(url);
    }
    
    /**
     * Fetches weather forecast for a specific city
     * @param city the city name
     * @return JsonNode containing forecast data
     * @throws Exception if API call fails
     */
    public JsonNode getWeatherForecast(String city) throws Exception {
        String url = configManager.getForecastUrl(city);
        logger.info("Fetching weather forecast for city: {}", city);
        
        return makeApiCall(url);
    }
    
    /**
     * Fetches current weather data using coordinates
     * @param latitude the latitude
     * @param longitude the longitude
     * @return JsonNode containing weather data
     * @throws Exception if API call fails
     */
    public JsonNode getCurrentWeatherByCoordinates(double latitude, double longitude) throws Exception {
        String url = configManager.getCurrentWeatherByCoordinatesUrl(latitude, longitude);
        logger.info("Fetching current weather for coordinates: {}, {}", latitude, longitude);
        
        return makeApiCall(url);
    }
    
    /**
     * Fetches UV Index data for specific coordinates
     * @param latitude the latitude
     * @param longitude the longitude
     * @return JsonNode containing UV index data
     * @throws Exception if API call fails
     */
    public JsonNode getUVIndex(double latitude, double longitude) throws Exception {
        String url = configManager.getUVIndexUrl(latitude, longitude);
        logger.info("Fetching UV index for coordinates: {}, {}", latitude, longitude);
        
        return makeApiCall(url);
    }
    
    /**
     * Asynchronously fetches current weather data for a city
     * @param city the city name
     * @return CompletableFuture containing JsonNode with weather data
     */
    public CompletableFuture<JsonNode> getCurrentWeatherAsync(String city) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return getCurrentWeather(city);
            } catch (Exception e) {
                logger.error("Failed to fetch weather data asynchronously for city: {}", city, e);
                throw new RuntimeException("Async weather fetch failed for " + city, e);
            }
        });
    }
    
    /**
     * Makes the actual HTTP API call with retry logic
     * @param url the API endpoint URL
     * @return JsonNode containing the API response
     * @throws Exception if all retries fail
     */
    private JsonNode makeApiCall(String url) throws Exception {
        int maxRetries = configManager.getMaxRetries();
        Exception lastException = null;
        
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Accept", "application/json")
                    .header("User-Agent", "WeatherDataPipeline/1.0")
                    .GET()
                    .timeout(Duration.ofSeconds(30))
                    .build();
                
                logger.debug("Making API call (attempt {} of {}): {}", attempt, maxRetries, 
                    url.replaceAll("appid=[^&]*", "appid=***"));
                
                HttpResponse<String> response = httpClient.send(request, 
                    HttpResponse.BodyHandlers.ofString());
                
                if (response.statusCode() == 200) {
                    JsonNode jsonResponse = objectMapper.readTree(response.body());
                    logger.debug("API call successful on attempt {}", attempt);
                    return jsonResponse;
                    
                } else if (response.statusCode() == 401) {
                    throw new IllegalArgumentException("Invalid API key. Please check your OpenWeatherMap API key.");
                    
                } else if (response.statusCode() == 404) {
                    throw new IllegalArgumentException("City not found: " + extractCityFromUrl(url));
                    
                } else if (response.statusCode() == 429) {
                    logger.warn("API rate limit exceeded. Attempt {} of {}", attempt, maxRetries);
                    if (attempt < maxRetries) {
                        Thread.sleep(2000 * attempt); // Exponential backoff
                        continue;
                    }
                    throw new RuntimeException("API rate limit exceeded after " + maxRetries + " attempts");
                    
                } else {
                    throw new RuntimeException("API call failed with status code: " + response.statusCode() + 
                        ", Response: " + response.body());
                }
                
            } catch (Exception e) {
                lastException = e;
                logger.warn("API call attempt {} failed: {}", attempt, e.getMessage());
                
                if (attempt < maxRetries) {
                    try {
                        Thread.sleep(1000 * attempt); // Progressive delay
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("API call interrupted", ie);
                    }
                } else {
                    logger.error("All {} API call attempts failed", maxRetries);
                }
            }
        }
        
        throw new RuntimeException("API call failed after " + maxRetries + " attempts", lastException);
    }
    
    /**
     * Extracts city name from URL for error reporting
     * @param url the API URL
     * @return city name or "unknown" if not found
     */
    private String extractCityFromUrl(String url) {
        try {
            String[] parts = url.split("q=");
            if (parts.length > 1) {
                return parts[1].split("&")[0];
            }
        } catch (Exception e) {
            logger.debug("Could not extract city from URL: {}", url);
        }
        return "unknown";
    }
    
    /**
     * Tests the API connection with a simple call
     * @return true if connection is successful, false otherwise
     */
    public boolean testApiConnection() {
        try {
            JsonNode response = getCurrentWeather("London");
            logger.info("API connection test successful");
            return response != null;
            
        } catch (Exception e) {
            logger.error("API connection test failed: {}", e.getMessage());
            return false;
        }
    }
    
    /**
     * Validates the API key by making a test call
     * @throws IllegalArgumentException if API key is invalid
     */
    public void validateApiKey() {
        try {
            getCurrentWeather("London");
            logger.info("API key validation successful");
            
        } catch (IllegalArgumentException e) {
            if (e.getMessage().contains("Invalid API key")) {
                throw e;
            }
            logger.warn("API key validation inconclusive: {}", e.getMessage());
            
        } catch (Exception e) {
            logger.warn("API key validation failed with unexpected error: {}", e.getMessage());
        }
    }
    
    /**
     * Closes the HTTP client and releases resources
     */
    public void shutdown() {
        // HttpClient doesn't need explicit shutdown in Java 11+
        logger.info("WeatherApiService shutdown completed");
    }
}