package com.pipeline.weather.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * Weather data model representing the structure from OpenWeatherMap API
 * and for database storage
 */
public class WeatherData {
    
    private Long id;
    private String city;
    private String country;
    private BigDecimal latitude;
    private BigDecimal longitude;
    
    // Temperature data
    private BigDecimal temperature;
    private BigDecimal feelsLike;
    private BigDecimal tempMin;
    private BigDecimal tempMax;
    
    // Atmospheric data
    private Integer humidity;
    private Integer pressure;
    private Integer seaLevel;
    private Integer groundLevel;
    
    // Wind data
    private BigDecimal windSpeed;
    private Integer windDirection;
    private BigDecimal windGust;
    
    // Weather conditions
    private String weatherMain;
    private String weatherDescription;
    private Integer cloudiness;
    private Integer visibility;
    private BigDecimal uvIndex;
    
    // Time data
    private LocalDateTime sunriseTime;
    private LocalDateTime sunsetTime;
    private LocalDateTime apiTimestamp;
    private LocalDateTime recordedAt;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
    
    // Spark-generated transformed fields
    private String temperatureCategory;      // Freezing, Cold, Mild, Warm, Hot
    private BigDecimal heatIndex;           // Calculated comfort temperature
    private String windCategory;            // Light, Moderate, Strong, Very Strong
    private String comfortLevel;            // Comfortable, Moderate, Uncomfortable, Humid/Dry
    private Integer dataQualityScore;       // 100 or 75
    private LocalDateTime processingTimestamp; // When Spark processed the data
    private BigDecimal temperatureFahrenheit;  // Temperature in Fahrenheit
    
    // Constructors
    public WeatherData() {
        this.recordedAt = LocalDateTime.now();
        this.createdAt = LocalDateTime.now();
    }
    
    public WeatherData(String city) {
        this();
        this.city = city;
    }
    
    // Static factory method to create from OpenWeatherMap JSON response
    public static WeatherData fromApiResponse(com.fasterxml.jackson.databind.JsonNode jsonNode) {
        WeatherData weather = new WeatherData();
        
        try {
            // City and country - handle both current weather and forecast API
            if (jsonNode.has("name")) {
                // Current weather API structure
                weather.setCity(jsonNode.get("name").asText());
            } else if (jsonNode.has("city") && jsonNode.get("city").has("name")) {
                // Forecast API structure
                weather.setCity(jsonNode.get("city").get("name").asText());
            }
            
            if (jsonNode.has("sys") && jsonNode.get("sys").has("country")) {
                weather.setCountry(jsonNode.get("sys").get("country").asText());
            } else if (jsonNode.has("city") && jsonNode.get("city").has("country")) {
                weather.setCountry(jsonNode.get("city").get("country").asText());
            }
            
            // Coordinates - handle both structures
            if (jsonNode.has("coord")) {
                // Current weather API structure
                com.fasterxml.jackson.databind.JsonNode coord = jsonNode.get("coord");
                if (coord.has("lat") && coord.has("lon")) {
                    weather.setLatitude(new BigDecimal(coord.get("lat").asText()));
                    weather.setLongitude(new BigDecimal(coord.get("lon").asText()));
                }
            } else if (jsonNode.has("city") && jsonNode.get("city").has("coord")) {
                // Forecast API structure
                com.fasterxml.jackson.databind.JsonNode coord = jsonNode.get("city").get("coord");
                if (coord.has("lat") && coord.has("lon")) {
                    weather.setLatitude(new BigDecimal(coord.get("lat").asText()));
                    weather.setLongitude(new BigDecimal(coord.get("lon").asText()));
                }
            }
            
            // Main weather data
            if (jsonNode.has("main")) {
                com.fasterxml.jackson.databind.JsonNode main = jsonNode.get("main");
                
                if (main.has("temp")) {
                    weather.setTemperature(new BigDecimal(main.get("temp").asText()));
                }
                if (main.has("feels_like")) {
                    weather.setFeelsLike(new BigDecimal(main.get("feels_like").asText()));
                }
                if (main.has("temp_min")) {
                    weather.setTempMin(new BigDecimal(main.get("temp_min").asText()));
                }
                if (main.has("temp_max")) {
                    weather.setTempMax(new BigDecimal(main.get("temp_max").asText()));
                }
                if (main.has("humidity")) {
                    weather.setHumidity(main.get("humidity").asInt());
                }
                if (main.has("pressure")) {
                    weather.setPressure(main.get("pressure").asInt());
                }
                if (main.has("sea_level")) {
                    weather.setSeaLevel(main.get("sea_level").asInt());
                }
                if (main.has("grnd_level")) {
                    weather.setGroundLevel(main.get("grnd_level").asInt());
                }
            }
            
            // Wind data
            if (jsonNode.has("wind")) {
                com.fasterxml.jackson.databind.JsonNode wind = jsonNode.get("wind");
                
                if (wind.has("speed")) {
                    weather.setWindSpeed(new BigDecimal(wind.get("speed").asText()));
                }
                if (wind.has("deg")) {
                    weather.setWindDirection(wind.get("deg").asInt());
                }
                if (wind.has("gust")) {
                    weather.setWindGust(new BigDecimal(wind.get("gust").asText()));
                }
            }
            
            // Weather description
            if (jsonNode.has("weather") && jsonNode.get("weather").isArray() && 
                jsonNode.get("weather").size() > 0) {
                com.fasterxml.jackson.databind.JsonNode weatherNode = jsonNode.get("weather").get(0);
                if (weatherNode.has("main")) {
                    weather.setWeatherMain(weatherNode.get("main").asText());
                }
                if (weatherNode.has("description")) {
                    weather.setWeatherDescription(weatherNode.get("description").asText());
                }
            }
            
            // Cloudiness
            if (jsonNode.has("clouds") && jsonNode.get("clouds").has("all")) {
                weather.setCloudiness(jsonNode.get("clouds").get("all").asInt());
            }
            
            // Visibility
            if (jsonNode.has("visibility")) {
                weather.setVisibility(jsonNode.get("visibility").asInt());
            }
            
            // Sunrise and sunset - handle both structures
            if (jsonNode.has("sys")) {
                com.fasterxml.jackson.databind.JsonNode sys = jsonNode.get("sys");
                if (sys.has("sunrise")) {
                    long sunriseUnix = sys.get("sunrise").asLong();
                    weather.setSunriseTime(LocalDateTime.ofInstant(Instant.ofEpochSecond(sunriseUnix), ZoneOffset.UTC));
                }
                if (sys.has("sunset")) {
                    long sunsetUnix = sys.get("sunset").asLong();
                    weather.setSunsetTime(LocalDateTime.ofInstant(Instant.ofEpochSecond(sunsetUnix), ZoneOffset.UTC));
                }
            } else if (jsonNode.has("city")) {
                com.fasterxml.jackson.databind.JsonNode city = jsonNode.get("city");
                if (city.has("sunrise")) {
                    long sunriseUnix = city.get("sunrise").asLong();
                    weather.setSunriseTime(LocalDateTime.ofInstant(Instant.ofEpochSecond(sunriseUnix), ZoneOffset.UTC));
                }
                if (city.has("sunset")) {
                    long sunsetUnix = city.get("sunset").asLong();
                    weather.setSunsetTime(LocalDateTime.ofInstant(Instant.ofEpochSecond(sunsetUnix), ZoneOffset.UTC));
                }
            }
            
            // API timestamp
            if (jsonNode.has("dt")) {
                long apiUnixTime = jsonNode.get("dt").asLong();
                weather.setApiTimestamp(LocalDateTime.ofInstant(Instant.ofEpochSecond(apiUnixTime), ZoneOffset.UTC));
            }
            
        } catch (Exception e) {
            System.err.println("Error parsing field in weather response: " + e.getMessage());
            // Continue parsing other fields even if one fails
        }
        
        return weather;
    }
    
    /**
     * Creates WeatherData from forecast API response (takes first forecast entry)
     * @param jsonNode JsonNode containing forecast API response
     * @return WeatherData object
     */
    public static WeatherData fromForecastResponse(com.fasterxml.jackson.databind.JsonNode jsonNode) {
        WeatherData weather = new WeatherData();
        
        try {
            // Extract city information
            if (jsonNode.has("city")) {
                com.fasterxml.jackson.databind.JsonNode city = jsonNode.get("city");
                
                if (city.has("name")) {
                    weather.setCity(city.get("name").asText());
                }
                if (city.has("country")) {
                    weather.setCountry(city.get("country").asText());
                }
                
                // Coordinates
                if (city.has("coord")) {
                    com.fasterxml.jackson.databind.JsonNode coord = city.get("coord");
                    if (coord.has("lat") && coord.has("lon")) {
                        weather.setLatitude(new BigDecimal(coord.get("lat").asText()));
                        weather.setLongitude(new BigDecimal(coord.get("lon").asText()));
                    }
                }
                
                // Sunrise and sunset
                if (city.has("sunrise")) {
                    long sunriseUnix = city.get("sunrise").asLong();
                    weather.setSunriseTime(LocalDateTime.ofInstant(Instant.ofEpochSecond(sunriseUnix), ZoneOffset.UTC));
                }
                if (city.has("sunset")) {
                    long sunsetUnix = city.get("sunset").asLong();
                    weather.setSunsetTime(LocalDateTime.ofInstant(Instant.ofEpochSecond(sunsetUnix), ZoneOffset.UTC));
                }
            }
            
            // Extract first forecast entry for current conditions
            if (jsonNode.has("list") && jsonNode.get("list").isArray() && jsonNode.get("list").size() > 0) {
                com.fasterxml.jackson.databind.JsonNode firstForecast = jsonNode.get("list").get(0);
                
                // Main weather data
                if (firstForecast.has("main")) {
                    com.fasterxml.jackson.databind.JsonNode main = firstForecast.get("main");
                    
                    if (main.has("temp")) {
                        weather.setTemperature(new BigDecimal(main.get("temp").asText()));
                    }
                    if (main.has("feels_like")) {
                        weather.setFeelsLike(new BigDecimal(main.get("feels_like").asText()));
                    }
                    if (main.has("temp_min")) {
                        weather.setTempMin(new BigDecimal(main.get("temp_min").asText()));
                    }
                    if (main.has("temp_max")) {
                        weather.setTempMax(new BigDecimal(main.get("temp_max").asText()));
                    }
                    if (main.has("humidity")) {
                        weather.setHumidity(main.get("humidity").asInt());
                    }
                    if (main.has("pressure")) {
                        weather.setPressure(main.get("pressure").asInt());
                    }
                    if (main.has("sea_level")) {
                        weather.setSeaLevel(main.get("sea_level").asInt());
                    }
                    if (main.has("grnd_level")) {
                        weather.setGroundLevel(main.get("grnd_level").asInt());
                    }
                }
                
                // Wind data
                if (firstForecast.has("wind")) {
                    com.fasterxml.jackson.databind.JsonNode wind = firstForecast.get("wind");
                    
                    if (wind.has("speed")) {
                        weather.setWindSpeed(new BigDecimal(wind.get("speed").asText()));
                    }
                    if (wind.has("deg")) {
                        weather.setWindDirection(wind.get("deg").asInt());
                    }
                    if (wind.has("gust")) {
                        weather.setWindGust(new BigDecimal(wind.get("gust").asText()));
                    }
                }
                
                // Weather description
                if (firstForecast.has("weather") && firstForecast.get("weather").isArray() && 
                    firstForecast.get("weather").size() > 0) {
                    com.fasterxml.jackson.databind.JsonNode weatherNode = firstForecast.get("weather").get(0);
                    if (weatherNode.has("main")) {
                        weather.setWeatherMain(weatherNode.get("main").asText());
                    }
                    if (weatherNode.has("description")) {
                        weather.setWeatherDescription(weatherNode.get("description").asText());
                    }
                }
                
                // Cloudiness
                if (firstForecast.has("clouds") && firstForecast.get("clouds").has("all")) {
                    weather.setCloudiness(firstForecast.get("clouds").get("all").asInt());
                }
                
                // Visibility
                if (firstForecast.has("visibility")) {
                    weather.setVisibility(firstForecast.get("visibility").asInt());
                }
                
                // API timestamp
                if (firstForecast.has("dt")) {
                    long apiUnixTime = firstForecast.get("dt").asLong();
                    weather.setApiTimestamp(LocalDateTime.ofInstant(Instant.ofEpochSecond(apiUnixTime), ZoneOffset.UTC));
                }
            }
            
        } catch (Exception e) {
            System.err.println("Error parsing forecast response: " + e.getMessage());
            e.printStackTrace();
        }
        
        return weather;
    }
    
    /**
     * Enhances existing WeatherData with UV Index information
     * @param uvResponse JsonNode containing UV index response
     */
    public void enhanceWithUVIndex(com.fasterxml.jackson.databind.JsonNode uvResponse) {
        try {
            if (uvResponse != null && uvResponse.has("value")) {
                this.setUvIndex(new BigDecimal(uvResponse.get("value").asText()));
            }
        } catch (Exception e) {
            // UV index enhancement failed, but don't fail the entire object
            System.err.println("Failed to enhance with UV index: " + e.getMessage());
        }
    }
    
    // Getters and Setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    
    public String getCity() { return city; }
    public void setCity(String city) { this.city = city; }
    
    public String getCountry() { return country; }
    public void setCountry(String country) { this.country = country; }
    
    public BigDecimal getLatitude() { return latitude; }
    public void setLatitude(BigDecimal latitude) { this.latitude = latitude; }
    
    public BigDecimal getLongitude() { return longitude; }
    public void setLongitude(BigDecimal longitude) { this.longitude = longitude; }
    
    public BigDecimal getTemperature() { return temperature; }
    public void setTemperature(BigDecimal temperature) { this.temperature = temperature; }
    
    public BigDecimal getFeelsLike() { return feelsLike; }
    public void setFeelsLike(BigDecimal feelsLike) { this.feelsLike = feelsLike; }
    
    public BigDecimal getTempMin() { return tempMin; }
    public void setTempMin(BigDecimal tempMin) { this.tempMin = tempMin; }
    
    public BigDecimal getTempMax() { return tempMax; }
    public void setTempMax(BigDecimal tempMax) { this.tempMax = tempMax; }
    
    public Integer getHumidity() { return humidity; }
    public void setHumidity(Integer humidity) { this.humidity = humidity; }
    
    public Integer getPressure() { return pressure; }
    public void setPressure(Integer pressure) { this.pressure = pressure; }
    
    public Integer getSeaLevel() { return seaLevel; }
    public void setSeaLevel(Integer seaLevel) { this.seaLevel = seaLevel; }
    
    public Integer getGroundLevel() { return groundLevel; }
    public void setGroundLevel(Integer groundLevel) { this.groundLevel = groundLevel; }
    
    public BigDecimal getWindSpeed() { return windSpeed; }
    public void setWindSpeed(BigDecimal windSpeed) { this.windSpeed = windSpeed; }
    
    public Integer getWindDirection() { return windDirection; }
    public void setWindDirection(Integer windDirection) { this.windDirection = windDirection; }
    
    public BigDecimal getWindGust() { return windGust; }
    public void setWindGust(BigDecimal windGust) { this.windGust = windGust; }
    
    public String getWeatherMain() { return weatherMain; }
    public void setWeatherMain(String weatherMain) { this.weatherMain = weatherMain; }
    
    public String getWeatherDescription() { return weatherDescription; }
    public void setWeatherDescription(String weatherDescription) { this.weatherDescription = weatherDescription; }
    
    public Integer getCloudiness() { return cloudiness; }
    public void setCloudiness(Integer cloudiness) { this.cloudiness = cloudiness; }
    
    public Integer getVisibility() { return visibility; }
    public void setVisibility(Integer visibility) { this.visibility = visibility; }
    
    public BigDecimal getUvIndex() { return uvIndex; }
    public void setUvIndex(BigDecimal uvIndex) { this.uvIndex = uvIndex; }
    
    public LocalDateTime getSunriseTime() { return sunriseTime; }
    public void setSunriseTime(LocalDateTime sunriseTime) { this.sunriseTime = sunriseTime; }
    
    public LocalDateTime getSunsetTime() { return sunsetTime; }
    public void setSunsetTime(LocalDateTime sunsetTime) { this.sunsetTime = sunsetTime; }
    
    public LocalDateTime getApiTimestamp() { return apiTimestamp; }
    public void setApiTimestamp(LocalDateTime apiTimestamp) { this.apiTimestamp = apiTimestamp; }
    
    public LocalDateTime getRecordedAt() { return recordedAt; }
    public void setRecordedAt(LocalDateTime recordedAt) { this.recordedAt = recordedAt; }
    
    public LocalDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }
    
    public LocalDateTime getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(LocalDateTime updatedAt) { this.updatedAt = updatedAt; }
    
    // Getters and Setters for Spark-transformed fields
    public String getTemperatureCategory() { return temperatureCategory; }
    public void setTemperatureCategory(String temperatureCategory) { this.temperatureCategory = temperatureCategory; }
    
    public BigDecimal getHeatIndex() { return heatIndex; }
    public void setHeatIndex(BigDecimal heatIndex) { this.heatIndex = heatIndex; }
    
    public String getWindCategory() { return windCategory; }
    public void setWindCategory(String windCategory) { this.windCategory = windCategory; }
    
    public String getComfortLevel() { return comfortLevel; }
    public void setComfortLevel(String comfortLevel) { this.comfortLevel = comfortLevel; }
    
    public Integer getDataQualityScore() { return dataQualityScore; }
    public void setDataQualityScore(Integer dataQualityScore) { this.dataQualityScore = dataQualityScore; }
    
    public LocalDateTime getProcessingTimestamp() { return processingTimestamp; }
    public void setProcessingTimestamp(LocalDateTime processingTimestamp) { this.processingTimestamp = processingTimestamp; }
    
    public BigDecimal getTemperatureFahrenheit() { return temperatureFahrenheit; }
    public void setTemperatureFahrenheit(BigDecimal temperatureFahrenheit) { this.temperatureFahrenheit = temperatureFahrenheit; }
    
    @Override
    public String toString() {
        return String.format("WeatherData{city='%s', country='%s', temp=%.1f°C, desc='%s', timestamp=%s}",
                city, country, temperature != null ? temperature.doubleValue() : null, 
                weatherDescription, apiTimestamp);
    }
}