package com.pipeline.weather.service;

import com.pipeline.weather.model.WeatherData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Mock Spark service to demonstrate transformations without Java 24 compatibility issues
 * This shows exactly what transformations would be performed by Spark
 */
public class MockSparkTransformationService {
    private static final Logger logger = LoggerFactory.getLogger(MockSparkTransformationService.class);
    
    /**
     * Simulates Spark transformations on weather data
     */
    public List<EnhancedWeatherData> processWeatherDataWithTransformations(List<WeatherData> weatherDataList) {
        logger.info("🔥 Starting Data Transformations with Mock Spark Engine");
        logger.info("📊 Processing {} weather records", weatherDataList.size());
        
        List<EnhancedWeatherData> transformedData = weatherDataList.stream()
            .map(this::transformWeatherData)
            .collect(Collectors.toList());
        
        // Display transformation statistics
        displayTransformationStatistics(transformedData);
        
        return transformedData;
    }
    
    /**
     * Transforms individual weather data with all the enhancements
     */
    private EnhancedWeatherData transformWeatherData(WeatherData original) {
        EnhancedWeatherData enhanced = new EnhancedWeatherData();
        
        // Copy original data
        enhanced.setOriginalData(original);
        
        // Transform temperature to different units and categories
        double tempC = original.getTemperature() != null ? original.getTemperature().doubleValue() : 0.0;
        double tempF = (tempC * 9.0/5.0) + 32.0;
        enhanced.setTemperatureFahrenheit(BigDecimal.valueOf(Math.round(tempF * 100.0) / 100.0));
        
        // Temperature Category
        enhanced.setTemperatureCategory(categorizeTemperature(tempC));
        
        // Heat Index Calculation
        double humidity = original.getHumidity() != null ? original.getHumidity() : 0.0;
        enhanced.setHeatIndex(calculateHeatIndex(tempC, humidity));
        
        // Wind Category
        double windSpeed = original.getWindSpeed() != null ? original.getWindSpeed().doubleValue() : 0.0;
        enhanced.setWindCategory(categorizeWind(windSpeed));
        
        // Comfort Level Assessment
        enhanced.setComfortLevel(assessComfortLevel(tempC, humidity));
        
        // Data Quality Score
        enhanced.setDataQualityScore(calculateDataQuality(original));
        
        // Weather Severity Index
        enhanced.setWeatherSeverityIndex(calculateWeatherSeverity(original));
        
        // Additional computed fields
        enhanced.setIsExtreme(isExtremeWeather(tempC, windSpeed, humidity));
        enhanced.setRecommendation(generateRecommendation(tempC, humidity, windSpeed, original.getWeatherDescription()));
        
        return enhanced;
    }
    
    private String categorizeTemperature(double tempC) {
        if (tempC < 0) return "Freezing";
        else if (tempC <= 10) return "Cold";
        else if (tempC <= 20) return "Mild";
        else if (tempC <= 30) return "Warm";
        else return "Hot";
    }
    
    private BigDecimal calculateHeatIndex(double tempC, double humidity) {
        if (tempC >= 27 && humidity >= 40) {
            double heatIndex = tempC + (0.5 * (tempC - 27)) + (humidity / 100 * 2);
            return BigDecimal.valueOf(Math.round(heatIndex * 100.0) / 100.0);
        }
        return BigDecimal.valueOf(tempC);
    }
    
    private String categorizeWind(double windSpeed) {
        if (windSpeed < 5) return "Light";
        else if (windSpeed <= 15) return "Moderate";
        else if (windSpeed <= 25) return "Strong";
        else return "Very Strong";
    }
    
    private String assessComfortLevel(double tempC, double humidity) {
        if (tempC >= 18 && tempC <= 24 && humidity >= 40 && humidity <= 60) return "Comfortable";
        else if (tempC < 5 || tempC > 35) return "Uncomfortable";
        else if (humidity > 80 || humidity < 20) return "Humid/Dry";
        else return "Moderate";
    }
    
    private int calculateDataQuality(WeatherData data) {
        int score = 100;
        if (data.getTemperature() == null) score -= 20;
        if (data.getHumidity() == null) score -= 15;
        if (data.getPressure() == null) score -= 15;
        if (data.getWindSpeed() == null) score -= 10;
        if (data.getWeatherDescription() == null || data.getWeatherDescription().isEmpty()) score -= 10;
        return Math.max(score, 0);
    }
    
    private int calculateWeatherSeverity(WeatherData data) {
        int severity = 0;
        
        if (data.getTemperature() != null) {
            double temp = data.getTemperature().doubleValue();
            if (temp < -10 || temp > 40) severity += 3;
            else if (temp < 0 || temp > 35) severity += 2;
            else if (temp < 5 || temp > 30) severity += 1;
        }
        
        if (data.getWindSpeed() != null) {
            double wind = data.getWindSpeed().doubleValue();
            if (wind > 25) severity += 3;
            else if (wind > 15) severity += 2;
            else if (wind > 10) severity += 1;
        }
        
        if (data.getWeatherDescription() != null) {
            String desc = data.getWeatherDescription().toLowerCase();
            if (desc.contains("storm") || desc.contains("tornado") || desc.contains("hurricane")) severity += 5;
            else if (desc.contains("heavy rain") || desc.contains("snow")) severity += 3;
            else if (desc.contains("rain") || desc.contains("thunderstorm")) severity += 2;
            else if (desc.contains("fog") || desc.contains("mist")) severity += 1;
        }
        
        return Math.min(severity, 10); // Cap at 10
    }
    
    private boolean isExtremeWeather(double tempC, double windSpeed, double humidity) {
        return tempC < -5 || tempC > 40 || windSpeed > 20 || humidity > 90 || humidity < 10;
    }
    
    private String generateRecommendation(double tempC, double humidity, double windSpeed, String description) {
        if (tempC > 35) return "🌡️ Stay hydrated and avoid direct sunlight";
        else if (tempC < 0) return "🧥 Dress warmly and be careful of ice";
        else if (windSpeed > 20) return "💨 Strong winds - secure loose objects";
        else if (humidity > 80) return "💧 High humidity - stay cool and hydrated";
        else if (description != null && description.toLowerCase().contains("rain")) return "☔ Carry an umbrella";
        else return "😊 Pleasant weather conditions";
    }
    
    private void displayTransformationStatistics(List<EnhancedWeatherData> data) {
        logger.info("\n🔍 === SPARK TRANSFORMATION RESULTS ===");
        
        // Temperature distribution
        var tempCategories = data.stream()
            .collect(Collectors.groupingBy(EnhancedWeatherData::getTemperatureCategory, Collectors.counting()));
        logger.info("🌡️ Temperature Distribution:");
        tempCategories.forEach((category, count) -> 
            logger.info("   {} : {} cities", category, count));
        
        // Comfort level distribution
        var comfortLevels = data.stream()
            .collect(Collectors.groupingBy(EnhancedWeatherData::getComfortLevel, Collectors.counting()));
        logger.info("😊 Comfort Level Distribution:");
        comfortLevels.forEach((level, count) -> 
            logger.info("   {} : {} cities", level, count));
        
        // Wind categories
        var windCategories = data.stream()
            .collect(Collectors.groupingBy(EnhancedWeatherData::getWindCategory, Collectors.counting()));
        logger.info("💨 Wind Category Distribution:");
        windCategories.forEach((category, count) -> 
            logger.info("   {} : {} cities", category, count));
        
        // Extreme weather count
        long extremeCount = data.stream().mapToLong(d -> d.isExtreme() ? 1 : 0).sum();
        logger.info("⚠️ Extreme Weather Conditions: {} out of {} cities", extremeCount, data.size());
        
        // Average data quality
        double avgQuality = data.stream().mapToInt(EnhancedWeatherData::getDataQualityScore).average().orElse(0);
        logger.info("📊 Average Data Quality Score: {:.1f}%", avgQuality);
        
        logger.info("✅ Transformation completed successfully!\n");
    }
    
    /**
     * Enhanced weather data with all transformations
     */
    public static class EnhancedWeatherData {
        private WeatherData originalData;
        private BigDecimal temperatureFahrenheit;
        private String temperatureCategory;
        private BigDecimal heatIndex;
        private String windCategory;
        private String comfortLevel;
        private int dataQualityScore;
        private int weatherSeverityIndex;
        private boolean isExtreme;
        private String recommendation;
        
        // Getters and setters
        public WeatherData getOriginalData() { return originalData; }
        public void setOriginalData(WeatherData originalData) { this.originalData = originalData; }
        
        public BigDecimal getTemperatureFahrenheit() { return temperatureFahrenheit; }
        public void setTemperatureFahrenheit(BigDecimal temperatureFahrenheit) { this.temperatureFahrenheit = temperatureFahrenheit; }
        
        public String getTemperatureCategory() { return temperatureCategory; }
        public void setTemperatureCategory(String temperatureCategory) { this.temperatureCategory = temperatureCategory; }
        
        public BigDecimal getHeatIndex() { return heatIndex; }
        public void setHeatIndex(BigDecimal heatIndex) { this.heatIndex = heatIndex; }
        
        public String getWindCategory() { return windCategory; }
        public void setWindCategory(String windCategory) { this.windCategory = windCategory; }
        
        public String getComfortLevel() { return comfortLevel; }
        public void setComfortLevel(String comfortLevel) { this.comfortLevel = comfortLevel; }
        
        public int getDataQualityScore() { return dataQualityScore; }
        public void setDataQualityScore(int dataQualityScore) { this.dataQualityScore = dataQualityScore; }
        
        public int getWeatherSeverityIndex() { return weatherSeverityIndex; }
        public void setWeatherSeverityIndex(int weatherSeverityIndex) { this.weatherSeverityIndex = weatherSeverityIndex; }
        
        public boolean isExtreme() { return isExtreme; }
        public void setIsExtreme(boolean isExtreme) { this.isExtreme = isExtreme; }
        
        public String getRecommendation() { return recommendation; }
        public void setRecommendation(String recommendation) { this.recommendation = recommendation; }
        
        // Convert back to WeatherData for database storage
        public WeatherData toWeatherDataForStorage() {
            WeatherData data = new WeatherData();
            data.setCity(originalData.getCity());
            data.setCountry(originalData.getCountry());
            data.setTemperature(originalData.getTemperature());
            data.setFeelsLike(originalData.getFeelsLike());
            data.setHumidity(originalData.getHumidity());
            data.setPressure(originalData.getPressure());
            data.setWindSpeed(originalData.getWindSpeed());
            data.setWindDirection(originalData.getWindDirection());
            data.setWeatherDescription(originalData.getWeatherDescription());
            data.setVisibility(originalData.getVisibility());
            data.setApiTimestamp(originalData.getApiTimestamp());
            return data;
        }
    }
}