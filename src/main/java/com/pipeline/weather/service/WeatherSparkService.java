package com.pipeline.weather.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pipeline.weather.model.WeatherData;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

/**
 * Service class for Apache Spark data transformations on weather data.
 * Handles conversion of API data to Spark DataFrames, performs transformations,
 * and prepares data for PostgreSQL storage.
 */
public class WeatherSparkService implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(WeatherSparkService.class);
    
    private transient SparkSession spark;
    private transient JavaSparkContext javaSparkContext;
    
    /**
     * Initializes Spark session and context with Java 24 compatibility
     */
    public void initializeSpark() {
        try {
            // Set system properties for Java 24 compatibility
            System.setProperty("HADOOP_USER_NAME", System.getProperty("user.name", "spark"));
            System.setProperty("hadoop.home.dir", System.getProperty("java.io.tmpdir"));
            System.setProperty("java.security.krb5.realm", "");
            System.setProperty("java.security.krb5.kdc", "");
            
            // Create a custom SecurityManager workaround
            System.setProperty("hadoop.security.authentication", "simple");
            System.setProperty("hadoop.security.authorization", "false");
            
            SparkConf conf = new SparkConf()
                .setAppName("Weather Data Pipeline with Spark")
                .setMaster("local[*]")  // Use all available cores
                .set("spark.sql.adaptive.enabled", "true")
                .set("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
                .set("spark.sql.execution.arrow.pyspark.enabled", "false")
                .set("spark.sql.warehouse.dir", System.getProperty("java.io.tmpdir") + "/spark-warehouse")
                .set("spark.driver.host", "0.0.0.0")  // Allow binding to all interfaces in Kubernetes
                .set("spark.ui.enabled", "false")  // Disable UI to avoid port conflicts
                .set("spark.sql.catalogImplementation", "in-memory")
                .set("spark.hadoop.fs.defaultFS", "file:///")
                .set("spark.sql.shuffle.partitions", "4")  // Reduce for local processing
                // Disable problematic features for Java 24
                .set("spark.sql.hive.metastore.jars", "")
                .set("spark.sql.hive.metastore.version", "")
                .set("spark.security.credentials.hadoopfs.enabled", "false");
            
            spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();
            
            javaSparkContext = new JavaSparkContext(spark.sparkContext());
            
            logger.info("🚀 Apache Spark session initialized successfully!");
            logger.info("📊 Spark Version: {}", spark.version());
            logger.info("🖥️ Running on: {} cores", spark.sparkContext().defaultParallelism());
            
        } catch (Exception e) {
            logger.error("❌ Failed to initialize Spark session", e);
            
            // Try alternative initialization without problematic Hadoop components
            try {
                logger.info("🔄 Attempting alternative Spark initialization...");
                initializeSparkAlternative();
            } catch (Exception fallbackException) {
                logger.error("❌ Alternative Spark initialization also failed", fallbackException);
                throw new RuntimeException("Spark initialization failed completely", fallbackException);
            }
        }
    }
    
    /**
     * Alternative Spark initialization method
     */
    private void initializeSparkAlternative() {
        // Minimal Spark configuration without Hadoop dependencies
        SparkConf conf = new SparkConf()
            .setAppName("Weather-Pipeline-Minimal")
            .setMaster("local[1]")
            .set("spark.ui.enabled", "false")
            .set("spark.sql.catalogImplementation", "in-memory")
            .set("spark.sql.warehouse.dir", System.getProperty("java.io.tmpdir"));
        
        spark = SparkSession.builder()
            .config(conf)
            .getOrCreate();
        
        logger.info("✅ Alternative Spark initialization successful!");
    }
    
    /**
     * Processes a list of weather data using Spark transformations
     * @param weatherDataList List of weather data from API
     * @return Processed Dataset ready for database storage
     */
    public Dataset<Row> processWeatherData(List<WeatherData> weatherDataList) {
        if (spark == null) {
            initializeSpark();
        }
        
        try {
            logger.info("Starting Spark data processing for {} weather records", weatherDataList.size());
            
            // Convert Java objects to Spark Dataset
            Dataset<WeatherData> weatherDataset = spark.createDataset(weatherDataList, 
                Encoders.bean(WeatherData.class));
            
            // Convert to DataFrame for SQL operations
            Dataset<Row> weatherDF = weatherDataset.toDF();
            
            // Register as temporary view for SQL operations
            weatherDF.createOrReplaceTempView("weather_raw");
            
            // Perform transformations using Spark SQL
            Dataset<Row> transformedDF = spark.sql(
                "SELECT " +
                "  city as city_name, " +
                "  country, " +
                "  temperature, " +
                "  feelsLike as feels_like, " +
                "  humidity, " +
                "  pressure, " +
                "  weatherDescription as weather_description, " +
                "  windSpeed as wind_speed, " +
                "  windDirection as wind_direction, " +
                "  visibility, " +
                "  uvIndex as uv_index, " +
                "  apiTimestamp as timestamp, " +
                "  CASE " +
                "    WHEN temperature < 0 THEN 'Freezing' " +
                "    WHEN temperature BETWEEN 0 AND 10 THEN 'Cold' " +
                "    WHEN temperature BETWEEN 10 AND 20 THEN 'Mild' " +
                "    WHEN temperature BETWEEN 20 AND 30 THEN 'Warm' " +
                "    ELSE 'Hot' " +
                "  END as temperature_category, " +
                "  CASE " +
                "    WHEN temperature >= 27 AND humidity >= 40 THEN " +
                "      ROUND(temperature + (0.5 * (temperature - 27)) + (humidity / 100 * 2), 2) " +
                "    ELSE temperature " +
                "  END as heat_index, " +
                "  CASE " +
                "    WHEN windSpeed < 5 THEN 'Light' " +
                "    WHEN windSpeed BETWEEN 5 AND 15 THEN 'Moderate' " +
                "    WHEN windSpeed BETWEEN 15 AND 25 THEN 'Strong' " +
                "    ELSE 'Very Strong' " +
                "  END as wind_category, " +
                "  CASE " +
                "    WHEN temperature BETWEEN 18 AND 24 AND humidity BETWEEN 40 AND 60 THEN 'Comfortable' " +
                "    WHEN temperature < 5 OR temperature > 35 THEN 'Uncomfortable' " +
                "    WHEN humidity > 80 OR humidity < 20 THEN 'Humid/Dry' " +
                "    ELSE 'Moderate' " +
                "  END as comfort_level " +
                "FROM weather_raw"
            );
            
            // Additional transformations
            transformedDF = transformedDF
                .withColumn("data_quality_score", 
                    functions.when(functions.col("temperature").isNotNull()
                        .and(functions.col("humidity").isNotNull())
                        .and(functions.col("pressure").isNotNull()), 100)
                    .otherwise(75))
                .withColumn("processing_timestamp", 
                    functions.current_timestamp())
                .withColumn("temperature_celsius", 
                    functions.col("temperature"))
                .withColumn("temperature_fahrenheit", 
                    functions.round(functions.col("temperature").multiply(9.0/5.0).plus(32), 2));
            
            // Cache the transformed dataset for better performance
            transformedDF.cache();
            
            // Show sample data and statistics
            logger.info("Transformed data sample:");
            transformedDF.show(5, false);
            
            logger.info("Data transformation statistics:");
            transformedDF.describe("temperature", "humidity", "pressure", "wind_speed").show();
            
            // Validate data quality
            long totalRecords = transformedDF.count();
            long validRecords = transformedDF.filter(
                functions.col("temperature").isNotNull()
                .and(functions.col("humidity").isNotNull())
                .and(functions.col("city_name").isNotNull())
            ).count();
            
            double dataQualityPercentage = (double) validRecords / totalRecords * 100;
            logger.info("Data Quality: {}/{} records valid ({:.2f}%)", 
                validRecords, totalRecords, dataQualityPercentage);
            
            return transformedDF;
            
        } catch (Exception e) {
            logger.error("Error during Spark data processing", e);
            throw new RuntimeException("Spark data processing failed", e);
        }
    }
    
    /**
     * Performs advanced analytics on weather data
     * @param weatherDF The processed weather DataFrame
     */
    public void performAnalytics(Dataset<Row> weatherDF) {
        try {
            logger.info("Performing advanced analytics on weather data");
            
            // Temperature statistics by city
            Dataset<Row> tempStats = weatherDF
                .groupBy("city_name")
                .agg(
                    functions.avg("temperature").alias("avg_temperature"),
                    functions.min("temperature").alias("min_temperature"),
                    functions.max("temperature").alias("max_temperature"),
                    functions.stddev("temperature").alias("temp_stddev")
                );
            
            logger.info("Temperature statistics by city:");
            tempStats.show();
            
            // Weather pattern analysis
            Dataset<Row> weatherPatterns = weatherDF
                .groupBy("weather_description", "temperature_category")
                .count()
                .orderBy(functions.desc("count"));
            
            logger.info("Weather patterns:");
            weatherPatterns.show();
            
            // Comfort level distribution
            Dataset<Row> comfortDistribution = weatherDF
                .groupBy("comfort_level")
                .count()
                .withColumn("percentage", 
                    functions.round(functions.col("count").multiply(100.0)
                        .divide(functions.sum("count").over()), 2));
            
            logger.info("Comfort level distribution:");
            comfortDistribution.show();
            
        } catch (Exception e) {
            logger.error("Error during analytics processing", e);
        }
    }
    
    /**
     * Converts Spark DataFrame to List of WeatherData for database storage
     * @param processedDF The processed Spark DataFrame
     * @return List of enriched WeatherData objects
     */
    public List<WeatherData> convertToWeatherDataList(Dataset<Row> processedDF) {
        try {
            // Convert back to Dataset<WeatherData> with enriched fields
            List<Row> rows = processedDF.collectAsList();
            
            return rows.stream()
                .map(this::rowToWeatherData)
                .toList();
                
        } catch (Exception e) {
            logger.error("Error converting DataFrame to WeatherData list", e);
            throw new RuntimeException("DataFrame conversion failed", e);
        }
    }
    
    /**
     * Converts a Spark Row to WeatherData object
     */
    private WeatherData rowToWeatherData(Row row) {
        WeatherData weatherData = new WeatherData();
        
        // Map Spark DataFrame columns to WeatherData fields
        try {
            if (row.schema().fieldNames().length > 0) {
                // Basic fields
                if (hasColumn(row, "city_name")) {
                    weatherData.setCity(row.getAs("city_name"));
                }
                if (hasColumn(row, "country")) {
                    weatherData.setCountry(row.getAs("country"));
                }
                if (hasColumn(row, "temperature")) {
                    Object temp = row.getAs("temperature");
                    if (temp != null) {
                        weatherData.setTemperature(new java.math.BigDecimal(temp.toString()));
                    }
                }
                if (hasColumn(row, "feels_like")) {
                    Object feelsLike = row.getAs("feels_like");
                    if (feelsLike != null) {
                        weatherData.setFeelsLike(new java.math.BigDecimal(feelsLike.toString()));
                    }
                }
                if (hasColumn(row, "humidity")) {
                    weatherData.setHumidity(row.getAs("humidity"));
                }
                if (hasColumn(row, "pressure")) {
                    weatherData.setPressure(row.getAs("pressure"));
                }
                if (hasColumn(row, "weather_description")) {
                    weatherData.setWeatherDescription(row.getAs("weather_description"));
                }
                if (hasColumn(row, "wind_speed")) {
                    Object windSpeed = row.getAs("wind_speed");
                    if (windSpeed != null) {
                        weatherData.setWindSpeed(new java.math.BigDecimal(windSpeed.toString()));
                    }
                }
                if (hasColumn(row, "wind_direction")) {
                    weatherData.setWindDirection(row.getAs("wind_direction"));
                }
                if (hasColumn(row, "visibility")) {
                    weatherData.setVisibility(row.getAs("visibility"));
                }
                if (hasColumn(row, "uv_index")) {
                    Object uvIndex = row.getAs("uv_index");
                    if (uvIndex != null) {
                        weatherData.setUvIndex(new java.math.BigDecimal(uvIndex.toString()));
                    }
                }
                if (hasColumn(row, "timestamp")) {
                    Object timestamp = row.getAs("timestamp");
                    if (timestamp != null) {
                        // Handle different timestamp formats
                        if (timestamp instanceof java.sql.Timestamp) {
                            weatherData.setApiTimestamp(((java.sql.Timestamp) timestamp).toLocalDateTime());
                        }
                    }
                }
                
                // Map Spark-transformed fields
                if (hasColumn(row, "temperature_category")) {
                    weatherData.setTemperatureCategory(row.getAs("temperature_category"));
                }
                if (hasColumn(row, "heat_index")) {
                    Object heatIndex = row.getAs("heat_index");
                    if (heatIndex != null) {
                        weatherData.setHeatIndex(new java.math.BigDecimal(heatIndex.toString()));
                    }
                }
                if (hasColumn(row, "wind_category")) {
                    weatherData.setWindCategory(row.getAs("wind_category"));
                }
                if (hasColumn(row, "comfort_level")) {
                    weatherData.setComfortLevel(row.getAs("comfort_level"));
                }
                if (hasColumn(row, "data_quality_score")) {
                    weatherData.setDataQualityScore(row.getAs("data_quality_score"));
                }
                if (hasColumn(row, "processing_timestamp")) {
                    Object processingTimestamp = row.getAs("processing_timestamp");
                    if (processingTimestamp != null) {
                        if (processingTimestamp instanceof java.sql.Timestamp) {
                            weatherData.setProcessingTimestamp(((java.sql.Timestamp) processingTimestamp).toLocalDateTime());
                        }
                    }
                }
                if (hasColumn(row, "temperature_fahrenheit")) {
                    Object tempF = row.getAs("temperature_fahrenheit");
                    if (tempF != null) {
                        weatherData.setTemperatureFahrenheit(new java.math.BigDecimal(tempF.toString()));
                    }
                }
            }
        } catch (Exception e) {
            logger.warn("Error mapping row to WeatherData: {}", e.getMessage());
        }
        
        return weatherData;
    }
    
    /**
     * Helper method to check if a column exists in the Row
     */
    private boolean hasColumn(Row row, String columnName) {
        try {
            String[] fieldNames = row.schema().fieldNames();
            for (String field : fieldNames) {
                if (field.equals(columnName)) {
                    return true;
                }
            }
            return false;
        } catch (Exception e) {
            return false;
        }
    }
    
    /**
     * Shuts down Spark session and context
     */
    public void shutdown() {
        try {
            if (javaSparkContext != null) {
                javaSparkContext.close();
                logger.info("JavaSparkContext closed");
            }
            
            if (spark != null) {
                spark.stop();
                logger.info("Spark session stopped");
            }
        } catch (Exception e) {
            logger.error("Error during Spark shutdown", e);
        }
    }
    
    /**
     * Gets the current Spark session
     * @return SparkSession instance
     */
    public SparkSession getSparkSession() {
        if (spark == null) {
            initializeSpark();
        }
        return spark;
    }
    
    /**
     * Checks if Spark is properly initialized
     * @return true if Spark is ready, false otherwise
     */
    public boolean isSparkInitialized() {
        return spark != null && !spark.sparkContext().isStopped();
    }
}