# Multi-stage Docker build for Weather Data Pipeline with Apache Spark
FROM maven:3.8-openjdk-17 AS builder

# Set working directory
WORKDIR /app

# Copy pom.xml first for better caching
COPY pom.xml .

# Download dependencies (this layer will be cached if pom.xml doesn't change)
RUN mvn dependency:go-offline -B

# Copy source code
COPY src ./src

# Build the application
RUN mvn clean package -DskipTests

# Runtime stage
FROM openjdk:17-slim

# Install required packages
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    procps \
    net-tools \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV JAVA_HOME=/usr/local/openjdk-17
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV SPARK_MASTER_URL=spark://spark-master:7077
ENV SPARK_DRIVER_MEMORY=1g
ENV SPARK_EXECUTOR_MEMORY=1g

# Create app directory
WORKDIR /app

# Copy the built JAR from builder stage
COPY --from=builder /app/target/weather-data-pipeline-1.0.0.jar app.jar

# Create necessary directories
RUN mkdir -p ./lib ./logs

# Copy resources
COPY --from=builder /app/src/main/resources ./resources

# Create directories for Spark
RUN mkdir -p /opt/spark/logs /opt/spark/work /tmp/spark-events

# Download and install Spark (matching your current version 3.5.0)
RUN wget -q https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz \
    && tar -xzf spark-3.5.0-bin-hadoop3.tgz -C /opt/ \
    && mv /opt/spark-3.5.0-bin-hadoop3 /opt/spark \
    && rm spark-3.5.0-bin-hadoop3.tgz

# Set proper permissions
RUN chmod -R 755 /opt/spark

# Expose ports
EXPOSE 8080 8081 4040 7077 6066

# Create startup script
RUN echo '#!/bin/bash\n\
echo "Starting Weather Data Pipeline with Apache Spark..."\n\
echo "Java Version: $(java -version 2>&1 | head -n 1)"\n\
echo "Spark Version: $(/opt/spark/bin/spark-submit --version 2>&1 | grep version | head -n 1)"\n\
echo "Available Memory: $(free -h | grep Mem)"\n\
echo "Available CPU: $(nproc) cores"\n\
echo ""\n\
echo "Environment Variables:"\n\
echo "WEATHER_API_KEY: ${WEATHER_API_KEY:-Not Set}"\n\
echo "DB_HOST: ${DB_HOST:-postgres}"\n\
echo "DB_PORT: ${DB_PORT:-5432}"\n\
echo "DB_NAME: ${DB_NAME:-weather_db}"\n\
echo ""\n\
# Add JVM options for Java 17 compatibility with Spark\n\
export JAVA_OPTS="\
--add-opens=java.base/java.lang=ALL-UNNAMED \
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
--add-opens=java.base/java.io=ALL-UNNAMED \
--add-opens=java.base/java.net=ALL-UNNAMED \
--add-opens=java.base/java.nio=ALL-UNNAMED \
--add-opens=java.base/java.util=ALL-UNNAMED \
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
--add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
--add-opens=java.base/sun.security.action=ALL-UNNAMED \
--add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"\n\
\n\
echo "Starting application..."\n\
# Use the JAR file directly (it should include all dependencies)\n\
java $JAVA_OPTS -jar app.jar "$@"' > /app/start.sh

# Make startup script executable
RUN chmod +x /app/start.sh

# Health check - check if Java process is running
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD pgrep java || exit 1

# Default command
ENTRYPOINT ["/app/start.sh"]
CMD ["spark"]