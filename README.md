# Java Data Ingestion Pipeline on Kubernetes

A production-ready, enterprise-grade weather data ingestion pipeline built with Java, Apache Spark, and Kubernetes. This project demonstrates modern data engineering practices by fetching real-time weather data from OpenWeatherMap API, processing it with Spark transformations, and storing it in PostgreSQL with automated scheduling capabilities.

## 🚀 What This Project Does

This pipeline provides a comprehensive solution for real-time weather data ingestion and processing:

- **Data Ingestion**: Fetches weather data from OpenWeatherMap API for multiple cities worldwide
- **Spark Processing**: Applies Apache Spark transformations for data cleaning, enrichment, and analytics
- **Database Storage**: Persists processed data in PostgreSQL with proper schema management
- **Container Orchestration**: Runs seamlessly on Kubernetes with automated scheduling via CronJobs
- **Multi-Environment Support**: Deployable locally, in Docker containers, or on Kubernetes clusters
- **Production-Ready**: Includes proper logging, error handling, configuration management, and monitoring

The pipeline processes weather data for major cities including London, Delhi, Tokyo, Paris, Sydney, Mumbai, Berlin, Toronto, Singapore, and Dubai, providing temperature readings, weather descriptions, humidity levels, and atmospheric pressure with timestamp tracking.

## 📋 Prerequisites

### Core Requirements

#### Java Development Environment
- **Java 17 or higher** - Required for modern JVM features and compatibility
  - Download from [OpenJDK](https://openjdk.org/) or [Oracle JDK](https://www.oracle.com/java/technologies/downloads/)
  - Verify installation: `java -version`

#### Build Tools
- **Apache Maven 3.8+** - For project building and dependency management
  - Download from [Maven Official Site](https://maven.apache.org/download.cgi)
  - Verify installation: `mvn -version`

#### Container Platform
- **Docker Desktop** - For containerization and local testing
  - Download from [Docker Official Site](https://www.docker.com/products/docker-desktop/)
  - Minimum 4GB RAM allocation recommended
  - Verify installation: `docker --version`

### Kubernetes Deployment (Optional but Recommended)

#### Kubernetes Runtime
- **Minikube** - For local Kubernetes development
  - Download from [Minikube Official Site](https://minikube.sigs.k8s.io/docs/start/)
  - Alternative: Docker Desktop with Kubernetes enabled
  - Verify installation: `minikube version`

#### Kubernetes CLI
- **kubectl** - For Kubernetes cluster management
  - Download from [Kubernetes Official Site](https://kubernetes.io/docs/tasks/tools/)
  - Verify installation: `kubectl version`

### External Services

#### Weather API Access
- **OpenWeatherMap API Key** - For weather data access
  - Sign up at [OpenWeatherMap](https://openweathermap.org/api)
  - Free tier provides 1,000 API calls per day
  - Current Weather Data API subscription required

#### Database
- **PostgreSQL 13+** - For data persistence
  - Automatically handled via Docker containers
  - No manual installation required

### System Requirements

#### Hardware Specifications
- **Memory**: Minimum 8GB RAM (16GB recommended for Kubernetes deployment)
- **Storage**: At least 10GB free space for Docker images and data
- **CPU**: Multi-core processor recommended for Spark processing

#### Operating System Support
- **Windows 10/11** with WSL2 enabled for Docker Desktop
- **macOS 10.14+** with Docker Desktop support
- **Linux** distributions with Docker support

#### Network Requirements
- **Internet Access** for API calls and Docker image downloads
- **Firewall Configuration** allowing Docker and Kubernetes networking
- **Port Availability**: 5432 (PostgreSQL), 8080 (Spark UI), various Kubernetes ports

### Development Tools (Optional but Helpful)

#### IDE and Editors
- **IntelliJ IDEA** or **Eclipse** for Java development
- **VS Code** with Java and Kubernetes extensions
- **pgAdmin4** or **DBeaver** for database management

#### Monitoring and Debugging
- **Kubernetes Dashboard** for cluster visualization
- **Docker Desktop Dashboard** for container management
- **Spark Web UI** for job monitoring (automatically available)

### Apache Spark Environment

#### Spark Distribution
- **Apache Spark 3.2.4** - Bundled with the application
- No separate Spark installation required
- Uses embedded Spark with local[*] master for processing

#### Scala Compatibility
- **Scala 2.12** runtime - Bundled with Spark dependencies
- No separate Scala installation required

### Configuration Requirements

#### Environment Setup
- **API Key Configuration** - OpenWeatherMap API key setup
- **Database Credentials** - PostgreSQL connection parameters
- **Resource Limits** - Memory and CPU allocation for containers

#### Security Considerations
- **Secrets Management** - Kubernetes secrets for sensitive data
- **Network Policies** - Container-to-container communication security
- **Image Security** - Using official base images with security updates

## 🏗️ Architecture Overview

This pipeline implements a modern microservices architecture with the following components:

- **Weather API Service**: Handles HTTP communication with OpenWeatherMap API
- **Spark Processing Engine**: Performs distributed data transformations and analytics
- **Database Service**: Manages PostgreSQL connections and data persistence
- **Configuration Manager**: Centralizes application configuration management
- **Kubernetes Orchestration**: Provides automated scheduling, scaling, and monitoring

The application supports multiple execution modes:
- **Standalone Mode**: Direct Java execution for development and testing
- **Spark Mode**: Distributed processing with Apache Spark transformations
- **Containerized Mode**: Docker-based execution with isolated environments
- **Kubernetes Mode**: Production deployment with automated scheduling and monitoring

## 🔧 Technical Stack

- **Language**: Java 17
- **Processing Framework**: Apache Spark 3.2.4
- **Database**: PostgreSQL 13+
- **Container Platform**: Docker
- **Orchestration**: Kubernetes
- **Build Tool**: Maven 3.8+
- **External API**: OpenWeatherMap API
- **Logging**: SLF4J with simple logging implementation

## 📦 Project Structure Highlights

The project includes optimized Docker configurations, comprehensive Kubernetes manifests, automated scheduling capabilities, and production-ready monitoring solutions. It demonstrates enterprise-level practices including proper error handling, configuration management, security considerations, and scalable architecture patterns.

## 🚦 Getting Started

Once you have all prerequisites installed, you'll be ready to build, deploy, and run this weather data ingestion pipeline across different environments - from local development to production Kubernetes clusters.

## 📄 License

This project is open source and available under the MIT License.

## 🤝 Contributing

Contributions are welcome! Please feel free to submit pull requests, report issues, or suggest improvements.

---

*Built with ❤️ for modern data engineering practices*