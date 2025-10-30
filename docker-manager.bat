@echo off
setlocal enabledelayedexpansion

REM Weather Data Pipeline Docker Management Script for Windows

set RED=[91m
set GREEN=[92m
set YELLOW=[93m
set BLUE=[94m
set NC=[0m

:print_header
echo %BLUE%================================%NC%
echo %BLUE% Weather Data Pipeline Manager %NC%
echo %BLUE%================================%NC%
goto :eof

:print_status
echo %GREEN%[INFO]%NC% %~1
goto :eof

:print_warning
echo %YELLOW%[WARNING]%NC% %~1
goto :eof

:print_error
echo %RED%[ERROR]%NC% %~1
goto :eof

:check_docker
docker info >nul 2>&1
if %errorlevel% neq 0 (
    call :print_error "Docker is not running. Please start Docker and try again."
    exit /b 1
)
goto :eof

:check_env
if not exist .env (
    call :print_warning ".env file not found. Creating from template..."
    if exist .env.template (
        copy .env.template .env >nul
        call :print_warning "Please edit .env file with your OpenWeatherMap API key"
        call :print_warning "You can get a free API key from: https://openweathermap.org/api"
    ) else (
        call :print_error ".env.template not found"
        exit /b 1
    )
)
goto :eof

:build_image
call :print_status "Building Weather Data Pipeline Docker image..."
docker build -t weather-pipeline:latest .
if %errorlevel% equ 0 (
    call :print_status "Docker image built successfully!"
) else (
    call :print_error "Failed to build Docker image"
    exit /b 1
)
goto :eof

:start_services
call :print_status "Starting Weather Data Pipeline services..."
docker-compose up -d
if %errorlevel% equ 0 (
    call :print_status "Services started! Available endpoints:"
    echo   - PostgreSQL: localhost:5432
    echo   - pgAdmin: http://localhost:8080 (admin@weather.com / admin)
    echo   - Spark UI: http://localhost:4040
    echo.
    call :print_status "Waiting for services to be ready..."
    timeout /t 10 /nobreak >nul
    call :print_status "Services are running successfully!"
) else (
    call :print_error "Failed to start services"
    exit /b 1
)
goto :eof

:stop_services
call :print_status "Stopping Weather Data Pipeline services..."
docker-compose down
call :print_status "Services stopped!"
goto :eof

:view_logs
set service=%~1
if "%service%"=="" set service=weather-pipeline
call :print_status "Viewing logs for %service%..."
docker-compose logs -f %service%
goto :eof

:run_pipeline
call :print_status "Running Weather Data Pipeline (single execution)..."
docker-compose run --rm weather-pipeline spark
goto :eof

:show_status
call :print_status "Service Status:"
docker-compose ps
echo.
call :print_status "Resource Usage:"
docker stats --no-stream --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}"
goto :eof

:cleanup
call :print_status "Cleaning up Docker resources..."
docker-compose down -v
docker image rm weather-pipeline:latest 2>nul
call :print_status "Cleanup completed!"
goto :eof

:help
call :print_header
echo Usage: %0 {build^|start^|stop^|restart^|logs^|run^|status^|cleanup^|help}
echo.
echo Commands:
echo   build     - Build the Docker image
echo   start     - Start all services (PostgreSQL, pgAdmin, Weather Pipeline)
echo   stop      - Stop all services
echo   restart   - Restart all services
echo   logs      - View logs (optional: specify service name)
echo   run       - Run the pipeline once
echo   status    - Show service status and resource usage
echo   cleanup   - Remove all containers, volumes, and images
echo   help      - Show this help message
echo.
echo Examples:
echo   %0 build
echo   %0 start
echo   %0 logs weather-pipeline
echo   %0 status
goto :eof

REM Main script logic
set command=%~1
if "%command%"=="" set command=help

call :print_header
call :check_docker
if %errorlevel% neq 0 exit /b 1

if "%command%"=="build" (
    call :build_image
) else if "%command%"=="start" (
    call :check_env
    call :start_services
) else if "%command%"=="stop" (
    call :stop_services
) else if "%command%"=="restart" (
    call :stop_services
    call :start_services
) else if "%command%"=="logs" (
    call :view_logs %2
) else if "%command%"=="run" (
    call :check_env
    call :run_pipeline
) else if "%command%"=="status" (
    call :show_status
) else if "%command%"=="cleanup" (
    call :cleanup
) else (
    call :help
)