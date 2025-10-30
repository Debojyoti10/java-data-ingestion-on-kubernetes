# Weather Pipeline API Key Setup Script (PowerShell)
# This script helps you securely set up your OpenWeatherMap API key

Write-Host "=== Weather Data Pipeline - API Key Setup ===" -ForegroundColor Green
Write-Host ""

# Check if kubectl is available
try {
    kubectl version --client --short | Out-Null
    Write-Host "✅ kubectl is available" -ForegroundColor Green
} catch {
    Write-Host "❌ kubectl is not installed or not in PATH" -ForegroundColor Red
    Write-Host "Please install kubectl and try again"
    exit 1
}

# Check if we're connected to a Kubernetes cluster
try {
    kubectl cluster-info | Out-Null
    Write-Host "✅ Connected to Kubernetes cluster" -ForegroundColor Green
} catch {
    Write-Host "❌ Not connected to a Kubernetes cluster" -ForegroundColor Red
    Write-Host "Please configure kubectl to connect to your cluster"
    exit 1
}

Write-Host ""

# Check if namespace exists
try {
    kubectl get namespace weather-pipeline | Out-Null
    Write-Host "✅ Namespace 'weather-pipeline' exists" -ForegroundColor Green
} catch {
    Write-Host "❌ Namespace 'weather-pipeline' does not exist" -ForegroundColor Red
    Write-Host "Please create the namespace first:"
    Write-Host "kubectl apply -f k8s/namespace.yaml"
    exit 1
}

Write-Host ""

# Prompt for API key
Write-Host "📝 Please enter your OpenWeatherMap API key:" -ForegroundColor Yellow
Write-Host "   (Get one from: https://openweathermap.org/api)" -ForegroundColor Gray
Write-Host ""

$API_KEY = Read-Host "API Key" -AsSecureString
$API_KEY_Plain = [Runtime.InteropServices.Marshal]::PtrToStringAuto([Runtime.InteropServices.Marshal]::SecureStringToBSTR($API_KEY))

if ([string]::IsNullOrEmpty($API_KEY_Plain)) {
    Write-Host "❌ API key cannot be empty" -ForegroundColor Red
    exit 1
}

if ($API_KEY_Plain.Length -lt 20) {
    Write-Host "⚠️  Warning: API key seems too short. OpenWeatherMap API keys are typically 32 characters." -ForegroundColor Yellow
    $continue = Read-Host "Continue anyway? (y/N)"
    if ($continue -notmatch "^[Yy]$") {
        Write-Host "Aborted"
        exit 1
    }
}

Write-Host ""
Write-Host "🔄 Updating Kubernetes secret..." -ForegroundColor Blue

# Create or update the secret
try {
    $secretData = @{
        "WEATHER_API_KEY" = $API_KEY_Plain
        "DB_PASSWORD" = "Debo*002"
    }
    
    # Convert to base64 for Kubernetes secret
    $base64ApiKey = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($API_KEY_Plain))
    $base64Password = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes("Debo*002"))
    
    # Create secret YAML content
    $secretYaml = @"
apiVersion: v1
kind: Secret
metadata:
  name: weather-pipeline-secrets
  namespace: weather-pipeline
type: Opaque
data:
  WEATHER_API_KEY: $base64ApiKey
  DB_PASSWORD: $base64Password
"@
    
    # Apply the secret
    $secretYaml | kubectl apply -f -
    
    Write-Host "✅ Secret updated successfully!" -ForegroundColor Green
    Write-Host ""
    Write-Host "🔍 Verifying secret..." -ForegroundColor Blue
    
    $apiKeyLength = (kubectl get secret weather-pipeline-secrets -n weather-pipeline -o jsonpath='{.data.WEATHER_API_KEY}' | ForEach-Object { [System.Text.Encoding]::UTF8.GetString([System.Convert]::FromBase64String($_)) }).Length
    Write-Host "$apiKeyLength characters in API key" -ForegroundColor Green
    
    Write-Host ""
    Write-Host "✅ Setup complete! You can now deploy the weather pipeline:" -ForegroundColor Green
    Write-Host "   kubectl apply -f k8s/" -ForegroundColor Gray
    Write-Host ""
    Write-Host "📊 To monitor the deployment:" -ForegroundColor Blue
    Write-Host "   kubectl get all -n weather-pipeline" -ForegroundColor Gray
    Write-Host "   kubectl logs -f job/weather-pipeline-job -n weather-pipeline" -ForegroundColor Gray
    
} catch {
    Write-Host "❌ Failed to update secret: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

# Clear the API key from memory
$API_KEY_Plain = $null
$API_KEY = $null
[System.GC]::Collect()