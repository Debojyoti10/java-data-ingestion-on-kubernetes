#!/bin/bash

# Weather Pipeline API Key Setup Script
# This script helps you securely set up your OpenWeatherMap API key

echo "=== Weather Data Pipeline - API Key Setup ==="
echo ""

# Check if kubectl is available
if ! command -v kubectl &> /dev/null; then
    echo "❌ kubectl is not installed or not in PATH"
    echo "Please install kubectl and try again"
    exit 1
fi

# Check if we're connected to a Kubernetes cluster
if ! kubectl cluster-info &> /dev/null; then
    echo "❌ Not connected to a Kubernetes cluster"
    echo "Please configure kubectl to connect to your cluster"
    exit 1
fi

echo "✅ kubectl is available and connected to cluster"
echo ""

# Check if namespace exists
if ! kubectl get namespace weather-pipeline &> /dev/null; then
    echo "❌ Namespace 'weather-pipeline' does not exist"
    echo "Please create the namespace first:"
    echo "kubectl apply -f k8s/namespace.yaml"
    exit 1
fi

echo "✅ Namespace 'weather-pipeline' exists"
echo ""

# Prompt for API key
echo "📝 Please enter your OpenWeatherMap API key:"
echo "   (Get one from: https://openweathermap.org/api)"
echo ""
read -s -p "API Key: " API_KEY
echo ""

if [ -z "$API_KEY" ]; then
    echo "❌ API key cannot be empty"
    exit 1
fi

if [ ${#API_KEY} -lt 20 ]; then
    echo "⚠️  Warning: API key seems too short. OpenWeatherMap API keys are typically 32 characters."
    read -p "Continue anyway? (y/N): " -n 1 -r
    echo ""
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Aborted"
        exit 1
    fi
fi

echo ""
echo "🔄 Updating Kubernetes secret..."

# Create or update the secret
kubectl create secret generic weather-pipeline-secrets \
    --from-literal=WEATHER_API_KEY="$API_KEY" \
    --from-literal=DB_PASSWORD="Debo*002" \
    --namespace=weather-pipeline \
    --dry-run=client -o yaml | kubectl apply -f -

if [ $? -eq 0 ]; then
    echo "✅ Secret updated successfully!"
    echo ""
    echo "🔍 Verifying secret..."
    kubectl get secret weather-pipeline-secrets -n weather-pipeline -o jsonpath='{.data.WEATHER_API_KEY}' | base64 -d | wc -c
    echo " characters in API key"
    echo ""
    echo "✅ Setup complete! You can now deploy the weather pipeline:"
    echo "   kubectl apply -f k8s/"
    echo ""
    echo "📊 To monitor the deployment:"
    echo "   kubectl get all -n weather-pipeline"
    echo "   kubectl logs -f job/weather-pipeline-job -n weather-pipeline"
else
    echo "❌ Failed to update secret"
    exit 1
fi

# Clear the API key from memory (bash history)
unset API_KEY
history -c