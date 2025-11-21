#!/bin/bash

# Script to build and deploy the telemetry profiling service to Cloud Run
# This script builds the Docker image, pushes it to GCR, and deploys to Cloud Run

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
DOCKERFILE="${PROJECT_ROOT}/Dockerfile.profiling"
SERVICE_NAME="${SERVICE_NAME:-telemetry-profiling}"
PROJECT_ID="${PROJECT_ID:-}"
REGION="${REGION:-us-central1}"
GCS_BUCKET="${GCS_BUCKET:-}"
IMAGE_NAME="gcr.io/${PROJECT_ID}/${SERVICE_NAME}"
IMAGE_TAG="${IMAGE_TAG:-latest}"

# Function to print colored messages
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Check prerequisites
check_prerequisites() {
    print_info "Checking prerequisites..."
    
    if ! command_exists docker; then
        print_error "docker not found. Please install Docker."
        exit 1
    fi
    
    if ! command_exists gcloud; then
        print_error "gcloud not found. Please install Google Cloud SDK."
        exit 1
    fi
    
    if [ -z "$PROJECT_ID" ]; then
        print_error "PROJECT_ID environment variable is required"
        print_info "Set it with: export PROJECT_ID=your-project-id"
        exit 1
    fi
    
    if [ -z "$GCS_BUCKET" ]; then
        print_warn "GCS_BUCKET not set. Profiles will not be uploaded."
        print_info "Set it with: export GCS_BUCKET=your-bucket-name"
    fi
    
    print_info "Prerequisites check passed"
}

# Authenticate with GCP
authenticate_gcp() {
    print_info "Authenticating with Google Cloud..."
    
    # Set project
    gcloud config set project "$PROJECT_ID" || {
        print_error "Failed to set GCP project"
        exit 1
    }
    
    # Configure Docker to use gcloud as credential helper
    gcloud auth configure-docker gcr.io --quiet || {
        print_error "Failed to configure Docker for GCR"
        exit 1
    }
    
    print_info "GCP authentication successful"
}

# Build Docker image
build_image() {
    print_info "Building Docker image..."
    print_info "Dockerfile: $DOCKERFILE"
    print_info "Image: ${IMAGE_NAME}:${IMAGE_TAG}"
    
    cd "$PROJECT_ROOT"
    
    if docker build \
        --platform linux/amd64 \
        -f "$DOCKERFILE" \
        -t "${IMAGE_NAME}:${IMAGE_TAG}" \
        -t "${IMAGE_NAME}:latest" \
        .; then
        print_info "Docker image built successfully"
    else
        print_error "Failed to build Docker image"
        exit 1
    fi
}

# Push Docker image to GCR
push_image() {
    print_info "Pushing Docker image to GCR..."
    print_info "Image: ${IMAGE_NAME}:${IMAGE_TAG}"
    
    if docker push "${IMAGE_NAME}:${IMAGE_TAG}"; then
        print_info "Image pushed successfully"
    else
        print_error "Failed to push image to GCR"
        exit 1
    fi
    
    # Also push latest tag
    if [ "$IMAGE_TAG" != "latest" ]; then
        if docker push "${IMAGE_NAME}:latest"; then
            print_info "Latest tag pushed successfully"
        else
            print_warn "Failed to push latest tag (non-fatal)"
        fi
    fi
}

# Deploy to Cloud Run
deploy_to_cloud_run() {
    print_info "Deploying to Cloud Run..."
    print_info "Service: $SERVICE_NAME"
    print_info "Region: $REGION"
    print_info "Image: ${IMAGE_NAME}:${IMAGE_TAG}"
    
    # Build environment variables
    local env_vars=(
        "METRICS_PORT=8080"
        "PROFILE_DIR=/profiles"
        "ENABLE_PPROF=true"
        "MOCK_GOOGLE_METRICS=true"
        "UPLOAD_TO_GCS=true"
    )
    
    if [ -n "$GCS_BUCKET" ]; then
        env_vars+=("GCS_BUCKET=${GCS_BUCKET}")
        env_vars+=("GCS_PREFIX=profiles/")
    fi
    
    # Build the gcloud run deploy command
    local deploy_cmd=(
        gcloud run deploy "$SERVICE_NAME"
        --image "${IMAGE_NAME}:${IMAGE_TAG}"
        --platform managed
        --region "$REGION"
        --allow-unauthenticated
        --memory 4Gi
        --cpu 2
        --timeout 3600
        --max-instances 1
        --min-instances 0
        --no-cpu-throttling
        --execution-environment gen2
    )
    
    # Add environment variables
    for env_var in "${env_vars[@]}"; do
        deploy_cmd+=(--set-env-vars "$env_var")
    done
    
    # Execute deployment
    if "${deploy_cmd[@]}"; then
        print_info "Deployment successful!"
        
        # Get service URL
        local service_url=$(gcloud run services describe "$SERVICE_NAME" \
            --region "$REGION" \
            --format 'value(status.url)' 2>/dev/null || echo "")
        
        if [ -n "$service_url" ]; then
            print_info ""
            print_info "Service URL: $service_url"
            print_info ""
            print_info "To trigger the profiling job, visit: $service_url"
            print_info "Or use: curl $service_url"
        fi
    else
        print_error "Failed to deploy to Cloud Run"
        exit 1
    fi
}

# Show help
show_help() {
    cat << EOF
Build and deploy telemetry profiling service to Cloud Run

Usage:
    PROJECT_ID=your-project-id GCS_BUCKET=your-bucket $0
    OR
    $0 --project your-project-id --bucket your-bucket

Environment Variables:
    PROJECT_ID          - GCP project ID (required)
    GCS_BUCKET          - GCS bucket name for profile uploads (optional)
    SERVICE_NAME         - Cloud Run service name (default: telemetry-profiling)
    REGION               - Cloud Run region (default: us-central1)
    IMAGE_TAG            - Docker image tag (default: latest)

Examples:
    # Basic deployment
    PROJECT_ID=my-project GCS_BUCKET=my-bucket $0
    
    # With custom service name and region
    PROJECT_ID=my-project GCS_BUCKET=my-bucket SERVICE_NAME=profiling REGION=us-east1 $0
    
    # With custom image tag
    PROJECT_ID=my-project IMAGE_TAG=v1.0.0 $0

EOF
}

# Parse arguments
if [ "$1" = "--help" ] || [ "$1" = "-h" ]; then
    show_help
    exit 0
fi

while [[ $# -gt 0 ]]; do
    case $1 in
        --project|-p)
            PROJECT_ID="$2"
            shift 2
            ;;
        --bucket|-b)
            GCS_BUCKET="$2"
            shift 2
            ;;
        --service|-s)
            SERVICE_NAME="$2"
            shift 2
            ;;
        --region|-r)
            REGION="$2"
            shift 2
            ;;
        --tag|-t)
            IMAGE_TAG="$2"
            shift 2
            ;;
        *)
            print_error "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

# Update IMAGE_NAME with PROJECT_ID
if [ -n "$PROJECT_ID" ]; then
    IMAGE_NAME="gcr.io/${PROJECT_ID}/${SERVICE_NAME}"
fi

# Main execution
main() {
    print_info "Starting Cloud Run deployment..."
    print_info "Project: $PROJECT_ID"
    print_info "Service: $SERVICE_NAME"
    print_info "Region: $REGION"
    print_info "GCS Bucket: ${GCS_BUCKET:-not set}"
    print_info ""
    
    check_prerequisites
    authenticate_gcp
    build_image
    push_image
    deploy_to_cloud_run
    
    print_info ""
    print_info "Deployment completed successfully!"
    print_info ""
    print_info "To view logs:"
    print_info "  gcloud run logs read $SERVICE_NAME --region $REGION"
    print_info ""
    print_info "To delete the service:"
    print_info "  gcloud run services delete $SERVICE_NAME --region $REGION"
}

# Run main function
main

