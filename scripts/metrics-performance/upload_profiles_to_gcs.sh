#!/bin/bash

# Standalone script to upload profiling data to Google Cloud Storage
# This can be used independently or called from other scripts

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
PROFILE_DIR="${PROFILE_DIR:-/profiles}"
GCS_BUCKET="${GCS_BUCKET:-}"
GCS_PREFIX="${GCS_PREFIX:-profiles/}"

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

# Main upload function
upload_profiles() {
    if [ -z "$GCS_BUCKET" ]; then
        print_error "GCS_BUCKET environment variable is required"
        echo "Usage: GCS_BUCKET=your-bucket-name $0"
        exit 1
    fi
    
    if [ ! -d "$PROFILE_DIR" ]; then
        print_error "Profile directory does not exist: $PROFILE_DIR"
        exit 1
    fi
    
    if ! command_exists gsutil; then
        print_error "gsutil not found. Please install Google Cloud SDK."
        exit 1
    fi
    
    # Check if there are any files to upload
    if [ -z "$(ls -A "$PROFILE_DIR" 2>/dev/null)" ]; then
        print_warn "No files found in $PROFILE_DIR"
        exit 0
    fi
    
    # Authenticate if needed (for service account key)
    if [ -n "$GOOGLE_APPLICATION_CREDENTIALS" ] && [ -f "$GOOGLE_APPLICATION_CREDENTIALS" ]; then
        print_info "Using service account credentials from: $GOOGLE_APPLICATION_CREDENTIALS"
        export GOOGLE_APPLICATION_CREDENTIALS
    fi
    
    # Create timestamped directory in GCS
    local timestamp=$(date +%Y%m%d_%H%M%S)
    local gcs_path="gs://${GCS_BUCKET}/${GCS_PREFIX}${timestamp}/"
    
    print_info "Uploading profiles from: $PROFILE_DIR"
    print_info "Destination: $gcs_path"
    
    # Upload all profile files (excluding dot files)
    # Use find to exclude files starting with "."
    local upload_count=0
    local failed_count=0
    
    while IFS= read -r file; do
        if [ -f "$file" ]; then
            local filename=$(basename "$file")
            # Skip files starting with "." (dot files)
            if [[ "$filename" == .* ]]; then
                continue
            fi
            if gsutil cp "$file" "${gcs_path}${filename}" 2>/dev/null; then
                upload_count=$((upload_count + 1))
                print_info "  ✓ Uploaded: $filename"
            else
                failed_count=$((failed_count + 1))
                print_warn "  ✗ Failed: $filename"
            fi
        fi
    done < <(find "$PROFILE_DIR" -maxdepth 1 -type f ! -name ".*" 2>/dev/null)
    
    if [ $upload_count -gt 0 ]; then
        print_info "Successfully uploaded $upload_count files to: $gcs_path"
        if [ $failed_count -gt 0 ]; then
            print_warn "$failed_count files failed to upload"
        fi
        
        # List uploaded files
        print_info ""
        print_info "Uploaded files:"
        gsutil ls "$gcs_path" | head -20
        
        # Print download command
        print_info ""
        print_info "To download profiles:"
        print_info "  gsutil -m cp -r $gcs_path ./profiles/"
        
        return 0
    else
        print_error "Failed to upload any files to GCS"
        return 1
    fi
}

# Show help
show_help() {
    cat << EOF
Upload profiling data to Google Cloud Storage

Usage:
    GCS_BUCKET=your-bucket-name $0
    OR
    $0 --bucket your-bucket-name

Environment Variables:
    GCS_BUCKET          - GCS bucket name (required)
    GCS_PREFIX          - Prefix path in bucket (default: profiles/)
    PROFILE_DIR         - Local directory with profiles (default: /profiles)
    GOOGLE_APPLICATION_CREDENTIALS - Path to service account key file (optional)

Examples:
    # Using environment variable
    GCS_BUCKET=my-profiling-bucket $0
    
    # With custom prefix
    GCS_BUCKET=my-bucket GCS_PREFIX=telemetry/profiles/ $0
    
    # With service account key
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json GCS_BUCKET=my-bucket $0

EOF
}

# Parse arguments
if [ "$1" = "--help" ] || [ "$1" = "-h" ]; then
    show_help
    exit 0
fi

if [ "$1" = "--bucket" ] || [ "$1" = "-b" ]; then
    if [ -z "$2" ]; then
        print_error "Bucket name required"
        show_help
        exit 1
    fi
    GCS_BUCKET="$2"
fi

# Run upload
upload_profiles

