#!/bin/bash

# Script to run telemetry profiling, collect data, and upload to Google Cloud Storage
# This script:
# 1. Starts the telemetry service with profiling enabled
# 2. Waits for the service to be ready
# 3. Captures profiling data
# 4. Uploads profiles to Google Cloud Storage
# 5. Cleans up

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Detect if running in Docker
IS_DOCKER=false
if [ -f /.dockerenv ] || grep -q docker /proc/self/cgroup 2>/dev/null; then
    IS_DOCKER=true
fi

# Get script directory for relative paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Function to check if a command exists (needed early for configuration)
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Configuration - adjust defaults based on environment
if [ "$IS_DOCKER" = "true" ]; then
    # Docker environment defaults
    PROFILE_DIR="${PROFILE_DIR:-/profiles}"
    TELEMETRY_BINARY="${TELEMETRY_BINARY:-/usr/local/bin/telemetry}"
    METRICS_GENERATOR_BINARY="${METRICS_GENERATOR_BINARY:-/usr/local/bin/generate_hydrated_metrics}"
    ENV_FILE="${ENV_FILE:-/workspace/tel_local.env}"
else
    # Local environment defaults
    PROFILE_DIR="${PROFILE_DIR:-${PROJECT_ROOT}/profiles}"
    
    # Try to find binaries in common locations
    if [ -z "$TELEMETRY_BINARY" ]; then
        # Check in PATH first
        if command -v telemetry >/dev/null 2>&1; then
            TELEMETRY_BINARY="$(command -v telemetry)"
        # Check in project bin directory
        elif [ -f "${PROJECT_ROOT}/bin/telemetry" ]; then
            TELEMETRY_BINARY="${PROJECT_ROOT}/bin/telemetry"
        # Check in cmd/telemetry (if built locally)
        elif [ -f "${PROJECT_ROOT}/cmd/telemetry/telemetry" ]; then
            TELEMETRY_BINARY="${PROJECT_ROOT}/cmd/telemetry/telemetry"
        else
            TELEMETRY_BINARY="telemetry"  # Fallback to PATH
        fi
    fi
    
    if [ -z "$METRICS_GENERATOR_BINARY" ]; then
        # Check in PATH first
        if command -v generate_hydrated_metrics >/dev/null 2>&1; then
            METRICS_GENERATOR_BINARY="$(command -v generate_hydrated_metrics)"
        # Check in project bin directory
        elif [ -f "${PROJECT_ROOT}/bin/generate_hydrated_metrics" ]; then
            METRICS_GENERATOR_BINARY="${PROJECT_ROOT}/bin/generate_hydrated_metrics"
        # Check in tools directory (if built locally)
        elif [ -f "${PROJECT_ROOT}/tools/generate_hydrated_metrics" ]; then
            METRICS_GENERATOR_BINARY="${PROJECT_ROOT}/tools/generate_hydrated_metrics"
        else
            METRICS_GENERATOR_BINARY="generate_hydrated_metrics"  # Fallback to PATH
        fi
    fi
    
    # Environment file - check common locations
    if [ -z "$ENV_FILE" ]; then
        if [ -f "${PROJECT_ROOT}/tel_local.env" ]; then
            ENV_FILE="${PROJECT_ROOT}/tel_local.env"
        elif [ -f "${SCRIPT_DIR}/tel_local.env" ]; then
            ENV_FILE="${SCRIPT_DIR}/tel_local.env"
        elif [ -f "./tel_local.env" ]; then
            ENV_FILE="./tel_local.env"
        else
            ENV_FILE=""  # No default, will be optional
        fi
    fi
fi

# Note: METRICS_SERVER_PORT defaults to 8080 in the telemetry service
# Set METRICS_SERVER_PORT=9090 in environment if you want to use 9090
METRICS_PORT="${METRICS_SERVER_PORT:-${METRICS_PORT:-8080}}"
PPROF_PORT="${PPROF_PORT:-6060}"
GCS_BUCKET="${GCS_BUCKET:-}"
GCS_PREFIX="${GCS_PREFIX:-profiles/}"
PROFILE_DURATION="${PROFILE_DURATION:-120}"  # Increased default to capture full metrics processing
TRACE_DURATION="${TRACE_DURATION:-10}"
WAIT_MAX_ATTEMPTS="${WAIT_MAX_ATTEMPTS:-60}"
DOCKER_HOST_IP="${DOCKER_HOST_IP:-}"
TRIGGER_USAGE_METRICS="${TRIGGER_USAGE_METRICS:-true}"
CORRELATION_ID="${CORRELATION_ID:-}"
GENERATE_METRICS="${GENERATE_METRICS:-true}"

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

print_step() {
    echo -e "${BLUE}[STEP]${NC} $1"
}

# Function to check if service is ready
wait_for_service() {
    local max_attempts=${WAIT_MAX_ATTEMPTS:-60}
    local attempt=0
    
    print_info "Waiting for telemetry service to be ready on port $METRICS_PORT..."
    print_info "Will wait up to $((max_attempts * 2)) seconds..."
    
    while [ $attempt -lt $max_attempts ]; do
        # Check if process is still running
        if [ -f "${PROFILE_DIR}/telemetry.pid" ]; then
            local pid=$(cat "${PROFILE_DIR}/telemetry.pid" 2>/dev/null || echo "")
            if [ -n "$pid" ] && ! kill -0 "$pid" 2>/dev/null; then
                print_error "Telemetry service process (PID: $pid) is not running!"
                print_error "Last 20 lines of service log:"
                tail -20 "${PROFILE_DIR}/telemetry.log" 2>/dev/null || true
                return 1
            fi
        fi
        
        # Try multiple endpoints to check if service is ready
        if curl -s --max-time 2 "http://localhost:${METRICS_PORT}/debug/pprof/" >/dev/null 2>&1; then
            print_info "Service is ready! (pprof endpoint responding)"
            return 0
        elif curl -s --max-time 2 "http://localhost:${METRICS_PORT}/metrics" >/dev/null 2>&1; then
            print_info "Service is ready! (metrics endpoint responding)"
            return 0
        elif curl -s --max-time 2 "http://localhost:${METRICS_PORT}/" >/dev/null 2>&1; then
            print_info "Service is ready! (root endpoint responding)"
            return 0
        elif curl -s --max-time 2 "http://localhost:${METRICS_PORT}/v1/usage" >/dev/null 2>&1; then
            print_info "Service is ready! (usage endpoint responding)"
            return 0
        fi
        
        # Show progress every 10 attempts
        if [ $((attempt % 10)) -eq 0 ] && [ $attempt -gt 0 ]; then
            print_info "Still waiting... (attempt $attempt/$max_attempts)"
        fi
        
        attempt=$((attempt + 1))
        sleep 2
    done
    
    print_error "Service did not become ready after $((max_attempts * 2)) seconds"
    
    # Show diagnostic information
    print_error "Diagnostic information:"
    if [ -f "${PROFILE_DIR}/telemetry.log" ]; then
        print_error "Last 30 lines of service log:"
        tail -30 "${PROFILE_DIR}/telemetry.log" 2>/dev/null || true
    fi
    
    if [ -f "${PROFILE_DIR}/telemetry.pid" ]; then
        local pid=$(cat "${PROFILE_DIR}/telemetry.pid" 2>/dev/null || echo "")
        if [ -n "$pid" ]; then
            print_error "Process status:"
            ps aux | grep -E "^[^ ]+ +$pid " || print_warn "Process $pid not found in process list"
        fi
    fi
    
    print_error "Port $METRICS_PORT status:"
    netstat -tuln 2>/dev/null | grep ":$METRICS_PORT " || \
    ss -tuln 2>/dev/null | grep ":$METRICS_PORT " || \
    print_warn "Could not check port status"
    
    return 1
}

# Function to adjust database hosts for Docker container access to host
adjust_database_hosts() {
    if [ "$IS_DOCKER" = "true" ]; then
        print_info "Running in Docker container, adjusting database hosts for host access..."
        
        # Determine host address
        # On Mac/Windows Docker Desktop, use host.docker.internal
        # On Linux, try to detect host IP or use host.docker.internal
        if [ -z "$DOCKER_HOST_IP" ]; then
            # Try to ping host.docker.internal first (works on Mac/Windows)
            if ping -c 1 host.docker.internal >/dev/null 2>&1 2>/dev/null; then
                DOCKER_HOST_IP="host.docker.internal"
            else
                # On Linux, try to get the default gateway (host IP)
                DOCKER_HOST_IP=$(ip route 2>/dev/null | grep default | awk '{print $3}' | head -1)
                if [ -z "$DOCKER_HOST_IP" ]; then
                    # Fallback to host.docker.internal
                    DOCKER_HOST_IP="host.docker.internal"
                fi
            fi
        fi
        
        print_info "Using host IP: $DOCKER_HOST_IP for database connections"
        
        # Adjust database hosts if they point to localhost or 127.0.0.1
        if [ -n "$DB_HOST" ] && ([ "$DB_HOST" = "localhost" ] || [ "$DB_HOST" = "127.0.0.1" ]); then
            export DB_HOST="$DOCKER_HOST_IP"
            print_info "Adjusted DB_HOST from localhost/127.0.0.1 to: $DB_HOST"
        fi
        
        if [ -n "$METRICS_DB_HOST" ] && ([ "$METRICS_DB_HOST" = "localhost" ] || [ "$METRICS_DB_HOST" = "127.0.0.1" ]); then
            export METRICS_DB_HOST="$DOCKER_HOST_IP"
            print_info "Adjusted METRICS_DB_HOST from localhost/127.0.0.1 to: $METRICS_DB_HOST"
        fi
    else
        print_info "Not running in Docker, using database hosts as-is"
    fi
}

# Function to start telemetry service in background
start_telemetry_service() {
    print_step "Starting telemetry service with profiling enabled..."
    
    # Load environment variables if file exists
    if [ -n "$ENV_FILE" ] && [ -f "$ENV_FILE" ]; then
        print_info "Loading environment variables from $ENV_FILE"
        set -a
        source "$ENV_FILE"
        set +a
    elif [ -n "$ENV_FILE" ]; then
        print_warn "Environment file specified but not found: $ENV_FILE"
        print_warn "Continuing without loading environment file..."
    else
        print_info "No environment file specified, using environment variables as-is"
    fi
    
    # Adjust database hosts for Docker container access to host
    adjust_database_hosts
    
    # Enable pprof
    export ENABLE_PPROF=true
    export MOCK_GOOGLE_METRICS=true
    export GODEBUG="gctrace=1"
    
    # Start service in background
    print_info "Starting telemetry service on port $METRICS_PORT"
    print_info "pprof endpoints available at: http://localhost:$METRICS_PORT/debug/pprof/"
    
    # Start service and redirect output to log file
    print_info "Starting telemetry binary: $TELEMETRY_BINARY"
    
    # Check if binary exists or is in PATH
    if [ ! -f "$TELEMETRY_BINARY" ] && ! command_exists "$TELEMETRY_BINARY"; then
        print_error "Telemetry binary not found at: $TELEMETRY_BINARY"
        print_error "Please set TELEMETRY_BINARY environment variable or ensure 'telemetry' is in PATH"
        exit 1
    fi
    
    # Define log file path
    local telemetry_log="${PROFILE_DIR}/telemetry.log"
    print_info "Telemetry logs will be written to: $telemetry_log"
    
    # Ensure log file exists and is writable (create/truncate for fresh logs)
    touch "$telemetry_log" 2>/dev/null || {
        print_error "Cannot create log file: $telemetry_log"
        exit 1
    }
    
    # Start service in background and redirect both stdout and stderr to log file
    "$TELEMETRY_BINARY" > "$telemetry_log" 2>&1 &
    TELEMETRY_PID=$!
    
    # Store PID immediately for cleanup
    echo $TELEMETRY_PID > "${PROFILE_DIR}/telemetry.pid"
    
    print_info "Telemetry service started with PID: $TELEMETRY_PID"
    print_info "Logs are being written to: $telemetry_log"
    
    # Give it a moment to start
    sleep 2
    
    # Check if process is still running
    if ! kill -0 $TELEMETRY_PID 2>/dev/null; then
        print_error "Telemetry service process died immediately after start!"
        print_error "Service log:"
        cat "${PROFILE_DIR}/telemetry.log" 2>/dev/null || print_warn "No log file found"
        exit 1
    fi
    
    # Wait for service to be ready (with timeout)
    # Don't exit if service doesn't become ready - continue anyway
    if ! wait_for_service; then
        print_warn "Service did not become fully ready, but process is running"
        print_warn "Will continue with profiling - service may become ready during profiling"
        print_warn "If service fails, check logs at: $telemetry_log"
    else
        print_info "Service is ready and responding"
    fi
}

# Function to start all profiles in background
start_all_profiles_background() {
    local cpu_duration=${1:-60}
    local trace_duration=${2:-10}
    
    print_step "Starting all profiles in background using start_telemetry_with_profiling.sh..."
    
    # Verify endpoint is accessible
    if ! curl -s --max-time 5 "http://localhost:${METRICS_PORT}/debug/pprof/" >/dev/null 2>&1; then
        print_error "pprof endpoint is not accessible at http://localhost:${METRICS_PORT}/debug/pprof/"
        return 1
    fi
    
    local timestamp=$(date +%Y%m%d_%H%M%S)
    # Get script directory (where this script is located)
    local script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    local profile_script="${script_dir}/start_telemetry_with_profiling.sh"
    
    # If not found in same directory, try /usr/local/bin (Docker container path)
    if [ ! -f "$profile_script" ]; then
        if [ -f "/usr/local/bin/start_telemetry_with_profiling.sh" ]; then
            profile_script="/usr/local/bin/start_telemetry_with_profiling.sh"
        else
            print_error "Profiling script not found: $profile_script"
            print_error "Also checked: /usr/local/bin/start_telemetry_with_profiling.sh"
            return 1
        fi
    fi
    
    # Make sure script is executable
    chmod +x "$profile_script"
    
    print_info "Starting all profiles in background with duration ${cpu_duration}s..."
    print_info "Using: $profile_script all ${cpu_duration}"
    
    # Start profiling script in background
    (
        "$profile_script" all "${cpu_duration}" > "${PROFILE_DIR}/.profile_script_log_$$" 2>&1
        echo $? > "${PROFILE_DIR}/.profile_script_exit_$$"
    ) &
    
    local profile_pid=$!
    echo "$profile_pid" > "${PROFILE_DIR}/.profile_script_pid_$$"
    echo "$timestamp" > "${PROFILE_DIR}/.profile_timestamp_$$"
    
    print_info "All profiles started in background (PID: $profile_pid)"
    print_info "Profiles will capture metrics processing performance for ${cpu_duration} seconds"
    print_info "Profile script output: ${PROFILE_DIR}/.profile_script_log_$$"
    
    return 0
}

# Function to wait for all background profiles to complete
wait_for_all_profiles() {
    local profile_pid_file="${PROFILE_DIR}/.profile_script_pid_$$"
    local timestamp_file="${PROFILE_DIR}/.profile_timestamp_$$"
    
    if [ ! -f "$profile_pid_file" ]; then
        print_warn "Background profile PID file not found"
        return 1
    fi
    
    local timestamp=$(cat "$timestamp_file" 2>/dev/null || echo "")
    local pid=$(cat "$profile_pid_file" 2>/dev/null || echo "")
    
    if [ -z "$pid" ]; then
        print_warn "No background profile PID found"
        return 1
    fi
    
    print_step "Waiting for all background profiles to complete..."
    print_info "Waiting for profile script (PID: $pid) to complete..."
    
    # Wait for the background profile script process
    local wait_count=0
    local max_wait=$((PROFILE_DURATION + 120))  # Wait at least as long as profile duration + buffer
    
    while kill -0 "$pid" 2>/dev/null && [ $wait_count -lt $max_wait ]; do
        sleep 2
        wait_count=$((wait_count + 2))
        
        # Show progress
        if [ $((wait_count % 20)) -eq 0 ]; then
            print_info "Still waiting for profile script... (${wait_count}s elapsed)"
            
            # Check if profile files are being created
            local profile_count=$(ls -1 "${PROFILE_DIR}"/cpu_*.prof "${PROFILE_DIR}"/heap_*.prof "${PROFILE_DIR}"/trace_*.trace 2>/dev/null | wc -l || echo "0")
            if [ "$profile_count" -gt 0 ]; then
                print_info "Found ${profile_count} profile file(s) so far"
            fi
        fi
    done
    
    # Check if process completed successfully
    if ! kill -0 "$pid" 2>/dev/null; then
        local exit_code=$(cat "${PROFILE_DIR}/.profile_script_exit_$$" 2>/dev/null || echo "")
        if [ "$exit_code" = "0" ]; then
            print_info "Profile script completed successfully"
        else
            print_warn "Profile script exited with code: ${exit_code}"
            print_warn "Check log: ${PROFILE_DIR}/.profile_script_log_$$"
        fi
    else
        print_warn "Profile script is still running after ${wait_count}s (max wait: ${max_wait}s)"
    fi
    
    # If we hit max wait, check if process is still running
    if [ $wait_count -ge $max_wait ] && kill -0 "$pid" 2>/dev/null; then
        print_warn "Timeout waiting for profile script (${wait_count}s). Process may still be running..."
        if kill -0 "$pid" 2>/dev/null; then
            print_warn "Profile script process $pid is still running after timeout"
        fi
    fi
    
    print_info "All background profiles completed (waited ${wait_count}s)"
    
    # Give processes a moment to finish writing files
    sleep 2
    
    # Read file paths BEFORE cleanup (they'll be cleaned up later)
    local cpu_file=$(cat "${PROFILE_DIR}/.cpu_file_$$" 2>/dev/null || echo "")
    local trace_file=$(cat "${PROFILE_DIR}/.trace_file_$$" 2>/dev/null || echo "")
    
    # If CPU file path not found, try to find it by pattern
    if [ -z "$cpu_file" ]; then
        print_info "CPU file path not in temp file, searching for CPU profile files..."
        cpu_file=$(ls -t "${PROFILE_DIR}"/cpu_*.prof 2>/dev/null | head -1 || echo "")
        if [ -n "$cpu_file" ]; then
            print_info "Found CPU profile file by pattern: $cpu_file"
            echo "$cpu_file" > "${PROFILE_DIR}/.cpu_file_$$"
        fi
    fi
    
    # Store CPU file path for later verification (before cleanup)
    if [ -n "$cpu_file" ]; then
        echo "$cpu_file" > "${PROFILE_DIR}/.cpu_file_verified_$$"
        print_info "Stored CPU file path for verification: $cpu_file"
    else
        print_warn "CPU file path not found in temp files"
    fi
    
    # Verify and report on each profile
    
    # Check CPU profile with detailed diagnostics
    local cpu_pid=$(cat "${PROFILE_DIR}/.cpu_pid_$$" 2>/dev/null || echo "")
    local cpu_exit=$(cat "${PROFILE_DIR}/.cpu_exit_$$" 2>/dev/null || echo "unknown")
    local cpu_size=$(cat "${PROFILE_DIR}/.cpu_size_$$" 2>/dev/null || echo "0")
    local cpu_status=$(cat "${PROFILE_DIR}/.cpu_status_$$" 2>/dev/null || echo "UNKNOWN")
    local cpu_log="${PROFILE_DIR}/.cpu_profile_log_$$"
    
    print_info "Checking CPU profile status..."
    print_info "CPU PID: $cpu_pid, Exit code: $cpu_exit, Status: $cpu_status, File: $cpu_file"
    
    # Wait a bit more if process is still running
    if [ -n "$cpu_pid" ] && kill -0 "$cpu_pid" 2>/dev/null; then
        print_info "CPU profile process still running, waiting for it to complete..."
        local wait_count=0
        while kill -0 "$cpu_pid" 2>/dev/null && [ $wait_count -lt 30 ]; do
            sleep 2
            wait_count=$((wait_count + 2))
            if [ $((wait_count % 10)) -eq 0 ]; then
                print_info "Still waiting for CPU profile process... (${wait_count}s)"
            fi
        done
        if kill -0 "$cpu_pid" 2>/dev/null; then
            print_warn "CPU profile process still running after ${wait_count}s, but checking file anyway..."
        fi
    fi
    
    # Give file system a moment to sync
    sleep 2
    
    # If CPU file path is empty, try to find it by pattern
    if [ -z "$cpu_file" ]; then
        print_info "CPU file path not found, searching for CPU profile files..."
        cpu_file=$(ls -t "${PROFILE_DIR}"/cpu_*.prof 2>/dev/null | head -1 || echo "")
        if [ -n "$cpu_file" ]; then
            print_info "Found CPU profile file by pattern: $cpu_file"
        fi
    fi
    
    if [ -n "$cpu_file" ] && [ -f "$cpu_file" ] && [ -s "$cpu_file" ]; then
        # Re-check file size
        cpu_size=$(stat -f%z "$cpu_file" 2>/dev/null || stat -c%s "$cpu_file" 2>/dev/null || echo "0")
        print_info "CPU profile file exists: $cpu_file (${cpu_size} bytes)"
        
        if [ "$cpu_size" -gt 1024 ]; then
            print_info "CPU profile completed successfully: $cpu_file (${cpu_size} bytes)"
            return 0  # Return success from wait function
        elif [ "$cpu_size" -gt 100 ]; then
            # Check if it's an error message
            local first_line=$(head -1 "$cpu_file" 2>/dev/null || echo "")
            if echo "$first_line" | grep -qiE "(error|not found|404|500|html|<!DOCTYPE)"; then
                print_error "CPU profile file contains error message (${cpu_size} bytes)"
                print_error "First line: $first_line"
                if [ -f "$cpu_log" ]; then
                    print_error "CPU profile log:"
                    tail -20 "$cpu_log" 2>/dev/null || true
                fi
            else
                print_warn "CPU profile file is small but appears valid: $cpu_file (${cpu_size} bytes)"
            fi
        else
            print_error "CPU profile file is too small (${cpu_size} bytes), may be invalid"
            if [ -f "$cpu_log" ]; then
                print_error "CPU profile log:"
                cat "$cpu_log" 2>/dev/null || true
            fi
            if [ -f "$cpu_file" ]; then
                print_error "CPU profile file content:"
                head -20 "$cpu_file" 2>/dev/null || true
            fi
        fi
    else
        print_error "CPU profile file not found or empty: $cpu_file"
        if [ -f "$cpu_log" ]; then
            print_error "CPU profile log:"
            cat "$cpu_log" 2>/dev/null || true
        fi
        print_error "CPU profile PID: $cpu_pid, Exit code: $cpu_exit"
    fi
    
    # Cleanup log file (but keep it if there was an error for debugging)
    if [ "$cpu_status" = "SUCCESS" ] || [ "$cpu_size" -gt 1024 ]; then
        rm -f "$cpu_log" 2>/dev/null || true
    else
        print_warn "Keeping CPU profile log for debugging: $cpu_log"
    fi
    rm -f "${PROFILE_DIR}/.cpu_pid_$$" "${PROFILE_DIR}/.cpu_size_$$" "${PROFILE_DIR}/.cpu_status_$$" 2>/dev/null || true
    
    # Check trace profile
    if [ -n "$trace_file" ] && [ -f "$trace_file" ] && [ -s "$trace_file" ]; then
        local trace_size=$(stat -f%z "$trace_file" 2>/dev/null || stat -c%s "$trace_file" 2>/dev/null || echo "0")
        local trace_exit=$(cat "${PROFILE_DIR}/.trace_exit_$$" 2>/dev/null || echo "1")
        if [ "$trace_exit" = "0" ] && [ "$trace_size" -gt 100 ]; then
            print_info "Trace profile completed successfully: $trace_file (${trace_size} bytes)"
        else
            print_warn "Trace profile may be incomplete: $trace_file (${trace_size} bytes, exit: $trace_exit)"
        fi
    else
        print_warn "Trace profile file not found or empty: $trace_file"
    fi
    
    # Check heap profile captures
    local heap_pid=$(cat "${PROFILE_DIR}/.heap_pid_$$" 2>/dev/null || echo "")
    local heap_status=$(cat "${PROFILE_DIR}/.heap_capture_status_$$" 2>/dev/null || echo "UNKNOWN")
    local heap_log="${PROFILE_DIR}/.heap_capture_log_$$"
    
    if [ -n "$heap_pid" ]; then
        print_info "Checking heap profile captures..."
        print_info "Heap capture PID: $heap_pid, Status: $heap_status"
        
        # Wait a bit more if process is still running
        if kill -0 "$heap_pid" 2>/dev/null; then
            print_info "Heap capture process still running, waiting a bit more..."
            sleep 5
        fi
        
        # Count heap profile files captured during processing
        local heap_files=($(ls -t "${PROFILE_DIR}"/heap_during_processing_*.prof 2>/dev/null || echo ""))
        local heap_count=${#heap_files[@]}
        
        if [ $heap_count -gt 0 ]; then
            print_info "Heap profiles captured during processing: ${heap_count} files"
            if [ -f "$heap_log" ]; then
                print_info "Heap capture log:"
                cat "$heap_log" 2>/dev/null || true
            fi
            # Show first and last heap profile sizes
            if [ $heap_count -gt 0 ]; then
                # Get first file (oldest, last in sorted list)
                local first_file="${heap_files[$((heap_count - 1))]}"
                # Get last file (newest, first in sorted list)
                local last_file="${heap_files[0]}"
                
                if [ -n "$first_file" ] && [ -f "$first_file" ]; then
                    local first_size=$(stat -f%z "$first_file" 2>/dev/null || stat -c%s "$first_file" 2>/dev/null || echo "0")
                    print_info "First heap profile: $first_file (${first_size} bytes)"
                fi
                
                if [ -n "$last_file" ] && [ -f "$last_file" ]; then
                    local last_size=$(stat -f%z "$last_file" 2>/dev/null || stat -c%s "$last_file" 2>/dev/null || echo "0")
                    print_info "Last heap profile: $last_file (${last_size} bytes)"
                fi
            fi
        else
            print_warn "No heap profiles captured during processing"
            if [ -f "$heap_log" ]; then
                print_warn "Heap capture log:"
                cat "$heap_log" 2>/dev/null || true
            fi
        fi
    fi
    
    # Cleanup temp files (but keep error logs for debugging)
    # Note: .cpu_file_verified_$$ and .cpu_file_$$ are kept for main function to verify
    # Don't delete .cpu_file_$$ yet - main function needs it as fallback
    rm -f "${PROFILE_DIR}/.all_profile_pids_$$" \
          "${PROFILE_DIR}/.profile_timestamp_$$" \
          "${PROFILE_DIR}/.trace_file_$$" \
          "${PROFILE_DIR}/.cpu_exit_$$" \
          "${PROFILE_DIR}/.trace_exit_$$" \
          "${PROFILE_DIR}/.cpu_status_$$" \
          "${PROFILE_DIR}/.heap_pid_$$" \
          "${PROFILE_DIR}/.heap_capture_status_$$" \
          "${PROFILE_DIR}/.heap_capture_log_$$" \
          "${PROFILE_DIR}/.heap_interval_$$" \
          "${PROFILE_DIR}/.heap_count_$$" \
          "${PROFILE_DIR}/.heap_timestamp_$$" 2>/dev/null || true
    
    return 0
}

# Function to capture CPU profile (synchronous, for fallback)
capture_cpu_profile() {
    local duration=${1:-120}
    local output_file=${2:-"${PROFILE_DIR}/cpu_$(date +%Y%m%d_%H%M%S).prof"}
    
    print_info "Capturing CPU profile for ${duration} seconds..."
    print_info "Output file: $output_file"
    
    # Simple curl command to capture CPU profile
    curl -s "http://localhost:${METRICS_PORT}/debug/pprof/profile?seconds=${duration}" > "$output_file" 2>&1
    local curl_exit=$?
    
    # Check if file was created and has content
    if [ -f "$output_file" ] && [ -s "$output_file" ]; then
        local file_size=$(stat -f%z "$output_file" 2>/dev/null || stat -c%s "$output_file" 2>/dev/null || echo "0")
        if [ "$file_size" -gt 1024 ]; then
            print_info "CPU profile saved successfully: $output_file (${file_size} bytes)"
            return 0
        elif [ "$file_size" -gt 100 ]; then
            print_warn "CPU profile file is small (${file_size} bytes) but appears valid"
            print_info "CPU profile saved: $output_file"
            return 0
        else
            print_error "CPU profile file is too small (${file_size} bytes), may be invalid"
            return 1
        fi
    else
        print_error "Failed to capture CPU profile - file not created or empty (curl exit: $curl_exit)"
        return 1
    fi
}

# Function to capture memory profile
capture_memory_profile() {
    local output_file="${PROFILE_DIR}/heap_$(date +%Y%m%d_%H%M%S).prof"
    
    print_info "Capturing memory (heap) profile..."
    print_info "Output file: $output_file"
    
    local http_code=$(curl -s -w "%{http_code}" --max-time 30 \
        "http://localhost:${METRICS_PORT}/debug/pprof/heap" \
        -o "$output_file" 2>&1 | tail -n1)
    
    if [ -f "$output_file" ] && [ -s "$output_file" ]; then
        local file_size=$(stat -f%z "$output_file" 2>/dev/null || stat -c%s "$output_file" 2>/dev/null || echo "0")
        if [ "$file_size" -gt 100 ]; then
            print_info "Memory profile saved successfully: $output_file (${file_size} bytes)"
            return 0
        else
            print_warn "Memory profile file is small (${file_size} bytes), may be invalid"
            return 1
        fi
    else
        print_warn "Failed to capture memory profile (HTTP code: ${http_code:-unknown})"
        return 1
    fi
}

# Function to capture snapshot profiles (memory, goroutine, etc.)
# CPU and trace profiles are captured in background before this
capture_all_profiles() {
    local cpu_duration=${1:-30}  # Not used, CPU is captured in background
    local trace_duration=${2:-10}  # Not used, trace is captured in background
    
    print_step "Capturing snapshot profiles (memory, goroutine, etc.)..."
    print_info "Note: CPU and trace profiles were already captured in background during metrics processing"
    
    # Verify service is still running before capturing
    if [ -f "${PROFILE_DIR}/telemetry.pid" ]; then
        local pid=$(cat "${PROFILE_DIR}/telemetry.pid" 2>/dev/null || echo "")
        if [ -n "$pid" ] && ! kill -0 "$pid" 2>/dev/null; then
            print_error "Telemetry service is not running (PID: $pid)"
            return 1
        fi
    fi
    
    # Verify pprof endpoint is accessible
    print_info "Verifying pprof endpoint is accessible..."
    if ! curl -s --max-time 5 "http://localhost:${METRICS_PORT}/debug/pprof/" >/dev/null 2>&1; then
        print_error "pprof endpoint is not accessible. Service may have stopped."
        return 1
    fi
    print_info "pprof endpoint is accessible"
    
    # Note: CPU, trace, and heap profiles are already captured in background during processing
    print_info "Skipping CPU, trace, and heap profiles (already captured in background during metrics processing)"
    print_info "Heap profiles were captured periodically during processing"
    
    # Capture a final memory profile after processing completes
    print_info "Capturing final memory profile after processing..."
    if ! capture_memory_profile; then
        print_warn "Final memory profile capture failed, but continuing..."
    fi
    
    # Allocs profile
    local allocs_file="${PROFILE_DIR}/allocs_$(date +%Y%m%d_%H%M%S).prof"
    if curl -s --max-time 30 "http://localhost:${METRICS_PORT}/debug/pprof/allocs" > "$allocs_file" && \
       [ -f "$allocs_file" ] && [ -s "$allocs_file" ]; then
        print_info "Allocs profile captured: $allocs_file"
    else
        print_warn "Failed to capture allocs profile"
    fi
    
    # Goroutine profile
    curl -s "http://localhost:${METRICS_PORT}/debug/pprof/goroutine?debug=1" > "${PROFILE_DIR}/goroutine_$(date +%Y%m%d_%H%M%S).prof" && \
        print_info "Goroutine profile captured" || print_warn "Failed to capture goroutine profile"
    
    # Block profile
    curl -s "http://localhost:${METRICS_PORT}/debug/pprof/block" > "${PROFILE_DIR}/block_$(date +%Y%m%d_%H%M%S).prof" && \
        print_info "Block profile captured" || print_warn "Failed to capture block profile"
    
    # Mutex profile
    curl -s "http://localhost:${METRICS_PORT}/debug/pprof/mutex" > "${PROFILE_DIR}/mutex_$(date +%Y%m%d_%H%M%S).prof" && \
        print_info "Mutex profile captured" || print_warn "Failed to capture mutex profile"
    
    # Threadcreate profile
    curl -s "http://localhost:${METRICS_PORT}/debug/pprof/threadcreate" > "${PROFILE_DIR}/threadcreate_$(date +%Y%m%d_%H%M%S).prof" && \
        print_info "Threadcreate profile captured" || print_warn "Failed to capture threadcreate profile"
    
    # Note: Trace profile is already captured in background, skip it here
    print_info "Skipping trace profile (already captured in background during metrics processing)"
    
    # Additional info
    curl -s "http://localhost:${METRICS_PORT}/debug/pprof/cmdline" > "${PROFILE_DIR}/cmdline_$(date +%Y%m%d_%H%M%S).txt" && \
        print_info "Cmdline captured" || print_warn "Failed to capture cmdline"
    
    curl -s "http://localhost:${METRICS_PORT}/debug/pprof/symbol" > "${PROFILE_DIR}/symbol_$(date +%Y%m%d_%H%M%S).txt" && \
        print_info "Symbol captured" || print_warn "Failed to capture symbol"
    
    print_info "All profiles captured and saved to: $PROFILE_DIR"
}

# Function to generate metrics for past hour using existing resources
generate_metrics_for_past_hour() {
    print_step "Generating metrics for the past hour using existing resources..."
    
    # Check if binary exists or is in PATH
    if [ ! -f "$METRICS_GENERATOR_BINARY" ] && ! command_exists "$METRICS_GENERATOR_BINARY"; then
        print_warn "Metrics generator binary not found at: $METRICS_GENERATOR_BINARY"
        print_warn "Please set METRICS_GENERATOR_BINARY environment variable or ensure 'generate_hydrated_metrics' is in PATH"
        print_warn "Skipping metrics generation"
        return 1
    fi
    
    print_info "Running metrics generator with:"
    print_info "  -use-existing-resources: true"
    print_info "  -generate-past-hour: true"
    
    # Load environment variables if file exists
    local env_vars=""
    if [ -n "$ENV_FILE" ] && [ -f "$ENV_FILE" ]; then
        print_info "Loading environment variables from $ENV_FILE"
        set -a
        source "$ENV_FILE"
        set +a
        
        # Adjust database hosts for Docker if needed
        adjust_database_hosts
    fi
    
    # Run the metrics generator
    print_info "Executing: $METRICS_GENERATOR_BINARY -use-existing-resources=true -generate-past-hour=true"
    
    # Run the metrics generator and capture output
    if "$METRICS_GENERATOR_BINARY" \
        -use-existing-resources=true \
        -generate-past-hour=true \
        > "${PROFILE_DIR}/metrics_generator.log" 2>&1; then
        print_info "Successfully generated metrics for the past hour"
        print_info "Metrics generator log saved to: ${PROFILE_DIR}/metrics_generator.log"
        # Show last few lines of log
        print_info "Last 10 lines of metrics generator output:"
        tail -10 "${PROFILE_DIR}/metrics_generator.log" 2>/dev/null || true
        return 0
    else
        local exit_code=$?
        print_error "Failed to generate metrics (exit code: $exit_code)"
        print_error "Metrics generator log:"
        tail -30 "${PROFILE_DIR}/metrics_generator.log" 2>/dev/null || true
        return 1
    fi
}

# Function to trigger usage metrics collection
trigger_usage_metrics() {
    local correlation_id="${CORRELATION_ID:-$(uuidgen 2>/dev/null || echo "273b9dcb-f070-472b-8513-e7c16cd1cac5")}"
    local endpoint="http://localhost:${METRICS_PORT}/v1/usage"
    
    print_step "Triggering usage metrics collection..."
    print_info "Endpoint: $endpoint"
    print_info "Correlation ID: $correlation_id"
    
    # Retry logic - try up to 5 times with increasing delays
    local max_attempts=5
    local attempt=0
    local success=0
    
    while [ $attempt -lt $max_attempts ] && [ $success -eq 0 ]; do
        attempt=$((attempt + 1))
        
        if [ $attempt -gt 1 ]; then
            local wait_time=$((attempt * 2))
            print_info "Retry attempt $attempt of $max_attempts (waiting ${wait_time}s before retry)..."
            sleep $wait_time
        fi
        
        # Check if service is responding
        if ! curl -s --max-time 2 "http://localhost:${METRICS_PORT}/v1/usage" >/dev/null 2>&1; then
            if [ $attempt -lt $max_attempts ]; then
                print_warn "Service endpoint not responding yet, will retry..."
                continue
            fi
        fi
        
        local response=$(curl -s -w "\n%{http_code}" -X POST \
            -H "X-Correlation-Id: $correlation_id" \
            -H "Content-Type: application/json" \
            "$endpoint" 2>&1)
        
        local http_code=$(echo "$response" | tail -n1)
        local body=$(echo "$response" | sed '$d')
        
        if [ "$http_code" = "202" ] || [ "$http_code" = "200" ]; then
            print_info "Usage metrics collection triggered successfully (HTTP $http_code)"
            if [ -n "$body" ]; then
                print_info "Response: $body"
            fi
            success=1
            return 0
        else
            if [ $attempt -lt $max_attempts ]; then
                print_warn "Usage metrics request returned HTTP $http_code, will retry..."
                if [ -n "$body" ]; then
                    print_warn "Response: $body"
                fi
            else
                print_warn "Usage metrics collection request returned HTTP $http_code after $max_attempts attempts"
                if [ -n "$body" ]; then
                    print_warn "Response: $body"
                fi
                return 1
            fi
        fi
    done
    
    return 1
}

# Function to stop telemetry service
stop_telemetry_service() {
    print_step "Stopping telemetry service..."
    
    if [ -f "${PROFILE_DIR}/telemetry.pid" ]; then
        local pid=$(cat "${PROFILE_DIR}/telemetry.pid")
        if kill -0 "$pid" 2>/dev/null; then
            print_info "Stopping telemetry service (PID: $pid)..."
            kill "$pid" 2>/dev/null || true
            sleep 2
            # Force kill if still running
            if kill -0 "$pid" 2>/dev/null; then
                print_warn "Force killing telemetry service..."
                kill -9 "$pid" 2>/dev/null || true
            fi
            print_info "Telemetry service stopped"
        fi
        rm -f "${PROFILE_DIR}/telemetry.pid"
    fi
}

# Function to upload profiles to GCS
upload_to_gcs() {
    if [ -z "$GCS_BUCKET" ]; then
        print_warn "GCS_BUCKET not set, skipping upload"
        return 0
    fi
    
    print_step "Uploading profiles to Google Cloud Storage..."
    
    if ! command_exists gsutil; then
        print_error "gsutil not found. Please install Google Cloud SDK."
        return 1
    fi
    
    # Authenticate if needed (for service account key)
    if [ -n "$GOOGLE_APPLICATION_CREDENTIALS" ] && [ -f "$GOOGLE_APPLICATION_CREDENTIALS" ]; then
        print_info "Using service account credentials from: $GOOGLE_APPLICATION_CREDENTIALS"
        export GOOGLE_APPLICATION_CREDENTIALS
    fi
    
    # Create timestamped directory in GCS
    local timestamp=$(date +%Y%m%d_%H%M%S)
    local gcs_path="gs://${GCS_BUCKET}/${GCS_PREFIX}${timestamp}/"
    
    print_info "Uploading profiles to: $gcs_path"
    
    # Upload all profile files
    if gsutil -m cp -r "${PROFILE_DIR}"/* "$gcs_path" 2>&1; then
        print_info "Successfully uploaded profiles to: $gcs_path"
        
        # List uploaded files
        print_info "Uploaded files:"
        gsutil ls "$gcs_path" | head -20
        
        return 0
    else
        print_error "Failed to upload profiles to GCS"
        return 1
    fi
}

# Cleanup function
cleanup() {
    print_step "Cleaning up..."
    stop_telemetry_service
}

# Trap signals for cleanup
trap cleanup EXIT INT TERM

# Main execution
main() {
    print_step "=== Telemetry Profiling and Upload Tool ==="
    print_info "Environment: $([ "$IS_DOCKER" = "true" ] && echo "Docker" || echo "Local")"
    print_info "Profile directory: $PROFILE_DIR"
    print_info "Metrics port: $METRICS_PORT"
    print_info "Profile duration: ${PROFILE_DURATION}s"
    print_info "GCS bucket: ${GCS_BUCKET:-not set}"
    print_info "Telemetry binary: $TELEMETRY_BINARY"
    print_info "Metrics generator binary: $METRICS_GENERATOR_BINARY"
    if [ -n "$ENV_FILE" ]; then
        print_info "Environment file: $ENV_FILE"
    fi
    
    # Create profile directory
    mkdir -p "$PROFILE_DIR"
    
    # Generate metrics for past hour using existing resources
    if [ "${GENERATE_METRICS:-true}" = "true" ]; then
        generate_metrics_for_past_hour
        if [ $? -ne 0 ]; then
            print_warn "Metrics generation failed, but continuing with profiling..."
        fi
    else
        print_info "Skipping metrics generation (GENERATE_METRICS=false)"
    fi
    
    # Start telemetry service
    start_telemetry_service
    
    # Wait a bit for service to stabilize
    print_info "Waiting 5 seconds for service to stabilize..."
    sleep 5
    
    # Start ALL profiles in background BEFORE triggering metrics
    # This ensures we capture the performance of metrics processing across all profile types
    print_step "Starting all profiles in background to capture metrics processing performance..."
    if ! start_all_profiles_background "$PROFILE_DURATION" "$TRACE_DURATION"; then
        print_warn "Failed to start background profiles, but continuing..."
    else
        print_info "All profiles started in background (CPU: ${PROFILE_DURATION}s, Trace: ${TRACE_DURATION}s)"
        print_info "Profiles will capture metrics processing performance"
    fi
    
    # Small delay to ensure all profiles have started
    sleep 3
    
    # Verify profiles are actively running before triggering metrics
    print_info "Verifying profiles are actively capturing..."
    local profile_pids_file="${PROFILE_DIR}/.all_profile_pids_$$"
    if [ -f "$profile_pids_file" ]; then
        local pids=($(cat "$profile_pids_file" 2>/dev/null || echo ""))
        local active_count=0
        for pid in "${pids[@]}"; do
            if kill -0 "$pid" 2>/dev/null; then
                active_count=$((active_count + 1))
            fi
        done
        if [ $active_count -gt 0 ]; then
            print_info "Verified ${active_count} profile(s) are actively running and capturing"
        else
            print_warn "No active profiles found, but continuing..."
        fi
    fi
    
    # Start periodic heap profile captures now (before triggering metrics)
    # This ensures heap profiles are captured during the actual processing
    local heap_interval=$(cat "${PROFILE_DIR}/.heap_interval_$$" 2>/dev/null || echo "10")
    local heap_count=$(cat "${PROFILE_DIR}/.heap_count_$$" 2>/dev/null || echo "1")
    local heap_timestamp=$(cat "${PROFILE_DIR}/.heap_timestamp_$$" 2>/dev/null || echo "$(date +%Y%m%d_%H%M%S)")
    
    if [ -n "$heap_interval" ] && [ -n "$heap_count" ]; then
        print_info "Starting periodic heap profile captures (${heap_count} captures every ${heap_interval}s)..."
        (
            set +e  # Don't exit on error
            local capture_num=0
            while [ $capture_num -lt $heap_count ]; do
                sleep $heap_interval
                local heap_file="${PROFILE_DIR}/heap_during_processing_${heap_timestamp}_${capture_num}.prof"
                if curl -s --max-time 10 \
                    "http://localhost:${METRICS_PORT}/debug/pprof/heap" \
                    -o "$heap_file" 2>/dev/null; then
                    if [ -f "$heap_file" ] && [ -s "$heap_file" ]; then
                        local file_size=$(stat -f%z "$heap_file" 2>/dev/null || stat -c%s "$heap_file" 2>/dev/null || echo "0")
                        echo "Captured heap profile #$((capture_num + 1)): $heap_file (${file_size} bytes)" >> "${PROFILE_DIR}/.heap_capture_log_$$"
                    fi
                fi
                capture_num=$((capture_num + 1))
            done
            echo "SUCCESS" > "${PROFILE_DIR}/.heap_capture_status_$$"
        ) &
        
        local heap_pid=$!
        echo "$heap_pid" > "${PROFILE_DIR}/.heap_pid_$$"
        profile_pids+=($heap_pid)
        printf "%s\n" "${profile_pids[@]}" > "${PROFILE_DIR}/.all_profile_pids_$$"
        print_info "Periodic heap profile capture started (PID: $heap_pid)"
    fi
    
    # Trigger usage metrics collection
    # All background profiles will capture the performance of this processing
    if [ "${TRIGGER_USAGE_METRICS:-true}" = "true" ]; then
        print_step "Triggering usage metrics collection (profiles are actively capturing during processing)..."
        
        # Verify service is running before triggering
        if [ -f "${PROFILE_DIR}/telemetry.pid" ]; then
            local pid=$(cat "${PROFILE_DIR}/telemetry.pid" 2>/dev/null || echo "")
            if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
                print_info "Service process is running (PID: $pid), proceeding to trigger usage metrics"
            else
                print_warn "Service process may not be running, but attempting to trigger usage metrics anyway"
            fi
        fi
        
        local trigger_start=$(date +%s)
        if trigger_usage_metrics; then
            local trigger_end=$(date +%s)
            local trigger_duration=$((trigger_end - trigger_start))
            print_info "Metrics collection triggered successfully (took ${trigger_duration}s). Profiles are actively capturing processing performance..."
            print_info "CPU, trace, and heap profiles are capturing during processing..."
            print_info "Profiles will continue capturing for the full duration to capture all processing activity"
        else
            print_warn "Usage metrics trigger returned an error, but continuing with profiling..."
            print_warn "Service may still be starting up - profiles will capture when service becomes ready"
        fi
    else
        print_info "Skipping usage metrics trigger (TRIGGER_USAGE_METRICS=false)"
    fi
    
    # Wait for all background profiles to complete
    # This ensures we capture the full metrics processing cycle across all profile types
    print_step "Waiting for all background profiles to complete (capturing metrics processing)..."
    print_info "Profiles are actively capturing during metrics processing..."
    
    # Monitor profile activity during wait
    local wait_start=$(date +%s)
    local profiles_success=true
    if wait_for_all_profiles; then
        local wait_end=$(date +%s)
        local wait_duration=$((wait_end - wait_start))
        print_info "All background profiles completed successfully (waited ${wait_duration}s)"
        print_info "Profiles captured the full metrics processing cycle"
    else
        local wait_end=$(date +%s)
        local wait_duration=$((wait_end - wait_start))
        print_warn "Some background profiles may not have completed successfully (waited ${wait_duration}s)"
        profiles_success=false
    fi
    
    # Verify CPU profile was captured successfully
    # Read from verified file (saved before cleanup) or try original
    local cpu_file=$(cat "${PROFILE_DIR}/.cpu_file_verified_$$" 2>/dev/null || cat "${PROFILE_DIR}/.cpu_file_$$" 2>/dev/null || echo "")
    local cpu_captured=false
    
    # If temp files don't have the path, try to find CPU profile file by pattern
    if [ -z "$cpu_file" ]; then
        print_info "CPU file path not found in temp files, searching for CPU profile files..."
        cpu_file=$(ls -t "${PROFILE_DIR}"/cpu_*.prof 2>/dev/null | head -1 || echo "")
        if [ -n "$cpu_file" ]; then
            print_info "Found CPU profile file by pattern: $cpu_file"
        fi
    fi
    
    # Also check for fallback CPU profiles
    if [ -z "$cpu_file" ] || [ ! -f "$cpu_file" ] || [ ! -s "$cpu_file" ]; then
        print_info "Checking for fallback CPU profile files..."
        local fallback_file=$(ls -t "${PROFILE_DIR}"/cpu_fallback_*.prof 2>/dev/null | head -1 || echo "")
        if [ -n "$fallback_file" ] && [ -f "$fallback_file" ] && [ -s "$fallback_file" ]; then
            print_info "Found fallback CPU profile file: $fallback_file"
            cpu_file="$fallback_file"
        fi
    fi
    
    # Give file system a moment to sync
    sleep 1
    
    if [ -n "$cpu_file" ] && [ -f "$cpu_file" ] && [ -s "$cpu_file" ]; then
        local cpu_size=$(stat -f%z "$cpu_file" 2>/dev/null || stat -c%s "$cpu_file" 2>/dev/null || echo "0")
        print_info "CPU profile file found: $cpu_file (${cpu_size} bytes)"
        
        # Check if file contains valid profile data
        if head -1 "$cpu_file" 2>/dev/null | grep -qiE "(error|not found|404|500|html|<!DOCTYPE)"; then
            print_warn "CPU profile file contains error message, not valid"
            cpu_file=""
        elif [ "$cpu_size" -gt 1024 ]; then
            print_info "CPU profile verified: $cpu_file (${cpu_size} bytes)"
            cpu_captured=true
        elif [ "$cpu_size" -gt 100 ]; then
            print_warn "CPU profile file exists but is small: $cpu_file (${cpu_size} bytes)"
            print_warn "File may be incomplete, but marking as captured"
            cpu_captured=true
        else
            print_warn "CPU profile file exists but is too small: $cpu_file (${cpu_size} bytes)"
        fi
    else
        print_warn "CPU profile file not found or empty: ${cpu_file:-not set}"
        # List all CPU profile files for debugging
        print_info "Available CPU profile files in ${PROFILE_DIR}:"
        ls -lh "${PROFILE_DIR}"/cpu_*.prof 2>/dev/null || print_warn "No CPU profile files found"
    fi
    
    # # Fallback: Try to capture CPU profile synchronously if background failed
    # if [ "$cpu_captured" = false ]; then
    #     print_warn "CPU profile was not captured successfully. Attempting fallback capture..."
    #     local cpu_file_fallback="${PROFILE_DIR}/cpu_fallback_$(date +%Y%m%d_%H%M%S).prof"
    #     print_info "Capturing fallback CPU profile (30 seconds)..."
    #     if capture_cpu_profile 30 "$cpu_file_fallback"; then
    #         print_info "Fallback CPU profile captured successfully: $cpu_file_fallback"
    #         cpu_captured=true
    #     else
    #         print_error "Fallback CPU profile capture also failed"
    #         print_error "CPU profiling may not be working. Check service logs and pprof endpoint."
    #     fi
    # fi
    
    # Cleanup verified file and temp files
    rm -f "${PROFILE_DIR}/.cpu_file_verified_$$" \
          "${PROFILE_DIR}/.cpu_file_$$" 2>/dev/null || true
    
    # Capture snapshot profiles (memory, goroutine, etc.) after metrics processing
    # These are point-in-time snapshots, not continuous like CPU/trace
    print_step "Capturing snapshot profiles (memory, goroutine, etc.) after metrics processing..."
    capture_all_profiles 10 "$TRACE_DURATION"
    
    # Upload to GCS if bucket is specified
    if [ -n "$GCS_BUCKET" ]; then
        upload_to_gcs
    else
        print_warn "GCS_BUCKET not set. Profiles are available in $PROFILE_DIR"
        print_info "To upload manually, set GCS_BUCKET and run:"
        print_info "  gsutil -m cp -r $PROFILE_DIR/* gs://YOUR_BUCKET/profiles/"
    fi
    
    # Stop service
    stop_telemetry_service
    
    print_step "=== Profiling Complete ==="
    print_info "Profiles saved to: $PROFILE_DIR"
    if [ -n "$GCS_BUCKET" ]; then
        print_info "Profiles uploaded to: gs://${GCS_BUCKET}/${GCS_PREFIX}"
    fi
}

# Run main function
main "$@"

