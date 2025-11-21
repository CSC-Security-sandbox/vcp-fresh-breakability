#!/bin/bash

# Script to invoke telemetry binary in background for profiling
# This script starts the telemetry service in the background with profiling enabled

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Check if running in Docker/Cloud Run (use /workspace or /app as fallback)
if [ -d "/workspace" ]; then
    PROJECT_ROOT="/workspace"
elif [ -d "/app" ]; then
    PROJECT_ROOT="/app"
else
    PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
fi
PROFILE_DIR="${PROFILE_DIR:-${PROJECT_ROOT}/profiles}"
# In Docker, binaries are in /usr/local/bin
if [ -f "/usr/local/bin/telemetry" ]; then
    TELEMETRY_BINARY="/usr/local/bin/telemetry"
elif [ -f "${PROJECT_ROOT}/app/telemetry" ]; then
    TELEMETRY_BINARY="${PROJECT_ROOT}/app/telemetry"
else
    TELEMETRY_BINARY="${PROJECT_ROOT}/app/telemetry"
fi
ENV_FILE="${ENV_FILE:-${PROJECT_ROOT}/tel_local.env}"
# Use METRICS_SERVER_PORT if METRICS_PORT is not set (for compatibility)
METRICS_PORT="${METRICS_PORT:-${METRICS_SERVER_PORT:-8080}}"
METRICS_SERVER_PORT="${METRICS_SERVER_PORT:-${METRICS_PORT:-8080}}"
# GCS Configuration
GCS_BUCKET="${GCS_BUCKET:-}"
GCS_PREFIX="${GCS_PREFIX:-profiles/}"
UPLOAD_TO_GCS="${UPLOAD_TO_GCS:-true}"
# Profile Duration Configuration
CPU_PROFILE_DURATION="${CPU_PROFILE_DURATION:-50}"
TRACE_PROFILE_DURATION="${TRACE_PROFILE_DURATION:-10}"
PERIODIC_PROFILE_DURATION="${PERIODIC_PROFILE_DURATION:-50}"
PERIODIC_PROFILE_INTERVAL="${PERIODIC_PROFILE_INTERVAL:-15}"
# Metrics Generation Configuration
GENERATE_METRICS="${GENERATE_METRICS:-true}"  # Set to "false" to skip metrics generation

# Create profiles directory if it doesn't exist
mkdir -p "$PROFILE_DIR"

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

# Function to check if port is in use
check_port() {
    local port=$1
    # In Docker/Cloud Run, use netstat or ss instead of lsof
    if command_exists lsof; then
        if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1 ; then
            return 0
        else
            return 1
        fi
    elif command_exists netstat; then
        if netstat -tuln 2>/dev/null | grep -q ":$port "; then
            return 0
        else
            return 1
        fi
    elif command_exists ss; then
        if ss -tuln 2>/dev/null | grep -q ":$port "; then
            return 0
        else
            return 1
        fi
    else
        # If no tools available, assume port is free
        return 1
    fi
}

# Function to build the telemetry binary
build_telemetry() {
    print_info "Building telemetry service with debug information..."
    cd "$PROJECT_ROOT"
    if ! go build -trimpath=false -gcflags="all=-N -l" -o "$TELEMETRY_BINARY" ./telemetry/main.go; then
        print_error "Failed to build telemetry service"
        exit 1
    fi
    print_info "Telemetry service built successfully"
}

# Function to wait for service to be ready
wait_for_service() {
    local max_attempts=${1:-60}
    local attempt=0
    local telemetry_log="${PROFILE_DIR}/telemetry.log"
    
    print_info "Waiting for telemetry service to be ready on port ${METRICS_PORT}..."
    
    while [ $attempt -lt $max_attempts ]; do
        # Check if service process is still running
        local pid_file="${PROFILE_DIR}/telemetry.pid"
        if [ -f "$pid_file" ]; then
            local pid=$(cat "$pid_file" 2>/dev/null)
            if [ -n "$pid" ] && ! kill -0 "$pid" 2>/dev/null; then
                print_error "Telemetry service process (PID: $pid) is not running!"
                if [ -f "$telemetry_log" ]; then
                    print_error "Last 20 lines of telemetry.log:"
                    tail -20 "$telemetry_log" 2>/dev/null || true
                fi
                return 1
            fi
        fi
        
        # Try to connect to the service
        if curl -s --max-time 2 "http://localhost:${METRICS_PORT}/health" >/dev/null 2>&1 || \
           curl -s --max-time 2 "http://localhost:${METRICS_PORT}/v1/usage" >/dev/null 2>&1 || \
           curl -s --max-time 2 "http://localhost:${METRICS_PORT}/debug/pprof/" >/dev/null 2>&1; then
            print_info "Service is ready!"
            return 0
        fi
        
        # Check if port is listening (service might be starting)
        if check_port "$METRICS_PORT"; then
            print_info "Port ${METRICS_PORT} is listening, waiting for service to respond..."
        fi
        
        attempt=$((attempt + 1))
        if [ $((attempt % 5)) -eq 0 ]; then
            print_info "Still waiting... (${attempt}/${max_attempts})"
            # Show recent log entries if available
            if [ -f "$telemetry_log" ] && [ -s "$telemetry_log" ]; then
                print_info "Recent telemetry service log entries:"
                tail -3 "$telemetry_log" 2>/dev/null | sed 's/^/  /' || true
            fi
        fi
        sleep 1
    done
    
    print_warn "Service did not become ready within ${max_attempts} seconds"
    if [ -f "$telemetry_log" ]; then
        print_warn "Last 30 lines of telemetry.log:"
        tail -30 "$telemetry_log" 2>/dev/null | sed 's/^/  /' || true
    fi
    return 1
}

# Function to trigger usage endpoint
trigger_usage_endpoint() {
    local correlation_id="${CORRELATION_ID:-$(uuidgen 2>/dev/null || echo "$(date +%s)-$$")}"
    local endpoint="http://localhost:${METRICS_PORT}/v1/usage"
    
    print_info "Triggering usage endpoint to generate metrics..."
    print_info "Endpoint: $endpoint"
    print_info "Correlation ID: $correlation_id"
    
    if ! command_exists curl; then
        print_error "curl not found. Please install curl to trigger usage endpoint." >&2
        return 1
    fi
    
    # Retry logic - try up to 3 times with delays
    local max_attempts=3
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
        if ! curl -s --max-time 2 "$endpoint" >/dev/null 2>&1; then
            if [ $attempt -lt $max_attempts ]; then
                print_warn "Service endpoint not responding yet, will retry..."
                continue
            fi
        fi
        
        # Trigger the usage endpoint
        local response=$(curl -s -w "\n%{http_code}" -X POST \
            -H "X-Correlation-Id: $correlation_id" \
            -H "Content-Type: application/json" \
            --max-time 30 \
            "$endpoint" 2>&1)
        
        local http_code=$(echo "$response" | tail -n1)
        local body=$(echo "$response" | sed '$d')
        
        if [ "$http_code" = "202" ] || [ "$http_code" = "200" ]; then
            print_info "Usage endpoint triggered successfully (HTTP $http_code)"
            if [ -n "$body" ]; then
                print_info "Response: $body"
            fi
            success=1
        else
            print_warn "Usage endpoint returned HTTP $http_code"
            if [ -n "$body" ]; then
                print_warn "Response: $body"
            fi
            if [ $attempt -lt $max_attempts ]; then
                print_info "Will retry..."
            fi
        fi
    done
    
    if [ $success -eq 1 ]; then
        print_info "Usage endpoint triggered successfully"
        sleep 3  # Wait a moment for processing to start
        return 0
    else
        print_warn "Failed to trigger usage endpoint after $max_attempts attempts, but continuing..."
        return 1
    fi
}

# Function to generate metrics for past hour using existing resources
generate_metrics_for_past_hour() {
    # Skip if GENERATE_METRICS is set to false
    if [ "${GENERATE_METRICS:-true}" != "true" ]; then
        print_info "Skipping metrics generation (GENERATE_METRICS=false)"
        return 0
    fi
    
    print_info "Generating metrics for the past hour using existing resources..."
    
    # Find metrics generator binary
    local metrics_generator_binary=""
    if [ -n "$METRICS_GENERATOR_BINARY" ]; then
        metrics_generator_binary="$METRICS_GENERATOR_BINARY"
    elif [ -f "/usr/local/bin/generate_hydrated_metrics" ]; then
        metrics_generator_binary="/usr/local/bin/generate_hydrated_metrics"
    elif command_exists generate_hydrated_metrics; then
        metrics_generator_binary="$(command -v generate_hydrated_metrics)"
    elif [ -f "${PROJECT_ROOT}/bin/generate_hydrated_metrics" ]; then
        metrics_generator_binary="${PROJECT_ROOT}/bin/generate_hydrated_metrics"
    elif [ -f "${PROJECT_ROOT}/tools/generate_hydrated_metrics" ]; then
        metrics_generator_binary="${PROJECT_ROOT}/tools/generate_hydrated_metrics"
    else
        print_warn "Metrics generator binary not found"
        print_warn "Please set METRICS_GENERATOR_BINARY environment variable or ensure 'generate_hydrated_metrics' is in PATH"
        print_warn "Skipping metrics generation"
        return 1
    fi
    
    print_info "Using metrics generator: $metrics_generator_binary"
    print_info "Running with: -use-existing-resources=true -generate-past-hour=true"
    
    # Load environment variables if file exists
    if [ -f "$ENV_FILE" ]; then
        print_info "Loading environment variables from $ENV_FILE"
        set -a
        source "$ENV_FILE"
        set +a
    fi
    
    # Run the metrics generator
    local metrics_log="${PROFILE_DIR}/metrics_generator.log"
    print_info "Executing: $metrics_generator_binary -use-existing-resources=true -generate-past-hour=true"
    print_info "Log file: $metrics_log"
    print_info "Note: This may fail if volumes cannot be created. This is OK for profiling purposes."
    
    # Run with timeout to prevent hanging
    local timeout_seconds=1200  # 5 minutes timeout
    if timeout "$timeout_seconds" "$metrics_generator_binary" \
        -use-existing-resources=true \
        -generate-past-hour=true \
        > "$metrics_log" 2>&1; then
        print_info "Successfully generated metrics for the past hour"
        print_info "Metrics generator log saved to: $metrics_log"
        
        # Show last few lines of log
        if [ -f "$metrics_log" ] && [ -s "$metrics_log" ]; then
            print_info "Last 10 lines of metrics generator output:"
            tail -10 "$metrics_log" 2>/dev/null || true
        fi
        return 0
    else
        local exit_code=$?
        # Check if it's a timeout
        if [ $exit_code -eq 124 ]; then
            print_warn "Metrics generation timed out after ${timeout_seconds} seconds"
        else
            print_warn "Metrics generation failed (exit code: $exit_code)"
        fi
        
        # Check if the error is related to volume creation
        if [ -f "$metrics_log" ]; then
            if grep -q "CreateVolume\|Failed to create volume" "$metrics_log" 2>/dev/null; then
                print_warn "Metrics generator failed due to volume creation errors."
                print_warn "This is expected if volumes cannot be created in the environment."
                print_warn "Profiling will continue without generated metrics."
            else
                print_warn "Metrics generator log (last 30 lines):"
                tail -30 "$metrics_log" 2>/dev/null || true
            fi
        fi
        # Return 0 (success) even on failure, since this is optional for profiling
        return 0
    fi
}

# Function to start telemetry service in background
start_telemetry_background() {
    print_info "Starting telemetry service in background with profiling enabled..."
    
    # Check if binary exists
    if [ ! -f "$TELEMETRY_BINARY" ]; then
        print_warn "Telemetry binary not found, building..."
        build_telemetry
    fi
    
    # Check if port is already in use
    if check_port "$METRICS_PORT"; then
        print_warn "Port $METRICS_PORT is already in use"
        print_info "Service may already be running"
    fi
    
    # Load environment variables
    if [ -f "$ENV_FILE" ]; then
        print_info "Loading environment variables from $ENV_FILE"
        set -a
        source "$ENV_FILE"
        set +a
    else
        print_warn "Environment file $ENV_FILE not found. Using system environment variables."
    fi
    
    # Enable pprof
    export ENABLE_PPROF=true
    
    # Enable mock mode for Google metrics (don't actually send to Google)
    export MOCK_GOOGLE_METRICS=true
    
    # Set Go runtime flags for profiling
    export GODEBUG="gctrace=1"
    
    # Ensure METRICS_SERVER_PORT is set (telemetry service uses this)
    export METRICS_SERVER_PORT="${METRICS_SERVER_PORT:-${METRICS_PORT:-8080}}"
    export METRICS_PORT="${METRICS_PORT:-${METRICS_SERVER_PORT:-8080}}"
    
    # Fix database host for Docker environment
    # If running in Docker and DB_HOST is localhost/127.0.0.1, use DOCKER_HOST_IP
    if [ -n "$DOCKER_HOST_IP" ] && [ "$DOCKER_HOST_IP" != "localhost" ] && [ "$DOCKER_HOST_IP" != "127.0.0.1" ]; then
        # Check if DB_HOST is localhost or 127.0.0.1
        if [ -z "$DB_HOST" ] || [ "$DB_HOST" = "localhost" ] || [ "$DB_HOST" = "127.0.0.1" ]; then
            print_info "Overriding DB_HOST from '${DB_HOST:-localhost}' to '$DOCKER_HOST_IP' for Docker environment"
            export DB_HOST="$DOCKER_HOST_IP"
        fi
        
        # Check if METRICS_DB_HOST is localhost or 127.0.0.1
        if [ -z "$METRICS_DB_HOST" ] || [ "$METRICS_DB_HOST" = "localhost" ] || [ "$METRICS_DB_HOST" = "127.0.0.1" ]; then
            print_info "Overriding METRICS_DB_HOST from '${METRICS_DB_HOST:-localhost}' to '$DOCKER_HOST_IP' for Docker environment"
            export METRICS_DB_HOST="$DOCKER_HOST_IP"
        fi
    fi
    
    print_info "Database configuration:"
    print_info "  DB_HOST: ${DB_HOST:-not set}"
    print_info "  METRICS_DB_HOST: ${METRICS_DB_HOST:-not set}"
    
    # Define log file path
    local telemetry_log="${PROFILE_DIR}/telemetry.log"
    print_info "Telemetry logs will be written to: $telemetry_log"
    
    # Start the service in background
    print_info "Starting telemetry service on port $METRICS_PORT"
    print_info "Using METRICS_SERVER_PORT: $METRICS_SERVER_PORT"
    print_info "pprof endpoints available at: http://localhost:$METRICS_PORT/debug/pprof/"
    print_info "Prometheus metrics available at: http://localhost:$METRICS_PORT/metrics"
    
    cd "$PROJECT_ROOT"
    print_info "Starting binary: $TELEMETRY_BINARY"
    print_info "Log file: $telemetry_log"
    
    # Start the service and capture both stdout and stderr
    "$TELEMETRY_BINARY" > "$telemetry_log" 2>&1 &
    local telemetry_pid=$!
    
    # Give the process a moment to start
    sleep 2
    
    # Check if process is still running
    if ! kill -0 "$telemetry_pid" 2>/dev/null; then
        print_error "Telemetry service failed to start (PID: $telemetry_pid)"
        if [ -f "$telemetry_log" ]; then
            print_error "Service output:"
            cat "$telemetry_log" 2>/dev/null || true
        fi
        return 1
    fi
    
    # Store PID for reference
    echo $telemetry_pid > "${PROFILE_DIR}/telemetry.pid"
    
    print_info "Telemetry service started in background with PID: $telemetry_pid"
    print_info "PID stored in: ${PROFILE_DIR}/telemetry.pid"
    print_info "Logs are being written to: $telemetry_log"
    print_info ""
    print_info "To stop the service, run: kill $telemetry_pid"
    print_info "Or use: kill \$(cat ${PROFILE_DIR}/telemetry.pid)"
    
    return 0
}

# Function to stop telemetry service
stop_telemetry_service() {
    local pid_file="${PROFILE_DIR}/telemetry.pid"
    
    if [ ! -f "$pid_file" ]; then
        print_warn "Telemetry PID file not found: $pid_file"
        print_info "Service may not be running or was started differently"
        return 1
    fi
    
    local telemetry_pid=$(cat "$pid_file" 2>/dev/null)
    
    if [ -z "$telemetry_pid" ]; then
        print_warn "Telemetry PID not found in file: $pid_file"
        return 1
    fi
    
    # Check if process is still running
    if ! kill -0 "$telemetry_pid" 2>/dev/null; then
        print_info "Telemetry service (PID: $telemetry_pid) is not running"
        rm -f "$pid_file" 2>/dev/null || true
        return 0
    fi
    
    print_info "Stopping telemetry service (PID: $telemetry_pid)..."
    
    # Try graceful shutdown first (SIGTERM)
    kill "$telemetry_pid" 2>/dev/null
    
    # Wait for process to terminate (max 10 seconds)
    local max_wait=10
    local elapsed=0
    while [ $elapsed -lt $max_wait ]; do
        if ! kill -0 "$telemetry_pid" 2>/dev/null; then
            print_info "Telemetry service stopped gracefully"
            rm -f "$pid_file" 2>/dev/null || true
            return 0
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done
    
    # If still running, force kill (SIGKILL)
    if kill -0 "$telemetry_pid" 2>/dev/null; then
        print_warn "Telemetry service did not stop gracefully, forcing termination..."
        kill -9 "$telemetry_pid" 2>/dev/null
        sleep 1
        
        if ! kill -0 "$telemetry_pid" 2>/dev/null; then
            print_info "Telemetry service force stopped"
            rm -f "$pid_file" 2>/dev/null || true
            return 0
        else
            print_error "Failed to stop telemetry service (PID: $telemetry_pid)"
            return 1
        fi
    fi
    
    rm -f "$pid_file" 2>/dev/null || true
    return 0
}

# Function to capture CPU profile asynchronously in background
capture_cpu_profile_async() {
    local duration=${1:-${CPU_PROFILE_DURATION}}
    local output_file="${PROFILE_DIR}/cpu_$(date +%Y%m%d_%H%M%S).prof"
    
    print_info "Starting CPU profile capture in background for ${duration} seconds..." >&2
    
    if ! command_exists go; then
        print_error "go tool not found. Please install Go to use profiling features." >&2
        return 1
    fi
    
    if ! command_exists curl; then
        print_error "curl not found. Please install curl to verify endpoints." >&2
        return 1
    fi
    
    # Note: Endpoint verification will be done in the background process to avoid blocking
    
    # Build the command for logging
    local pprof_url="http://localhost:${METRICS_PORT}/debug/pprof/profile?seconds=${duration}"
    local pprof_cmd="go tool pprof -proto -output=\"$output_file\" \"$pprof_url\""
    
    # Log to debug file
    local debug_log="${PROFILE_DIR}/.cpu_pprof_debug_$$.log"
    local pprof_log="${PROFILE_DIR}/.cpu_pprof_log_$$"
    local pid_file="${PROFILE_DIR}/.cpu_pprof_pid_$$"
    
    {
        echo "=== CPU Profile Capture Debug Log (Async) ==="
        echo "Timestamp: $(date)"
        echo "Command: $pprof_cmd"
        echo "Output file: $output_file"
        echo "Profile URL: $pprof_url"
        echo "Duration: ${duration} seconds"
        echo "Metrics port: ${METRICS_PORT}"
        echo "Parent PID: $$"
        echo "---"
    } > "$debug_log"
    
    # Log the command for debugging (redirect to stderr to avoid interfering with return value)
    print_info "Executing command in background: $pprof_cmd" >&2
    print_info "Output file: $output_file" >&2
    print_info "Profile URL: $pprof_url" >&2
    print_info "Duration: ${duration} seconds" >&2
    print_info "Metrics port: ${METRICS_PORT}" >&2
    
    # Start CPU profile capture in background (non-blocking)
    (
        {
            echo "Background process started at: $(date)"
            echo "PID: $$"
        } >> "$debug_log"
        
        # Verify endpoint is accessible (in background, non-blocking for caller)
        local max_attempts=10
        local attempt=0
        local endpoint_ready=false
        
        {
            echo "Verifying pprof endpoint is accessible..."
        } >> "$debug_log"
        
        while [ $attempt -lt $max_attempts ]; do
            if curl -s --max-time 2 "http://localhost:${METRICS_PORT}/debug/pprof/" >/dev/null 2>&1; then
                {
                    echo "pprof endpoint is accessible (attempt $((attempt + 1))/${max_attempts})"
                } >> "$debug_log"
                endpoint_ready=true
                break
            fi
            
            attempt=$((attempt + 1))
            if [ $attempt -lt $max_attempts ]; then
                sleep 2
            fi
        done
        
        if [ "$endpoint_ready" != "true" ]; then
            {
                echo "ERROR: pprof endpoint is not accessible after ${max_attempts} attempts"
                echo "Please check service logs: ${PROFILE_DIR}/telemetry.log"
            } >> "$debug_log"
            echo "ERROR: pprof endpoint not accessible" > "$pprof_log"
            exit 1
        fi
        
        # Wait a moment to ensure service is fully ready
        sleep 2
        
        {
            echo "Starting CPU profile capture..."
            echo "Command: go tool pprof -proto -output=$output_file $pprof_url"
        } >> "$debug_log"
        
        go tool pprof -proto -output="$output_file" \
            "$pprof_url" \
            > "$pprof_log" 2>&1
        local pprof_exit=$?
        
        # Log completion to debug file
        {
            echo "---"
            echo "Command completed at: $(date)"
            echo "Exit code: $pprof_exit"
            if [ -f "$output_file" ]; then
                local file_size=$(stat -f%z "$output_file" 2>/dev/null || stat -c%s "$output_file" 2>/dev/null || echo "0")
                echo "Output file size: ${file_size} bytes"
            else
                echo "Output file: NOT CREATED"
            fi
            if [ $pprof_exit -eq 0 ]; then
                echo "Status: SUCCESS"
            else
                echo "Status: FAILED"
                if [ -f "$pprof_log" ]; then
                    echo "Error output:"
                    cat "$pprof_log" 2>/dev/null || true
                fi
            fi
        } >> "$debug_log"
        
        # Clean up log file on success
        if [ $pprof_exit -eq 0 ] && [ -f "$output_file" ] && [ -s "$output_file" ]; then
            rm -f "$pprof_log" 2>/dev/null || true
        fi
    ) &
    
    local pprof_pid=$!
    
    # Store PID for reference
    echo $pprof_pid > "$pid_file"
    
    # Log PID to debug file
    {
        echo "Background process PID: $pprof_pid"
        echo "PID file: $pid_file"
    } >> "$debug_log"
    
    print_info "CPU profile capture started in background with PID: $pprof_pid" >&2
    print_info "Profile will be saved to: $output_file" >&2
    print_info "Debug log: $debug_log" >&2
    print_info "PID file: $pid_file" >&2
    print_info "The profile will run for ${duration} seconds in the background" >&2
    print_info "To check status, monitor: $debug_log" >&2
    print_info "To wait for completion, check if PID $pprof_pid is still running" >&2
    
    # Return the PID and output file for caller to track (this goes to stdout)
    echo "$pprof_pid|$output_file|$debug_log|$pid_file"
    return 0
}

# Function to capitalize first letter (bash 3.2 compatible)
capitalize_first() {
    local str="$1"
    local first=$(echo "$str" | cut -c1 | tr '[:lower:]' '[:upper:]')
    local rest=$(echo "$str" | cut -c2-)
    echo "${first}${rest}"
}

# Generic function to capture pprof profile asynchronously (for heap, goroutine, block, mutex, allocs, threadcreate)
capture_pprof_profile_async() {
    local profile_type=$1  # heap, goroutine, block, mutex, allocs, threadcreate
    local output_file="${PROFILE_DIR}/${profile_type}_$(date +%Y%m%d_%H%M%S).prof"
    
    print_info "Starting ${profile_type} profile capture in background..." >&2
    
    if ! command_exists go; then
        print_error "go tool not found. Please install Go to use profiling features." >&2
        return 1
    fi
    
    if ! command_exists curl; then
        print_error "curl not found. Please install curl to verify endpoints." >&2
        return 1
    fi
    
    # Build the command for logging
    local pprof_url="http://localhost:${METRICS_PORT}/debug/pprof/${profile_type}"
    local pprof_cmd="go tool pprof -proto -output=\"$output_file\" \"$pprof_url\""
    
    # Log to debug file
    local debug_log="${PROFILE_DIR}/.${profile_type}_pprof_debug_$$.log"
    local pprof_log="${PROFILE_DIR}/.${profile_type}_pprof_log_$$"
    local pid_file="${PROFILE_DIR}/.${profile_type}_pprof_pid_$$"
    local profile_type_display=$(capitalize_first "$profile_type")
    
    {
        echo "=== ${profile_type_display} Profile Capture Debug Log (Async) ==="
        echo "Timestamp: $(date)"
        echo "Command: $pprof_cmd"
        echo "Output file: $output_file"
        echo "Profile URL: $pprof_url"
        echo "Profile type: $profile_type"
        echo "Metrics port: ${METRICS_PORT}"
        echo "Parent PID: $$"
        echo "---"
    } > "$debug_log"
    
    # Log the command for debugging
    print_info "Executing command in background: $pprof_cmd" >&2
    print_info "Output file: $output_file" >&2
    print_info "Profile URL: $pprof_url" >&2
    
    # Start profile capture in background
    (
        {
            echo "Background process started at: $(date)"
            echo "PID: $$"
        } >> "$debug_log"
        
        go tool pprof -proto -output="$output_file" \
            "$pprof_url" \
            > "$pprof_log" 2>&1
        local pprof_exit=$?
        
        # Log completion to debug file
        {
            echo "---"
            echo "Command completed at: $(date)"
            echo "Exit code: $pprof_exit"
            if [ -f "$output_file" ]; then
                local file_size=$(stat -f%z "$output_file" 2>/dev/null || stat -c%s "$output_file" 2>/dev/null || echo "0")
                echo "Output file size: ${file_size} bytes"
            else
                echo "Output file: NOT CREATED"
            fi
            if [ $pprof_exit -eq 0 ]; then
                echo "Status: SUCCESS"
            else
                echo "Status: FAILED"
                if [ -f "$pprof_log" ]; then
                    echo "Error output:"
                    cat "$pprof_log" 2>/dev/null || true
                fi
            fi
        } >> "$debug_log"
        
        # Clean up log file on success
        if [ $pprof_exit -eq 0 ] && [ -f "$output_file" ] && [ -s "$output_file" ]; then
            rm -f "$pprof_log" 2>/dev/null || true
        fi
    ) &
    
    local pprof_pid=$!
    
    # Store PID for reference
    echo $pprof_pid > "$pid_file"
    
    # Log PID to debug file
    {
        echo "Background process PID: $pprof_pid"
        echo "PID file: $pid_file"
    } >> "$debug_log"
    
    print_info "${profile_type_display} profile capture started in background with PID: $pprof_pid" >&2
    print_info "Profile will be saved to: $output_file" >&2
    print_info "Debug log: $debug_log" >&2
    
    # Return the PID and output file for caller to track
    echo "$pprof_pid|$output_file|$debug_log|$pid_file"
    return 0
}

# Function to capture memory/heap profile asynchronously
capture_memory_profile_async() {
    capture_pprof_profile_async "heap"
}

# Function to capture goroutine profile asynchronously
capture_goroutine_profile_async() {
    capture_pprof_profile_async "goroutine"
}

# Function to capture block profile asynchronously
capture_block_profile_async() {
    capture_pprof_profile_async "block"
}

# Function to capture mutex profile asynchronously
capture_mutex_profile_async() {
    capture_pprof_profile_async "mutex"
}

# Function to capture allocs profile asynchronously
capture_allocs_profile_async() {
    capture_pprof_profile_async "allocs"
}

# Function to capture threadcreate profile asynchronously
capture_threadcreate_profile_async() {
    capture_pprof_profile_async "threadcreate"
}

# Function to capture periodic CPU profiles in 60-second blocks
capture_periodic_cpu_profiles_async() {
    local total_duration=${1:-${PERIODIC_PROFILE_DURATION}}  # Total duration in seconds
    local cpu_block_duration=60  # CPU profile duration per block (60 seconds max)
    
    print_info "Starting periodic CPU profile captures in ${cpu_block_duration}-second blocks for ${total_duration} seconds..." >&2
    
    local start_time=$(date +%s)
    local end_time=$((start_time + total_duration))
    local iteration=0
    local all_pids=()
    
    # Create a log file for periodic CPU captures
    local periodic_cpu_log="${PROFILE_DIR}/.periodic_cpu_profiles_$$.log"
    {
        echo "=== Periodic CPU Profile Capture Log ==="
        echo "Start time: $(date)"
        echo "Total duration: ${total_duration} seconds"
        echo "CPU block duration: ${cpu_block_duration} seconds"
        echo "---"
    } > "$periodic_cpu_log"
    
    while [ $(date +%s) -lt $end_time ]; do
        iteration=$((iteration + 1))
        local current_time=$(date +%s)
        local elapsed=$((current_time - start_time))
        local remaining=$((end_time - current_time))
        
        # Calculate how long this CPU profile should run
        # Use the smaller of: remaining time, cpu_block_duration
        local cpu_duration=$cpu_block_duration
        if [ $remaining -lt $cpu_block_duration ]; then
            cpu_duration=$remaining
        fi
        
        # If remaining time is less than 5 seconds, break (not worth starting a new profile)
        if [ $remaining -lt 5 ]; then
            break
        fi
        
        print_info "CPU Profile Block ${iteration}: Capturing for ${cpu_duration} seconds (${elapsed}s elapsed, ${remaining}s remaining)..." >&2
        
        {
            echo ""
            echo "--- CPU Profile Block ${iteration} at $(date) ---"
            echo "Elapsed: ${elapsed}s, Remaining: ${remaining}s"
            echo "CPU duration: ${cpu_duration}s"
        } >> "$periodic_cpu_log"
        
        # Start CPU profile capture in background
        local output_file="${PROFILE_DIR}/cpu_periodic_${iteration}_$(date +%Y%m%d_%H%M%S).prof"
        local pprof_url="http://localhost:${METRICS_PORT}/debug/pprof/profile?seconds=${cpu_duration}"
        
        (
            go tool pprof -proto -output="$output_file" \
                "$pprof_url" \
                > "${PROFILE_DIR}/.cpu_periodic_${iteration}_$$.log" 2>&1
            local exit_code=$?
            
            {
                echo "  cpu_block_${iteration}: exit_code=$exit_code, file=$output_file, duration=${cpu_duration}s"
                if [ -f "$output_file" ]; then
                    local file_size=$(stat -f%z "$output_file" 2>/dev/null || stat -c%s "$output_file" 2>/dev/null || echo "0")
                    echo "    size=${file_size} bytes"
                fi
            } >> "$periodic_cpu_log"
        ) &
        
        local pid=$!
        all_pids+=("$pid")
        
        print_info "  Started CPU profile block ${iteration} (PID: $pid, duration: ${cpu_duration}s) -> $output_file" >&2
        
        # Wait for the CPU profile to complete (it runs for cpu_duration seconds)
        # This ensures we don't overlap CPU profiles
        wait "$pid" 2>/dev/null || true
    done
    
    print_info "Periodic CPU capture completed. Waiting for any remaining background processes to finish..." >&2
    
    # Wait for any remaining background processes to complete
    local max_wait=30
    local wait_elapsed=0
    
    while [ $wait_elapsed -lt $max_wait ]; do
        local running=0
        for pid in "${all_pids[@]}"; do
            if kill -0 "$pid" 2>/dev/null; then
                running=$((running + 1))
            fi
        done
        
        if [ $running -eq 0 ]; then
            break
        fi
        
        sleep 1
        wait_elapsed=$((wait_elapsed + 1))
    done
    
    {
        echo ""
        echo "--- Summary ---"
        echo "Total CPU profile blocks: ${iteration}"
        echo "End time: $(date)"
    } >> "$periodic_cpu_log"
    
    print_info "Periodic CPU profile capture completed!" >&2
    print_info "Total CPU profile blocks: ${iteration}" >&2
    print_info "Periodic CPU log: $periodic_cpu_log" >&2
    
    echo "$periodic_cpu_log"
    return 0
}

# Function to capture periodic profiles
capture_periodic_profiles_async() {
    local duration=${1:-${PERIODIC_PROFILE_DURATION}}  # Total duration in seconds
    local interval=${2:-${PERIODIC_PROFILE_INTERVAL}}   # Interval between captures in seconds
    local profile_types=("heap" "goroutine" "block" "mutex" "allocs" "threadcreate")
    
    print_info "Starting periodic profile captures every ${interval} seconds for ${duration} seconds..." >&2
    print_info "Profiles: ${profile_types[*]}" >&2
    
    local start_time=$(date +%s)
    local end_time=$((start_time + duration))
    local iteration=0
    local all_pids=()
    
    # Create a log file for periodic captures
    local periodic_log="${PROFILE_DIR}/.periodic_profiles_$$.log"
    {
        echo "=== Periodic Profile Capture Log ==="
        echo "Start time: $(date)"
        echo "Duration: ${duration} seconds"
        echo "Interval: ${interval} seconds"
        echo "Profiles: ${profile_types[*]}"
        echo "---"
    } > "$periodic_log"
    
    while [ $(date +%s) -lt $end_time ]; do
        iteration=$((iteration + 1))
        local current_time=$(date +%s)
        local elapsed=$((current_time - start_time))
        local remaining=$((end_time - current_time))
        
        print_info "Iteration ${iteration}: Capturing profiles (${elapsed}s elapsed, ${remaining}s remaining)..." >&2
        
        {
            echo ""
            echo "--- Iteration ${iteration} at $(date) ---"
            echo "Elapsed: ${elapsed}s, Remaining: ${remaining}s"
        } >> "$periodic_log"
        
        # Capture all profiles asynchronously
        for profile_type in "${profile_types[@]}"; do
            local output_file="${PROFILE_DIR}/${profile_type}_periodic_${iteration}_$(date +%Y%m%d_%H%M%S).prof"
            local pprof_url="http://localhost:${METRICS_PORT}/debug/pprof/${profile_type}"
            local pprof_cmd="go tool pprof -proto -output=\"$output_file\" \"$pprof_url\""
            
            # Start profile capture in background
            (
                go tool pprof -proto -output="$output_file" \
                    "$pprof_url" \
                    > "${PROFILE_DIR}/.${profile_type}_periodic_${iteration}_$$.log" 2>&1
                local exit_code=$?
                
                {
                    echo "  ${profile_type}: exit_code=$exit_code, file=$output_file"
                    if [ -f "$output_file" ]; then
                        local file_size=$(stat -f%z "$output_file" 2>/dev/null || stat -c%s "$output_file" 2>/dev/null || echo "0")
                        echo "    size=${file_size} bytes"
                    fi
                } >> "$periodic_log"
            ) &
            
            local pid=$!
            all_pids+=("$pid")
            
            print_info "  Started ${profile_type} profile (PID: $pid) -> $output_file" >&2
        done
        
        # Wait for interval before next capture (except on last iteration)
        if [ $(date +%s) -lt $end_time ]; then
            sleep "$interval"
        fi
    done
    
    print_info "Periodic capture completed. Waiting for all background processes to finish..." >&2
    
    # Wait for all background processes to complete
    local max_wait=30
    local wait_elapsed=0
    
    while [ $wait_elapsed -lt $max_wait ]; do
        local running=0
        for pid in "${all_pids[@]}"; do
            if kill -0 "$pid" 2>/dev/null; then
                running=$((running + 1))
            fi
        done
        
        if [ $running -eq 0 ]; then
            break
        fi
        
        sleep 1
        wait_elapsed=$((wait_elapsed + 1))
    done
    
    {
        echo ""
        echo "--- Summary ---"
        echo "Total iterations: ${iteration}"
        echo "Total profiles captured: $((iteration * ${#profile_types[@]}))"
        echo "End time: $(date)"
    } >> "$periodic_log"
    
    print_info "Periodic profile capture completed!" >&2
    print_info "Total iterations: ${iteration}" >&2
    print_info "Total profiles: $((iteration * ${#profile_types[@]}))" >&2
    print_info "Periodic log: $periodic_log" >&2
    
    echo "$periodic_log"
    return 0
}

# Function to capture trace profile asynchronously
capture_trace_profile_async() {
    local duration=${1:-${TRACE_PROFILE_DURATION}}
    local output_file="${PROFILE_DIR}/trace_$(date +%Y%m%d_%H%M%S).trace"
    
    print_info "Starting trace profile capture in background for ${duration} seconds..." >&2
    
    if ! command_exists curl; then
        print_error "curl not found. Please install curl to capture trace profiles." >&2
        return 1
    fi
    
    # Build the command for logging
    local trace_url="http://localhost:${METRICS_PORT}/debug/pprof/trace?seconds=${duration}"
    local trace_cmd="curl -s --max-time $((duration + 15)) \"$trace_url\" > \"$output_file\""
    
    # Log to debug file
    local debug_log="${PROFILE_DIR}/.trace_pprof_debug_$$.log"
    local trace_log="${PROFILE_DIR}/.trace_pprof_log_$$"
    local pid_file="${PROFILE_DIR}/.trace_pprof_pid_$$"
    
    {
        echo "=== Trace Profile Capture Debug Log (Async) ==="
        echo "Timestamp: $(date)"
        echo "Command: $trace_cmd"
        echo "Output file: $output_file"
        echo "Profile URL: $trace_url"
        echo "Duration: ${duration} seconds"
        echo "Metrics port: ${METRICS_PORT}"
        echo "Parent PID: $$"
        echo "---"
    } > "$debug_log"
    
    # Log the command for debugging
    print_info "Executing command in background: $trace_cmd" >&2
    print_info "Output file: $output_file" >&2
    print_info "Profile URL: $trace_url" >&2
    print_info "Duration: ${duration} seconds" >&2
    
    # Start trace profile capture in background (non-blocking)
    (
        {
            echo "Background process started at: $(date)"
            echo "PID: $$"
        } >> "$debug_log"
        
        # Verify endpoint is accessible (in background, non-blocking for caller)
        local max_attempts=10
        local attempt=0
        local endpoint_ready=false
        
        {
            echo "Verifying pprof trace endpoint is accessible..."
        } >> "$debug_log"
        
        while [ $attempt -lt $max_attempts ]; do
            if curl -s --max-time 2 "http://localhost:${METRICS_PORT}/debug/pprof/" >/dev/null 2>&1; then
                {
                    echo "pprof endpoint is accessible (attempt $((attempt + 1))/${max_attempts})"
                } >> "$debug_log"
                endpoint_ready=true
                break
            fi
            
            attempt=$((attempt + 1))
            if [ $attempt -lt $max_attempts ]; then
                sleep 2
            fi
        done
        
        if [ "$endpoint_ready" != "true" ]; then
            {
                echo "ERROR: pprof endpoint is not accessible after ${max_attempts} attempts"
                echo "Please check service logs: ${PROFILE_DIR}/telemetry.log"
            } >> "$debug_log"
            echo "ERROR: pprof endpoint not accessible" > "$trace_log"
            exit 1
        fi
        
        # Wait a moment to ensure service is fully ready
        sleep 2
        
        {
            echo "Starting trace profile capture..."
            echo "Command: curl -s --max-time $((duration + 15)) $trace_url > $output_file"
        } >> "$debug_log"
        
        curl -s --max-time $((duration + 15)) \
            "$trace_url" \
            > "$output_file" 2>"$trace_log"
        local curl_exit=$?
        
        # Log completion to debug file
        {
            echo "---"
            echo "Command completed at: $(date)"
            echo "Exit code: $curl_exit"
            if [ -f "$output_file" ]; then
                local file_size=$(stat -f%z "$output_file" 2>/dev/null || stat -c%s "$output_file" 2>/dev/null || echo "0")
                echo "Output file size: ${file_size} bytes"
            else
                echo "Output file: NOT CREATED"
            fi
            if [ $curl_exit -eq 0 ]; then
                echo "Status: SUCCESS"
            else
                echo "Status: FAILED"
                if [ -f "$trace_log" ]; then
                    echo "Error output:"
                    cat "$trace_log" 2>/dev/null || true
                fi
            fi
        } >> "$debug_log"
        
        # Clean up log file on success
        if [ $curl_exit -eq 0 ] && [ -f "$output_file" ] && [ -s "$output_file" ]; then
            rm -f "$trace_log" 2>/dev/null || true
        fi
    ) &
    
    local trace_pid=$!
    
    # Store PID for reference
    echo $trace_pid > "$pid_file"
    
    # Log PID to debug file
    {
        echo "Background process PID: $trace_pid"
        echo "PID file: $pid_file"
    } >> "$debug_log"
    
    print_info "Trace profile capture started in background with PID: $trace_pid" >&2
    print_info "Profile will be saved to: $output_file" >&2
    print_info "Debug log: $debug_log" >&2
    print_info "The profile will run for ${duration} seconds in the background" >&2
    
    # Return the PID and output file for caller to track
    echo "$trace_pid|$output_file|$debug_log|$pid_file"
    return 0
}

# Function to capture cmdline asynchronously
capture_cmdline_async() {
    local output_file="${PROFILE_DIR}/cmdline_$(date +%Y%m%d_%H%M%S).txt"
    
    print_info "Starting cmdline capture in background..." >&2
    
    if ! command_exists curl; then
        print_error "curl not found. Please install curl to capture cmdline." >&2
        return 1
    fi
    
    local cmdline_url="http://localhost:${METRICS_PORT}/debug/pprof/cmdline"
    local debug_log="${PROFILE_DIR}/.cmdline_debug_$$.log"
    local pid_file="${PROFILE_DIR}/.cmdline_pid_$$"
    
    {
        echo "=== Cmdline Capture Debug Log (Async) ==="
        echo "Timestamp: $(date)"
        echo "URL: $cmdline_url"
        echo "Output file: $output_file"
        echo "Parent PID: $$"
        echo "---"
    } > "$debug_log"
    
    (
        {
            echo "Background process started at: $(date)"
            echo "PID: $$"
        } >> "$debug_log"
        
        curl -s "$cmdline_url" > "$output_file" 2>>"$debug_log"
        local curl_exit=$?
        
        {
            echo "---"
            echo "Command completed at: $(date)"
            echo "Exit code: $curl_exit"
            if [ -f "$output_file" ]; then
                local file_size=$(stat -f%z "$output_file" 2>/dev/null || stat -c%s "$output_file" 2>/dev/null || echo "0")
                echo "Output file size: ${file_size} bytes"
            fi
        } >> "$debug_log"
    ) &
    
    local cmdline_pid=$!
    echo $cmdline_pid > "$pid_file"
    
    print_info "Cmdline capture started in background with PID: $cmdline_pid" >&2
    echo "$cmdline_pid|$output_file|$debug_log|$pid_file"
    return 0
}

# Function to capture symbol asynchronously
capture_symbol_async() {
    local output_file="${PROFILE_DIR}/symbol_$(date +%Y%m%d_%H%M%S).txt"
    
    print_info "Starting symbol capture in background..." >&2
    
    if ! command_exists curl; then
        print_error "curl not found. Please install curl to capture symbol." >&2
        return 1
    fi
    
    local symbol_url="http://localhost:${METRICS_PORT}/debug/pprof/symbol"
    local debug_log="${PROFILE_DIR}/.symbol_debug_$$.log"
    local pid_file="${PROFILE_DIR}/.symbol_pid_$$"
    
    {
        echo "=== Symbol Capture Debug Log (Async) ==="
        echo "Timestamp: $(date)"
        echo "URL: $symbol_url"
        echo "Output file: $output_file"
        echo "Parent PID: $$"
        echo "---"
    } > "$debug_log"
    
    (
        {
            echo "Background process started at: $(date)"
            echo "PID: $$"
        } >> "$debug_log"
        
        curl -s "$symbol_url" > "$output_file" 2>>"$debug_log"
        local curl_exit=$?
        
        {
            echo "---"
            echo "Command completed at: $(date)"
            echo "Exit code: $curl_exit"
            if [ -f "$output_file" ]; then
                local file_size=$(stat -f%z "$output_file" 2>/dev/null || stat -c%s "$output_file" 2>/dev/null || echo "0")
                echo "Output file size: ${file_size} bytes"
            fi
        } >> "$debug_log"
    ) &
    
    local symbol_pid=$!
    echo $symbol_pid > "$pid_file"
    
    print_info "Symbol capture started in background with PID: $symbol_pid" >&2
    echo "$symbol_pid|$output_file|$debug_log|$pid_file"
    return 0
}

# Function to wait for any profile to complete
wait_for_profile() {
    local pid=$1
    local profile_name=${2:-"profile"}
    local max_wait=${3:-30}
    local elapsed=0
    local profile_display=$(capitalize_first "$profile_name")
    
    print_info "Waiting for ${profile_name} to complete (PID: $pid, max wait: ${max_wait}s)..."
    
    while [ $elapsed -lt $max_wait ]; do
        if ! kill -0 "$pid" 2>/dev/null; then
            print_info "${profile_display} process completed (PID: $pid)"
            return 0
        fi
        sleep 1
        elapsed=$((elapsed + 1))
        if [ $((elapsed % 5)) -eq 0 ]; then
            print_info "Still waiting... (${elapsed}/${max_wait}s)"
        fi
    done
    
    if kill -0 "$pid" 2>/dev/null; then
        print_warn "${profile_display} process is still running after ${max_wait} seconds"
        return 1
    else
        print_info "${profile_display} process completed"
        return 0
    fi
}

# Function to wait for CPU profile to complete
wait_for_cpu_profile() {
    local pid=$1
    local duration=${2:-${CPU_PROFILE_DURATION}}
    wait_for_profile "$pid" "CPU profile" $((duration + 10))
}

# Function to capture all profiles asynchronously (excluding CPU which is started separately)
capture_all_profiles_async() {
    local cpu_duration=${1:-${CPU_PROFILE_DURATION}}  # Not used, kept for compatibility
    local trace_duration=${2:-${TRACE_PROFILE_DURATION}}
    
    print_info "Starting all profile captures in background (excluding CPU)..." >&2
    print_info "Trace duration: ${trace_duration}s" >&2
    
    local profile_pids=()
    local profile_info_list=()
    local profile_info_files=()
    
    # Note: CPU profile is started separately, so we skip it here
    
    # Start all profiles in background (non-blocking)
    print_info "Starting all profiles in background (non-blocking)..." >&2
    
    # Start memory/heap profile in background
    print_info "Starting memory/heap profile..." >&2
    local heap_info_file="${PROFILE_DIR}/.heap_profile_info_$$.txt"
    capture_memory_profile_async > "$heap_info_file" 2>&1 &
    profile_info_files+=("heap|$heap_info_file")
    
    # Start goroutine profile in background
    print_info "Starting goroutine profile..." >&2
    local goroutine_info_file="${PROFILE_DIR}/.goroutine_profile_info_$$.txt"
    capture_goroutine_profile_async > "$goroutine_info_file" 2>&1 &
    profile_info_files+=("goroutine|$goroutine_info_file")
    
    # Start block profile in background
    print_info "Starting block profile..." >&2
    local block_info_file="${PROFILE_DIR}/.block_profile_info_$$.txt"
    capture_block_profile_async > "$block_info_file" 2>&1 &
    profile_info_files+=("block|$block_info_file")
    
    # Start mutex profile in background
    print_info "Starting mutex profile..." >&2
    local mutex_info_file="${PROFILE_DIR}/.mutex_profile_info_$$.txt"
    capture_mutex_profile_async > "$mutex_info_file" 2>&1 &
    profile_info_files+=("mutex|$mutex_info_file")
    
    # Start allocs profile in background
    print_info "Starting allocs profile..." >&2
    local allocs_info_file="${PROFILE_DIR}/.allocs_profile_info_$$.txt"
    capture_allocs_profile_async > "$allocs_info_file" 2>&1 &
    profile_info_files+=("allocs|$allocs_info_file")
    
    # Start threadcreate profile in background
    print_info "Starting threadcreate profile..." >&2
    local threadcreate_info_file="${PROFILE_DIR}/.threadcreate_profile_info_$$.txt"
    capture_threadcreate_profile_async > "$threadcreate_info_file" 2>&1 &
    profile_info_files+=("threadcreate|$threadcreate_info_file")
    
    # Start trace profile in background
    print_info "Starting trace profile..." >&2
    local trace_info_file="${PROFILE_DIR}/.trace_profile_info_$$.txt"
    capture_trace_profile_async "$trace_duration" > "$trace_info_file" 2>&1 &
    profile_info_files+=("trace|$trace_info_file")
    
    # Start cmdline capture in background
    print_info "Starting cmdline capture..." >&2
    local cmdline_info_file="${PROFILE_DIR}/.cmdline_profile_info_$$.txt"
    capture_cmdline_async > "$cmdline_info_file" 2>&1 &
    profile_info_files+=("cmdline|$cmdline_info_file")
    
    # Start symbol capture in background
    print_info "Starting symbol capture..." >&2
    local symbol_info_file="${PROFILE_DIR}/.symbol_profile_info_$$.txt"
    capture_symbol_async > "$symbol_info_file" 2>&1 &
    profile_info_files+=("symbol|$symbol_info_file")
    
    # Wait briefly for functions to return (they should return immediately)
    sleep 0.2
    
    # Read profile information from files
    for profile_file_info in "${profile_info_files[@]}"; do
        local profile_type=$(echo "$profile_file_info" | cut -d'|' -f1)
        local info_file=$(echo "$profile_file_info" | cut -d'|' -f2)
        
        if [ -f "$info_file" ] && [ -s "$info_file" ]; then
            # Extract the line that contains the pipe-separated info (skip log messages)
            local profile_info=$(cat "$info_file" 2>/dev/null | grep -E "^[0-9]+\|" | head -1)
            if [ -n "$profile_info" ]; then
                profile_info_list+=("${profile_type}|${profile_info}")
                local pid=$(echo "$profile_info" | cut -d'|' -f1)
                if [ -n "$pid" ]; then
                    profile_pids+=("$pid")
                fi
            else
                # Try to get PID from PID file pattern (different patterns for different profile types)
                local pid_file_pattern=""
                if [[ "$profile_type" == "cmdline" ]] || [[ "$profile_type" == "symbol" ]]; then
                    pid_file_pattern="${PROFILE_DIR}/.${profile_type}_pid_$$"
                elif [[ "$profile_type" == "trace" ]]; then
                    pid_file_pattern="${PROFILE_DIR}/.trace_pprof_pid_$$"
                else
                    pid_file_pattern="${PROFILE_DIR}/.${profile_type}_pprof_pid_$$"
                fi
                
                if [ -f "$pid_file_pattern" ]; then
                    local pid=$(cat "$pid_file_pattern" 2>/dev/null)
                    if [ -n "$pid" ]; then
                        # Construct minimal info from PID file
                        local output_file=""
                        local debug_log=""
                        if [[ "$profile_type" == "trace" ]]; then
                            output_file="${PROFILE_DIR}/${profile_type}_$(date +%Y%m%d_%H%M%S).trace"
                            debug_log="${PROFILE_DIR}/.${profile_type}_pprof_debug_$$.log"
                        elif [[ "$profile_type" == "cmdline" ]] || [[ "$profile_type" == "symbol" ]]; then
                            output_file="${PROFILE_DIR}/${profile_type}_$(date +%Y%m%d_%H%M%S).txt"
                            debug_log="${PROFILE_DIR}/.${profile_type}_debug_$$.log"
                        else
                            output_file="${PROFILE_DIR}/${profile_type}_$(date +%Y%m%d_%H%M%S).prof"
                            debug_log="${PROFILE_DIR}/.${profile_type}_pprof_debug_$$.log"
                        fi
                        profile_info_list+=("${profile_type}|${pid}|${output_file}|${debug_log}|${pid_file_pattern}")
                        profile_pids+=("$pid")
                    fi
                fi
            fi
        fi
    done
    
    # Return all profile information (format: type|pid|output_file|debug_log|pid_file)
    for info in "${profile_info_list[@]}"; do
        echo "$info"
    done
    
    print_info "All profiles started in background. Total PIDs: ${#profile_pids[@]}" >&2
    return 0
}

# Main execution
main() {
    # Start telemetry service in background
    start_telemetry_background
    
    # Wait for service to be ready
    if ! wait_for_service 60; then
        print_error "Service did not become ready, but continuing..."
    fi
    
    # Generate metrics for the past hour after service is ready (optional)
    # This step can be skipped if GENERATE_METRICS is set to false
    if [ "${GENERATE_METRICS:-true}" = "true" ]; then
        print_info ""
        print_info "Generating metrics for the past hour..."
        if generate_metrics_for_past_hour; then
            print_info "Metrics generation completed successfully"
        else
            print_warn "Metrics generation failed, but continuing with profiling..."
            print_warn "This is OK - profiling can proceed without generated metrics."
        fi
    else
        print_info ""
        print_info "Skipping metrics generation (GENERATE_METRICS=false)"
    fi
    
    # Start all profile collections in background
    print_info ""
    print_info "Starting all profile collections in background..."
    print_info "Periodic CPU profiles: 60-second blocks for ${PERIODIC_PROFILE_DURATION} seconds total"
    print_info "Trace duration: ${TRACE_PROFILE_DURATION} seconds"
    
    # Start periodic CPU profiles in 60-second blocks
    print_info ""
    print_info "Starting periodic CPU profiles in 60-second blocks for ${PERIODIC_PROFILE_DURATION} seconds..."
    local periodic_cpu_log_file="${PROFILE_DIR}/.periodic_cpu_profiles_log_$$.txt"
    capture_periodic_cpu_profiles_async ${PERIODIC_PROFILE_DURATION} > "$periodic_cpu_log_file" 2>&1 &
    local periodic_cpu_pid=$!
    print_info "Periodic CPU profile capture started in background (PID: $periodic_cpu_pid)"
    print_info "CPU profiles will run in 60-second blocks for ${PERIODIC_PROFILE_DURATION} seconds total"
    
    # Start periodic profile captures (heap, goroutine, block, mutex, allocs, threadcreate)
    print_info ""
    print_info "Starting periodic profile captures every ${PERIODIC_PROFILE_INTERVAL} seconds in background..."
    local periodic_log_file="${PROFILE_DIR}/.periodic_profiles_log_$$.txt"
    capture_periodic_profiles_async ${PERIODIC_PROFILE_DURATION} ${PERIODIC_PROFILE_INTERVAL} > "$periodic_log_file" 2>&1 &
    local periodic_pid=$!
    print_info "Periodic profile capture started in background (PID: $periodic_pid)"
    print_info "Periodic capture will run for ${PERIODIC_PROFILE_DURATION} seconds, capturing profiles every ${PERIODIC_PROFILE_INTERVAL} seconds"
    
    # Start other profile collections (excluding CPU which is already started) in background
    print_info ""
    print_info "Starting other profile collections (excluding CPU) in background..."
    local all_profiles_info_file="${PROFILE_DIR}/.all_profiles_info_$$.txt"
    capture_all_profiles_async ${CPU_PROFILE_DURATION} ${TRACE_PROFILE_DURATION} > "$all_profiles_info_file" 2>&1 &
    local all_profiles_start_pid=$!
    
    # Wait briefly for function to return (should be immediate)
    sleep 0.3
    
    # Parse all profile information from file
    local profile_pids=()
    local profile_types=()
    local profile_outputs=()
    local profile_logs=()
    
    print_info ""
    print_info "All profiles started successfully:"
    print_info ""
    
    # Trigger usage endpoint to generate metrics processing activity
    print_info ""
    print_info "Triggering usage endpoint to generate metrics..."
    trigger_usage_endpoint

    # Read profile information from file
    if [ -f "$all_profiles_info_file" ] && [ -s "$all_profiles_info_file" ]; then
        while IFS='|' read -r profile_type profile_pid output_file debug_log pid_file; do
            if [ -n "$profile_type" ] && [ -n "$profile_pid" ]; then
                # Skip CPU profile as it's already tracked separately
                if [ "$profile_type" != "cpu" ]; then
                    profile_types+=("$profile_type")
                    profile_pids+=("$profile_pid")
                    profile_outputs+=("$output_file")
                    profile_logs+=("$debug_log")
                    
                    print_info "  - ${profile_type}: PID $profile_pid, Output: $output_file"
                fi
            fi
        done < "$all_profiles_info_file"
    else
        print_warn "Profile info file not available yet, will check later"
    fi
    
    print_info ""
    print_info "Total profiles started: ${#profile_pids[@]} (excluding CPU which runs separately)"
    print_info ""
    
    # If we don't have profile info yet, try reading from file again
    if [ ${#profile_pids[@]} -eq 0 ] && [ -f "$all_profiles_info_file" ]; then
        print_info "Reading profile information from file..."
        sleep 0.2
        if [ -f "$all_profiles_info_file" ] && [ -s "$all_profiles_info_file" ]; then
            while IFS='|' read -r profile_type profile_pid output_file debug_log pid_file; do
                if [ -n "$profile_type" ] && [ -n "$profile_pid" ]; then
                    if [ "$profile_type" != "cpu" ]; then
                        profile_types+=("$profile_type")
                        profile_pids+=("$profile_pid")
                        profile_outputs+=("$output_file")
                        profile_logs+=("$debug_log")
                    fi
                fi
            done < "$all_profiles_info_file"
            print_info "Found ${#profile_pids[@]} profiles from file"
        fi
    fi
    
    print_info ""
    print_info "Continuing with other operations while periodic CPU profiles run in background..."
    print_info "Periodic CPU profiles will complete after ${PERIODIC_PROFILE_DURATION} seconds (60-second blocks)"
    print_info ""
    
    # Wait for other profiles to complete (non-blocking for periodic CPU)
    print_info "Waiting for non-CPU profiles to complete..."
    local max_wait=20  # Other profiles should complete quickly
    local elapsed=0
    local completed_count=0
    
    while [ $elapsed -lt $max_wait ] && [ $completed_count -lt ${#profile_pids[@]} ]; do
        completed_count=0
        for i in "${!profile_pids[@]}"; do
            local pid="${profile_pids[$i]}"
            if ! kill -0 "$pid" 2>/dev/null; then
                completed_count=$((completed_count + 1))
            fi
        done
        
        if [ $completed_count -lt ${#profile_pids[@]} ]; then
            sleep 2
            elapsed=$((elapsed + 2))
            if [ $((elapsed % 5)) -eq 0 ]; then
                print_info "Still waiting for other profiles... (${elapsed}/${max_wait}s, ${completed_count}/${#profile_pids[@]} completed)"
            fi
        fi
    done
    
    print_info ""
    print_info "Waiting for periodic CPU profiles to complete (${PERIODIC_PROFILE_DURATION} seconds timeout)..."
    print_info ""
    
    # Wait for periodic CPU profile capture to complete
    if [ -n "$periodic_cpu_pid" ]; then
        if wait_for_profile "$periodic_cpu_pid" "periodic CPU profile capture" ${PERIODIC_PROFILE_DURATION}; then
            print_info "Periodic CPU profile capture completed successfully"
        else
            print_warn "Periodic CPU profile capture may still be running or timed out"
        fi
    else
        print_warn "Periodic CPU profile PID not found, cannot wait for completion"
    fi
    
    print_info ""
    print_info "Profile collection summary:"
    print_info ""
    
    # Check results for each profile
    local success_count=0
    local failed_count=0
    local cpu_periodic_count=0
    
    # Check periodic CPU profiles
    if [ -n "$periodic_cpu_log_file" ] && [ -f "$periodic_cpu_log_file" ]; then
        print_info "Periodic CPU profiles (60-second blocks):"
        print_info "  Log file: $periodic_cpu_log_file"
        
        # Count periodic CPU profile files
        cpu_periodic_count=$(ls -1 "${PROFILE_DIR}"/cpu_periodic_*.prof 2>/dev/null | wc -l | tr -d ' ')
        if [ "$cpu_periodic_count" -gt 0 ]; then
            print_info "  Total CPU profile blocks captured: $cpu_periodic_count"
            print_info "  Profile files: ${PROFILE_DIR}/cpu_periodic_*.prof"
            success_count=$((success_count + cpu_periodic_count))
            
            # Show file sizes
            local total_size=0
            for file in "${PROFILE_DIR}"/cpu_periodic_*.prof; do
                if [ -f "$file" ]; then
                    local file_size=$(stat -f%z "$file" 2>/dev/null || stat -c%s "$file" 2>/dev/null || echo "0")
                    total_size=$((total_size + file_size))
                fi
            done
            print_info "  Total size: $((total_size / 1024 / 1024)) MB"
        else
            print_warn "  No periodic CPU profile files found yet (may still be capturing)"
            failed_count=$((failed_count + 1))
        fi
        print_info ""
    fi
    
    # Check other profiles
    for i in "${!profile_pids[@]}"; do
        local profile_type="${profile_types[$i]}"
        local output_file="${profile_outputs[$i]}"
        local debug_log="${profile_logs[$i]}"
        local pid="${profile_pids[$i]}"
        
        if [ -f "$output_file" ] && [ -s "$output_file" ]; then
            local file_size=$(stat -f%z "$output_file" 2>/dev/null || stat -c%s "$output_file" 2>/dev/null || echo "0")
            print_info "  ✓ ${profile_type}: $output_file (${file_size} bytes)"
            success_count=$((success_count + 1))
        else
            print_warn "  ✗ ${profile_type}: Failed or not ready (check: $debug_log)"
            failed_count=$((failed_count + 1))
        fi
    done
    
    local total_profiles=$((${#profile_pids[@]} + cpu_periodic_count))
    print_info ""
    print_info "Results: ${success_count} succeeded, ${failed_count} failed out of ${total_profiles} total"
    print_info ""
    
    # Wait for periodic capture to complete if still running
    if [ -n "$periodic_pid" ] && kill -0 "$periodic_pid" 2>/dev/null; then
        print_info "Waiting for periodic profile capture to complete..."
        wait_for_profile "$periodic_pid" "periodic profile capture" 60
    fi
    
    # Show periodic profile information
    if [ -n "$periodic_log_file" ] && [ -f "$periodic_log_file" ]; then
        print_info "Periodic profiles (captured every ${PERIODIC_PROFILE_INTERVAL} seconds):"
        print_info "  Log file: $periodic_log_file"
        
        # Count periodic profile files
        local periodic_count=$(ls -1 "${PROFILE_DIR}"/*_periodic_*.prof 2>/dev/null | wc -l | tr -d ' ')
        if [ "$periodic_count" -gt 0 ]; then
            print_info "  Total periodic profiles captured: $periodic_count"
            print_info "  Profile files: ${PROFILE_DIR}/*_periodic_*.prof"
            print_info "  Example: ls -lh ${PROFILE_DIR}/*_periodic_*.prof"
            
            # Show breakdown by profile type
            for profile_type in heap goroutine block mutex allocs threadcreate; do
                local type_count=$(ls -1 "${PROFILE_DIR}/${profile_type}_periodic_"*.prof 2>/dev/null | wc -l | tr -d ' ')
                if [ "$type_count" -gt 0 ]; then
                    print_info "    ${profile_type}: $type_count profiles"
                fi
            done
        else
            print_warn "  No periodic profile files found yet (may still be capturing)"
        fi
        print_info ""
    fi
    
    # Show analysis commands
    print_info "To analyze profiles:"
    
    # Show periodic CPU profile analysis commands
    if [ "$cpu_periodic_count" -gt 0 ]; then
        print_info "  Periodic CPU profiles (${cpu_periodic_count} blocks):"
        print_info "    Individual: go tool pprof ${PROFILE_DIR}/cpu_periodic_1_*.prof"
        print_info "    All: for f in ${PROFILE_DIR}/cpu_periodic_*.prof; do go tool pprof -http=:6060 \"\$f\"; done"
        print_info "    (or: go tool pprof -http=:6060 ${PROFILE_DIR}/cpu_periodic_1_*.prof)"
    fi
    
    # Show other profiles analysis commands
    for i in "${!profile_outputs[@]}"; do
        local profile_type="${profile_types[$i]}"
        local output_file="${profile_outputs[$i]}"
        
        if [ -f "$output_file" ] && [ -s "$output_file" ]; then
            if [[ "$profile_type" == "trace" ]]; then
                print_info "  ${profile_type}: go tool trace $output_file"
            else
                print_info "  ${profile_type}: go tool pprof $output_file"
                print_info "    (or: go tool pprof -http=:6060 $output_file)"
            fi
        fi
    done
    
    print_info ""
    print_info "Profiling complete!"
    
    # Check for any still-running profiles
    local running_pids=()
    
    # Check periodic CPU profile PID
    if [ -n "$periodic_cpu_pid" ] && kill -0 "$periodic_cpu_pid" 2>/dev/null; then
        running_pids+=("$periodic_cpu_pid")
    fi
    
    # Check periodic profile PID
    if [ -n "$periodic_pid" ] && kill -0 "$periodic_pid" 2>/dev/null; then
        running_pids+=("$periodic_pid")
    fi
    
    # Check other profile PIDs
    for pid in "${profile_pids[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            running_pids+=("$pid")
        fi
    done
    
    if [ ${#running_pids[@]} -gt 0 ]; then
        print_info ""
        print_warn "Some profile processes are still running:"
        for pid in "${running_pids[@]}"; do
            print_info "  PID $pid - check debug logs for status"
        done
    fi
    
    # Stop telemetry service at the end
    print_info ""
    print_info "Stopping telemetry service..."
    if stop_telemetry_service; then
        print_info "Telemetry service stopped successfully"
    else
        print_warn "Failed to stop telemetry service, but continuing..."
    fi
    
    print_info ""
    print_info "All profiling tasks completed!"
    
    # Upload profiles to GCS if enabled
    if [ "$UPLOAD_TO_GCS" = "true" ] && [ -n "$GCS_BUCKET" ]; then
        print_info ""
        print_info "Uploading profiles to Google Cloud Storage..."
        if upload_profiles_to_gcs; then
            print_info "Successfully uploaded all profiles to GCS"
        else
            print_warn "Failed to upload profiles to GCS, but continuing..."
        fi
    elif [ "$UPLOAD_TO_GCS" = "true" ] && [ -z "$GCS_BUCKET" ]; then
        print_warn "UPLOAD_TO_GCS is enabled but GCS_BUCKET is not set. Skipping upload."
    fi
}

# Function to upload profiles to Google Cloud Storage
upload_profiles_to_gcs() {
    if [ -z "$GCS_BUCKET" ]; then
        print_error "GCS_BUCKET environment variable is required for upload"
        return 1
    fi
    
    if [ ! -d "$PROFILE_DIR" ]; then
        print_error "Profile directory does not exist: $PROFILE_DIR"
        return 1
    fi
    
    # Check if there are any files to upload (excluding dot files)
    local file_count=$(find "$PROFILE_DIR" -maxdepth 1 -type f ! -name ".*" \( -name "*.prof" -o -name "*.trace" -o -name "*.txt" -o -name "*.log" \) 2>/dev/null | wc -l | tr -d ' ')
    if [ "$file_count" -eq 0 ]; then
        print_warn "No profile files found in $PROFILE_DIR to upload"
        return 0
    fi
    
    print_info "Found $file_count profile files to upload"
    
    # Check for gsutil or use Go client
    if command_exists gsutil; then
        upload_with_gsutil
    elif command_exists go; then
        print_warn "gsutil not found, attempting to use Go client..."
        upload_with_go_client
    else
        print_error "Neither gsutil nor Go client available for upload"
        return 1
    fi
}

# Function to upload using gsutil
upload_with_gsutil() {
    # Authenticate if needed (for service account key)
    if [ -n "$GOOGLE_APPLICATION_CREDENTIALS" ] && [ -f "$GOOGLE_APPLICATION_CREDENTIALS" ]; then
        print_info "Using service account credentials from: $GOOGLE_APPLICATION_CREDENTIALS"
        export GOOGLE_APPLICATION_CREDENTIALS
        gcloud auth activate-service-account --key-file="$GOOGLE_APPLICATION_CREDENTIALS" 2>/dev/null || true
    fi
    
    # Create timestamped directory in GCS
    local timestamp=$(date +%Y%m%d_%H%M%S)
    local gcs_path="gs://${GCS_BUCKET}/${GCS_PREFIX}${timestamp}/"
    
    print_info "Uploading profiles from: $PROFILE_DIR"
    print_info "Destination: $gcs_path"
    
    # Upload all profile files (prof, trace, txt, log)
    local upload_count=0
    local failed_count=0
    
    # Upload profile files (excluding periodic ones and dot files)
    # Use find to avoid shell expansion issues
    while IFS= read -r file; do
        if [ -f "$file" ]; then
            local filename=$(basename "$file")
            # Skip files starting with "." (dot files)
            if [[ "$filename" == .* ]]; then
                continue
            fi
            # Skip periodic profiles (handled separately)
            if [[ "$filename" == *_periodic_* ]]; then
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
    done < <(find "$PROFILE_DIR" -maxdepth 1 -type f ! -name ".*" \( -name "*.prof" -o -name "*.trace" -o -name "*.txt" -o -name "*.log" \) 2>/dev/null)
    
    # Upload periodic profiles if they exist (in a subdirectory)
    if ls "$PROFILE_DIR"/*_periodic_*.prof 1>/dev/null 2>&1; then
        print_info "Uploading periodic profiles..."
        for file in "$PROFILE_DIR"/*_periodic_*.prof; do
            if [ -f "$file" ]; then
                local filename=$(basename "$file")
                if gsutil cp "$file" "${gcs_path}periodic/${filename}" 2>/dev/null; then
                    upload_count=$((upload_count + 1))
                    print_info "  ✓ Uploaded periodic: $filename"
                else
                    failed_count=$((failed_count + 1))
                    print_warn "  ✗ Failed periodic: $filename"
                fi
            fi
        done
    fi
    
    if [ $upload_count -gt 0 ]; then
        print_info "Successfully uploaded $upload_count files to: $gcs_path"
        if [ $failed_count -gt 0 ]; then
            print_warn "$failed_count files failed to upload"
        fi
        
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

# Function to upload using Go client (fallback)
upload_with_go_client() {
    print_warn "Go client upload not yet implemented, please install gsutil"
    return 1
}

# Run main function
main

