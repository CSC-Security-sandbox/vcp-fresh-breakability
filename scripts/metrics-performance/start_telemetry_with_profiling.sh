#!/bin/bash

# Script to start telemetry service with performance profiling enabled
# This script enables pprof endpoints and provides tools to capture performance data
#
# ============================================================================
# USAGE INSTRUCTIONS
# ============================================================================
#
# STEP 1: Prerequisites
#   - Ensure Go is installed and available in PATH
#   - Ensure you have the required environment file: tel_local.env
#   - Ensure port 9090 (or METRICS_PORT) is not already in use
#
# STEP 2: Make the script executable (if not already)
#   chmod +x scripts/start_telemetry_with_profiling.sh
#
# STEP 3: Start the telemetry service
#   ./scripts/start_telemetry_with_profiling.sh start
#   OR simply:
#   ./scripts/start_telemetry_with_profiling.sh
#
#   The service will:
#   - Build the telemetry binary if it doesn't exist
#   - Enable pprof endpoints at http://localhost:9090/debug/pprof/
#   - Enable mock mode for Google metrics (MOCK_GOOGLE_METRICS=true)
#   - Start the service and log output to profiles/telemetry.log
#
# STEP 4: (Optional) Capture performance profiles in another terminal
#   
#   CPU Profile (30 seconds):
#   ./scripts/start_telemetry_with_profiling.sh cpu 30
#
#   Memory Profile:
#   ./scripts/start_telemetry_with_profiling.sh memory
#
#   All Profiles (CPU, Memory, Goroutine, Block, Mutex, Allocs, Threadcreate, Trace, Cmdline, Symbol):
#   ./scripts/start_telemetry_with_profiling.sh all 30
#
#   Open pprof Web UI:
#   ./scripts/start_telemetry_with_profiling.sh ui heap
#
# STEP 5: Analyze profiles
#   Profiles are saved to: profiles/
#   Analyze with: go tool pprof profiles/cpu_YYYYMMDD_HHMMSS.prof
#   Or use web UI: go tool pprof -http=:6060 profiles/cpu_YYYYMMDD_HHMMSS.prof
#
# ============================================================================
# QUICK START EXAMPLE
# ============================================================================
#
# Terminal 1 - Start the service:
#   $ ./scripts/start_telemetry_with_profiling.sh start
#
# Terminal 2 - Capture profiles while service is running:
#   $ ./scripts/start_telemetry_with_profiling.sh cpu 60
#   $ ./scripts/start_telemetry_with_profiling.sh memory
#   $ ./scripts/start_telemetry_with_profiling.sh allocs
#   $ ./scripts/start_telemetry_with_profiling.sh trace 10
#   $ ./scripts/start_telemetry_with_profiling.sh all 30
#
# Terminal 3 - View profiles in web UI:
#   $ ./scripts/start_telemetry_with_profiling.sh ui heap
#   # Then open http://localhost:6060 in your browser
#
# ============================================================================
# ENVIRONMENT VARIABLES
# ============================================================================
#
#   PPROF_PORT      - Port for pprof web UI (default: 6060)
#   METRICS_PORT    - Port for telemetry service (default: 9090)
#   ENABLE_PPROF    - Automatically set to 'true' by this script
#   MOCK_GOOGLE_METRICS - Automatically set to 'true' by this script
#
# ============================================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PROFILE_DIR="${PROJECT_ROOT}/profiles"
TELEMETRY_BINARY="${PROJECT_ROOT}/app/telemetry"
ENV_FILE="${PROJECT_ROOT}/tel_local.env"
PPROF_PORT="${PPROF_PORT:-6060}"
METRICS_PORT="${METRICS_PORT:-9090}"

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
    if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1 ; then
        return 0
    else
        return 1
    fi
}

# Function to build the telemetry binary
build_telemetry() {
    print_info "Building telemetry service with debug information..."
    cd "$PROJECT_ROOT"
    # Build with debug symbols and full path information for proper profiling
    # -trimpath=false: Preserves full file paths in debug info (needed for pprof)
    # -ldflags: Explicitly ensure debug symbols are NOT stripped
    #   -s: Strip symbol table and debug info (we DON'T use this)
    #   -w: Omit DWARF symbol table (we DON'T use this)
    # -gcflags="all=-N -l": Disable optimizations and inlining for better symbol resolution
    #   Note: This makes profiling more accurate for function names but may affect performance
    if ! go build -trimpath=false -gcflags="all=-N -l" -o "$TELEMETRY_BINARY" ./telemetry/main.go; then
        print_error "Failed to build telemetry service"
        exit 1
    fi
    print_info "Telemetry service built successfully with debug information"
    print_info "Debug symbols enabled with full paths for better profiling and debugging"
    print_info "Function names should now be visible in pprof profiles"
}

# Function to start the telemetry service
start_telemetry() {
    print_info "Starting telemetry service with profiling enabled..."
    
    # Check if binary exists
    if [ ! -f "$TELEMETRY_BINARY" ]; then
        print_warn "Telemetry binary not found, building..."
        build_telemetry
    fi
    
    # Check if port is already in use
    if check_port "$METRICS_PORT"; then
        print_error "Port $METRICS_PORT is already in use. Please stop the existing service first."
        exit 1
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
    
    # Start the service
    print_info "Starting telemetry service on port $METRICS_PORT"
    print_info "pprof endpoints available at: http://localhost:$METRICS_PORT/debug/pprof/"
    print_info "Prometheus metrics available at: http://localhost:$METRICS_PORT/metrics"
    if [ "${MOCK_GOOGLE_METRICS:-false}" = "true" ]; then
        print_info "MOCK MODE: Google metrics will NOT be sent to Google (MOCK_GOOGLE_METRICS=true)"
    fi
    print_info ""
    print_info "Press Ctrl+C to stop the service"
    print_info ""
    
    cd "$PROJECT_ROOT"
    "$TELEMETRY_BINARY" 2>&1 | tee "${PROFILE_DIR}/telemetry.log"
}

# Function to capture CPU profile
capture_cpu_profile() {
    local duration=${1:-30}
    local output_file="${PROFILE_DIR}/cpu_$(date +%Y%m%d_%H%M%S).prof"
    
    print_info "Capturing CPU profile for ${duration} seconds..."
    print_info "This will take ${duration} seconds - please wait..."
    
    if ! command_exists go; then
        print_error "go tool not found. Please install Go to use profiling features."
        return 1
    fi
    
    # Verify endpoint is accessible first
    print_info "Verifying pprof endpoint is accessible at http://localhost:${METRICS_PORT}/debug/pprof/..."
    
    # Try to access the endpoint using go tool pprof
    if ! go tool pprof -proto -output=/dev/null "http://localhost:${METRICS_PORT}/debug/pprof/" >/dev/null 2>&1; then
        print_error "pprof endpoint is not accessible at http://localhost:${METRICS_PORT}/debug/pprof/"
        print_error "Please ensure:"
        print_error "  1. The telemetry service is running"
        print_error "  2. pprof is enabled (ENABLE_PPROF=true)"
        print_error "  3. The service is listening on port ${METRICS_PORT}"
        print_error "  4. Check service logs for errors"
        return 1
    fi
    
    print_info "pprof endpoint is accessible"
    
    # Wait a moment to ensure service is fully ready
    print_info "Waiting 2 seconds to ensure service is fully ready..."
    sleep 2
    
    # Use go tool pprof to capture CPU profile
    print_info "Starting CPU profile capture (this will block for ${duration} seconds)..."
    print_info "Using: go tool pprof -proto -output=$output_file http://localhost:${METRICS_PORT}/debug/pprof/profile?seconds=${duration}"
    
    # Capture CPU profile using go tool pprof
    local pprof_exit=0
    go tool pprof -proto -output="$output_file" \
        "http://localhost:${METRICS_PORT}/debug/pprof/profile?seconds=${duration}" \
        > "${PROFILE_DIR}/.cpu_pprof_log_$$" 2>&1
    pprof_exit=$?
    
    # Wait a moment for file to be written
    sleep 1
    
    # Check if file was created and has content
    if [ -f "$output_file" ] && [ -s "$output_file" ]; then
        local file_size=$(stat -f%z "$output_file" 2>/dev/null || stat -c%s "$output_file" 2>/dev/null || echo "0")
        
        if [ "$file_size" -gt 1024 ]; then
            print_info "CPU profile saved to: $output_file (${file_size} bytes)"
            print_info "To analyze: go tool pprof $output_file"
            rm -f "${PROFILE_DIR}/.cpu_pprof_log_$$" 2>/dev/null || true
            return 0
        elif [ "$file_size" -gt 100 ]; then
            print_warn "CPU profile file is small (${file_size} bytes) but appears valid"
            print_info "CPU profile saved to: $output_file"
            rm -f "${PROFILE_DIR}/.cpu_pprof_log_$$" 2>/dev/null || true
            return 0
        else
            print_error "CPU profile file is too small (${file_size} bytes), may be invalid"
            if [ -f "${PROFILE_DIR}/.cpu_pprof_log_$$" ]; then
                print_error "pprof output:"
                cat "${PROFILE_DIR}/.cpu_pprof_log_$$" 2>/dev/null || true
            fi
            return 1
        fi
    else
        print_error "Failed to capture CPU profile (pprof exit: $pprof_exit)"
        if [ -f "${PROFILE_DIR}/.cpu_pprof_log_$$" ]; then
            print_error "pprof output:"
            cat "${PROFILE_DIR}/.cpu_pprof_log_$$" 2>/dev/null || true
        fi
        if [ -f "$output_file" ]; then
            local file_size=$(stat -f%z "$output_file" 2>/dev/null || stat -c%s "$output_file" 2>/dev/null || echo "0")
            print_error "File exists but may be empty or invalid (${file_size} bytes)"
        fi
        rm -f "${PROFILE_DIR}/.cpu_pprof_log_$$" 2>/dev/null || true
        return 1
    fi
}

# Function to capture memory profile
capture_memory_profile() {
    local output_file="${PROFILE_DIR}/memory_$(date +%Y%m%d_%H%M%S).prof"
    
    print_info "Capturing memory profile..."
    
    if ! command_exists go; then
        print_error "go tool not found. Please install Go to use profiling features."
        return 1
    fi
    
    if go tool pprof -proto -output="$output_file" \
        "http://localhost:${METRICS_PORT}/debug/pprof/heap" >/dev/null 2>&1; then
        local file_size=$(stat -f%z "$output_file" 2>/dev/null || stat -c%s "$output_file" 2>/dev/null || echo "0")
        print_info "Memory profile saved to: $output_file (${file_size} bytes)"
        print_info "To analyze: go tool pprof $output_file"
        return 0
    else
        print_error "Failed to capture memory profile"
        return 1
    fi
}

# Function to capture goroutine profile
capture_goroutine_profile() {
    local output_file="${PROFILE_DIR}/goroutine_$(date +%Y%m%d_%H%M%S).prof"
    
    print_info "Capturing goroutine profile..."
    
    if ! command_exists go; then
        print_error "go tool not found. Please install Go to use profiling features."
        return 1
    fi
    
    if go tool pprof -proto -output="$output_file" \
        "http://localhost:${METRICS_PORT}/debug/pprof/goroutine" >/dev/null 2>&1; then
        local file_size=$(stat -f%z "$output_file" 2>/dev/null || stat -c%s "$output_file" 2>/dev/null || echo "0")
        print_info "Goroutine profile saved to: $output_file (${file_size} bytes)"
        print_info "To analyze: go tool pprof $output_file"
        return 0
    else
        print_error "Failed to capture goroutine profile"
        return 1
    fi
}

# Function to capture block profile
capture_block_profile() {
    local output_file="${PROFILE_DIR}/block_$(date +%Y%m%d_%H%M%S).prof"
    
    print_info "Capturing block profile..."
    
    if ! command_exists go; then
        print_error "go tool not found. Please install Go to use profiling features."
        return 1
    fi
    
    if go tool pprof -proto -output="$output_file" \
        "http://localhost:${METRICS_PORT}/debug/pprof/block" >/dev/null 2>&1; then
        local file_size=$(stat -f%z "$output_file" 2>/dev/null || stat -c%s "$output_file" 2>/dev/null || echo "0")
        print_info "Block profile saved to: $output_file (${file_size} bytes)"
        print_info "To analyze: go tool pprof $output_file"
        return 0
    else
        print_error "Failed to capture block profile"
        return 1
    fi
}

# Function to capture mutex profile
capture_mutex_profile() {
    local output_file="${PROFILE_DIR}/mutex_$(date +%Y%m%d_%H%M%S).prof"
    
    print_info "Capturing mutex profile..."
    
    if ! command_exists go; then
        print_error "go tool not found. Please install Go to use profiling features."
        return 1
    fi
    
    if go tool pprof -proto -output="$output_file" \
        "http://localhost:${METRICS_PORT}/debug/pprof/mutex" >/dev/null 2>&1; then
        local file_size=$(stat -f%z "$output_file" 2>/dev/null || stat -c%s "$output_file" 2>/dev/null || echo "0")
        print_info "Mutex profile saved to: $output_file (${file_size} bytes)"
        print_info "To analyze: go tool pprof $output_file"
        return 0
    else
        print_error "Failed to capture mutex profile"
        return 1
    fi
}

# Function to capture allocs profile (memory allocations)
capture_allocs_profile() {
    local output_file="${PROFILE_DIR}/allocs_$(date +%Y%m%d_%H%M%S).prof"
    
    print_info "Capturing allocs profile (memory allocations)..."
    
    if ! command_exists go; then
        print_error "go tool not found. Please install Go to use profiling features."
        return 1
    fi
    
    if go tool pprof -proto -output="$output_file" \
        "http://localhost:${METRICS_PORT}/debug/pprof/allocs" >/dev/null 2>&1; then
        local file_size=$(stat -f%z "$output_file" 2>/dev/null || stat -c%s "$output_file" 2>/dev/null || echo "0")
        print_info "Allocs profile saved to: $output_file (${file_size} bytes)"
        print_info "To analyze: go tool pprof $output_file"
        return 0
    else
        print_error "Failed to capture allocs profile"
        return 1
    fi
}

# Function to capture threadcreate profile
capture_threadcreate_profile() {
    local output_file="${PROFILE_DIR}/threadcreate_$(date +%Y%m%d_%H%M%S).prof"
    
    print_info "Capturing threadcreate profile..."
    
    if ! command_exists go; then
        print_error "go tool not found. Please install Go to use profiling features."
        return 1
    fi
    
    if go tool pprof -proto -output="$output_file" \
        "http://localhost:${METRICS_PORT}/debug/pprof/threadcreate" >/dev/null 2>&1; then
        local file_size=$(stat -f%z "$output_file" 2>/dev/null || stat -c%s "$output_file" 2>/dev/null || echo "0")
        print_info "Threadcreate profile saved to: $output_file (${file_size} bytes)"
        print_info "To analyze: go tool pprof $output_file"
        return 0
    else
        print_error "Failed to capture threadcreate profile"
        return 1
    fi
}

# Function to capture trace profile (execution trace)
capture_trace_profile() {
    local duration=${1:-5}
    local output_file="${PROFILE_DIR}/trace_$(date +%Y%m%d_%H%M%S).trace"
    
    print_info "Capturing trace profile for ${duration} seconds..."
    print_info "This will take ${duration} seconds - please wait..."
    
    if ! command_exists go; then
        print_error "go tool not found. Please install Go to use profiling features."
        return 1
    fi
    
    # Use go tool trace to capture trace profile
    # Note: go tool trace doesn't have a direct -output flag, so we use curl for trace
    # Trace format is different from pprof profiles
    print_info "Capturing trace profile using curl (trace format requires direct HTTP download)..."
    
    local curl_exit=0
    curl -s --max-time $((duration + 15)) \
        "http://localhost:${METRICS_PORT}/debug/pprof/trace?seconds=${duration}" \
        > "$output_file" 2>&1
    curl_exit=$?
    
    # Wait a moment for file to be written
    sleep 1
    
    # Check if file was created and has content
    if [ $curl_exit -eq 0 ] && [ -f "$output_file" ] && [ -s "$output_file" ]; then
        local file_size=$(stat -f%z "$output_file" 2>/dev/null || stat -c%s "$output_file" 2>/dev/null || echo "0")
        if [ "$file_size" -gt 100 ]; then
            print_info "Trace profile saved to: $output_file (${file_size} bytes)"
            print_info "To analyze: go tool trace $output_file"
            return 0
        else
            print_error "Trace profile file is too small (${file_size} bytes), may be invalid"
            return 1
        fi
    else
        print_error "Failed to capture trace profile (curl exit: $curl_exit)"
        if [ -f "$output_file" ]; then
            print_error "File exists but may be empty or invalid"
        fi
        return 1
    fi
}

# Function to capture cmdline
capture_cmdline() {
    local output_file="${PROFILE_DIR}/cmdline_$(date +%Y%m%d_%H%M%S).txt"
    
    print_info "Capturing cmdline..."
    
    curl -s "http://localhost:${METRICS_PORT}/debug/pprof/cmdline" > "$output_file"
    
    if [ $? -eq 0 ]; then
        print_info "Cmdline saved to: $output_file"
        return 0
    else
        print_error "Failed to capture cmdline"
        return 1
    fi
}

# Function to capture symbol
capture_symbol() {
    local output_file="${PROFILE_DIR}/symbol_$(date +%Y%m%d_%H%M%S).txt"
    
    print_info "Capturing symbol..."
    
    curl -s "http://localhost:${METRICS_PORT}/debug/pprof/symbol" > "$output_file"
    
    if [ $? -eq 0 ]; then
        print_info "Symbol saved to: $output_file"
        return 0
    else
        print_error "Failed to capture symbol"
        return 1
    fi
}

# Function to capture all profiles
capture_all_profiles() {
    local duration=${1:-30}
    local trace_duration=${2:-5}
    
    print_info "Capturing all pprof profiles..."
    print_info "CPU profile will take ${duration} seconds - this will block..."
    
    # Capture CPU profile first (this will block for the duration)
    if capture_cpu_profile "$duration"; then
        print_info "CPU profile captured successfully"
    else
        print_warn "CPU profile capture failed, but continuing with other profiles..."
    fi
    
    # Capture other profiles (these are quick snapshots)
    capture_memory_profile
    capture_goroutine_profile
    capture_block_profile
    capture_mutex_profile
    capture_allocs_profile
    capture_threadcreate_profile
    
    # Capture trace profile (this will also block for trace_duration)
    print_info "Trace profile will take ${trace_duration} seconds - this will block..."
    if capture_trace_profile "$trace_duration"; then
        print_info "Trace profile captured successfully"
    else
        print_warn "Trace profile capture failed, but continuing..."
    fi
    
    capture_cmdline
    capture_symbol
    print_info "All profiles captured and saved to: $PROFILE_DIR"
}

# Function to open pprof web UI
open_pprof_ui() {
    local profile_type=${1:-heap}
    
    if ! command_exists go; then
        print_error "go tool not found. Please install Go to use profiling features."
        return 1
    fi
    
    print_info "Opening pprof web UI for $profile_type profile..."
    print_info "Access the UI at: http://localhost:6060"
    
    go tool pprof -http=:6060 "http://localhost:${METRICS_PORT}/debug/pprof/${profile_type}"
}

# Function to show help
show_help() {
    cat << EOF
Usage: $0 [COMMAND] [OPTIONS]

Commands:
    start                   Start the telemetry service with profiling enabled
    build                   Build the telemetry service binary
    cpu [duration]          Capture CPU profile (default: 30 seconds)
    memory                  Capture memory profile (heap)
    goroutine               Capture goroutine profile
    block                   Capture block profile
    mutex                   Capture mutex profile
    allocs                  Capture memory allocations profile
    threadcreate            Capture thread creation profile
    trace [duration]        Capture execution trace (default: 5 seconds)
    cmdline                 Capture command line arguments
    symbol                  Capture symbol lookup
    all [duration]          Capture all profiles (default: 30 seconds)
    ui [profile_type]       Open pprof web UI (default: heap)
    help                    Show this help message

Examples:
    $0 start                                    # Start the service
    $0 cpu 60                                   # Capture 60-second CPU profile
    $0 memory                                   # Capture memory (heap) profile
    $0 allocs                                   # Capture memory allocations profile
    $0 trace 10                                 # Capture 10-second execution trace
    $0 all 30                                   # Capture all profiles for 30 seconds
    $0 ui heap                                  # Open heap profile in web UI
    $0 ui cpu                                   # Open CPU profile in web UI

Environment Variables:
    PPROF_PORT              Port for pprof web UI (default: 6060)
    METRICS_PORT            Port for telemetry service (default: 9090)
    ENABLE_PPROF            Enable pprof endpoints (automatically set to true)

Profiles are saved to: $PROFILE_DIR

EOF
}

# Main script logic
case "${1:-start}" in
    start)
        start_telemetry
        ;;
    build)
        build_telemetry
        ;;
    cpu)
        capture_cpu_profile "${2:-30}"
        ;;
    memory)
        capture_memory_profile
        ;;
    goroutine)
        capture_goroutine_profile
        ;;
    block)
        capture_block_profile
        ;;
    mutex)
        capture_mutex_profile
        ;;
    allocs)
        capture_allocs_profile
        ;;
    threadcreate)
        capture_threadcreate_profile
        ;;
    trace)
        capture_trace_profile "${2:-5}"
        ;;
    cmdline)
        capture_cmdline
        ;;
    symbol)
        capture_symbol
        ;;
    all)
        capture_all_profiles "${2:-30}" "${3:-5}"
        ;;
    ui)
        open_pprof_ui "${2:-heap}"
        ;;
    help|--help|-h)
        show_help
        ;;
    *)
        print_error "Unknown command: $1"
        show_help
        exit 1
        ;;
esac

