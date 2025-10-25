#!/bin/bash

# VSA Control Plane Intelligent Documentation Generator
# CI/CD-ready script for automated documentation generation

set -euo pipefail  # Strict error handling for CI/CD

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
EXIT_CODE=0

# CI/CD friendly logging
log_info() { echo "[INFO] $1"; }
log_warn() { echo "[WARN] $1"; }
log_error() { echo "[ERROR] $1" >&2; }
log_success() { echo "[SUCCESS] $1"; }

log_info "VSA Control Plane Documentation Generator (AI-Enhanced)"
log_info "Repository: $REPO_ROOT"
log_info "Generator: $SCRIPT_DIR"
log_info "Mode: AI-First (GitHub Copilot preferred)"

# Validate repository structure
if [[ ! -f "$REPO_ROOT/go.mod" ]]; then
    log_error "Not in VSA Control Plane repository root"
    log_error "Expected go.mod file in: $REPO_ROOT"
    exit 1
fi

log_success "Repository structure validated"

# Validate required API specification files
CORE_API="$REPO_ROOT/core/core-api/api.yaml"
GOOGLE_PROXY="$REPO_ROOT/google-proxy/api/gcp-api.yaml" 
TELEMETRY_API="$REPO_ROOT/telemetry/api/telemetry-api.yaml"

log_info "Validating API specifications..."
api_count=0

if [[ -f "$CORE_API" ]]; then
    log_success "Core API found: $(basename "$CORE_API")"
    ((api_count++))
else
    log_warn "Core API not found: $CORE_API"
fi

if [[ -f "$GOOGLE_PROXY" ]]; then
    log_success "Google Proxy API found: $(basename "$GOOGLE_PROXY")"
    ((api_count++))
else
    log_warn "Google Proxy API not found: $GOOGLE_PROXY" 
fi

if [[ -f "$TELEMETRY_API" ]]; then
    log_success "Telemetry API found: $(basename "$TELEMETRY_API")"
    ((api_count++))
else
    log_warn "Telemetry API not found: $TELEMETRY_API"
fi

if [[ $api_count -eq 0 ]]; then
    log_error "No API specifications found - cannot generate documentation"
    exit 1
fi

log_info "Found $api_count API specifications"

# Validate data model directory
DATA_MODEL_DIR="$REPO_ROOT/core/datamodel"
if [[ -d "$DATA_MODEL_DIR" ]]; then
    model_files=$(find "$DATA_MODEL_DIR" -name "*.go" | wc -l)
    log_success "Data Models: $model_files Go files found"
else
    log_warn "Data Models directory not found: $DATA_MODEL_DIR"
fi

# Validate Python environment
if ! command -v python3 &> /dev/null; then
    log_error "python3 is required but not installed"
    exit 1
fi

log_success "Python environment validated"

# Check for GitHub Copilot (AI mode - preferred)
log_info "Checking for GitHub Copilot (AI-enhanced mode)..."
if command -v gh &> /dev/null; then
    if gh extension list | grep -q "github/gh-copilot"; then
        log_success "GitHub Copilot CLI detected - AI mode enabled!"
        log_info "Documentation will be generated using AI-enhanced analysis"
    else
        log_warn "GitHub Copilot CLI not installed"
        log_warn "Install with: gh extension install github/gh-copilot"
        log_warn "Falling back to template-based generation"
    fi
else
    log_warn "GitHub CLI not found"
    log_warn "Install from: https://cli.github.com/"
    log_warn "Then install Copilot: gh extension install github/gh-copilot"
    log_warn "Falling back to template-based generation"
fi

# Prepare output directory (auto-generated docs, not manual designs)
OUTPUT_DIR="$REPO_ROOT/doc/architecture/auto-gen-designs-docs"
mkdir -p "$OUTPUT_DIR"
log_info "Output directory: $OUTPUT_DIR"
log_info "Manual designs preserved in: doc/architecture/designs"

# Execute documentation generation
log_info "Starting documentation generation pipeline..."

cd "$SCRIPT_DIR"

log_info "Phase 1: Analyzing existing documentation..."
if ! python3 smart_doc_analyzer.py; then
    log_error "Documentation analysis failed"
    EXIT_CODE=1
fi

log_info "Phase 2: Generating/updating documentation..."
if ! python3 generate_smart_docs.py; then
    log_error "Documentation generation failed"
    EXIT_CODE=1
fi

# Report results
if [[ $EXIT_CODE -eq 0 ]]; then
    log_success "Documentation generation completed successfully!"
    
    # Generate summary report
    total_files=$(find "$OUTPUT_DIR" -name "*.md" | wc -l)
    recent_files=$(find "$OUTPUT_DIR" -name "*.md" -mmin -5 | wc -l)
    
    log_info "Documentation Summary:"
    log_info "  Total design documents: $total_files"
    log_info "  Recently updated: $recent_files documents"
    
    if [[ $recent_files -gt 0 ]]; then
        log_info "Recent updates:"
        find "$OUTPUT_DIR" -name "*.md" -mmin -5 -exec basename {} \; | head -5 | while read file; do
            log_info "    • $file"
        done
    fi
    
    # Validation checks
    pool_doc=$(find "$OUTPUT_DIR" -name "*pool*" -name "*.md" | head -1)
    if [[ -n "$pool_doc" ]]; then
        log_success "Pool documentation verified: $(basename "$pool_doc")"
    fi
    
    kms_doc=$(find "$OUTPUT_DIR" -name "*kms*" -name "*.md" | head -1)
    if [[ -n "$kms_doc" ]]; then
        log_success "KMS documentation verified: $(basename "$kms_doc")"
    fi
    
    log_success "Documentation available in: $OUTPUT_DIR"
    
else
    log_error "Documentation generation failed!"
    log_error "Check the error messages above for details."
fi

exit $EXIT_CODE