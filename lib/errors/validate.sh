#!/bin/bash

# Error Framework Validation Script
# Checks for duplicate error codes and missing mappings

set -e

# Check for status-only flag
if [ "$1" = "--status-only" ]; then
    # Get the directory where this script is located
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    cd "$SCRIPT_DIR"
    
    # Quick status check
    total_constants=$(grep -c "= [0-9]" errors.go 2>/dev/null || echo "0")
    total_mappings=$(jq 'length' errors.json 2>/dev/null || echo "0")
    duplicate_go_count=$(grep -n "= [0-9]" errors.go 2>/dev/null | grep -E "(Err|Error)" | cut -d'=' -f2 | tr -d ' ' | sort | uniq -d | wc -l)
    
    echo "Total error constants: $total_constants"
    echo "Total error mappings: $total_mappings"
    echo "Duplicate codes (Go): $duplicate_go_count"
    exit 0
fi

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "🔍 Error Framework Validation"
echo "============================"

# Check if we're in the right directory
if [ ! -f "errors.go" ]; then
    echo "❌ Error: errors.go not found in current directory"
    echo "Current directory: $(pwd)"
    exit 1
fi

if [ ! -f "errors.json" ]; then
    echo "❌ Error: errors.json not found in current directory"
    exit 1
fi

# Initialize validation status
validation_passed=true
duplicate_codes_go=""
duplicate_codes_json=""
missing_mappings=""
json_syntax_valid=true

# Single pass through errors.go to extract all error constants efficiently
echo ""
echo "Analyzing error constants..."
error_data=$(awk '/^[[:space:]]*Err[[:alnum:]]*[[:space:]]*=[[:space:]]*[0-9]+/ {
    gsub(/[[:space:]]/, "", $0)
    split($0, parts, "=")
    if (parts[2] ~ /^[0-9]+$/) {
        print parts[1] "=" parts[2]
    }
}' errors.go)

# Extract codes and check for duplicates in errors.go
echo "Checking for duplicate error codes in errors.go..."
codes=$(echo "$error_data" | cut -d'=' -f2 | sort)
duplicates_go=$(echo "$codes" | uniq -d)

if [ ! -z "$duplicates_go" ]; then
    duplicate_codes_go="$duplicates_go"
else
    echo "✅ No duplicate error codes found in errors.go"
fi

# Check for duplicate codes in errors.json
echo ""
echo "Checking for duplicate error codes in errors.json..."
json_codes=$(jq -r 'keys[]' errors.json 2>/dev/null | sort)
duplicates_json=$(echo "$json_codes" | uniq -d)

if [ ! -z "$duplicates_json" ]; then
    duplicate_codes_json="$duplicates_json"
    echo "❌ Found duplicate error codes in errors.json: $duplicates_json"
else
    echo "✅ No duplicate error codes found in errors.json"
fi

# Check for missing error mappings
echo ""
echo "Checking for missing error mappings..."

# Get all unique codes from errors.go
unique_codes=$(echo "$codes" | sort -u)

# Check each code individually against errors.json
missing_mappings=""
for code in $unique_codes; do
    if ! jq -e "has(\"$code\")" errors.json > /dev/null 2>&1; then
        missing_mappings="$missing_mappings $code"
    fi
done

# Validate JSON syntax
echo ""
echo "Validating errors.json syntax..."
if jq . errors.json > /dev/null; then
    echo "✅ errors.json has valid JSON syntax"
else
    echo "❌ errors.json has invalid JSON syntax"
    validation_passed=false
    json_syntax_valid=false
fi

# Summary
echo ""
echo "📊 Summary"
echo "=========="
total_constants=$(echo "$error_data" | wc -l)
total_mappings=$(jq 'length' errors.json 2>/dev/null || echo "0")
total_duplicates_go=$(echo "$duplicates_go" | wc -w)
total_duplicates_json=$(echo "$duplicates_json" | wc -w)
total_missing=$(echo "$missing_mappings" | wc -w)

printf "Total error constants:       %8d\n" "$total_constants"
printf "Total error mappings:        %8d\n" "$total_mappings"
printf "Duplicate codes (Go):        %8d\n" "$total_duplicates_go"
printf "Duplicate codes (JSON):      %8d\n" "$total_duplicates_json"
printf "Missing mappings:            %8d\n" "$total_missing"

# Determine overall status
if [ "$total_missing" -eq 0 ] && [ "$total_duplicates_go" -eq 0 ] && [ "$total_duplicates_json" -eq 0 ]; then
    echo "✅ All error constants have mappings and no duplicates"
elif [ "$total_missing" -eq 0 ] && [ "$total_duplicates_go" -eq 0 ]; then
    echo "⚠️  All constants have mappings and no Go duplicates, but there are JSON duplicates"
    validation_passed=false
elif [ "$total_missing" -eq 0 ]; then
    echo "⚠️  All constants have mappings, but there are duplicate codes"
    validation_passed=false
else
    echo "⚠️  Multiple issues found: missing mappings or duplicate codes"
    validation_passed=false
fi

# Final result
echo ""
if [ "$validation_passed" = true ]; then
    echo "🎉 Error framework validation passed!"
    exit 0
else
    echo "❌ Error framework validation failed!"
    echo ""
    echo "Issues found:"
    
    if [ ! -z "$duplicate_codes_go" ]; then
        echo ""
        echo "🔴 DUPLICATE ERROR CODES (errors.go):"
        echo "====================================="
        for code in $duplicate_codes_go; do
            echo "Code $code:"
            echo "$error_data" | grep "=$code$" | cut -d'=' -f1 | sed 's/^/  - /'
        done
    fi
    
    if [ ! -z "$duplicate_codes_json" ]; then
        echo ""
        echo "🔴 DUPLICATE ERROR CODES (errors.json):"
        echo "======================================="
        for code in $duplicate_codes_json; do
            echo "Code $code appears multiple times in errors.json"
        done
    fi
    
    if [ ! -z "$missing_mappings" ]; then
        echo ""
        echo "🟡 MISSING ERROR MAPPINGS:"
        echo "==========================="
        for code in $missing_mappings; do
            constant_name=$(echo "$error_data" | grep "=$code$" | head -1 | cut -d'=' -f1)
            echo "Code $code: $constant_name"
        done
    fi
    
    if [ "$json_syntax_valid" = false ]; then
        echo ""
        echo "🔴 JSON SYNTAX ERROR:"
        echo "====================="
        echo "errors.json has invalid JSON syntax"
    fi
    
    echo ""
    echo "📝 ACTION REQUIRED:"
    echo "=================="
    if [ ! -z "$duplicate_codes_go" ]; then
        echo "1. Fix duplicate error codes in errors.go by assigning unique codes"
    fi
    if [ ! -z "$duplicate_codes_json" ]; then
        echo "2. Fix duplicate error codes in errors.json"
    fi
    if [ ! -z "$missing_mappings" ]; then
        echo "3. Add missing mappings to errors.json"
    fi
    if [ "$json_syntax_valid" = false ]; then
        echo "4. Fix JSON syntax in errors.json"
    fi
    echo ""
    echo "After fixing issues, run validation again: ./lib/errors/validate.sh"
    exit 1
fi 