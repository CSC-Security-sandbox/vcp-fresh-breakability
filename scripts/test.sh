#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Define the coverage output file
COVERAGE_FILE="coverage.out"
FILTERED=false

if [ "$1" == "--filtered" ]; then
  FILTERED=true
fi

# Run Go unit tests with coverage
echo "Running Go unit tests with coverage..."
go test ./... -cover -coverprofile="$COVERAGE_FILE"

if [ "$FILTERED" = true ]; then
  echo "Filtering files from coverage report..."
  ./scripts/exclude-from-code-coverage.sh
  grep -v "_gen.go" $COVERAGE_FILE > "${COVERAGE_FILE}.tmp" && mv -f "${COVERAGE_FILE}.tmp" $COVERAGE_FILE
fi

# Check if the coverage file was generated
if [ -f "$COVERAGE_FILE" ]; then
  echo "Coverage report generated: $COVERAGE_FILE"
else
  echo "Failed to generate coverage report."
  exit 1
fi

# Extract the overall coverage percentage and save it to a file
OVERALL_COVERAGE=$(go tool cover -func="$COVERAGE_FILE" | grep -E '^total:' | awk '{printf "%.8f", $3}' | sed 's/%//')
# Remove the coverage file
rm -f "$COVERAGE_FILE" "$COVERAGE_FILE''"
echo "Overall coverage: $OVERALL_COVERAGE %"
echo "coverage=$OVERALL_COVERAGE" >> $GITHUB_ENV
