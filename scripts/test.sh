#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Define the coverage output file
COVERAGE_FILE="coverage.out"

# Run Go unit tests with coverage
echo "Running Go unit tests with coverage..."
go test ./... -cover -coverprofile="$COVERAGE_FILE"

# Check if the coverage file was generated
if [ -f "$COVERAGE_FILE" ]; then
  echo "Coverage report generated: $COVERAGE_FILE"
else
  echo "Failed to generate coverage report."
  exit 1
fi

# Extract the overall coverage percentage and save it to a file
OVERALL_COVERAGE=$(go tool cover -func="$COVERAGE_FILE" | grep -E '^total:' | awk '{print $3}' | sed 's/%//')
echo "Overall coverage: $OVERALL_COVERAGE %"
echo "coverage=$OVERALL_COVERAGE" >> $GITHUB_ENV


