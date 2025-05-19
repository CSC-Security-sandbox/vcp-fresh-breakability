#!/bin/bash

# Script to update version in Chart.yaml and values.yaml by replacing "0.0.0"
# Usage: ./updateHelmVersion.sh <version>

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <version>"
  exit 1
fi

VERSION=$1
VERSION="${VERSION#v}" # Remove the 'v' prefix if present

# Define the paths to the main chart and subcharts
FILES=("kubernetes/vsa-control-plane/Chart.yaml"
       "kubernetes/vsa-control-plane/charts/google-proxy/Chart.yaml"
       "kubernetes/vsa-control-plane/charts/core/Chart.yaml"
       "kubernetes/vsa-control-plane/charts/google-proxy/values.yaml"
       "kubernetes/vsa-control-plane/charts/core/values.yaml")

# Replace "0.0.0" with the provided version in the specified files
for FILE in "${FILES[@]}"; do
  if [ -f "$FILE" ]; then
    echo "Updating $FILE..."
    sed -i.bak "s/0.0.0/$VERSION/g" "$FILE"
    rm "${FILE}.bak"
  else
    echo "File $FILE not found!"
  fi
done

echo "Helm Version update completed!"