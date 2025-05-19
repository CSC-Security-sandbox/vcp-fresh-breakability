#!/bin/bash

check_required_files() {
  for file in "$@"; do
    if [[ ! -f $file ]]; then
      echo "$file not found. Please run the script from the correct directory."
      exit 1
    fi
  done
}

generate_checksums() {
  local checksum_file=$1
  shift
  local items=("$@")

  for item in "${items[@]}"; do
    if [[ -f $item ]]; then
      cksum "$item" >> "$checksum_file"
    elif [[ -d $item ]]; then
      find "$item" -type f -exec cksum {} >> "$checksum_file" \;
    fi
  done
}

generate_ontap_checksums() {
  check_required_files swagger.yaml swagger_operations.txt
  local checksum_file="tempChecksumsFile.checksum"
  generate_checksums "$checksum_file" "swagger.yaml" "swagger_operations.txt" "./client" "./models"
  LC_ALL=C sort -k 3 "$checksum_file" > newChecksumsFile.checksum
  rm -f "$checksum_file"
}

generate_ontap_priv_checksums() {
  check_required_files swagger.yaml
  local checksum_file="tempChecksumsFile.checksum"
  generate_checksums "$checksum_file" "swagger.yaml" "./client" "./models"
  LC_ALL=C sort -k 3 "$checksum_file" > newChecksumsFile.checksum
  rm -f "$checksum_file"
}