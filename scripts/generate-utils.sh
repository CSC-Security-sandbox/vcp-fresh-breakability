generate_checksums() {
  if [[ ! -f swagger.yaml ]]; then
    echo "swagger.yaml not found. Please run the script from the correct directory."
    exit 1
  fi

  if [[ ! -f swagger_operations.txt ]]; then
    echo "swagger_operations.txt not found. Please run the script from the correct directory."
    exit 1
  fi

  if [[ ! -f swagger_models.txt ]]; then
    echo "swagger_models.txt not found. Please run the script from the correct directory."
    exit 1
  fi

  cksum swagger.yaml > tempChecksumsFile.checksum
  cksum swagger_operations.txt >> tempChecksumsFile.checksum
  cksum swagger_models.txt >> tempChecksumsFile.checksum

  mkdir -p ./client
  find ./client -type f -exec cksum {} >> tempChecksumsFile.checksum \;

  mkdir -p ./models
  find ./models -type f -exec cksum {} >> tempChecksumsFile.checksum \;

  LC_ALL=C sort -k 3 tempChecksumsFile.checksum > newChecksumsFile.checksum

  rm -f tempChecksumsFile.checksum
}
