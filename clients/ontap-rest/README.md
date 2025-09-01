# ONTAP REST Client

This directory contains the generated Go client for the ONTAP REST API.

## Generating and Verifying the Swagger Client

### 1. Generate the ONTAP REST client

To generate the ONTAP REST client, run the following command from the project root directory:

```bash
./scripts/generate-ontap.sh
```

This script will:
- Install required dependencies (go-swagger and goimports) if not already installed
- Generate models based on operations defined in `swagger_operations.txt`
- Generate the client code with appropriate models and operations
- Format the generated code
- Generate mocks for testing
- Update checksums for verification

### 2. Verify the ONTAP REST client

To verify that the ONTAP REST client is up-to-date and matches the expected checksums, run:

```bash
./scripts/verify-ontap.sh
```

This script will:
- Check the current state of the client code against the stored checksums
- Verify that no unexpected changes have been made to the generated code
- Return an error code if the verification fails

## Updating the Client

When updating the ONTAP REST API specification:

1. Update the `swagger.yaml` file with the new API definitions
2. Update `swagger_operations.txt` if you need to include new operations
3. Run `./scripts/generate-ontap.sh` to regenerate the client
4. Run `./scripts/verify-ontap.sh` to ensure everything is properly generated
5. Commit the changes including the updated checksums
