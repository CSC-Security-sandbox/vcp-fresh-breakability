# Testing EMS Event Forwarding Locally

This guide explains how to test the EMS event forwarding functionality locally.

## Table of Contents
1. [Unit Tests](#unit-tests)
2. [Integration Tests](#integration-tests)
3. [Manual Testing](#manual-testing)
4. [Verification Steps](#verification-steps)

## Unit Tests

### 1. Test the Activity (`CreateEMSEventForwarding`)

Create a test file following the pattern in `core/orchestrator/activities/psc_activities_test.go`:

```go
func TestCreateEMSEventForwarding_Success(t *testing.T) {
    // Arrange
    mockProvider := new(vsa.MockProvider)
    originalGetProviderByNode := hyperscaler2.GetProviderByNode
    defer func() {
        hyperscaler2.GetProviderByNode = originalGetProviderByNode
    }()

    // Mock GetProviderByNode to return the mock provider
    hyperscaler2.GetProviderByNode = func(ctx context.Context, node *coremodel.Node) (vsa.Provider, error) {
        return mockProvider, nil
    }

    pscActivity := &activities.PSCActivity{
        SE: database.NewMockStorage(t),
    }

    node := &coremodel.Node{}
    mockProvider.On("CreateEMSEventForwarding", mock.Anything).Return(nil)

    // Run activity through Temporal test environment
    testSuite := &testsuite.WorkflowTestSuite{}
    env := testSuite.NewTestActivityEnvironment()
    env.RegisterActivity(pscActivity.CreateEMSEventForwarding)

    _, err := env.ExecuteActivity(pscActivity.CreateEMSEventForwarding, node, "35.239.71.238")
    assert.NoError(t, err)
    mockProvider.AssertExpectations(t)
}
```

### 2. Test the VSA Provider Method

Create a test file following the pattern in `core/vsa/security_log_forwarding_test.go`:

```go
func TestCreateEMSEventForwarding_Success(t *testing.T) {
    // Create mock SupportClient
    mockSupportClient := new(ontapRest.MockSupportClient)
    mockSupportClient.On("EMSEventDestinationCreate", mock.Anything).Return(nil)
    mockSupportClient.On("EMSEventFilterCreate", mock.Anything).Return(nil)
    mockSupportClient.On("EMSEventFilterRuleAdd", mock.Anything).Return(nil)
    mockSupportClient.On("EMSEventDestinationModify", mock.AnythingOfType("string"), mock.Anything).Return(nil)

    // Create mock RESTClient
    mockClient := new(ontapRest.MockRESTClient)
    mockClient.On("Support").Return(mockSupportClient)

    originalgetOntapClientFunc := getOntapClientFunc
    defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

    getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
        return mockClient, nil
    }

    provider := &OntapRestProvider{
        ClientParams: ontapRest.RESTClientParams{
            CertificateBasedAuthEnabled: false,
            Password:                    log.Secret("test-password"),
            InsecureSkipVerify:          true,
        },
        Logger: log.NewLogger().(*log.Slogger),
    }

    params := CreateEMSEventForwardingParams{
        DestinationName: "syslog-ems",
        DestinationIP:   "35.239.71.238",
        DestinationPort: 5140,
        Transport:       "tcp-unencrypted",
        TimestampFormat: "rfc-3164",
        MessageFormat:   "legacy-netapp",
        FilterName:      "syslog-ems",
        Severities:      []string{"INFORMATIONAL", "EMERGENCY", "ERROR", "ALERT", "NOTICE"},
    }

    err := provider.CreateEMSEventForwarding(params)
    assert.NoError(t, err)
    mockSupportClient.AssertExpectations(t)
}
```

**Note**: The test now mocks `SupportClient` instead of making direct HTTP calls, which is cleaner and follows the same pattern as other provider tests.

### Running Unit Tests

```bash
# Run all PSC activity tests
go test -v ./core/orchestrator/activities -run TestCreateEMSEventForwarding

# Run all VSA provider tests
go test -v ./core/vsa -run TestCreateEMSEventForwarding

# Run with coverage
go test -v -cover ./core/vsa -run TestCreateEMSEventForwarding
```

## Integration Tests

### Prerequisites

1. **Access to an ONTAP cluster** (VSA or simulator)
   - Management IP address
   - Admin credentials (username/password or certificate)
   - ONTAP version 9.6+ (for EMS REST API support)

2. **Environment Variables**
   ```bash
   export ONTAP_HOST=<cluster-management-ip>
   export ONTAP_USERNAME=admin
   export ONTAP_PASSWORD=<admin-password>
   # OR for certificate-based auth:
   export ONTAP_CERT_PATH=<path-to-cert>
   export ONTAP_KEY_PATH=<path-to-key>
   ```

3. **Test PSC Endpoint** (or mock syslog server)
   - IP: `35.239.71.238` (or your test endpoint)
   - Port: `5140`
   - Protocol: TCP

### Integration Test Script

Create `scripts/test-ems-forwarding.sh`:

```bash
#!/bin/bash
set -e

ONTAP_HOST=${ONTAP_HOST:-"localhost"}
ONTAP_USERNAME=${ONTAP_USERNAME:-"admin"}
ONTAP_PASSWORD=${ONTAP_PASSWORD:-"password"}
TEST_IP=${TEST_IP:-"35.239.71.238"}
TEST_PORT=${TEST_PORT:-5140}

echo "Testing EMS Event Forwarding on ONTAP cluster: $ONTAP_HOST"

# Test 1: Create EMS destination
echo "Step 1: Creating EMS destination..."
curl -k -X POST "https://$ONTAP_HOST/api/support/ems/destinations" \
  -u "$ONTAP_USERNAME:$ONTAP_PASSWORD" \
  -H "Content-Type: application/json" \
  -H "Accept: application/json" \
  -d "{
    \"name\": \"syslog-ems-test\",
    \"type\": \"syslog\",
    \"syslog\": {
      \"host\": \"$TEST_IP\",
      \"port\": $TEST_PORT,
      \"transport\": \"tcp-unencrypted\",
      \"format\": {
        \"timestamp_override\": \"rfc-3164\",
        \"message\": \"legacy-netapp\"
      }
    }
  }"

echo -e "\nStep 2: Creating EMS filter..."
curl -k -X POST "https://$ONTAP_HOST/api/support/ems/filters" \
  -u "$ONTAP_USERNAME:$ONTAP_PASSWORD" \
  -H "Content-Type: application/json" \
  -H "Accept: application/json" \
  -d "{
    \"name\": \"syslog-ems-test\"
  }"

echo -e "\nStep 3: Adding filter rule..."
curl -k -X POST "https://$ONTAP_HOST/api/support/ems/filters/syslog-ems-test/rules" \
  -u "$ONTAP_USERNAME:$ONTAP_PASSWORD" \
  -H "Content-Type: application/json" \
  -H "Accept: application/json" \
  -d "{
    \"type\": \"include\",
    \"message_criteria\": {
      \"severities\": [\"INFORMATIONAL\", \"EMERGENCY\", \"ERROR\", \"ALERT\", \"NOTICE\"]
    }
  }"

echo -e "\nStep 4: Linking filter to destination..."
curl -k -X PATCH "https://$ONTAP_HOST/api/support/ems/destinations/syslog-ems-test" \
  -u "$ONTAP_USERNAME:$ONTAP_PASSWORD" \
  -H "Content-Type: application/json" \
  -H "Accept: application/json" \
  -d "{
    \"filters\": [{\"name\": \"syslog-ems-test\"}]
  }"

echo -e "\nStep 5: Verifying configuration..."
curl -k -X GET "https://$ONTAP_HOST/api/support/ems/destinations/syslog-ems-test" \
  -u "$ONTAP_USERNAME:$ONTAP_PASSWORD" \
  -H "Accept: application/json"

echo -e "\n\nTest completed!"
```

## Manual Testing

### Option 1: Test with Real ONTAP Cluster

1. **Set up environment variables:**
   ```bash
   export ONTAP_HOST=<your-ontap-cluster-ip>
   export ONTAP_USERNAME=admin
   export ONTAP_PASSWORD=<password>
   ```

2. **Run the integration test script:**
   ```bash
   chmod +x scripts/test-ems-forwarding.sh
   ./scripts/test-ems-forwarding.sh
   ```

3. **Or test via Go code directly:**
   ```go
   package main

   import (
       "context"
       "fmt"
       "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
       ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
       "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
   )

   func main() {
       ctx := context.Background()
       provider := vsa.NewProvider(ctx, vsa.ProviderDetails{
           IPAddress:          "your-ontap-ip",
           Password:           "your-password",
           InsecureSkipVerify: true,
       })

       params := vsa.CreateEMSEventForwardingParams{
           DestinationName: "syslog-ems",
           DestinationIP:   "35.239.71.238",
           DestinationPort: 5140,
           Transport:       "tcp-unencrypted",
           TimestampFormat: "rfc-3164",
           MessageFormat:   "legacy-netapp",
           FilterName:      "syslog-ems",
           Severities:      []string{"INFORMATIONAL", "EMERGENCY", "ERROR", "ALERT", "NOTICE"},
       }

       err := provider.CreateEMSEventForwarding(params)
       if err != nil {
           fmt.Printf("Error: %v\n", err)
       } else {
           fmt.Println("Success!")
       }
   }
   ```

### Option 2: Test with Mock Syslog Server

1. **Set up a mock syslog server to receive events:**
   ```bash
   # Using netcat (nc) to listen on port 5140
   nc -l -k 5140
   
   # Or use a Python script:
   python3 -c "
   import socket
   s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
   s.bind(('0.0.0.0', 5140))
   s.listen(1)
   while True:
       conn, addr = s.accept()
       data = conn.recv(1024)
       print(f'Received: {data.decode()}')
       conn.close()
   "
   ```

2. **Configure ONTAP to forward to your local mock server:**
   - Use your local machine's IP address instead of `35.239.71.238`
   - Make sure the port is accessible

### Option 3: Test via Pool Creation Workflow

1. **Enable the feature flag:**
   ```bash
   export GIN_LOGGING_FEATURE_FLAG=true
   ```

2. **Create a test pool** (this will trigger the EMS event forwarding):
   ```bash
   # Use your pool creation API/CLI
   # The workflow will automatically call CreateEMSEventForwarding
   ```

3. **Monitor the workflow execution:**
   - Check Temporal UI for workflow execution
   - Look for `CreateEMSEventForwarding` activity execution
   - Check logs for any errors

## Verification Steps

### 1. Verify EMS Destination Created

```bash
curl -k -X GET "https://$ONTAP_HOST/api/support/ems/destinations/syslog-ems" \
  -u "$ONTAP_USERNAME:$ONTAP_PASSWORD" \
  -H "Accept: application/json"
```

Expected response should include:
- `name`: "syslog-ems"
- `type`: "syslog"
- `syslog.host`: Your PSC endpoint IP
- `syslog.port`: 5140

### 2. Verify EMS Filter Created

```bash
curl -k -X GET "https://$ONTAP_HOST/api/support/ems/filters/syslog-ems" \
  -u "$ONTAP_USERNAME:$ONTAP_PASSWORD" \
  -H "Accept: application/json"
```

Expected response should include:
- `name`: "syslog-ems"
- `rules`: Array with severity filters

### 3. Verify Filter Linked to Destination

```bash
curl -k -X GET "https://$ONTAP_HOST/api/support/ems/destinations/syslog-ems?fields=filters" \
  -u "$ONTAP_USERNAME:$ONTAP_PASSWORD" \
  -H "Accept: application/json"
```

Expected response should show the filter in the `filters` array.

### 4. Verify Events are Forwarded

1. **Trigger a test EMS event on ONTAP:**
   ```bash
   # Via ONTAP CLI (SSH to cluster)
   event log generate -message "test.ems.event" -severity ERROR
   ```

2. **Check your syslog server** (if using mock server) for the event

3. **Or check ONTAP event logs:**
   ```bash
   curl -k -X GET "https://$ONTAP_HOST/api/support/ems/events?message.name=test.ems.event" \
     -u "$ONTAP_USERNAME:$ONTAP_PASSWORD" \
     -H "Accept: application/json"
   ```

### 5. Verify via ONTAP CLI

```bash
# SSH to ONTAP cluster
ssh admin@<ontap-cluster-ip>

# Check destination
event notification destination show -name syslog-ems

# Check filter
event filter show -filter-name syslog-ems

# Check notification
event notification show -filter-name syslog-ems
```

## Troubleshooting

### Common Issues

1. **"Destination already exists" error:**
   - The implementation handles this gracefully
   - Check if destination was created in a previous test run
   - Delete it manually if needed:
     ```bash
     curl -k -X DELETE "https://$ONTAP_HOST/api/support/ems/destinations/syslog-ems" \
       -u "$ONTAP_USERNAME:$ONTAP_PASSWORD"
     ```

2. **Connection refused to syslog server:**
   - Verify the PSC endpoint IP and port are correct
   - Check firewall rules
   - Ensure the syslog server is listening

3. **Authentication errors:**
   - Verify ONTAP credentials
   - Check certificate paths if using certificate-based auth
   - Ensure user has admin privileges

4. **API not found errors:**
   - Verify ONTAP version is 9.6+
   - Check that EMS REST API is enabled
   - Some ONTAP versions may have different API paths

## Cleanup

After testing, you may want to clean up test resources:

```bash
# Delete destination (this will also remove the notification)
curl -k -X DELETE "https://$ONTAP_HOST/api/support/ems/destinations/syslog-ems" \
  -u "$ONTAP_USERNAME:$ONTAP_PASSWORD"

# Delete filter
curl -k -X DELETE "https://$ONTAP_HOST/api/support/ems/filters/syslog-ems" \
  -u "$ONTAP_USERNAME:$ONTAP_PASSWORD"
```

## Next Steps

- Add unit tests to `core/orchestrator/activities/psc_activities_test.go`
- Add integration tests to CI/CD pipeline
- Test with different ONTAP versions
- Test error scenarios (network failures, invalid configs, etc.)
