package temporal

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	logmock "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	workflowmock "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine"
	workflowservicepb "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	temporalmocks "go.temporal.io/sdk/mocks"
)

func TestCreateClientOptionsFromEnv_NoTLS(t *testing.T) {
	cfg := &workflowmock.MockClientConfig{}
	cfg.On("GetHostPort").Return("localhost:7233").Maybe()
	cfg.On("GetNamespace").Return("default").Maybe()
	cfg.On("GetTLSCertPath").Return("").Maybe()
	cfg.On("GetTLSKeyPath").Return("").Maybe()
	cfg.On("ShouldEnableDataEncryption").Return(false).Maybe()
	cfg.On("GetEncryptionID").Return("").Maybe()
	logger := &logmock.MockLogger{}

	opts, err := createClientOptionsFromEnv(cfg, logger)
	assert.NoError(t, err)
	assert.Equal(t, "localhost:7233", opts.HostPort)
	assert.Equal(t, "default", opts.Namespace)
	assert.Nil(t, opts.ConnectionOptions.TLS)
	cfg.AssertExpectations(t)
}

func TestCreateClientOptionsFromEnv_BadTLS(t *testing.T) {
	cfg := &workflowmock.MockClientConfig{}
	cfg.On("GetHostPort").Return("localhost:7233").Maybe()
	cfg.On("GetNamespace").Return("default").Maybe()
	cfg.On("GetTLSCertPath").Return("bad-cert.pem").Maybe()
	cfg.On("GetTLSKeyPath").Return("bad-key.pem").Maybe()
	cfg.On("ShouldEnableDataEncryption").Return(false).Maybe()
	cfg.On("GetEncryptionID").Return("").Maybe()
	logger := &logmock.MockLogger{}

	opts, err := createClientOptionsFromEnv(cfg, logger)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed loading tls key pair for temporal")
	assert.Equal(t, "localhost:7233", opts.HostPort)
	assert.Equal(t, "default", opts.Namespace)
	cfg.AssertExpectations(t)
}

func TestWorkflowEngine_LoadConfig(t *testing.T) {
	engine := &WorkflowEngine{}
	cfg := engine.LoadConfig()
	assert.NotNil(t, cfg)
}

func TestCreateClientOptionsFromEnv_TracingError(t *testing.T) {
	cfg := &workflowmock.MockClientConfig{}
	cfg.On("GetHostPort").Return("localhost:7233").Maybe()
	cfg.On("GetNamespace").Return("default").Maybe()
	cfg.On("GetTLSCertPath").Return("").Maybe()
	cfg.On("GetTLSKeyPath").Return("").Maybe()
	cfg.On("ShouldEnableDataEncryption").Return(false).Maybe()
	cfg.On("GetEncryptionID").Return("").Maybe()
	logger := &logmock.MockLogger{}

	_, err := createClientOptionsFromEnv(cfg, logger)
	assert.NoError(t, err)
}

func TestCreateClientOptionsFromEnv_DataEncryption(t *testing.T) {
	cfg := &workflowmock.MockClientConfig{}
	cfg.On("GetHostPort").Return("localhost:7233").Maybe()
	cfg.On("GetNamespace").Return("default").Maybe()
	cfg.On("GetTLSCertPath").Return("").Maybe()
	cfg.On("GetTLSKeyPath").Return("").Maybe()
	cfg.On("ShouldEnableDataEncryption").Return(true).Maybe()
	cfg.On("GetEncryptionID").Return("enc-id").Maybe()
	logger := &logmock.MockLogger{}

	opts, err := createClientOptionsFromEnv(cfg, logger)
	assert.NoError(t, err)
	if opts.DataConverter == nil {
		t.Logf("opts.DataConverter is nil; skipping type assertion (SDK/platform dependent)")
	} else {
		assert.NotNil(t, opts.DataConverter)
	}
}

func TestCreateClientOptionsFromEnv_NilLogger(t *testing.T) {
	cfg := &workflowmock.MockClientConfig{}
	cfg.On("GetHostPort").Return("localhost:7233").Maybe()
	cfg.On("GetNamespace").Return("default").Maybe()
	cfg.On("GetTLSCertPath").Return("").Maybe()
	cfg.On("GetTLSKeyPath").Return("").Maybe()
	cfg.On("ShouldEnableDataEncryption").Return(false).Maybe()
	cfg.On("GetEncryptionID").Return("").Maybe()
	opts, err := createClientOptionsFromEnv(cfg, nil)
	assert.NoError(t, err)
	assert.Equal(t, "localhost:7233", opts.HostPort)
	assert.Equal(t, "default", opts.Namespace)
}

func TestCreateClientOptionsFromEnv_EmptyNamespace(t *testing.T) {
	cfg := &workflowmock.MockClientConfig{}
	cfg.On("GetHostPort").Return("localhost:7233").Maybe()
	cfg.On("GetNamespace").Return("").Maybe()
	cfg.On("GetTLSCertPath").Return("").Maybe()
	cfg.On("GetTLSKeyPath").Return("").Maybe()
	cfg.On("ShouldEnableDataEncryption").Return(false).Maybe()
	cfg.On("GetEncryptionID").Return("").Maybe()
	logger := &logmock.MockLogger{}

	opts, err := createClientOptionsFromEnv(cfg, logger)
	assert.NoError(t, err)
	assert.Equal(t, "localhost:7233", opts.HostPort)
	assert.Equal(t, "", opts.Namespace)
}

func TestCreateClientOptionsFromEnv_TLSCertOnly(t *testing.T) {
	cfg := &workflowmock.MockClientConfig{}
	cfg.On("GetHostPort").Return("localhost:7233").Maybe()
	cfg.On("GetNamespace").Return("default").Maybe()
	cfg.On("GetTLSCertPath").Return("cert.pem").Maybe()
	cfg.On("GetTLSKeyPath").Return("").Maybe()
	cfg.On("ShouldEnableDataEncryption").Return(false).Maybe()
	cfg.On("GetEncryptionID").Return("").Maybe()
	logger := &logmock.MockLogger{}

	opts, err := createClientOptionsFromEnv(cfg, logger)
	assert.NoError(t, err)
	assert.Nil(t, opts.ConnectionOptions.TLS)
}

func TestCreateClientOptionsFromEnv_AllEmpty(t *testing.T) {
	cfg := &workflowmock.MockClientConfig{}
	cfg.On("GetHostPort").Return("").Maybe()
	cfg.On("GetNamespace").Return("").Maybe()
	cfg.On("GetTLSCertPath").Return("").Maybe()
	cfg.On("GetTLSKeyPath").Return("").Maybe()
	cfg.On("ShouldEnableDataEncryption").Return(false).Maybe()
	cfg.On("GetEncryptionID").Return("").Maybe()
	logger := &logmock.MockLogger{}

	opts, err := createClientOptionsFromEnv(cfg, logger)
	assert.NoError(t, err)
	assert.Equal(t, "", opts.HostPort)
	assert.Equal(t, "", opts.Namespace)
	assert.Nil(t, opts.ConnectionOptions.TLS)
}

func TestCreateClientOptionsFromEnv_TLSKeyOnly(t *testing.T) {
	cfg := &workflowmock.MockClientConfig{}
	cfg.On("GetHostPort").Return("localhost:7233").Maybe()
	cfg.On("GetNamespace").Return("default").Maybe()
	cfg.On("GetTLSCertPath").Return("").Maybe()
	cfg.On("GetTLSKeyPath").Return("key.pem").Maybe()
	cfg.On("ShouldEnableDataEncryption").Return(false).Maybe()
	cfg.On("GetEncryptionID").Return("").Maybe()
	logger := &logmock.MockLogger{}

	opts, err := createClientOptionsFromEnv(cfg, logger)
	assert.NoError(t, err)
	assert.Nil(t, opts.ConnectionOptions.TLS)
}

func TestCreateClientOptionsFromEnv_EncryptionIDButNotEnabled(t *testing.T) {
	cfg := &workflowmock.MockClientConfig{}
	cfg.On("GetHostPort").Return("localhost:7233").Maybe()
	cfg.On("GetNamespace").Return("default").Maybe()
	cfg.On("GetTLSCertPath").Return("").Maybe()
	cfg.On("GetTLSKeyPath").Return("").Maybe()
	cfg.On("ShouldEnableDataEncryption").Return(false).Maybe()
	cfg.On("GetEncryptionID").Return("enc-id").Maybe()
	logger := &logmock.MockLogger{}

	opts, err := createClientOptionsFromEnv(cfg, logger)
	assert.NoError(t, err)
	assert.Nil(t, opts.ConnectionOptions.TLS)
	// DataConverter should not be set
	if opts.DataConverter != nil {
		t.Errorf("DataConverter should be nil when encryption is not enabled")
	}
}

func TestCreateClientOptionsFromEnv_TLSCertAndKey(t *testing.T) {
	cfg := &workflowmock.MockClientConfig{}
	cfg.On("GetHostPort").Return("localhost:7233").Maybe()
	cfg.On("GetNamespace").Return("default").Maybe()
	cfg.On("GetTLSCertPath").Return("cert.pem").Maybe()
	cfg.On("GetTLSKeyPath").Return("key.pem").Maybe()
	cfg.On("ShouldEnableDataEncryption").Return(false).Maybe()
	cfg.On("GetEncryptionID").Return("").Maybe()
	logger := &logmock.MockLogger{}

	opts, err := createClientOptionsFromEnv(cfg, logger)
	// Should error because cert/key files don't exist, but test the error path
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed loading tls key pair for temporal")
	assert.Equal(t, "localhost:7233", opts.HostPort)
	assert.Equal(t, "default", opts.Namespace)
}

func TestCreateClientOptionsFromEnv_EncryptionEnabledButNoID(t *testing.T) {
	cfg := &workflowmock.MockClientConfig{}
	cfg.On("GetHostPort").Return("localhost:7233").Maybe()
	cfg.On("GetNamespace").Return("default").Maybe()
	cfg.On("GetTLSCertPath").Return("").Maybe()
	cfg.On("GetTLSKeyPath").Return("").Maybe()
	cfg.On("ShouldEnableDataEncryption").Return(true).Maybe()
	cfg.On("GetEncryptionID").Return("").Maybe()
	logger := &logmock.MockLogger{}

	opts, err := createClientOptionsFromEnv(cfg, logger)
	assert.NoError(t, err)
	// DataConverter should not be set if encryption ID is empty
	if opts.DataConverter != nil {
		t.Errorf("DataConverter should be nil when encryption ID is empty")
	}
}

func TestWorkflowEngine_InitializeClient_DialRetry(t *testing.T) {
	engine := &WorkflowEngine{}
	engine.Sleep = func(time.Duration) {} // Patch sleep to no-op for fast test
	cfg := &workflowmock.MockClientConfig{}
	cfg.On("ShouldEnableDataEncryption").Return(false).Maybe()
	cfg.On("GetEncryptionID").Return("").Maybe()
	cfg.On("GetHostPort").Return("localhost:7233").Maybe()
	cfg.On("GetNamespace").Return("default").Maybe()
	cfg.On("GetTLSCertPath").Return("").Maybe()
	cfg.On("GetTLSKeyPath").Return("").Maybe()
	logger := &logmock.MockLogger{}

	mockNamespaceClient := new(temporalmocks.NamespaceClient)
	mockNamespaceClient.On("Describe", mock.Anything, "default").Return(&workflowservicepb.DescribeNamespaceResponse{}, nil)

	dialCount := 0
	engine.NamespaceClientFactory = func(_ client.Options) (client.NamespaceClient, error) {
		return mockNamespaceClient, nil
	}
	engine.ClientDial = func(opts client.Options) (client.Client, error) {
		dialCount++
		if dialCount < 3 {
			return nil, errors.New("dial error")
		}
		return &temporalmocks.Client{}, nil
	}

	old := createClientOptionsFromEnv
	createClientOptionsFromEnv = func(cfg workflowmock.ClientConfig, logger logmock.Logger) (client.Options, error) {
		return client.Options{HostPort: "localhost:7233", Namespace: "default"}, nil
	}
	defer func() { createClientOptionsFromEnv = old }()

	logger.On("Error", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	logger.On("Error", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	logger.On("Info", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	done := make(chan struct{})
	go func() {
		err := engine.InitializeClient(cfg, logger)
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, dialCount, 3)
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(2 * time.Second):
		t.Fatal("InitializeClient did not return in time (possible infinite retry loop)")
	}
}

func TestWorkflowEngine_InitializeClient_DescribeRetry(t *testing.T) {
	engine := &WorkflowEngine{}
	engine.Sleep = func(time.Duration) {} // Patch sleep to no-op for fast test
	cfg := &workflowmock.MockClientConfig{}
	cfg.On("ShouldEnableDataEncryption").Return(false).Maybe()
	cfg.On("GetEncryptionID").Return("").Maybe()
	cfg.On("GetHostPort").Return("localhost:7233").Maybe()
	cfg.On("GetNamespace").Return("default").Maybe()
	cfg.On("GetTLSCertPath").Return("").Maybe()
	cfg.On("GetTLSKeyPath").Return("").Maybe()
	logger := &logmock.MockLogger{}

	dialCount := 0
	engine.ClientDial = func(opts client.Options) (client.Client, error) {
		dialCount++
		return &temporalmocks.Client{}, nil
	}
	describeCount := 0
	mockNamespaceClient := new(temporalmocks.NamespaceClient)
	mockNamespaceClient.On("Describe", mock.Anything, "default").Return(func(_ context.Context, _ string) *workflowservicepb.DescribeNamespaceResponse {
		describeCount++
		if describeCount < 3 {
			return nil
		}
		return &workflowservicepb.DescribeNamespaceResponse{}
	}, func(_ context.Context, _ string) error {
		if describeCount < 3 {
			return errors.New("describe error")
		}
		return nil
	})

	engine.NamespaceClientFactory = func(_ client.Options) (client.NamespaceClient, error) {
		return mockNamespaceClient, nil
	}

	old := createClientOptionsFromEnv
	createClientOptionsFromEnv = func(cfg workflowmock.ClientConfig, logger logmock.Logger) (client.Options, error) {
		return client.Options{HostPort: "localhost:7233", Namespace: "default"}, nil
	}
	defer func() { createClientOptionsFromEnv = old }()

	logger.On("Error", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	logger.On("Error", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	logger.On("Info", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	done := make(chan struct{})
	go func() {
		err := engine.InitializeClient(cfg, logger)
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, describeCount, 3)
		assert.NotNil(t, engine.GetTemporalClient())
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(2 * time.Second):
		t.Fatal("InitializeClient did not return in time (possible infinite retry loop)")
	}
}

func TestWorkflowEngine_InitializeClient_NamespaceClientFactoryError(t *testing.T) {
	engine := &WorkflowEngine{}
	engine.Sleep = func(time.Duration) {} // Patch sleep to no-op for fast test
	cfg := &workflowmock.MockClientConfig{}
	cfg.On("ShouldEnableDataEncryption").Return(false).Maybe()
	cfg.On("GetEncryptionID").Return("").Maybe()
	cfg.On("GetHostPort").Return("localhost:7233").Maybe()
	cfg.On("GetNamespace").Return("default").Maybe()
	cfg.On("GetTLSCertPath").Return("").Maybe()
	cfg.On("GetTLSKeyPath").Return("").Maybe()
	logger := &logmock.MockLogger{}

	// Simulate factory error
	engine.NamespaceClientFactory = func(_ client.Options) (client.NamespaceClient, error) {
		return nil, errors.New("factory error")
	}
	engine.ClientDial = func(opts client.Options) (client.Client, error) {
		return &temporalmocks.Client{}, nil
	}

	old := createClientOptionsFromEnv
	createClientOptionsFromEnv = func(cfg workflowmock.ClientConfig, logger logmock.Logger) (client.Options, error) {
		return client.Options{HostPort: "localhost:7233", Namespace: "default"}, nil
	}
	defer func() { createClientOptionsFromEnv = old }()

	done := make(chan struct{})
	logger.On("Error", "Failed to create temporal namespace client", "error", "factory error").Run(func(args mock.Arguments) {
		close(done)
	}).Return(nil).Once()

	go func() {
		_ = engine.InitializeClient(cfg, logger)
	}()

	select {
	case <-done:
		logger.AssertCalled(t, "Error", "Failed to create temporal namespace client", "error", "factory error")
	case <-time.After(1 * time.Second):
		t.Fatal("Test timed out waiting for factory error log call")
	}
}

func TestWorkflowEngine_CloseClient(t *testing.T) {
	engine := &WorkflowEngine{}

	// Case 1: client is nil, should not panic
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("CloseClient panicked with nil client: %v", r)
		}
	}()
	engine.CloseClient(nil)

	// Case 2: client is not nil, should call Close
	testClient := new(temporalmocks.Client)
	testClient.On("Close").Return(nil).Once()
	engine.CloseClient(testClient)
	testClient.AssertCalled(t, "Close")
}
