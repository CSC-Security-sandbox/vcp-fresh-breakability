package common

import "testing"

func TestLoadsConfigWithDefaultValues(t *testing.T) {
	config := LoadConfig()

	if config.RootUrl != "https://servicecontrol.googleapis.com" {
		t.Fatalf("Expected RootUrl to be 'https://servicecontrol.googleapis.com', got %s", config.RootUrl)
	}
	if config.OperationBatchSize != 200 {
		t.Fatalf("Expected OperationBatchSize to be 200, got %d", config.OperationBatchSize)
	}
	if config.PusherServiceName != "autopush-netapp.sandbox.googleapis.com" {
		t.Fatalf("Expected PusherServiceName to be 'autopush-netapp.sandbox.googleapis.com', got %s", config.PusherServiceName)
	}
	if config.PusherServiceProject != "netapp-au-se1-autopush-sde-tst" {
		t.Fatalf("Expected PusherServiceProject to be 'netapp-au-se1-autopush-sde-tst', got %s", config.PusherServiceProject)
	}
}

func TestLoadsConfigWithCustomEnvironmentValues(t *testing.T) {
	config := LoadConfig()

	if config.RootUrl != "https://servicecontrol.googleapis.com" {
		t.Fatalf("Expected RootUrl to be 'https://servicecontrol.googleapis.com', got %s", config.RootUrl)
	}
	if config.OperationBatchSize != 200 {
		t.Fatalf("Expected OperationBatchSize to be 200, got %d", config.OperationBatchSize)
	}
	if config.PusherServiceName != "autopush-netapp.sandbox.googleapis.com" {
		t.Fatalf("Expected PusherServiceName to be 'autopush-netapp.sandbox.googleapis.com', got %s", config.PusherServiceName)
	}
	if config.PusherServiceProject != "netapp-au-se1-autopush-sde-tst" {
		t.Fatalf("Expected PusherServiceProject to be 'netapp-au-se1-autopush-sde-tst', got %s", config.PusherServiceProject)
	}
}

func TestHandlesMissingEnvironmentVariablesGracefully(t *testing.T) {
	config := LoadConfig()

	if config.RootUrl != "https://servicecontrol.googleapis.com" {
		t.Fatalf("Expected RootUrl to default to 'https://servicecontrol.googleapis.com', got %s", config.RootUrl)
	}
	if config.OperationBatchSize != 200 {
		t.Fatalf("Expected OperationBatchSize to default to 200, got %d", config.OperationBatchSize)
	}
	if config.PusherServiceName != "autopush-netapp.sandbox.googleapis.com" {
		t.Fatalf("Expected PusherServiceName to default to 'autopush-netapp.sandbox.googleapis.com', got %s", config.PusherServiceName)
	}
	if config.PusherServiceProject != "netapp-au-se1-autopush-sde-tst" {
		t.Fatalf("Expected PusherServiceProject to default to 'netapp-au-se1-autopush-sde-tst', got %s", config.PusherServiceProject)
	}
}
