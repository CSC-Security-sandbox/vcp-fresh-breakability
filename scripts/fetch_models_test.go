package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pb33f/libopenapi"
	v2 "github.com/pb33f/libopenapi/datamodel/high/v2"
)

func SetupTestEnv() (*libopenapi.DocumentModel[v2.Swagger], []string) {
	workingDir, _ = os.Getwd()
	path := strings.Split(workingDir, "/")
	homeDir := path[:len(path)-1]
	homeDirStr := strings.Join(homeDir, "/")

	// Paths to the swagger spec file and the operation's list file
	specFilePath = homeDirStr + "/clients/ontap-rest/swagger.yaml"
	// Read the swagger spec file
	specFile, _ := os.ReadFile(specFilePath)

	// Create a new document model from the spec file
	docmodel := buildDocModel(specFile)

	operationsFilePath = homeDirStr + "/clients/ontap-rest/swagger_operations.txt"
	operationsList, err := readOperationsList(operationsFilePath)
	if err != nil {
		logger.Errorf("cannot open file: %e", err)
		panic(err)
	}

	return docmodel, operationsList
}

func TestRemoveDuplicates(t *testing.T) {
	input := []string{"a", "b", "a", "c", "b"}
	expected := []string{"a", "b", "c"}
	result := removeDuplicates(input)
	if len(result) != len(expected) {
		t.Fatalf("expected %d, got %d", len(expected), len(result))
	}
	for _, v := range expected {
		found := false
		for _, r := range result {
			if v == r {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected value %s not found in result", v)
		}
	}
}

func TestReadOperationsList(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "opslist")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = os.Remove(tmpFile.Name())
		if err != nil {
			t.Fatalf("failed to remove temp file: %v", err)
		}
	}()
	content := "op1\nop2\nop3\n"
	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatal(err)
	}
	err = tmpFile.Close()
	if err != nil {
		t.Fatalf("failed to close temp file: %v", err)
	}

	ops, err := readOperationsList(tmpFile.Name())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := []string{"op1", "op2", "op3"}
	if len(ops) != len(expected) {
		t.Fatalf("expected %d, got %d", len(expected), len(ops))
	}
	for i, v := range expected {
		if ops[i] != v {
			t.Errorf("expected %s, got %s", v, ops[i])
		}
	}
}

func TestSaveToFileAndRemoveDuplicates(t *testing.T) {
	tmpFile := filepath.Join(os.TempDir(), "models_test.txt")
	defer func() {
		err := os.Remove(tmpFile)
		if err != nil {
			t.Fatalf("failed to remove temp file: %v", err)
		}
	}()
	models := []string{"A", "B", "A", "C"}
	err := saveToFile(tmpFile, models)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	data, err := os.ReadFile(tmpFile)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 3 {
		t.Errorf("expected 3 unique models, got %d", len(lines))
	}
}

func TestBuildDocModel_Error(t *testing.T) {
	invalidSpec := []byte("invalid yaml")
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic for invalid spec")
		}
	}()
	buildDocModel(invalidSpec)
}

func TestMain_Success(t *testing.T) {
	// Setup temp working directory
	SetupTestEnv()

	main()

	// Check output file
	data, err := os.ReadFile(modelsFilePath)
	if err != nil {
		t.Fatalf("failed to read output file: %v", err)
	}
	if !strings.Contains(string(data), "aggregate") {
		t.Errorf("expected TestModel in output, got: %s", string(data))
	}

	// Check if the file is created
	if _, err := os.Stat(modelsFilePath); os.IsNotExist(err) {
		t.Fatalf("output file does not exist: %v", err)
	}

	// Delete the output file
	err = os.Remove(modelsFilePath)
	if err != nil {
		t.Fatalf("failed to remove output file: %v", err)
	}
}

func TestSaveToFile_ErrorCases(t *testing.T) {
	// Case 1: Invalid file path
	invalidPath := string([]byte{0x00}) // illegal filename
	err := saveToFile(invalidPath, []string{"A", "B"})
	if err == nil {
		t.Error("expected error for invalid file path, got nil")
	}
}

func TestMain_ErrorCases(t *testing.T) {
	// Backup original paths
	origSpecFilePath := specFilePath
	origOperationsFilePath := operationsFilePath
	origModelsFilePath := modelsFilePath

	defer func() {
		specFilePath = origSpecFilePath
		operationsFilePath = origOperationsFilePath
		modelsFilePath = origModelsFilePath
	}()

	// Case 1: operationsFilePath does not exist
	operationsFilePath = "/nonexistent/ops.txt"
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic when operations file does not exist")
		}
	}()
	main()
}
