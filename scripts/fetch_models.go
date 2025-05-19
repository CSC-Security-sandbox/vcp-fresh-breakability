package main

import (
	"bufio"
	"io"
	"os"
	"slices"
	"sort"
	"strings"

	"github.com/pb33f/libopenapi"
	"github.com/pb33f/libopenapi/datamodel/high/base"
	v2 "github.com/pb33f/libopenapi/datamodel/high/v2"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

// This script extracts all model references from the swagger.yaml file
// for the given list of operations and saves them to a text file.

var (
	extractFields = _extractFields
	logger        = log.NewLogger()
	workingDir, _ = os.Getwd()

	// Paths to the swagger spec file and the operation's list file
	specFilePath       = workingDir + "/swagger.yaml"
	operationsFilePath = workingDir + "/swagger_operations.txt"

	// Path to the output file where the model references will be saved
	modelsFilePath = workingDir + "/swagger_models.txt"
)

func main() {
	// Read the operation's list from the file
	operationsList, err := readOperationsList(operationsFilePath)
	if err != nil {
		logger.Errorf("cannot open file: %e", err)
		panic(err)
	}

	// Read the swagger spec file
	specFile, _ := os.ReadFile(specFilePath)

	// Create a new document model from the spec file
	docmodel := buildDocModel(specFile)

	// Extract all model references from the request and response bodies
	modelsList := extractModels(docmodel, operationsList)

	// Fetch all dependent models
	modelsList = appendNestedModels(docmodel, modelsList)

	// Save the model's list to a file
	err = saveToFile(modelsFilePath, modelsList)
	if err != nil {
		logger.Errorf("Error saving to file: %v\n", err)
		panic(err)
	}
}

func readOperationsList(operationsFilePath string) ([]string, error) {
	operationsList := []string{}
	operationsFile, err := os.Open(operationsFilePath)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err = operationsFile.Close(); err != nil {
			logger.Errorf("cannot close file: %e", err)
			panic(err)
		}
	}()
	operationsFileReader := bufio.NewReader(operationsFile)
	for {
		line, err := operationsFileReader.ReadString('\n')
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		operationsList = append(operationsList, strings.TrimSpace(line))
	}
	return operationsList, nil
}

func buildDocModel(specFile []byte) *libopenapi.DocumentModel[v2.Swagger] {
	document, err := libopenapi.NewDocument(specFile)
	if err != nil {
		logger.Errorf("cannot create new document: %e", err)
		panic(err)
	}
	docmodel, errors := document.BuildV2Model()
	if len(errors) > 0 {
		for i := range errors {
			logger.Errorf("error: %e\n", errors[i])
		}
		logger.Errorf("cannot create model from document: %d errors reported", len(errors))
		panic(errors)
	}
	return docmodel
}

func extractModels(docmodel *libopenapi.DocumentModel[v2.Swagger], operationsList []string) []string {
	modelsList := []string{}
	for _, path := range docmodel.Model.Paths.PathItems.FromNewest() {
		for _, operation := range path.GetOperations().FromNewest() {
			if slices.Contains(operationsList, operation.OperationId) {
				// Extract all model references from the request body
				if operation.Parameters != nil {
					for _, param := range operation.Parameters {
						if param.Schema != nil && param.Schema.GetReference() != "" {
							refArr := strings.Split(param.Schema.GetReference(), "/")
							modelsList = append(modelsList, refArr[len(refArr)-1])
						}
						if param.Schema != nil && param.Schema.Schema().Properties != nil && param.Schema.Schema().Properties.OrderedMap != nil {
							for _, prop := range param.Schema.Schema().Properties.OrderedMap.FromNewest() {
								if prop.Schema().Items != nil && prop.Schema().Items.A != nil {
									if ref := prop.Schema().Items.A.GetReference(); ref != "" {
										refArr := strings.Split(ref, "/")
										modelsList = append(modelsList, refArr[len(refArr)-1])
									}
								}
								if ref := prop.GetReference(); ref != "" {
									refArr := strings.Split(ref, "/")
									modelsList = append(modelsList, refArr[len(refArr)-1])
								}
							}
						}
					}
				}
				// Extract all model references from the response body
				if operation.Responses != nil {
					for _, response := range operation.Responses.Codes.FromNewest() {
						if response.Schema != nil && response.Schema.GetReference() != "" {
							refArr := strings.Split(response.Schema.GetReference(), "/")
							modelsList = append(modelsList, refArr[len(refArr)-1])
						}
					}
					response := operation.Responses.Default
					if response.Schema != nil && response.Schema.GetReference() != "" {
						refArr := strings.Split(response.Schema.GetReference(), "/")
						modelsList = append(modelsList, refArr[len(refArr)-1])
					}
				}
			}
		}
	}
	return modelsList
}

func appendNestedModels(docmodel *libopenapi.DocumentModel[v2.Swagger], modelsList []string) []string {
	for key, prop := range docmodel.Model.Definitions.Definitions.FromNewest() {
		if slices.Contains(modelsList, key) {
			dependentModels := fetchDependentModelsIterative(key, docmodel.Model.Definitions, prop)
			modelsList = append(modelsList, dependentModels...)
		}
	}
	return modelsList
}

func removeDuplicates(slice []string) []string {
	set := make(map[string]struct{})
	for _, item := range slice {
		set[item] = struct{}{}
	}

	res := []string{}
	for key := range set {
		res = append(res, key)
	}
	sort.Strings(res)
	return res
}

func saveToFile(modelsFilePath string, modelsList []string) error {
	outputFile, err := os.Create(modelsFilePath)
	if err != nil {
		logger.Errorf("Error creating file: %v\n", err)
		return err
	}
	defer func() {
		if err = outputFile.Close(); err != nil {
			logger.Errorf("Error closing file: %v\n", err)
			panic(err)
		}
	}()

	writer := bufio.NewWriter(outputFile)
	for _, val := range removeDuplicates(modelsList) {
		_, err = writer.WriteString(val + "\n")
		if err != nil {
			logger.Errorf("Error writing to file: %v\n", err)
			return err
		}
	}
	err = writer.Flush()
	if err != nil {
		logger.Errorf("Error flushing writer: %v\n", err)
		return err
	}

	return nil
}

func fetchDependentModelsIterative(modelName string, docDef *v2.Definitions, proxy *base.SchemaProxy) []string {
	dependentModels := []string{}
	stack := []string{modelName}
	stack2 := []*base.SchemaProxy{proxy}
	visited := make(map[string]bool)
	for len(stack) > 0 {
		currentModel := stack[len(stack)-1]
		def := stack2[len(stack2)-1]

		stack = stack[:len(stack)-1]
		stack2 = stack2[:len(stack2)-1]

		if visited[currentModel] {
			continue
		}
		visited[currentModel] = true
		dependentModels, stack, stack2 = extractFields(currentModel, def, dependentModels, stack, stack2)

		if temp, ok := docDef.Definitions.Get(currentModel); ok {
			def = temp
			dependentModels, stack, stack2 = extractFields(currentModel, def, dependentModels, stack, stack2)
		}
	}

	return dependentModels
}

func _extractFields(currentModel string, def *base.SchemaProxy, dependentModels []string, stack []string, stack2 []*base.SchemaProxy) ([]string, []string, []*base.SchemaProxy) {
	for key, prop := range def.Schema().Properties.FromNewest() {
		stack = append(stack, key)
		stack2 = append(stack2, prop)
		if prop.GetReference() != "" {
			refArr := strings.Split(prop.GetReference(), "/")
			dependentModels = append(dependentModels, refArr[len(refArr)-1])
			stack = append(stack, refArr[len(refArr)-1])
			stack2 = append(stack2, prop)
		}
		if prop.Schema().Items != nil && prop.Schema().Items.A != nil {
			if ref := prop.Schema().Items.A.GetReference(); ref != "" {
				refArr := strings.Split(ref, "/")
				dependentModels = append(dependentModels, refArr[len(refArr)-1])
				stack = append(stack, refArr[len(refArr)-1])
				stack2 = append(stack2, prop.Schema().Items.A)
			}
		}
	}

	if def.Schema().Properties != nil && def.Schema().Properties.OrderedMap != nil {
		for key, prop := range def.Schema().Properties.OrderedMap.FromNewest() {
			stack = append(stack, key)
			stack2 = append(stack2, prop)
			if prop.GetReference() != "" {
				refArr := strings.Split(prop.GetReference(), "/")
				dependentModels = append(dependentModels, refArr[len(refArr)-1])
				stack = append(stack, refArr[len(refArr)-1])
				stack2 = append(stack2, prop)
			}
			if prop.Schema().Items != nil && prop.Schema().Items.A != nil {
				if ref := prop.Schema().Items.A.GetReference(); ref != "" {
					refArr := strings.Split(ref, "/")
					dependentModels = append(dependentModels, refArr[len(refArr)-1])
					stack = append(stack, refArr[len(refArr)-1])
					stack2 = append(stack2, prop.Schema().Items.A)
				}
			}
		}
	}

	if def.Schema().Items != nil && def.Schema().Items.A != nil {
		stack = append(stack, currentModel+"Items")
		stack2 = append(stack2, def.Schema().Items.A)
		if ref := def.Schema().Items.A.GetReference(); ref != "" {
			refArr := strings.Split(ref, "/")
			dependentModels = append(dependentModels, refArr[len(refArr)-1])
			stack = append(stack, refArr[len(refArr)-1])
			stack2 = append(stack2, def.Schema().Items.A)
		}
	}

	return dependentModels, stack, stack2
}
