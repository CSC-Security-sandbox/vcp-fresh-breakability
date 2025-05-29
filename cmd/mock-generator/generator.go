//go:build !exclude_from_cover_pkg_all

package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"text/template"
)

var (
	generateFile        = _generateFile
	writeTemplateToFile = _writeTemplateToFile
	formatFile          = _formatFile
)

var execCommand = exec.Command

func generateMock(srcFileName, interfaceName string) {
	data := parseSource(srcFileName, interfaceName)
	generateFile(fmt.Sprintf("%s_mock.go", toSnakeCase(interfaceName)), mockTemplate, data)
	generateFile(fmt.Sprintf("%s_mock_test.go", toSnakeCase(interfaceName)), testTemplate, data)
	log.Println("Code generation complete!")
}

func _generateFile(fileName string, template *template.Template, data *templateData) {
	log.Printf("Generating: %s\n", fileName)
	writeTemplateToFile(fileName, template, data)
	formatFile(fileName)
}

func _writeTemplateToFile(fileName string, template *template.Template, data *templateData) {
	file, err := os.Create(fileName)
	panicOnError(err)
	defer func() { _ = file.Close() }()
	panicOnError(template.Execute(file, *data))
}

func _formatFile(fileName string) {
	log.Printf("Formatting: %s\n", fileName)
	panicOnError(execCommand("goimports", "-w", fileName).Run())
}
