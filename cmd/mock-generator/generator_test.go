package main

import (
	"os"
	"os/exec"
	"testing"
	"text/template"

	"github.com/stretchr/testify/assert"
)

func TestWriteTemplateToFile_Success(t *testing.T) {
	tmp := "test_output.go"

	defer func() {
		if err := os.Remove(tmp); err != nil {
			t.Logf("cleanup failed: %v", err)
		}
	}()

	tmpl := template.Must(template.New("t").Parse("package {{.PackageName}}"))
	data := &templateData{PackageName: "foo"}

	assert.NotPanics(t, func() {
		writeTemplateToFile(tmp, tmpl, data)
	})

	b, err := os.ReadFile(tmp)
	assert.NoError(t, err)
	assert.Equal(t, "package foo", string(b))
}

func TestWriteTemplateToFile_FileCreateError(t *testing.T) {
	tmpl := template.Must(template.New("t").Parse("x"))
	assert.Panics(t, func() {
		writeTemplateToFile("/no_such_dir/file.go", tmpl, &templateData{})
	})
}

func TestWriteTemplateToFile_TemplateError(t *testing.T) {
	tmp := "test_bad.go"

	defer func() {
		if err := os.Remove(tmp); err != nil {
			t.Logf("cleanup failed: %v", err)
		}
	}()

	tmpl := template.Must(template.New("t").Parse("{{.NoSuchField}}"))
	assert.Panics(t, func() {
		writeTemplateToFile(tmp, tmpl, &templateData{})
	})
}

func TestFormatFile_Success(t *testing.T) {
	tmp := "test_fmt.go"
	_ = os.WriteFile(tmp, []byte("package foo"), 0644)

	defer func() {
		if err := os.Remove(tmp); err != nil {
			t.Logf("cleanup failed: %v", err)
		}
	}()

	origExec := execCommand
	defer func() { execCommand = origExec }()
	execCommand = func(name string, arg ...string) *exec.Cmd {
		return exec.Command("true")
	}

	assert.NotPanics(t, func() {
		formatFile(tmp)
	})
}

func TestFormatFile_CommandError(t *testing.T) {
	origExec := execCommand
	defer func() { execCommand = origExec }()
	execCommand = func(name string, arg ...string) *exec.Cmd {
		return exec.Command("false")
	}
	assert.Panics(t, func() {
		formatFile("file.go")
	})
}

func TestGenerateFile_CallsWriteAndFormat(t *testing.T) {
	tmp := "test_gen.go"

	defer func() {
		if err := os.Remove(tmp); err != nil {
			t.Logf("cleanup failed: %v", err)
		}
	}()

	calledWrite := false
	calledFormat := false

	origWrite := writeTemplateToFile
	origFormat := formatFile
	defer func() {
		writeTemplateToFile = origWrite
		formatFile = origFormat
	}()
	writeTemplateToFile = func(fileName string, tmpl *template.Template, data *templateData) {
		calledWrite = true
	}
	formatFile = func(fileName string) {
		calledFormat = true
	}

	generateFile(tmp, template.Must(template.New("t").Parse("x")), &templateData{})
	assert.True(t, calledWrite)
	assert.True(t, calledFormat)
}

func TestGenerateMock_CallsGenerateFile(t *testing.T) {
	called := 0
	origParse := parseSource
	origGen := generateFile
	defer func() {
		parseSource = origParse
		generateFile = origGen
	}()
	parseSource = func(srcFileName, interfaceName string) *templateData {
		return &templateData{PackageName: "foo"}
	}
	generateFile = func(fileName string, tmpl *template.Template, data *templateData) {
		called++
	}
	generateMock("src.go", "Iface")
	assert.Equal(t, 2, called)
}
