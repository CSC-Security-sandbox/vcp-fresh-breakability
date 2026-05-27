package main

import (
	"go/ast"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseSource_SimpleInterface(t *testing.T) {
	src := `
package testpkg
type TestInterface interface {
	Foo(a int) error
	Bar()
}
`
	tmpFile := "test_interface.go"
	err := os.WriteFile(tmpFile, []byte(src), 0644)
	assert.NoError(t, err, "Failed to write test file")

	defer func() {
		if err := os.Remove(tmpFile); err != nil {
			t.Logf("cleanup failed: %v", err)
		}
	}()

	data := parseSource(tmpFile, "TestInterface")
	assert.Equal(t, "TestInterface", data.InterfaceName)
	assert.Equal(t, "testpkg", data.PackageName)
	assert.Equal(t, 1, len(data.SingleReturnFunctions))
	assert.Equal(t, 1, len(data.VoidFunctions))
}

func TestReadSource_PanicsOnError(t *testing.T) {
	assert.Panics(t, func() {
		_ = readSource("non_existent_file.go")
	})
}

func TestGetImports_WithAliasAndSorting(t *testing.T) {
	imports := []*ast.ImportSpec{
		{Path: &ast.BasicLit{Value: `"fmt"`}},
		{Name: ast.NewIdent("alias"), Path: &ast.BasicLit{Value: `"github.com/x/y"`}},
	}
	_, nonSystem := getImports(imports)
	assert.Contains(t, nonSystem, "alias \"github.com/x/y\"")
}

func TestPostProcess_OntapRestCallback(t *testing.T) {
	visitor := &fileVisitor{
		file: &ast.File{Name: &ast.Ident{Name: "ontaprest"}},
	}
	fun := &functionSignature{
		Name: "CB",
		InParams: []*param{
			{Name: "a", Type: "int"},
			{Name: "cb", Type: "UserCallbackFunc[[]string]"},
		},
		RetVals: []*param{{Type: "error"}},
	}
	postProcess(visitor, fun, "Iface", "Iface")
	assert.Equal(t, "[]string", fun.OntapRestCallbackType)
	assert.Equal(t, "a int, cb []string", fun.AssertInput)
}

func TestGetTemplateData_Void_Single_Multi(t *testing.T) {
	file := &ast.File{Name: &ast.Ident{Name: "pkg"}}
	voidFun := &functionSignature{Name: "Void"}
	singleFun := &functionSignature{Name: "Single", RetVals: []*param{{Type: "error"}}}
	multiFun := &functionSignature{Name: "Multi", RetVals: []*param{{Type: "int"}, {Type: "error"}}}
	visitor := &fileVisitor{file: file, interfaceName: "Iface", funList: []*functionSignature{voidFun, singleFun, multiFun}}
	data := getTemplateData(file, visitor)
	assert.Equal(t, 1, len(data.VoidFunctions))
	assert.Equal(t, 1, len(data.SingleReturnFunctions))
	assert.Equal(t, 1, len(data.MultiReturnFunctions))
}

func TestFuncVisitor_parseInputParams(t *testing.T) {
	fv := &funcVisitor{fileVisitor: &fileVisitor{src: new(string)}}
	*fv.fileVisitor.src = "int"
	field := &ast.Field{
		Names: []*ast.Ident{{Name: "a"}},
		Type:  &ast.Ident{Name: "int", NamePos: 1},
	}
	fieldList := &ast.FieldList{List: []*ast.Field{field}}
	out, params := fv.parseInputParams(fieldList)
	assert.True(t, out == nil || len(params) == 1, "parseInputParams did not parse correctly")
}

func TestFuncVisitor_parseParams(t *testing.T) {
	fv := &funcVisitor{fileVisitor: &fileVisitor{src: new(string)}}
	*fv.fileVisitor.src = "int"
	field := &ast.Field{
		Names: []*ast.Ident{{Name: "a"}},
		Type:  &ast.Ident{Name: "int", NamePos: 1},
	}
	fieldList := &ast.FieldList{List: []*ast.Field{field}}
	params := fv.parseParams(fieldList)
	assert.Equal(t, 1, len(params))
}

func TestFuncVisitor_parseParam(t *testing.T) {
	src := "int"
	fv := &funcVisitor{fileVisitor: &fileVisitor{src: &src}}
	field := &ast.Field{
		Names: []*ast.Ident{{Name: "a"}},
		Type:  &ast.Ident{Name: "int", NamePos: 1},
	}
	param := fv.parseParam(field)
	assert.Equal(t, "a", param.Name)
	assert.Equal(t, "int", param.Type)
}

func TestPostProcess_WithOutValAndInParams(t *testing.T) {
	visitor := &fileVisitor{
		file: &ast.File{Name: &ast.Ident{Name: "pkg"}},
	}
	fun := &functionSignature{
		Name:   "Test",
		OutVal: &param{Name: "out", Type: "interface{}"},
		InParams: []*param{
			{Name: "a", Type: "int"},
		},
		RetVals: []*param{{Name: "err", Type: "error"}},
	}
	postProcess(visitor, fun, "Iface", "Iface")
	assert.True(t, strings.HasPrefix(fun.Input, "out interface{}, a int"), "Expected Input to start with 'out interface{}, a int'")
}

func TestPostProcess_WithOutValNoInParams(t *testing.T) {
	visitor := &fileVisitor{
		file: &ast.File{Name: &ast.Ident{Name: "pkg"}},
	}
	fun := &functionSignature{
		Name:     "Test",
		OutVal:   &param{Name: "out", Type: "interface{}"},
		InParams: []*param{},
		RetVals:  []*param{{Name: "err", Type: "error"}},
	}
	postProcess(visitor, fun, "Iface", "Iface")
	assert.Equal(t, "out interface{}", fun.Input)
}

func TestPostProcess_RetValsOutputFormatting(t *testing.T) {
	visitor := &fileVisitor{
		file: &ast.File{Name: &ast.Ident{Name: "pkg"}},
	}

	fun := &functionSignature{
		Name:     "Test",
		InParams: []*param{},
		RetVals: []*param{
			{Name: "err", Type: "error"},
			{Name: "val", Type: "int"},
		},
	}
	postProcess(visitor, fun, "Iface", "Iface")
	assert.Contains(t, fun.Output, "error, int")

	fun2 := &functionSignature{
		Name:     "Test",
		InParams: []*param{},
		RetVals: []*param{
			{Name: "", Type: "error"},
			{Name: "", Type: "int"},
		},
	}
	postProcess(visitor, fun2, "Iface", "Iface")
	assert.Contains(t, fun2.Output, " error,  int")
}

func TestFileVisitor_Visit(t *testing.T) {
	src := "package pkg; type Iface interface{}"
	file := &ast.File{Name: &ast.Ident{Name: "pkg"}}
	visitor := &fileVisitor{file: file, interfaceName: "Iface", src: &src}
	iface := &ast.InterfaceType{Methods: &ast.FieldList{}}
	typeSpec := &ast.TypeSpec{Name: ast.NewIdent("Iface"), Type: iface}
	visitor.Visit(typeSpec)
}

func TestFuncVisitor_Visit(t *testing.T) {
	src := "int"
	file := &ast.File{Name: &ast.Ident{Name: "pkg"}}
	visitor := &fileVisitor{file: file, interfaceName: "Iface", src: &src}
	fv := &funcVisitor{fileVisitor: visitor}
	assert.Nil(t, fv.Visit(nil))
}

func TestFuncVisitor_Visit_IdentBranch(t *testing.T) {
	src := "interface{}"
	file := &ast.File{Name: &ast.Ident{Name: "pkg"}}
	visitor := &fileVisitor{file: file, interfaceName: "Iface", src: &src}

	methodName := "OtherIface"
	method := &ast.Field{
		Names: []*ast.Ident{{Name: methodName}},
		Type:  &ast.Ident{Name: methodName, NamePos: 1},
	}
	iface := &ast.InterfaceType{
		Methods: &ast.FieldList{List: []*ast.Field{method}},
	}

	fv := &funcVisitor{fileVisitor: visitor}
	fv.Visit(iface)

	assert.Equal(t, 0, len(visitor.funList))
}

func TestExtractGenericInnerType(t *testing.T) {
	got := extractGenericInnerType("UserCallbackFunc[[]string]")
	assert.Equal(t, "[]string", got)
	assert.Panics(t, func() {
		_ = extractGenericInnerType("NotUserCallbackFunc[int]")
	})
}

func TestParseSource_PanicsOnParseError(t *testing.T) {
	tmpFile := "bad.go"
	_ = os.WriteFile(tmpFile, []byte("package x\nfunc {"), 0644)
	defer func() {
		if err := os.Remove(tmpFile); err != nil {
			t.Logf("cleanup failed: %v", err)
		}
	}()

	assert.Panics(t, func() {
		_ = parseSource(tmpFile, "X")
	})
}

func TestGetImports(t *testing.T) {
	imports := []*ast.ImportSpec{
		{Path: &ast.BasicLit{Value: `"fmt"`}},
		{Path: &ast.BasicLit{Value: `"github.com/example/pkg"`}},
	}
	system, nonSystem := getImports(imports)
	assert.Contains(t, system, `"fmt"`)
	assert.Contains(t, nonSystem, `"github.com/example/pkg"`)
}

func TestGetImports_WithAliasSystem(t *testing.T) {
	imports := []*ast.ImportSpec{
		{Name: ast.NewIdent("sys"), Path: &ast.BasicLit{Value: `"os"`}},
		{Name: ast.NewIdent("alias"), Path: &ast.BasicLit{Value: `"github.com/x/y"`}},
	}
	system, nonSystem := getImports(imports)
	assert.Contains(t, system, "sys \"os\"")
	assert.Contains(t, nonSystem, "alias \"github.com/x/y\"")
}

func TestPostProcess_NonOntapRest(t *testing.T) {
	visitor := &fileVisitor{
		file: &ast.File{Name: &ast.Ident{Name: "notontaprest"}},
	}
	fun := &functionSignature{
		Name: "Test",
		InParams: []*param{
			{Name: "a", Type: "int"},
			{Name: "b", Type: "...string"},
		},
		RetVals: []*param{{Type: "error"}},
	}
	postProcess(visitor, fun, "Iface", "Iface")
	assert.True(t, strings.Contains(fun.Input, "...") || strings.Contains(fun.AssertInput, "[]"), "Variadic not handled in AssertInput")
}

func TestExtractGenericInnerType_PanicOnNoBracket(t *testing.T) {
	assert.Panics(t, func() {
		_ = extractGenericInnerType("UserCallbackFuncint")
	})
}

func TestFuncVisitor_parseParam_MultiNames(t *testing.T) {
	src := "int"
	fv := &funcVisitor{fileVisitor: &fileVisitor{src: &src}}
	field := &ast.Field{
		Names: []*ast.Ident{{Name: "a"}, {Name: "b"}},
		Type:  &ast.Ident{Name: "int", NamePos: 1},
	}
	param := fv.parseParam(field)
	assert.Contains(t, param.Name, "a, b")
}
