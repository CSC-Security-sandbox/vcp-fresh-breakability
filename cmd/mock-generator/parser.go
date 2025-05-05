//go:build !exclude_from_cover_pkg_all

package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"log"
	"os"
	"sort"
	"strings"
)

type param struct {
	Name       string
	Type       string
	AssertType string
	Variant    bool
}

type functionSignature struct {
	InterfaceName          string
	InterfaceNameUpper     string
	Name                   string
	Input                  string // parameter string in the function signature
	AssertInput            string // parameter string in the assert function signature
	Output                 string // return value string in the function signature
	InParams               []*param
	OutVal                 *param // this is an input parameter, treated as an output parameter.
	RetVals                []*param
	OntapRestCallbackType  string
	OntapRestCallbackValue string
}

type fileVisitor struct {
	file          *ast.File
	interfaceName string
	src           *string
	parsed        bool
	funList       []*functionSignature
}

type templateData struct {
	PackageName                string
	InterfaceName              string
	InterfaceNameUpper         string
	SystemImports              []string
	NonSystemImports           []string
	VoidFunctions              []*functionSignature
	SingleReturnFunctions      []*functionSignature
	MultiReturnFunctions       []*functionSignature
	OntapRestCallbackFunctions []*functionSignature
}

func (v *fileVisitor) Visit(node ast.Node) ast.Visitor {
	switch t := node.(type) {
	case *ast.Ident:
		if t.Name == v.interfaceName && !v.parsed {
			log.Printf("Parsing interface: %s\n", v.interfaceName)
			switch spec := t.Obj.Decl.(type) {
			case *ast.TypeSpec:
				switch typ := spec.Type.(type) {
				case *ast.InterfaceType:
					log.Printf("Processing functions... len(typ.Methods.List): %d\n", len(typ.Methods.List))
					ast.Walk(&funcVisitor{fileVisitor: v}, typ)
				}
			}
			v.parsed = true
		}
	}
	return v
}

type funcVisitor struct {
	fileVisitor *fileVisitor
}

func (v *funcVisitor) Visit(node ast.Node) ast.Visitor {
	switch typ := node.(type) {
	case *ast.InterfaceType:
		for _, method := range typ.Methods.List {
			switch fun := method.Type.(type) {
			case *ast.FuncType:
				outval, params := v.parseInputParams(fun.Params)
				v.fileVisitor.funList = append(v.fileVisitor.funList, &functionSignature{
					Name:     method.Names[0].Name,
					InParams: params,
					OutVal:   outval,
					RetVals:  v.parseParams(fun.Results),
				})
			case *ast.Ident:
				visitor := &fileVisitor{file: v.fileVisitor.file, interfaceName: fun.Name, src: v.fileVisitor.src}
				ast.Walk(visitor, v.fileVisitor.file)
				v.fileVisitor.funList = append(v.fileVisitor.funList, visitor.funList...)
			}
		}
	}
	return nil
}

func (v *funcVisitor) parseInputParams(fieldList *ast.FieldList) (*param, []*param) {
	var outParam *param
	params := make([]*param, 0)
	if fieldList != nil {
		for _, field := range fieldList.List {
			param := v.parseParam(field)
			if param.Name == "out" && param.Type == "interface{}" {
				outParam = param
			} else {
				params = append(params, param)
			}
		}
	}
	return outParam, params
}

func (v *funcVisitor) parseParams(fieldList *ast.FieldList) []*param {
	params := make([]*param, 0)
	if fieldList != nil {
		for _, field := range fieldList.List {
			params = append(params, v.parseParam(field))
		}
	}
	return params
}

func (v *funcVisitor) parseParam(field *ast.Field) *param {
	paramName := ""
	for i, n := range field.Names {
		if i > 0 && i < len(field.Names) {
			paramName = paramName + ", "
		}
		paramName = paramName + n.Name
	}
	paramType := (*v.fileVisitor.src)[field.Type.Pos()-1 : field.Type.End()-1]
	return &param{Name: paramName, Type: paramType, AssertType: strings.Replace(paramType, "...", "[]", 1), Variant: strings.HasPrefix(paramType, "...")}
}

func parseSource(sourceFileName, interfaceName string) *templateData {
	src := readSource(sourceFileName)
	file, err := parser.ParseFile(token.NewFileSet(), sourceFileName, src, parser.AllErrors)
	panicOnError(err)

	visitor := &fileVisitor{file: file, interfaceName: interfaceName, src: &src}
	ast.Walk(visitor, file)

	return getTemplateData(file, visitor)
}

func readSource(fileName string) string {
	data, err := os.ReadFile(fileName)
	panicOnError(err)
	return string(data)
}

func getTemplateData(file *ast.File, visitor *fileVisitor) *templateData {
	interfaceName := visitor.interfaceName

	interfaceNameUpper := strings.ToUpper(interfaceName[:1]) + interfaceName[1:]

	systemImports, nonSystemImports := getImports(file.Imports)

	voidList := make([]*functionSignature, 0)
	soloList := make([]*functionSignature, 0)
	multiList := make([]*functionSignature, 0)

	ontapRestCallbackFuncList := make([]*functionSignature, 0)

	for _, fun := range visitor.funList {
		log.Printf("Analysing function... %s", fun.Name)

		postProcess(visitor, fun, interfaceName, interfaceNameUpper)

		if fun.OntapRestCallbackType != "" {
			switch fun.OntapRestCallbackType {
			case "[]string":
				fun.OntapRestCallbackValue = `"",""`
			default:
				fun.OntapRestCallbackValue = `{},{}`
			}
			ontapRestCallbackFuncList = append(ontapRestCallbackFuncList, fun)
		} else {
			if len(fun.RetVals) < 1 {
				voidList = append(voidList, fun)
			} else if len(fun.RetVals) < 2 {
				soloList = append(soloList, fun)
			} else {
				multiList = append(multiList, fun)
			}
		}
	}

	return &templateData{
		PackageName:                file.Name.Name,
		SystemImports:              systemImports,
		NonSystemImports:           nonSystemImports,
		InterfaceName:              interfaceName,
		InterfaceNameUpper:         interfaceNameUpper,
		VoidFunctions:              voidList,
		SingleReturnFunctions:      soloList,
		MultiReturnFunctions:       multiList,
		OntapRestCallbackFunctions: ontapRestCallbackFuncList,
	}
}

func getImports(importSpecs []*ast.ImportSpec) ([]string, []string) {
	var nonSystemImports []string

	systemImports := []string{
		`"reflect"`,
		`"runtime"`,
		`"sync"`,
		`"testing"`,
	}

	nameMapping := make(map[string]string)
	nameMapping[`"runtime"`] = "systemruntime"

	for _, importSpec := range importSpecs {
		if isSystemImport(importSpec.Path.Value) {
			systemImports = append(systemImports, importSpec.Path.Value)
		} else {
			nonSystemImports = append(nonSystemImports, importSpec.Path.Value)
		}

		if importSpec.Name != nil {
			nameMapping[importSpec.Path.Value] = importSpec.Name.Name
		}
	}

	sort.Strings(systemImports)
	sort.Strings(nonSystemImports)

	for i, path := range systemImports {
		if name, found := nameMapping[path]; found {
			systemImports[i] = name + " " + path
		}
	}
	for i, path := range nonSystemImports {
		if name, found := nameMapping[path]; found {
			nonSystemImports[i] = name + " " + path
		}
	}

	return systemImports, nonSystemImports
}

func postProcess(visitor *fileVisitor, funSig *functionSignature, interfaceName string, interfaceNameUpper string) {
	// Prepare the function input parameter string
	if funSig.OutVal != nil {
		funSig.Input = funSig.OutVal.Name + " " + funSig.OutVal.Type
		if len(funSig.InParams) > 0 {
			funSig.Input = funSig.Input + ", "
		}
	}

	// Prepare the function output parameter string
	for i, out := range funSig.RetVals {
		if i > 0 {
			funSig.Output = funSig.Output + ", "
		}
		if out.Name != "" {
			funSig.Output = funSig.Output + out.Type
		} else {
			funSig.Output = funSig.Output + out.Name + " " + out.Type
		}
	}

	if visitor.file.Name.Name == "ontaprest" &&
		(len(funSig.InParams) == 2 && strings.HasPrefix(funSig.InParams[1].Type, "UserCallbackFunc")) &&
		(len(funSig.RetVals) == 1 && funSig.RetVals[0].Type == "error") {
		callbackType := extractGenericInnerType(funSig.InParams[1].Type)

		funSig.Input = funSig.InParams[0].Name + " " + funSig.InParams[0].Type + ", " + funSig.InParams[1].Name + " " + funSig.InParams[1].Type
		funSig.AssertInput = funSig.InParams[0].Name + " " + funSig.InParams[0].Type + ", " + funSig.InParams[1].Name + " " + callbackType
		funSig.OntapRestCallbackType = callbackType
	} else {
		for i, in := range funSig.InParams {
			if i > 0 {
				funSig.Input = funSig.Input + ", "
			}
			funSig.Input = funSig.Input + in.Name + " " + in.Type
		}
		funSig.AssertInput = strings.Replace(funSig.Input, "...", "[]", 1)
	}

	funSig.InterfaceName = interfaceName
	funSig.InterfaceNameUpper = interfaceNameUpper
}

func extractGenericInnerType(s string) string {
	if !strings.HasPrefix(s, "UserCallbackFunc") {
		panic(fmt.Sprintf("malformed generic type in ontaprest.UserCallbackFunc: %s", s))
	}
	s = strings.TrimPrefix(s, "UserCallbackFunc")
	_, a, ok := strings.Cut(s, "[")
	if !ok {
		panic(fmt.Sprintf("malformed generic type in ontaprest.UserCallbackFunc: %s", s))
	}
	return strings.TrimRight(a, "]")
}
