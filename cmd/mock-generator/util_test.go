package main

import (
	"errors"
	"testing"
)

func TestPanicOnError_Panics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on error")
		}
	}()
	panicOnError(errors.New("fail"))
}

func TestPanicOnError_NoPanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Error("unexpected panic")
		}
	}()
	panicOnError(nil)
}

func TestIsSystemImport(t *testing.T) {
	tests := []struct {
		path string
		want bool
	}{
		{`"fmt"`, true},
		{`"os"`, true},
		{`"strings"`, true},
		{`"github.com/foo/bar"`, false},
		{`"example.com/pkg"`, false},
		{`"net/http"`, true},
	}
	for _, tt := range tests {
		if got := isSystemImport(tt.path); got != tt.want {
			t.Errorf("isSystemImport(%q) = %v, want %v", tt.path, got, tt.want)
		}
	}
}

func TestToSnakeCase(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"MyInterface", "my_interface"},
		{"HTTPServer", "http_server"},
		{"SomeID", "some_id"},
		{"simple", "simple"},
		{"X", "x"},
	}
	for _, tt := range tests {
		if got := toSnakeCase(tt.in); got != tt.want {
			t.Errorf("toSnakeCase(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}
