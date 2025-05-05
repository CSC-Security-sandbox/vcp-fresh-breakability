//go:build !exclude_from_cover_pkg_all

package main

import (
	"regexp"
	"strings"
)

func panicOnError(err error) {
	if err != nil {
		panic(err)
	}
}

func isSystemImport(path string) bool {
	return !strings.Contains(path, ".") || strings.Index(path, "/") <= strings.Index(path, ".")
}

func toSnakeCase(name string) string {
	snake := regexp.MustCompile("(.)([A-Z][a-z]+)").ReplaceAllString(name, "${1}_${2}")
	snake = regexp.MustCompile("([a-z0-9])([A-Z])").ReplaceAllString(snake, "${1}_${2}")
	return strings.ToLower(snake)
}
