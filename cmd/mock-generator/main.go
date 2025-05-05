//go:build !exclude_from_cover_pkg_all

package main

import (
	"log"
	"os"
)

func main() {
	if len(os.Args) != 3 {
		log.Printf("Usage: %s <source-dir> <interface-name>\n", os.Args[0])
		os.Exit(1)
	}

	generateMock(os.Args[1], os.Args[2])
}
