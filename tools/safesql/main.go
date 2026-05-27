// SafeSQL - Safe SQL Execution Framework for Production Databases
//
// SafeSQL provides a secure way to execute SQL queries against production databases
// with validation, impact analysis, state drift detection, and audit logging.
package main

import (
	"fmt"
	"os"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/tools/safesql/cmd"
)

func main() {
	if err := cmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
