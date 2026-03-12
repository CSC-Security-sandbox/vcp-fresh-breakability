package handlers

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseCLIError(t *testing.T) {
	t.Run("extracts error message from Error: prefix", func(t *testing.T) {
		message := ParseCLIError("Error: File not found")
		assert.Equal(t, "File not found", message)
	})

	t.Run("extracts error message case insensitive", func(t *testing.T) {
		message := ParseCLIError("error: permission denied")
		assert.Equal(t, "permission denied", message)
	})

	t.Run("returns full output when no error prefix found", func(t *testing.T) {
		output := "Something went wrong with the operation"
		message := ParseCLIError(output)
		assert.Equal(t, output, message)
	})

	t.Run("handles multiline error output", func(t *testing.T) {
		output := `Command failed
Error: Access denied
Please check permissions`
		message := ParseCLIError(output)
		assert.Equal(t, "Access denied", message)
	})
}

func TestOntapCodeToInt(t *testing.T) {
	t.Run("parses numeric code", func(t *testing.T) {
		assert.Equal(t, 404, OntapCodeToInt("404"))
		assert.Equal(t, 400, OntapCodeToInt("400"))
		assert.Equal(t, 13115, OntapCodeToInt("13115"))
	})
	t.Run("returns 400 for unparseable", func(t *testing.T) {
		assert.Equal(t, 400, OntapCodeToInt(""))
		assert.Equal(t, 400, OntapCodeToInt("  "))
		assert.Equal(t, 400, OntapCodeToInt("bad"))
		assert.Equal(t, 400, OntapCodeToInt("4x"))
	})
}

func TestParseSnaplockAbortError(t *testing.T) {
	t.Run("operation is complete returns short message", func(t *testing.T) {
		output := `Error: command failed: SnapLock legal-hold operation is complete. Run "snaplock legal-hold show -operation-id 16842766 -instance" to view the status of operation.`
		msg := ParseSnaplockAbortError(output)
		assert.Equal(t, "SnapLock legal-hold operation is complete; abort only applies to in-progress operations", msg)
	})
	t.Run("not found returns short message", func(t *testing.T) {
		output := "Error: operation 999 not found"
		msg := ParseSnaplockAbortError(output)
		assert.Equal(t, "SnapLock legal-hold operation not found", msg)
	})
	t.Run("other error uses ParseCLIError", func(t *testing.T) {
		output := "Error: permission denied"
		msg := ParseSnaplockAbortError(output)
		assert.Equal(t, "permission denied", msg)
	})
	t.Run("empty output returns as-is", func(t *testing.T) {
		msg := ParseSnaplockAbortError("")
		assert.Empty(t, msg)
	})
}

func TestIsCLISuccess(t *testing.T) {
	t.Run("returns true for success messages", func(t *testing.T) {
		testCases := []struct {
			name   string
			output string
		}{
			{"empty output", ""},
			{"simple success", "OK"},
			{"deleted successfully", "Deleted successfully"},
			{"operation completed", "Operation completed"},
			{"snaplock legal-hold show with No error and Number of Files Failed", `Info: Use 'exit' command to return.

                      Vserver: gcnv-fbc248f62e293c2-svm-01
                       Volume: snaplock_vol_ss_comp
                 Operation ID: 16842754
              Litigation Name: litigation-001
                         Path: /
               Operation Type: begin
                       Status: Completed
    Number of Files Processed: 5
       Number of Files Failed: 0
      Number of Files Skipped: 0
     Number of Inodes Ignored: 0
               Status Details: No error
`},
			{"status details no error", "Operation Status: Completed\n             Status Details: No error\n"},
			{"number of files failed zero", "Number of Files Processed: 0\n     Number of Files Failed: 0\n    Number of Files Skipped: 0\n           Operation Status: Completed\n"},
			{"list output with operation state Failed", "Operation ID   Vserver   Volume          Operation Status\n-------------- --------- --------------- ----------------\n16842760       svm1      snaplock_vol1    Completed\n16842761       svm1      snaplock_vol1    Failed\n2 entries were displayed.\n"},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				assert.True(t, IsCLISuccess(tc.output))
			})
		}
	})

	t.Run("returns false for error messages", func(t *testing.T) {
		testCases := []struct {
			name   string
			output string
		}{
			{"error keyword", "Error: something went wrong"},
			{"failed keyword", "Operation failed"},
			{"not found", "File not found"},
			{"permission denied", "Permission denied"},
			{"access denied", "Access denied"},
			{"invalid", "Invalid parameter"},
			{"case insensitive error", "ERROR: test"},
			{"case insensitive failed", "FAILED to complete"},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				assert.False(t, IsCLISuccess(tc.output))
			})
		}
	})
}

func TestStripOntapLoginBanner(t *testing.T) {
	t.Run("removes first login banner with double newlines", func(t *testing.T) {
		output := "\n\nThis is your first recorded login.\n\nVserver   Volume       Aggregate    State      Type       Size  Available Used%\n--------- ------------ ------------ ---------- ---- ---------- ---------- -----\ngcnv-76ad899c86ae3ab-svm-01 clivo10 aggr1 online RW        1GB    972.3MB    0%\n\n"
		got := StripOntapLoginBanner(output)
		assert.NotContains(t, got, "This is your first recorded login.")
		assert.Contains(t, got, "Vserver   Volume")
		assert.Contains(t, got, "gcnv-76ad899c86ae3ab-svm-01 clivo10 aggr1")
	})

	t.Run("returns empty unchanged", func(t *testing.T) {
		assert.Equal(t, "", StripOntapLoginBanner(""))
	})

	t.Run("leaves output without banner unchanged", func(t *testing.T) {
		output := "Vserver   Volume\n--------- -----\nvol1      aggr1\n"
		assert.Equal(t, output, StripOntapLoginBanner(output))
	})

	t.Run("handles Windows line endings", func(t *testing.T) {
		output := "\r\n\r\nThis is your first recorded login.\r\n\r\nVserver   Volume\r\n"
		got := StripOntapLoginBanner(output)
		assert.NotContains(t, got, "This is your first recorded login.")
		assert.Contains(t, got, "Vserver   Volume")
	})
}
