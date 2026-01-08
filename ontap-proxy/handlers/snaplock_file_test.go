package handlers

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildSnaplockDeleteCommand(t *testing.T) {
	t.Run("builds correct command format", func(t *testing.T) {
		filePath := "/vol/test-volume/dir/file.txt"
		vserverName := "test-svm"

		command := BuildSnaplockDeleteCommand(filePath, vserverName)

		// Command format: vserver context -vserver <vserver_name> -username snaplock-user;vol file privileged-delete -file <filepath>
		assert.Contains(t, command, "vserver context -vserver "+vserverName)
		assert.Contains(t, command, "-username "+SnaplockUsername)
		assert.Contains(t, command, "vol file privileged-delete")
		assert.Contains(t, command, "-file "+filePath)
	})

	t.Run("handles special characters in path", func(t *testing.T) {
		filePath := "/vol/test-volume/dir with spaces/file-name_v1.txt"
		vserverName := "svm_production"

		command := BuildSnaplockDeleteCommand(filePath, vserverName)

		assert.Contains(t, command, filePath)
		assert.Contains(t, command, vserverName)
	})
}
