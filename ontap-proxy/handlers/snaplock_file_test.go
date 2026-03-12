package handlers

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/utils"
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

func TestBuildSnaplockLegalHoldBeginCommand(t *testing.T) {
	cmd := BuildSnaplockLegalHoldBeginCommand("lit1", "vol1", "/dir1", "svm1")
	assert.Contains(t, cmd, "snaplock legal-hold begin")
	assert.Contains(t, cmd, "-litigation-name lit1")
	assert.Contains(t, cmd, "-volume vol1")
	assert.Contains(t, cmd, "-path /dir1")
	assert.Contains(t, cmd, "-vserver svm1")
}

func TestBuildSnaplockLegalHoldEndCommand(t *testing.T) {
	cmd := BuildSnaplockLegalHoldEndPathCommand("lit1", "vol1", "/", "svm1")
	assert.Contains(t, cmd, "snaplock legal-hold end")
	assert.Contains(t, cmd, "-litigation-name lit1")
	assert.Contains(t, cmd, "-volume vol1")
	assert.Contains(t, cmd, "-path /")
	assert.Contains(t, cmd, "-vserver svm1")
}

func TestBuildSnaplockLegalHoldEndPathCommand(t *testing.T) {
	cmd := BuildSnaplockLegalHoldEndPathCommand("lit1", "vol1", "/dir1", "svm1")
	assert.Contains(t, cmd, "snaplock legal-hold end")
	assert.Contains(t, cmd, "-path /dir1")
}

func TestBuildSnaplockLegalHoldAbortCommand(t *testing.T) {
	cmd := BuildSnaplockLegalHoldAbortCommand("16908292", "svm1")
	assert.Contains(t, cmd, "snaplock legal-hold abort")
	assert.Contains(t, cmd, "-operation-id 16908292")
	assert.Contains(t, cmd, "vserver context -vserver svm1")
	// Abort command must not include -vserver (causes "unable to parse command" on some ONTAP)
	assert.NotContains(t, cmd, "abort -operation-id 16908292 -vserver")
}

func TestBuildSnaplockLegalHoldShowCommand(t *testing.T) {
	cmd := BuildSnaplockLegalHoldShowCommand("vs1", "slc_vol1")
	assert.Contains(t, cmd, "snaplock legal-hold show")
	assert.Contains(t, cmd, "-vserver vs1")
	assert.Contains(t, cmd, "-volume slc_vol1")
	assert.Contains(t, cmd, "-instance")
}

func TestBuildSnaplockLegalHoldShowForLitigationCommand(t *testing.T) {
	cmd := BuildSnaplockLegalHoldShowForLitigationCommand("vs1", "vol1", "lit1")
	assert.Contains(t, cmd, "snaplock legal-hold show")
	assert.Contains(t, cmd, "-vserver vs1")
	assert.Contains(t, cmd, "-volume vol1")
	assert.Contains(t, cmd, "-litigation-name lit1")
	assert.Contains(t, cmd, "-instance")
}

func TestParseOperationIDFromBeginEndOutput(t *testing.T) {
	t.Run("extracts operation ID from begin output", func(t *testing.T) {
		output := `SnapLock legal-hold begin operation is queued. Run "snaplock legal-hold show -operation-id 16842773 -instance" to view the operation status.`
		id, ok := utils.ParseOperationIDFromBeginEndOutput(output)
		require.True(t, ok)
		assert.Equal(t, 16842773, id)
	})
	t.Run("extracts operation ID from end output", func(t *testing.T) {
		output := `SnapLock legal-hold end operation is queued. Run "snaplock legal-hold show -operation-id 16842775 -instance" to view the operation status.`
		id, ok := utils.ParseOperationIDFromBeginEndOutput(output)
		require.True(t, ok)
		assert.Equal(t, 16842775, id)
	})
	t.Run("returns false when no operation ID", func(t *testing.T) {
		id, ok := utils.ParseOperationIDFromBeginEndOutput("some other output")
		assert.False(t, ok)
		assert.Equal(t, 0, id)
	})
}

func TestParseSnaplockLegalHoldShowInstanceOutput(t *testing.T) {
	// Sample -instance output (one block); Litigation Name and Path with leading spaces.
	output := `
                     Vserver: vs1
                      Volume: slc_vol1
                Operation ID: 16842786
             Litigation Name: litigation1
                        Path: /
              Operation Type: begin
                      Status: Completed
`
	records, err := utils.ParseSnaplockLegalHoldShowInstanceOutput(output)
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, "litigation1", records[0].Name)
	assert.Equal(t, "/", records[0].Path)
}

func TestParseSnaplockLegalHoldShowInstanceOutput_DedupesByName(t *testing.T) {
	// Two blocks with same litigation name (e.g. begin and end operations).
	output := `
                     Vserver: vs1
                      Volume: vol1
             Litigation Name: lit1
                        Path: /a
                     Vserver: vs1
                      Volume: vol1
             Litigation Name: lit1
                        Path: /b
`
	records, err := utils.ParseSnaplockLegalHoldShowInstanceOutput(output)
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, "lit1", records[0].Name)
	assert.Equal(t, "/a", records[0].Path) // first path wins
}

func TestParseSnaplockLegalHoldShowInstanceOutput_EmptyOutput(t *testing.T) {
	records, err := utils.ParseSnaplockLegalHoldShowInstanceOutput("")
	require.NoError(t, err)
	assert.Empty(t, records)
}

func TestBuildSnaplockLegalHoldShowOperationCommand(t *testing.T) {
	cmd := BuildSnaplockLegalHoldShowOperationCommand("16908292")
	assert.Contains(t, cmd, "snaplock legal-hold show")
	assert.Contains(t, cmd, "-operation-id 16908292")
	assert.Contains(t, cmd, "-instance")
}

func TestParseSnaplockLegalHoldShowOperationOutput(t *testing.T) {
	output := "                     Vserver: vs1\n                      Volume: vol1\n                Operation ID: 16908292\n             Litigation Name: lit1\n                        Path: /dir1\n              Operation Type: begin\n                      Status: In-Progress\n   Number of Files Processed: 10\n     Number of Files Failed: 0\n"
	rec, err := utils.ParseSnaplockLegalHoldShowOperationOutput(output)
	require.NoError(t, err)
	require.NotNil(t, rec)
	assert.Equal(t, 16908292, rec.OperationID)
	assert.Equal(t, "In-Progress", rec.Status)
	assert.Equal(t, "/dir1", rec.Path)
	assert.Equal(t, "begin", rec.OperationType)
	assert.Equal(t, "10", rec.NumFilesProcessed)
	assert.Equal(t, "0", rec.NumFilesFailed)
}

func TestParseSnaplockLegalHoldShowInstanceOutputToOperations(t *testing.T) {
	output := `
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
`
	ops := utils.ParseSnaplockLegalHoldShowInstanceOutputToOperations(output)
	require.Len(t, ops, 1)
	assert.Equal(t, "litigation-001", ops[0].LitigationName)
	assert.Equal(t, 16842754, ops[0].OperationID)
	assert.Equal(t, "Completed", ops[0].Status)
	assert.Equal(t, "/", ops[0].Path)
	assert.Equal(t, "begin", ops[0].OperationType)
	assert.Equal(t, "5", ops[0].NumFilesProcessed)
	assert.Equal(t, "0", ops[0].NumFilesFailed)
	assert.Equal(t, "0", ops[0].NumFilesSkipped)
	assert.Equal(t, "0", ops[0].NumInodesIgnored)
	assert.Equal(t, "No error", ops[0].StatusDetails)
}

func TestParseSnaplockLegalHoldShowOperationOutput_StatusDetails(t *testing.T) {
	output := `                      Vserver: gcnv-fbc248f62e293c2-svm-01
                       Volume: snaplock_vol_ss_comp
                 Operation ID: 16842767
              Litigation Name: litigation-003
                         Path: /
               Operation Type: end
                       Status: Failed
    Number of Files Processed: 0
       Number of Files Failed: 0
      Number of Files Skipped: 0
     Number of Inodes Ignored: 0
               Status Details: Failed to setup legal-hold operation on path /vol/snaplock_vol_ss_comp. Reason: No such litigation.
`
	rec, err := utils.ParseSnaplockLegalHoldShowOperationOutput(output)
	require.NoError(t, err)
	require.NotNil(t, rec)
	assert.Equal(t, 16842767, rec.OperationID)
	assert.Equal(t, "Failed", rec.Status)
	assert.Equal(t, "end", rec.OperationType)
	assert.Equal(t, "0", rec.NumFilesProcessed)
	assert.Equal(t, "Failed to setup legal-hold operation on path /vol/snaplock_vol_ss_comp. Reason: No such litigation.", rec.StatusDetails)
}

func TestMapOperationStatusToState(t *testing.T) {
	assert.Equal(t, "completed", utils.MapOperationStatusToState("Completed"))
	assert.Equal(t, "in_progress", utils.MapOperationStatusToState("In-Progress"))
	assert.Equal(t, "failed", utils.MapOperationStatusToState("Failed"))
	assert.Equal(t, "aborting", utils.MapOperationStatusToState("Aborting"))
	assert.Equal(t, "in_progress", utils.MapOperationStatusToState("unknown"))
}
