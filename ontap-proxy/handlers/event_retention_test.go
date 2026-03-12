package handlers

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRetentionPeriodAPIToCLI(t *testing.T) {
	tests := []struct {
		api  string
		cli  string
		desc string
	}{
		{RetentionPeriodInfinite, RetentionPeriodInfinite, "keyword"},
		{RetentionPeriodUnspecified, RetentionPeriodUnspecified, "keyword"},
		{"P7Y", "7 years", "years"},
		{"P10Y", "10 years", "years"},
		{"P30M", "30 months", "months"},
		{"P6M", "6 months", "months"},
		{"P7D", "7 days", "days"},
		{"P365D", "365 days", "days"},
		{"PT24H", "24 hours", "hours"},
		{"PT1H", "1 hours", "hours"},
		{"PT30M", "30 minutes", "minutes"},
		{"PT60S", "60 seconds", "seconds"},
		{"  P7Y  ", "7 years", "trimmed"},
		{"P7y", "7 years", "lowercase"},
	}
	for _, tt := range tests {
		t.Run(tt.desc+"/"+tt.api, func(t *testing.T) {
			got := RetentionPeriodAPIToCLI(tt.api)
			assert.Equal(t, tt.cli, got, "API %q -> CLI %q", tt.api, got)
		})
	}
}

const eventRetentionContextPrefix = "vserver context -username snaplock-user; "

func TestBuildEventRetentionPolicyCommands(t *testing.T) {
	t.Run("show all", func(t *testing.T) {
		cmd := BuildEventRetentionPolicyShowCommand("")
		assert.Equal(t, eventRetentionContextPrefix+"snaplock event-retention policy show", cmd)
	})
	t.Run("show one", func(t *testing.T) {
		cmd := BuildEventRetentionPolicyShowCommand("p1")
		assert.Equal(t, eventRetentionContextPrefix+"snaplock event-retention policy show -name p1", cmd)
	})
	t.Run("create with space in period", func(t *testing.T) {
		cmd := BuildEventRetentionPolicyCreateCommand("p1", "7 years")
		assert.Contains(t, cmd, `"7 years"`)
		assert.Contains(t, cmd, "snaplock event-retention policy create")
		assert.True(t, strings.HasPrefix(cmd, eventRetentionContextPrefix))
	})
	t.Run("delete", func(t *testing.T) {
		cmd := BuildEventRetentionPolicyDeleteCommand("p1")
		assert.Equal(t, eventRetentionContextPrefix+"snaplock event-retention policy delete -name p1", cmd)
	})
	t.Run("modify", func(t *testing.T) {
		cmd := BuildEventRetentionPolicyModifyCommand("p1", "30 months")
		assert.Contains(t, cmd, `"30 months"`)
		assert.Contains(t, cmd, "snaplock event-retention policy modify")
		assert.True(t, strings.HasPrefix(cmd, eventRetentionContextPrefix))
	})
	t.Run("show with policy name that would need quoting is quoted", func(t *testing.T) {
		cmd := BuildEventRetentionPolicyShowCommand("policy with space")
		assert.Contains(t, cmd, `"policy with space"`)
	})
}

func TestRetentionPeriodCLIToAPI(t *testing.T) {
	tests := []struct {
		cli  string
		api  string
		desc string
	}{
		{"7 years", "P7Y", "years"},
		{"1 years", "P1Y", "one year"},
		{"30 months", "P30M", "months"},
		{RetentionPeriodInfinite, RetentionPeriodInfinite, "infinite"},
		{RetentionPeriodUnspecified, RetentionPeriodUnspecified, "unspecified"},
		{"24 hours", "PT24H", "hours"},
		{"30 minutes", "PT30M", "minutes"},
	}
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			got := RetentionPeriodCLIToAPI(tt.cli)
			assert.Equal(t, tt.api, got)
		})
	}
}

const sampleEventRetentionShowOutput = `
Info: Use 'exit' command to return.

Vserver           Name                Retention Period
----------------- ------------------- --------------------
gcnv-dfcb696927b4c65-svm-01 litigation-hold-7y 1 years
gcnv-dfcb696927b4c65-svm-01 litigation-hold-7y-2 7 years
gcnv-dfcb696927b4c65-svm-01 litigation-hold-7y-3 7 years
gcnv-dfcb696927b4c65-svm-01 litigation-hold-7y-4 7 years
4 entries were displayed.
`

func TestParseEventRetentionPolicyShowOutput(t *testing.T) {
	rows, err := ParseEventRetentionPolicyShowOutput(sampleEventRetentionShowOutput)
	assert.NoError(t, err)
	assert.Len(t, rows, 4)
	assert.Equal(t, "gcnv-dfcb696927b4c65-svm-01", rows[0].Vserver)
	assert.Equal(t, "litigation-hold-7y", rows[0].Name)
	assert.Equal(t, "P1Y", rows[0].RetentionPeriod)
	assert.Equal(t, "litigation-hold-7y-2", rows[1].Name)
	assert.Equal(t, "P7Y", rows[1].RetentionPeriod)
}

const sampleEventRetentionKeyValueOutput = `
Vserver: gcnv-dfcb696927b4c65-svm-01
Name: my-ebr-policy
Retention Period: 7 years
`

// Matches ONTAP single-policy show: "Policy Name:" and "Event Retention Period:" (no Vserver).
const sampleEventRetentionKeyValueOutputONTAP = `
Info: Use 'exit' command to return.

           Policy Name: my-policy-name
Event Retention Period: 7 years
`

func TestParseEventRetentionPolicyShowOutput_KeyValue(t *testing.T) {
	rows, err := ParseEventRetentionPolicyShowOutput(sampleEventRetentionKeyValueOutput)
	assert.NoError(t, err)
	assert.Len(t, rows, 1)
	assert.Equal(t, "gcnv-dfcb696927b4c65-svm-01", rows[0].Vserver)
	assert.Equal(t, "my-ebr-policy", rows[0].Name)
	assert.Equal(t, "P7Y", rows[0].RetentionPeriod)
}

func TestParseEventRetentionPolicyShowOutput_KeyValueEventRetentionPeriod(t *testing.T) {
	rows, err := ParseEventRetentionPolicyShowOutput(sampleEventRetentionKeyValueOutputONTAP)
	assert.NoError(t, err)
	assert.Len(t, rows, 1)
	assert.Equal(t, "my-policy-name", rows[0].Name)
	assert.Equal(t, "P7Y", rows[0].RetentionPeriod)
	// ONTAP single-policy show does not output Vserver in this format
	assert.Equal(t, "", rows[0].Vserver)
}

func TestQuoteCLIArg(t *testing.T) {
	tests := []struct {
		in   string
		want string
		desc string
	}{
		{"", `""`, "empty"},
		{"   ", `""`, "whitespace only"},
		{RetentionPeriodInfinite, RetentionPeriodInfinite, "no spaces"},
		{"7 years", `"7 years"`, "with space"},
		{"30 months", `"30 months"`, "two words"},
		{"a\tb", `"a\tb"`, "with tab"},
	}
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			got := quoteCLIArg(tt.in)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestParseEventRetentionPolicyShowOutput_EmptyOrInvalid(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		_, err := ParseEventRetentionPolicyShowOutput("")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "could not find table separator")
	})
	t.Run("only banner", func(t *testing.T) {
		_, err := ParseEventRetentionPolicyShowOutput("\nInfo: Use 'exit' command to return.\n\n")
		assert.Error(t, err)
	})
	t.Run("no name in key-value", func(t *testing.T) {
		// Key-value with only Event Retention Period (no Name) should fail
		_, err := ParseEventRetentionPolicyShowOutput("Event Retention Period: 7 years\n")
		assert.Error(t, err)
	})
}

func TestParseEventRetentionPolicyShowOutput_TableWithInfinite(t *testing.T) {
	output := `
Vserver           Name                Retention Period
----------------- ------------------- --------------------
svm1              policy-infinite     infinite
1 entries were displayed.
`
	rows, err := ParseEventRetentionPolicyShowOutput(output)
	assert.NoError(t, err)
	assert.Len(t, rows, 1)
	assert.Equal(t, "svm1", rows[0].Vserver)
	assert.Equal(t, "policy-infinite", rows[0].Name)
	assert.Equal(t, RetentionPeriodInfinite, rows[0].RetentionPeriod)
}

func TestParseEventRetentionPolicyShowOutput_KeyValueMissingRetention(t *testing.T) {
	// Only Policy Name, no Event Retention Period -> retention becomes "unspecified"
	output := `
Policy Name: no-retention-policy
`
	rows, err := ParseEventRetentionPolicyShowOutput(output)
	assert.NoError(t, err)
	assert.Len(t, rows, 1)
	assert.Equal(t, "no-retention-policy", rows[0].Name)
	assert.Equal(t, RetentionPeriodUnspecified, rows[0].RetentionPeriod)
}

func TestRetentionPeriodAPIToCLI_EdgeCases(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		assert.Equal(t, "", RetentionPeriodAPIToCLI(""))
	})
	t.Run("unknown passthrough", func(t *testing.T) {
		got := RetentionPeriodAPIToCLI("unknown-format")
		assert.Equal(t, "unknown-format", got)
	})
}

func TestRetentionPeriodCLIToAPI_EdgeCases(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		assert.Equal(t, "", RetentionPeriodCLIToAPI(""))
	})
	t.Run("unknown passthrough", func(t *testing.T) {
		got := RetentionPeriodCLIToAPI("unknown")
		assert.Equal(t, "unknown", got)
	})
	t.Run("days and seconds", func(t *testing.T) {
		assert.Equal(t, "P7D", RetentionPeriodCLIToAPI("7 days"))
		assert.Equal(t, "PT60S", RetentionPeriodCLIToAPI("60 seconds"))
	})
}

func TestBuildEventRetentionPolicyCreateCommand_Infinite(t *testing.T) {
		cmd := BuildEventRetentionPolicyCreateCommand("p1", RetentionPeriodInfinite)
	assert.Contains(t, cmd, "snaplock event-retention policy create")
	assert.Contains(t, cmd, "-name p1")
	// infinite has no space, so not quoted
	assert.Contains(t, cmd, "-retention-period "+RetentionPeriodInfinite)
	assert.True(t, strings.HasPrefix(cmd, eventRetentionContextPrefix))
}

func TestParseEventRetentionPolicyShowOutput_TableUnspecified(t *testing.T) {
	output := `
Vserver           Name                Retention Period
----------------- ------------------- --------------------
svm1              policy-unspec       unspecified
1 entries were displayed.
`
	rows, err := ParseEventRetentionPolicyShowOutput(output)
	assert.NoError(t, err)
	assert.Len(t, rows, 1)
	assert.Equal(t, RetentionPeriodUnspecified, rows[0].RetentionPeriod)
}

// TestParseEventRetentionPolicyShowOutput_EntriesDisplayedBeforeSeparator covers
// the branch where "entries were displayed" is seen before any table separator (break at line 115).
func TestParseEventRetentionPolicyShowOutput_EntriesDisplayedBeforeSeparator(t *testing.T) {
	output := "\n0 entries were displayed.\n"
	_, err := ParseEventRetentionPolicyShowOutput(output)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "could not find table separator")
}

// TestParseEventRetentionPolicyShowOutput_TableBlankLineAndShortLines covers
// blank data line (continue), len(parts) < 3 (continue), len(parts) < 2+retentionTokens (continue), empty name (continue).
func TestParseEventRetentionPolicyShowOutput_TableBlankLineAndShortLines(t *testing.T) {
	output := `
Vserver           Name                Retention Period
----------------- ------------------- --------------------

svm1              onlytwo
svm1              7 years
svm1              7 years
svm1              real-policy         infinite
1 entries were displayed.
`
	rows, err := ParseEventRetentionPolicyShowOutput(output)
	assert.NoError(t, err)
	assert.Len(t, rows, 1)
	assert.Equal(t, "real-policy", rows[0].Name)
	assert.Equal(t, RetentionPeriodInfinite, rows[0].RetentionPeriod)
}

// TestParseEventRetentionPolicyShowOutput_KeyValueInvalidLine covers key-value parser
// skipping lines where ":" is at position 0 (idx <= 0).
func TestParseEventRetentionPolicyShowOutput_KeyValueInvalidLine(t *testing.T) {
	output := `
:
Policy Name: my-policy
Event Retention Period: 5 years
`
	rows, err := ParseEventRetentionPolicyShowOutput(output)
	assert.NoError(t, err)
	assert.Len(t, rows, 1)
	assert.Equal(t, "my-policy", rows[0].Name)
	assert.Equal(t, "P5Y", rows[0].RetentionPeriod)
}

// Exact ONTAP default table format from real CLI (Operation ID first; Vserver and Volume one token when single space between).
const sampleOperationsShowOutputOntapFormat = `
Info: Use 'exit' command to return.


Operation ID   Vserver         Volume          Operation Status
-------------- --------------- --------------- ----------------
16842753       gcnv-dfcb696927b4c65-svm-01 snaplock_vol1 Completed
16842754       gcnv-dfcb696927b4c65-svm-01 snaplock_vol1 Completed
16842755       gcnv-dfcb696927b4c65-svm-01 snaplock_vol1 Completed
16842756       gcnv-dfcb696927b4c65-svm-01 snaplock_vol1 Completed
4 entries were displayed.

`

func TestParseEventRetentionOperationShowOutput_OntapDefaultTable(t *testing.T) {
	rows, err := ParseEventRetentionOperationShowOutput(sampleOperationsShowOutputOntapFormat)
	assert.NoError(t, err)
	require.Len(t, rows, 4)
	assert.Equal(t, int64(16842753), rows[0].OperationID)
	assert.Equal(t, "gcnv-dfcb696927b4c65-svm-01", rows[0].Vserver)
	assert.Equal(t, "snaplock_vol1", rows[0].VolumeName)
	assert.Equal(t, "completed", rows[0].State)
	assert.Equal(t, int64(16842756), rows[3].OperationID)
	assert.Equal(t, "completed", rows[3].State)
}

// Two-part split: only one run of 2+ spaces (after operation ID), rest is "vserver volume state".
const sampleOperationsShowOutputTwoPartSplit = `
Operation ID   Vserver         Volume          Operation Status
-------------- --------------- --------------- ----------------
16842753       gcnv-dfcb696927b4c65-svm-01 snaplock_vol1 Completed
16842754       gcnv-dfcb696927b4c65-svm-01 snaplock_vol1 Completed
2 entries were displayed.
`

func TestParseEventRetentionOperationShowOutput_TwoPartSplit(t *testing.T) {
	rows, err := ParseEventRetentionOperationShowOutput(sampleOperationsShowOutputTwoPartSplit)
	assert.NoError(t, err)
	require.Len(t, rows, 2)
	assert.Equal(t, int64(16842753), rows[0].OperationID)
	assert.Equal(t, "gcnv-dfcb696927b4c65-svm-01", rows[0].Vserver)
	assert.Equal(t, "snaplock_vol1", rows[0].VolumeName)
	assert.Equal(t, "completed", rows[0].State)
}

// Key-value output (snaplock event-retention show -operation-id N -instance).
// Keys match parseEventRetentionOperationKeyValueOutput switch (e.g. "Operation Id", "Policy Name").
const sampleOperationShowKeyValueOutput = `
Vserver: svm1
Operation Id: 16842754
State: in_progress
Path: /
Policy Name: p1day
Volume Name: vol1
Num Files Processed: 10
Num Files Failed: 0
Num Files Skipped: 1
Num Inodes Ignored: 2
`

func TestParseEventRetentionOperationShowOutput_KeyValue(t *testing.T) {
	rows, err := ParseEventRetentionOperationShowOutput(sampleOperationShowKeyValueOutput)
	assert.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, int64(16842754), rows[0].OperationID)
	assert.Equal(t, "svm1", rows[0].Vserver)
	assert.Equal(t, "in_progress", rows[0].State)
	assert.Equal(t, "/", rows[0].Path)
	assert.Equal(t, "p1day", rows[0].PolicyName)
	assert.Equal(t, "vol1", rows[0].VolumeName)
	assert.Equal(t, int64(10), rows[0].NumFilesProcessed)
	assert.Equal(t, int64(0), rows[0].NumFilesFailed)
	assert.Equal(t, int64(1), rows[0].NumFilesSkipped)
	assert.Equal(t, int64(2), rows[0].NumInodesIgnored)
}

func TestParseEventRetentionOperationShowOutput_InvalidReturnsError(t *testing.T) {
	_, err := ParseEventRetentionOperationShowOutput("garbage with no table or key-value")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "could not find table separator")
}

func TestBuildEventRetentionOperationCommands(t *testing.T) {
	t.Run("show_list", func(t *testing.T) {
		cmd := BuildEventRetentionOperationShowCommand(0)
		assert.Contains(t, cmd, "snaplock event-retention show")
		assert.NotContains(t, cmd, "-operation-id")
	})
	t.Run("show_single", func(t *testing.T) {
		cmd := BuildEventRetentionOperationShowCommand(16842754)
		assert.Contains(t, cmd, "snaplock event-retention show")
		assert.Contains(t, cmd, "-operation-id 16842754")
	})
	t.Run("apply", func(t *testing.T) {
		cmd := BuildEventRetentionOperationApplyCommand("vol1", "p1day", "/")
		assert.Contains(t, cmd, "snaplock event-retention apply")
		assert.Contains(t, cmd, "-volume")
		assert.Contains(t, cmd, "-policy-name")
		assert.Contains(t, cmd, "-path")
	})
	t.Run("abort", func(t *testing.T) {
		cmd := BuildEventRetentionOperationAbortCommand(16842754)
		assert.Contains(t, cmd, "snaplock event-retention abort")
		assert.Contains(t, cmd, "-operation-id 16842754")
	})
}

// Table with 3 parts: Operation ID, "vserver volume", state.
const sampleOperationsShowOutputThreeParts = `
Operation ID   Vserver         Volume          Operation Status
-------------- --------------- --------------- ----------------
16842760       svm1 vol1       Completed
1 entries were displayed.
`

func TestParseEventRetentionOperationShowOutput_ThreeParts(t *testing.T) {
	rows, err := ParseEventRetentionOperationShowOutput(sampleOperationsShowOutputThreeParts)
	assert.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, int64(16842760), rows[0].OperationID)
	assert.Equal(t, "svm1", rows[0].Vserver)
	assert.Equal(t, "vol1", rows[0].VolumeName)
	assert.Equal(t, "completed", rows[0].State)
}

// Format B: Vserver first, then Operation Id, State, Path, etc.
const sampleOperationsShowOutputFormatB = `
Vserver   Operation Id   State        Path   Policy   Volume
--------- -------------- ----------- ------ -------- -------
svm1      16842761       completed   /      p1day    vol1
1 entries were displayed.
`

func TestParseEventRetentionOperationShowOutput_FormatB(t *testing.T) {
	rows, err := ParseEventRetentionOperationShowOutput(sampleOperationsShowOutputFormatB)
	assert.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, int64(16842761), rows[0].OperationID)
	assert.Equal(t, "svm1", rows[0].Vserver)
	assert.Equal(t, "completed", rows[0].State)
	assert.Equal(t, "/", rows[0].Path)
	assert.Equal(t, "p1day", rows[0].PolicyName)
	assert.Equal(t, "vol1", rows[0].VolumeName)
}
