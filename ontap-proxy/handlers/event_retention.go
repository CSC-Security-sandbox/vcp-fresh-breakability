package handlers

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Retention period keywords used by API and ONTAP CLI (case-insensitive in API).
const (
	RetentionPeriodInfinite    = "infinite"
	RetentionPeriodUnspecified = "unspecified"
)

// RetentionPeriodAPIToCLI converts the API retention period (ISO-8601 or "infinite"/"unspecified")
// to the format expected by ONTAP CLI. CLI does not accept raw ISO-8601 like P7Y or P30M; it expects
// "infinite", "unspecified", or a phrase like "7 years", "30 months".
// API format: P7Y, P30M, P7D, PT24H, PT30M, infinite, unspecified.
func RetentionPeriodAPIToCLI(apiPeriod string) string {
	s := strings.TrimSpace(strings.ToLower(apiPeriod))
	if s == "" {
		return ""
	}
	switch s {
	case RetentionPeriodInfinite, RetentionPeriodUnspecified:
		return s
	}
	// PT = time: hours, minutes, seconds (match before P so PT30M is minutes not months)
	if matches := retentionTimeRegex.FindStringSubmatch(s); len(matches) == 3 {
		if n, err := strconv.Atoi(matches[1]); err == nil {
			switch matches[2] {
			case "h":
				return fmt.Sprintf("%d hours", n)
			case "m":
				return fmt.Sprintf("%d minutes", n)
			case "s":
				return fmt.Sprintf("%d seconds", n)
			}
		}
	}
	// P = date: years, months, days
	if matches := retentionDateRegex.FindStringSubmatch(s); len(matches) == 3 {
		if n, err := strconv.Atoi(matches[1]); err == nil {
			switch matches[2] {
			case "y":
				return fmt.Sprintf("%d years", n)
			case "m":
				return fmt.Sprintf("%d months", n)
			case "d":
				return fmt.Sprintf("%d days", n)
			}
		}
	}
	return apiPeriod
}

var (
	retentionTimeRegex = regexp.MustCompile(`^pt(\d+)([hms])$`) // PT24H, PT30M, PT60S
	retentionDateRegex = regexp.MustCompile(`^p(\d+)([ymd])$`)  // P7Y, P30M, P7D
	retentionCLIRegex  = regexp.MustCompile(`(?i)^(\d+)\s*(years?|months?|days?|hours?|minutes?|seconds?)$`)
)

// RetentionPeriodCLIToAPI converts CLI retention output (e.g. "7 years", "1 years", "infinite")
// to API format (e.g. "P7Y", "P1Y", "infinite").
func RetentionPeriodCLIToAPI(cliPeriod string) string {
	s := strings.TrimSpace(strings.ToLower(cliPeriod))
	if s == "" {
		return ""
	}
	if s == RetentionPeriodInfinite || s == RetentionPeriodUnspecified {
		return s
	}
	if m := retentionCLIRegex.FindStringSubmatch(s); len(m) == 3 {
		n, err := strconv.Atoi(m[1])
		if err != nil {
			return cliPeriod
		}
		switch m[2] {
		case "year", "years":
			return fmt.Sprintf("P%dY", n)
		case "month", "months":
			return fmt.Sprintf("P%dM", n)
		case "day", "days":
			return fmt.Sprintf("P%dD", n)
		case "hour", "hours":
			return fmt.Sprintf("PT%dH", n)
		case "minute", "minutes":
			return fmt.Sprintf("PT%dM", n)
		case "second", "seconds":
			return fmt.Sprintf("PT%dS", n)
		}
	}
	return cliPeriod
}

// EventRetentionPolicyRow is one row parsed from "snaplock event-retention policy show" CLI output.
type EventRetentionPolicyRow struct {
	Vserver         string
	Name            string
	RetentionPeriod string // API format (e.g. P7Y)
}

// ParseEventRetentionPolicyShowOutput parses the CLI output of "snaplock event-retention policy show"
// (with or without -name). Supports (1) tabular output (header + separator + rows) and (2) key-value output
// for single-policy show (e.g. "Name: my-policy\nRetention Period: 7 years"). Returns retention in API format.
func ParseEventRetentionPolicyShowOutput(cliOutput string) ([]EventRetentionPolicyRow, error) {
	output := StripOntapLoginBanner(cliOutput)
	lines := strings.Split(output, "\n")

	// Find separator line (dashes) for tabular output
	separatorIdx := -1
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if strings.Contains(trimmed, "entries were displayed") {
			break
		}
		if strings.Count(trimmed, "-") > len(trimmed)/2 && len(trimmed) > 10 {
			separatorIdx = i
			break
		}
	}
	if separatorIdx < 0 {
		// No table: try key-value format (single-policy "show -name X" output)
		if row := parseEventRetentionKeyValueOutput(output); row != nil {
			return []EventRetentionPolicyRow{*row}, nil
		}
		return nil, fmt.Errorf("could not find table separator or valid key-value output in CLI output")
	}

	// Split data lines: columns are Vserver, Name, Retention Period. Retention is last 1 or 2 tokens.
	tokensSep := regexp.MustCompile(`\s+`)
	var rows []EventRetentionPolicyRow
	for i := separatorIdx + 1; i < len(lines); i++ {
		trimmed := strings.TrimSpace(lines[i])
		if trimmed == "" {
			continue
		}
		if strings.Contains(trimmed, "entries were displayed") {
			break
		}
		parts := tokensSep.Split(trimmed, -1)
		if len(parts) < 3 {
			continue
		}
		// Retention is last column: "N years" (2 tokens) or "infinite"/"unspecified" (1 token)
		last := strings.ToLower(parts[len(parts)-1])
		retentionTokens := 1
		if last == "years" || last == "year" || last == "months" || last == "month" ||
			last == "days" || last == "day" || last == "hours" || last == "hour" ||
			last == "minutes" || last == "minute" || last == "seconds" || last == "second" {
			retentionTokens = 2
		}
		if len(parts) < 2+retentionTokens {
			continue
		}
		retentionCLI := strings.Join(parts[len(parts)-retentionTokens:], " ")
		vserver := parts[0]
		name := strings.Join(parts[1:len(parts)-retentionTokens], " ")
		if name == "" {
			continue
		}
		rows = append(rows, EventRetentionPolicyRow{
			Vserver:         vserver,
			Name:            name,
			RetentionPeriod: RetentionPeriodCLIToAPI(retentionCLI),
		})
	}
	return rows, nil
}

// parseEventRetentionKeyValueOutput parses key-value style output from "snaplock event-retention policy show -name X".
// ONTAP uses "Policy Name:", "Event Retention Period:" and optionally "Vserver:".
func parseEventRetentionKeyValueOutput(output string) *EventRetentionPolicyRow {
	row := &EventRetentionPolicyRow{}
	for _, line := range strings.Split(output, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		idx := strings.Index(trimmed, ":")
		if idx <= 0 {
			continue
		}
		key := strings.TrimSpace(trimmed[:idx])
		val := strings.TrimSpace(trimmed[idx+1:])
		switch strings.ToLower(key) {
		case "vserver":
			row.Vserver = val
		case "name", "policy name":
			row.Name = val
		case "retention period", "event retention period":
			row.RetentionPeriod = RetentionPeriodCLIToAPI(val)
		}
	}
	if row.Name == "" {
		return nil
	}
	if row.RetentionPeriod == "" {
		row.RetentionPeriod = RetentionPeriodUnspecified
	}
	return row
}

// BuildEventRetentionPolicyShowCommand builds the ONTAP CLI command to show policies.
// Uses vserver context -username SnaplockUsername; no -vserver in context.
func BuildEventRetentionPolicyShowCommand(policyName string) string {
	if policyName != "" {
		return fmt.Sprintf("vserver context -username %s; snaplock event-retention policy show -name %s", SnaplockUsername, quoteCLIArg(policyName))
	}
	return fmt.Sprintf("vserver context -username %s; snaplock event-retention policy show", SnaplockUsername)
}

// BuildEventRetentionPolicyCreateCommand builds the ONTAP CLI command for creating an EBR policy.
// retentionPeriodCLI should be the output of RetentionPeriodAPIToCLI (e.g. "7 years", "infinite").
func BuildEventRetentionPolicyCreateCommand(name, retentionPeriodCLI string) string {
	return fmt.Sprintf("vserver context -username %s; snaplock event-retention policy create -name %s -retention-period %s",
		SnaplockUsername, quoteCLIArg(name), quoteCLIArg(retentionPeriodCLI))
}

// BuildEventRetentionPolicyDeleteCommand builds the ONTAP CLI command for deleting an EBR policy.
func BuildEventRetentionPolicyDeleteCommand(policyName string) string {
	return fmt.Sprintf("vserver context -username %s; snaplock event-retention policy delete -name %s", SnaplockUsername, quoteCLIArg(policyName))
}

// BuildEventRetentionPolicyModifyCommand builds the ONTAP CLI command for modifying retention period.
// retentionPeriodCLI should be the output of RetentionPeriodAPIToCLI.
func BuildEventRetentionPolicyModifyCommand(policyName, retentionPeriodCLI string) string {
	return fmt.Sprintf("vserver context -username %s; snaplock event-retention policy modify -name %s -retention-period %s",
		SnaplockUsername, quoteCLIArg(policyName), quoteCLIArg(retentionPeriodCLI))
}

// --- EBR operations (apply / show / abort) ---

// EventRetentionOperationRow is one row parsed from "snaplock event-retention show" CLI output.
type EventRetentionOperationRow struct {
	Vserver           string
	OperationID       int64
	State             string
	Path              string
	PolicyName        string
	VolumeName        string
	NumFilesProcessed int64
	NumFilesFailed    int64
	NumFilesSkipped   int64
	NumInodesIgnored  int64
}

// BuildEventRetentionOperationShowCommand builds the ONTAP CLI command to list EBR operations.
// If operationID is 0, lists all; otherwise shows the single operation with that ID.
func BuildEventRetentionOperationShowCommand(operationID int64) string {
	base := fmt.Sprintf("vserver context -username %s; snaplock event-retention show", SnaplockUsername)
	if operationID != 0 {
		base += fmt.Sprintf(" -operation-id %d", operationID)
	}
	return base
}

// BuildEventRetentionOperationApplyCommand builds the ONTAP CLI command to apply an EBR policy to a path.
// volumeNameOrUUID is the volume name or UUID (CLI -volume flag).
func BuildEventRetentionOperationApplyCommand(volumeNameOrUUID, policyName, path string) string {
	return fmt.Sprintf("vserver context -username %s; snaplock event-retention apply -volume %s -policy-name %s -path %s",
		SnaplockUsername, quoteCLIArg(volumeNameOrUUID), quoteCLIArg(policyName), quoteCLIArg(path))
}

// BuildEventRetentionOperationAbortCommand builds the ONTAP CLI command to abort an EBR operation.
func BuildEventRetentionOperationAbortCommand(operationID int64) string {
	return fmt.Sprintf("vserver context -username %s; snaplock event-retention abort -operation-id %d",
		SnaplockUsername, operationID)
}

// ParseEventRetentionOperationShowOutput parses the CLI output of "snaplock event-retention show".
// Supports tabular output (header + separator + rows). Returns rows with operation id, state, path, policy, volume, etc.
func ParseEventRetentionOperationShowOutput(cliOutput string) ([]EventRetentionOperationRow, error) {
	output := StripOntapLoginBanner(cliOutput)
	lines := strings.Split(output, "\n")

	separatorIdx := -1
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if strings.Contains(trimmed, "entries were displayed") {
			break
		}
		if strings.Count(trimmed, "-") > len(trimmed)/2 && len(trimmed) > 10 {
			separatorIdx = i
			break
		}
	}
	if separatorIdx < 0 {
		if row := parseEventRetentionOperationKeyValueOutput(output); row != nil {
			return []EventRetentionOperationRow{*row}, nil
		}
		return nil, fmt.Errorf("could not find table separator or valid key-value output in CLI output")
	}

	tokensSep := regexp.MustCompile(`\s{2,}|\t`)
	var rows []EventRetentionOperationRow
	for i := separatorIdx + 1; i < len(lines); i++ {
		trimmed := strings.TrimSpace(strings.TrimRight(lines[i], "\r"))
		if trimmed == "" {
			continue
		}
		if strings.Contains(trimmed, "entries were displayed") {
			break
		}
		parts := tokensSep.Split(trimmed, -1)
		if len(parts) < 2 {
			continue
		}
		row := parseEventRetentionOperationRowParts(parts)
		if row != nil {
			rows = append(rows, *row)
		}
	}
	return rows, nil
}

// parseEventRetentionOperationRowParts parses a row split by 2+ spaces or tab.
// ONTAP "snaplock event-retention show" can use either column order:
//   - Operation ID, Vserver, Volume, Operation Status (default table; spacing varies so we may get 2–4 parts)
//   - Vserver, Operation Id, State, Path, Policy, Volume (with -instance or other views)
//
// We detect by checking if first column is numeric (Operation ID first) or not (Vserver first).
func parseEventRetentionOperationRowParts(parts []string) *EventRetentionOperationRow {
	if len(parts) < 2 {
		return nil
	}
	row := &EventRetentionOperationRow{}
	p0 := strings.TrimSpace(parts[0])
	p1 := strings.TrimSpace(parts[1])

	// Format A: first column is Operation ID (numeric)
	if id, err := strconv.ParseInt(p0, 10, 64); err == nil {
		row.OperationID = id
		if len(parts) >= 4 {
			row.Vserver = p1
			row.VolumeName = strings.TrimSpace(parts[2])
			row.State = strings.ToLower(strings.TrimSpace(strings.TrimRight(parts[3], "\r")))
		} else if len(parts) == 3 {
			// 3 parts: id, "vserver volume", state
			vvol := strings.SplitN(p1, " ", 2)
			row.Vserver = strings.TrimSpace(vvol[0])
			if len(vvol) > 1 {
				row.VolumeName = strings.TrimSpace(vvol[1])
			}
			p2 := strings.TrimSpace(strings.TrimRight(parts[2], "\r"))
			row.State = strings.ToLower(p2)
		} else {
			// 2 parts: id, "vserver volume state" — only one run of 2+ spaces (after id)
			tokens := strings.Fields(p1)
			if len(tokens) >= 3 {
				row.State = strings.ToLower(strings.TrimSpace(strings.TrimRight(tokens[len(tokens)-1], "\r")))
				row.VolumeName = tokens[len(tokens)-2]
				row.Vserver = strings.Join(tokens[:len(tokens)-2], " ")
			} else if len(tokens) == 2 {
				row.Vserver = tokens[0]
				row.VolumeName = tokens[1]
			} else if len(tokens) == 1 {
				row.Vserver = tokens[0]
			}
		}
		return row
	}
	if len(parts) < 4 {
		return nil
	}
	p2 := strings.TrimSpace(parts[2])
	p3 := strings.TrimSpace(parts[3])

	// Format B: "Vserver  Operation Id  State  Path  Policy  Volume  ..."
	row.Vserver = p0
	id, err := strconv.ParseInt(p1, 10, 64)
	if err != nil {
		return nil
	}
	row.OperationID = id
	row.State = p2
	if len(parts) > 3 {
		row.Path = p3
	}
	if len(parts) > 4 {
		row.PolicyName = strings.TrimSpace(parts[4])
	}
	if len(parts) > 5 {
		row.VolumeName = strings.TrimSpace(parts[5])
	}
	if len(parts) > 6 {
		row.NumFilesProcessed, _ = strconv.ParseInt(strings.TrimSpace(parts[6]), 10, 64)
	}
	if len(parts) > 7 {
		row.NumFilesFailed, _ = strconv.ParseInt(strings.TrimSpace(parts[7]), 10, 64)
	}
	if len(parts) > 8 {
		row.NumFilesSkipped, _ = strconv.ParseInt(strings.TrimSpace(parts[8]), 10, 64)
	}
	if len(parts) > 9 {
		row.NumInodesIgnored, _ = strconv.ParseInt(strings.TrimSpace(parts[9]), 10, 64)
	}
	return row
}

func parseEventRetentionOperationKeyValueOutput(output string) *EventRetentionOperationRow {
	row := &EventRetentionOperationRow{}
	for _, line := range strings.Split(output, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		idx := strings.Index(trimmed, ":")
		if idx <= 0 {
			continue
		}
		key := strings.TrimSpace(trimmed[:idx])
		val := strings.TrimSpace(trimmed[idx+1:])
		switch strings.ToLower(key) {
		case "vserver":
			row.Vserver = val
		case "operation id":
			row.OperationID, _ = strconv.ParseInt(val, 10, 64)
		case "state", "operation status":
			row.State = strings.ToLower(val)
		case "path":
			row.Path = val
		case "policy", "policy name":
			row.PolicyName = val
		case "volume", "volume name":
			row.VolumeName = val
		case "num files processed", "files processed":
			row.NumFilesProcessed, _ = strconv.ParseInt(val, 10, 64)
		case "num files failed", "files failed":
			row.NumFilesFailed, _ = strconv.ParseInt(val, 10, 64)
		case "num files skipped", "files skipped":
			row.NumFilesSkipped, _ = strconv.ParseInt(val, 10, 64)
		case "num inodes ignored", "inodes ignored":
			row.NumInodesIgnored, _ = strconv.ParseInt(val, 10, 64)
		}
	}
	if row.OperationID == 0 && row.State == "" {
		return nil
	}
	return row
}
