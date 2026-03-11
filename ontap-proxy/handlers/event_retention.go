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
	retentionTimeRegex  = regexp.MustCompile(`^pt(\d+)([hms])$`) // PT24H, PT30M, PT60S
	retentionDateRegex  = regexp.MustCompile(`^p(\d+)([ymd])$`)  // P7Y, P30M, P7D
	retentionCLIRegex = regexp.MustCompile(`(?i)^(\d+)\s*(years?|months?|days?|hours?|minutes?|seconds?)$`)
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
	Vserver          string
	Name             string
	RetentionPeriod  string // API format (e.g. P7Y)
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
