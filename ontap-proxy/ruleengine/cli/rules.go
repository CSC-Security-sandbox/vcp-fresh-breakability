package cli

import (
	"context"
	"fmt"
	"strings"
)

// CLICondition is a function that validates a CLI command.
// Returns (true, "") if valid, or (false, "reason") if invalid.
// Used for local validation (argument checking) before credential setup.
type CLICondition func(cmd *CLICommand) (bool, string)

// CLIExternalValidator is a function that validates a CLI command with context.
// Returns (true, "") if valid, or (false, "reason") if invalid.
// Used for external API calls (e.g., core API validation) after credential setup.
// Similar to validateVolumeCreation in rules_v2/validators.go.
type CLIExternalValidator func(ctx context.Context, cmd *CLICommand) (bool, string)

// CLIRule defines access control and validation for CLI commands.
type CLIRule struct {
	// Pattern is the command pattern to match (supports wildcards with *)
	// Examples: "volume show", "volume *", "storage aggregate *"
	Pattern string

	// Allow indicates whether the command is allowed (true) or denied (false)
	Allow bool

	// Reason is the message returned when a command is denied
	Reason string

	// Condition is an optional validation function (use CLIAnd, CLIHasArgs, CLIIfPresentThenValue, etc.)
	// Similar to the DSL conditions in rules_v2/rule_map.go
	// Evaluated BEFORE credential setup - for local argument validation only.
	Condition CLICondition

	// ExternalValidator is an optional function for external API validation.
	// Evaluated AFTER credential setup - has access to auth data in context.
	// Similar to validateVolumeCreation in rules_v2/validators.go.
	ExternalValidator CLIExternalValidator

	// InjectArguments lists arguments to inject into the command before execution.
	// Arguments are added only if not already present in the command.
	// Example: {"-is-space-enforcement-logical": "true", "-is-space-reporting-logical": "true"}
	InjectArguments map[string]string

	// RemoveFields lists fields to remove from the CLI response
	RemoveFields []string
}

// cliRules is the ordered list of CLI rules. First match wins.
// Uses DSL-like conditions similar to rules_v2/rule_map.go for consistency.
// Only includes CLI equivalents of REST APIs defined in rule_map.go.
var cliRules = []CLIRule{
	// Storage Volumes - corresponds to /api/storage/volumes in rule_map.go
	// Supports both "volume" and shorthand "vol"
	{
		Pattern: "volume show",
		Allow:   true,
		RemoveFields: []string{
			"Used Size",
			"Used Percentage",
			"Physical Used",
			"Footprint",
			"Total Metadata",
			"Space Guarantee",
			"Space SLO",
			"Storage Efficiency",
			"Deduplication",
			"Compression",
			"Sis Space Saved",
			"Dedupe Space Saved",
			"Dedupe Space Shared",
			"Compression Space Saved",
			"Efficiency",
		},
	},
	{
		Pattern: "vol show",
		Allow:   true,
		RemoveFields: []string{
			"Used Size",
			"Used Percentage",
			"Physical Used",
			"Footprint",
			"Total Metadata",
			"Space Guarantee",
			"Space SLO",
			"Storage Efficiency",
			"Deduplication",
			"Compression",
			"Sis Space Saved",
			"Dedupe Space Saved",
			"Dedupe Space Shared",
			"Compression Space Saved",
			"Efficiency",
		},
	},
	{
		Pattern: "volume show-footprint",
		Allow:   false,
		Reason:  "not allowed",
	},
	{
		Pattern: "vol show-footprint",
		Allow:   false,
		Reason:  "not allowed",
	},
	{
		Pattern: "volume create",
		Allow:   true,
		Condition: CLIAnd(
			CLIHasArgs("-vserver", "-volume", "-size"),
			CLIIfPresentThenValue("-space-guarantee", "none"),
			// Enforce logical space settings - user cannot override
			CLIIfPresentThenEquals("-is-space-enforcement-logical", "true"),
			CLIIfPresentThenEquals("-is-space-reporting-logical", "true"),
		),
		ExternalValidator: validateVolumeCreate,
		InjectArguments: map[string]string{
			"-is-space-enforcement-logical": "true",
			"-is-space-reporting-logical":   "true",
		},
	},
	{
		Pattern: "vol create",
		Allow:   true,
		Condition: CLIAnd(
			CLIHasArgs("-vserver", "-volume", "-size"),
			CLIIfPresentThenValue("-space-guarantee", "none"),
			CLIIfPresentThenEquals("-is-space-enforcement-logical", "true"),
			CLIIfPresentThenEquals("-is-space-reporting-logical", "true"),
		),
		ExternalValidator: validateVolumeCreate,
		InjectArguments: map[string]string{
			"-is-space-enforcement-logical": "true",
			"-is-space-reporting-logical":   "true",
		},
	},
	{
		Pattern: "volume modify",
		Allow:   true,
		Condition: CLIAnd(
			CLIHasArgs("-vserver", "-volume"),
			CLIIfPresentThenValue("-space-guarantee", "none"),
			// Enforce logical space settings - user cannot override
			CLIIfPresentThenEquals("-is-space-enforcement-logical", "true"),
			CLIIfPresentThenEquals("-is-space-reporting-logical", "true"),
		),
		ExternalValidator: validateVolumeUpdate,
	},
	{
		Pattern: "vol modify",
		Allow:   true,
		Condition: CLIAnd(
			CLIHasArgs("-vserver", "-volume"),
			CLIIfPresentThenValue("-space-guarantee", "none"),
			// Enforce logical space settings - user cannot override
			CLIIfPresentThenEquals("-is-space-enforcement-logical", "true"),
			CLIIfPresentThenEquals("-is-space-reporting-logical", "true"),
		),
		ExternalValidator: validateVolumeUpdate,
	},
	{
		Pattern:           "volume delete",
		Allow:             true,
		Condition:         CLIHasArgs("-vserver", "-volume"),
		ExternalValidator: validateVolumeDelete,
	},
	{
		Pattern:           "vol delete",
		Allow:             true,
		Condition:         CLIHasArgs("-vserver", "-volume"),
		ExternalValidator: validateVolumeDelete,
	},
	// "volume destroy" / "vol destroy" — same as delete; triggers reconciliation on volume delete
	{
		Pattern:           "volume destroy",
		Allow:             true,
		Condition:         CLIHasArgs("-vserver", "-volume"),
		ExternalValidator: validateVolumeDelete,
	},
	{
		Pattern:           "vol destroy",
		Allow:             true,
		Condition:         CLIHasArgs("-vserver", "-volume"),
		ExternalValidator: validateVolumeDelete,
	},
	{
		Pattern:           "volume rename",
		Allow:             true,
		Condition:         CLIHasArgs("-vserver", "-volume", "-newname"),
		ExternalValidator: validateVolumeRename,
	},
	{
		Pattern:           "vol rename",
		Allow:             true,
		Condition:         CLIHasArgs("-vserver", "-volume", "-newname"),
		ExternalValidator: validateVolumeRename,
	},
	{
		Pattern:           "volume size",
		Allow:             true,
		Condition:         CLIHasArgs("-vserver", "-volume", "-new-size"),
		ExternalValidator: validateVolumeUpdate,
	},
	{
		Pattern:           "vol size",
		Allow:             true,
		Condition:         CLIHasArgs("-vserver", "-volume", "-new-size"),
		ExternalValidator: validateVolumeUpdate,
	},

	// Security Certificates - corresponds to /api/security/certificates in rule_map.go
	// Supports both "security" and shorthand "sec"
	{
		Pattern: "security certificate show",
		Allow:   true,
	},
	{
		Pattern: "sec certificate show",
		Allow:   true,
	},
	{
		Pattern: "security certificate install",
		Allow:   true,
	},
	{
		Pattern: "sec certificate install",
		Allow:   true,
	},
	{
		Pattern: "security certificate delete",
		Allow:   false,
		Reason:  "Certificate deletion not allowed",
	},
	{
		Pattern: "sec certificate delete",
		Allow:   false,
		Reason:  "Certificate deletion not allowed",
	},
	{
		Pattern: "storage disk *",
		Allow:   false,
		Reason:  "Disk operations not allowed",
	},
	// Diagnostic settings - block "set diag"
	{
		Pattern: "set diag",
		Allow:   false,
		Reason:  "Diagnostic settings not allowed",
	},
}

// GetCLIRules returns the list of CLI rules.
// Rules are evaluated in order; first match wins.
func GetCLIRules() []CLIRule {
	return cliRules
}

// MatchCLIRule finds the matching rule for a CLI command.
// Returns the matching rule and true if found, or nil and false if no rule matches.
// If no rule matches, the command should be allowed through (allow by default).
func MatchCLIRule(cmd *CLICommand) (*CLIRule, bool) {
	if cmd == nil {
		return nil, false
	}

	for i := range cliRules {
		if cmd.MatchesPattern(cliRules[i].Pattern) {
			return &cliRules[i], true
		}
	}

	// No matching rule - allow by default (return nil to indicate no rule)
	return nil, false
}

// EvaluateRule evaluates a rule against a command, including any local conditions.
// Returns (allowed, reason) where reason is empty if allowed.
// Note: This does NOT evaluate ExternalValidator - use EvaluateExternalValidator for that.
func EvaluateRule(rule *CLIRule, cmd *CLICommand) (bool, string) {
	if rule == nil {
		return false, "No rule provided"
	}

	// Check if rule allows the command
	if !rule.Allow {
		reason := rule.Reason
		if reason == "" {
			reason = "Command not allowed"
		}
		return false, reason
	}

	// Check condition if present (use CLIAnd, CLIHasArgs, etc. like DSL)
	if rule.Condition != nil {
		if ok, reason := rule.Condition(cmd); !ok {
			return false, reason
		}
	}

	return true, ""
}

// EvaluateExternalValidator evaluates a rule's external validator if present.
// This should be called AFTER credential setup, as external validators may need auth data.
// Returns (allowed, reason) where reason is empty if allowed or no validator present.
func EvaluateExternalValidator(ctx context.Context, rule *CLIRule, cmd *CLICommand) (bool, string) {
	if rule == nil || rule.ExternalValidator == nil {
		return true, "" // No external validator - pass through
	}
	return rule.ExternalValidator(ctx, cmd)
}

// HasExternalValidator returns true if the rule has an external validator.
func HasExternalValidator(rule *CLIRule) bool {
	return rule != nil && rule.ExternalValidator != nil
}

// HasInjectArguments returns true if the rule has arguments to inject.
func HasInjectArguments(rule *CLIRule) bool {
	return rule != nil && len(rule.InjectArguments) > 0
}

// ApplyInjectArguments injects arguments into a CLI command.
// Arguments are added only if not already present in the command.
// Returns the modified command string.
func ApplyInjectArguments(cmd *CLICommand, rule *CLIRule) string {
	if rule == nil || len(rule.InjectArguments) == 0 {
		return cmd.RawInput
	}

	// Start with original command
	result := cmd.RawInput

	// Add each argument if not already present
	var injected []string
	for argName, argValue := range rule.InjectArguments {
		if !cmd.HasArgument(argName) {
			injected = append(injected, fmt.Sprintf("%s %s", argName, argValue))
		}
	}

	// Append injected arguments to the command
	if len(injected) > 0 {
		result = result + " " + strings.Join(injected, " ")
	}

	return result
}

// GetInjectedArguments returns the list of arguments that would be injected.
// Useful for logging.
func GetInjectedArguments(cmd *CLICommand, rule *CLIRule) map[string]string {
	if rule == nil || len(rule.InjectArguments) == 0 {
		return nil
	}

	injected := make(map[string]string)
	for argName, argValue := range rule.InjectArguments {
		if !cmd.HasArgument(argName) {
			injected[argName] = argValue
		}
	}

	if len(injected) == 0 {
		return nil
	}
	return injected
}

// Condition builder functions for CLI rules
// These mirror the DSL conditions in rules_v2/rule_map.go for consistency

// CLIHasArgs checks that all specified arguments are present.
// Similar to HasFields() in the REST DSL.
// Example: CLIHasArgs("-vserver", "-volume")
func CLIHasArgs(argNames ...string) CLICondition {
	return func(cmd *CLICommand) (bool, string) {
		for _, argName := range argNames {
			if !cmd.HasArgument(argName) {
				return false, fmt.Sprintf("Missing required argument: %s", argName)
			}
		}
		return true, ""
	}
}

// CLIIfPresentThenValue validates an argument value only if the argument is present.
// Similar to IfPresentThenValue() in the REST DSL.
// Example: CLIIfPresentThenValue("-space-guarantee", "none", "volume")
func CLIIfPresentThenValue(argName string, allowedValues ...string) CLICondition {
	return func(cmd *CLICommand) (bool, string) {
		if !cmd.HasArgument(argName) {
			return true, "" // Argument not present, condition passes
		}
		value := cmd.GetArgument(argName)
		for _, allowed := range allowedValues {
			if strings.EqualFold(value, allowed) {
				return true, ""
			}
		}
		return false, fmt.Sprintf("Invalid value for %s: %q (allowed: %v)", argName, value, allowedValues)
	}
}

// CLIIfPresentThenEquals validates an argument equals a specific value if present.
// Similar to IfPresentThenEquals() in the REST DSL.
// Example: CLIIfPresentThenEquals("-force", "true")
func CLIIfPresentThenEquals(argName, expectedValue string) CLICondition {
	return func(cmd *CLICommand) (bool, string) {
		if !cmd.HasArgument(argName) {
			return true, "" // Argument not present, condition passes
		}
		value := cmd.GetArgument(argName)
		if strings.EqualFold(value, expectedValue) {
			return true, ""
		}
		return false, fmt.Sprintf("Argument %s must be %q, got %q", argName, expectedValue, value)
	}
}

// CLIAnd combines multiple conditions with AND logic.
// Similar to And() in the REST DSL.
// Example: CLIAnd(CLIHasArgs("-vserver"), CLIIfPresentThenValue("-type", "dp", "rw"))
func CLIAnd(conditions ...CLICondition) CLICondition {
	return func(cmd *CLICommand) (bool, string) {
		for _, cond := range conditions {
			if ok, reason := cond(cmd); !ok {
				return false, reason
			}
		}
		return true, ""
	}
}

// CLIOr combines multiple conditions with OR logic.
// Similar to Or() in the REST DSL.
func CLIOr(conditions ...CLICondition) CLICondition {
	return func(cmd *CLICommand) (bool, string) {
		var lastReason string
		for _, cond := range conditions {
			if ok, reason := cond(cmd); ok {
				return true, ""
			} else {
				lastReason = reason
			}
		}
		return false, lastReason
	}
}

// CLIHasFlag checks that a specific flag is present.
// Example: CLIHasFlag("-force")
func CLIHasFlag(flagName string) CLICondition {
	return func(cmd *CLICommand) (bool, string) {
		if cmd.HasFlag(flagName) {
			return true, ""
		}
		return false, fmt.Sprintf("Missing required flag: %s", flagName)
	}
}
