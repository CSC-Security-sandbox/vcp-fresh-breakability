package cli

import (
	"context"
	"testing"
)

func TestMatchCLIRule(t *testing.T) {
	// Tests match CLI rules defined in rules.go which correspond to rule_map.go

	t.Run("volume commands - corresponds to /api/storage/volumes", func(t *testing.T) {
		tests := []struct {
			name      string
			input     string
			wantAllow bool
		}{
			{
				name:      "volume show allowed",
				input:     "volume show -vserver vs1",
				wantAllow: true,
			},
			{
				name:      "volume create allowed",
				input:     "volume create -vserver vs1 -volume vol1 -size 100g -aggregate aggr1",
				wantAllow: true,
			},
			{
				name:      "volume modify allowed",
				input:     "volume modify -vserver vs1 -volume vol1 -size 200g",
				wantAllow: true,
			},
			{
				name:      "volume delete allowed",
				input:     "volume delete -vserver vs1 -volume vol1",
				wantAllow: true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				cmd, err := ParseCLICommand(tt.input)
				if err != nil {
					t.Fatalf("Failed to parse command: %v", err)
				}

				rule, found := MatchCLIRule(cmd)
				if !found && tt.wantAllow {
					t.Error("Expected to find a matching rule")
				}

				if rule.Allow != tt.wantAllow {
					t.Errorf("Allow = %v, want %v", rule.Allow, tt.wantAllow)
				}
			})
		}
	})

	t.Run("security certificate commands - corresponds to /api/security/certificates", func(t *testing.T) {
		tests := []struct {
			input     string
			wantAllow bool
		}{
			{"security certificate show", true},
			{"security certificate install -vserver vs1 -type server", true},
			{"security certificate delete -vserver vs1 -common-name cert1", false},
		}

		for _, tt := range tests {
			t.Run(tt.input, func(t *testing.T) {
				cmd, err := ParseCLICommand(tt.input)
				if err != nil {
					t.Fatalf("Failed to parse command: %v", err)
				}

				rule, _ := MatchCLIRule(cmd)

				if rule.Allow != tt.wantAllow {
					t.Errorf("Allow = %v, want %v", rule.Allow, tt.wantAllow)
				}
			})
		}
	})

	t.Run("nil command", func(t *testing.T) {
		rule, found := MatchCLIRule(nil)

		if found {
			t.Error("Expected not to find a rule for nil command")
		}
		if rule != nil {
			t.Error("Expected nil rule for nil command")
		}
	})

	t.Run("unknown command - no matching rule", func(t *testing.T) {
		cmd := &CLICommand{
			Command:     "unknown",
			Subcommand:  "command",
			FullCommand: "unknown command",
		}

		rule, found := MatchCLIRule(cmd)

		if found {
			t.Error("Expected not to find a rule for unknown command")
		}
		if rule != nil {
			t.Error("Expected nil rule for unknown command")
		}
	})
}

func TestEvaluateRule(t *testing.T) {
	t.Run("allowed rule", func(t *testing.T) {
		rule := &CLIRule{
			Pattern: "volume show",
			Allow:   true,
		}
		cmd := &CLICommand{FullCommand: "volume show"}

		allowed, reason := EvaluateRule(rule, cmd)

		if !allowed {
			t.Errorf("Expected allowed, got denied: %s", reason)
		}
		if reason != "" {
			t.Errorf("Expected empty reason, got %q", reason)
		}
	})

	t.Run("denied rule", func(t *testing.T) {
		rule := &CLIRule{
			Pattern: "system *",
			Allow:   false,
			Reason:  "System commands not allowed",
		}
		cmd := &CLICommand{FullCommand: "system node show"}

		allowed, reason := EvaluateRule(rule, cmd)

		if allowed {
			t.Error("Expected denied, got allowed")
		}
		if reason != "System commands not allowed" {
			t.Errorf("Reason = %q, want %q", reason, "System commands not allowed")
		}
	})

	t.Run("rule with passing condition", func(t *testing.T) {
		rule := &CLIRule{
			Pattern: "volume create",
			Allow:   true,
			Condition: func(cmd *CLICommand) (bool, string) {
				return true, ""
			},
		}
		cmd := &CLICommand{FullCommand: "volume create"}

		allowed, reason := EvaluateRule(rule, cmd)

		if !allowed {
			t.Errorf("Expected allowed, got denied: %s", reason)
		}
	})

	t.Run("rule with failing condition", func(t *testing.T) {
		rule := &CLIRule{
			Pattern: "volume create",
			Allow:   true,
			Condition: func(cmd *CLICommand) (bool, string) {
				return false, "Missing required argument"
			},
		}
		cmd := &CLICommand{FullCommand: "volume create"}

		allowed, reason := EvaluateRule(rule, cmd)

		if allowed {
			t.Error("Expected denied, got allowed")
		}
		if reason != "Missing required argument" {
			t.Errorf("Reason = %q, want %q", reason, "Missing required argument")
		}
	})

	t.Run("nil rule", func(t *testing.T) {
		allowed, reason := EvaluateRule(nil, &CLICommand{})

		if allowed {
			t.Error("Expected denied for nil rule")
		}
		if reason != "No rule provided" {
			t.Errorf("Reason = %q, want %q", reason, "No rule provided")
		}
	})
}

func TestCLIRuleConditions(t *testing.T) {
	// Test rule with DSL-like conditions (similar to rules_v2/rule_map.go)
	volumeCreateRule := &CLIRule{
		Pattern: "volume create",
		Allow:   true,
		Condition: CLIAnd(
			CLIHasArgs("-vserver", "-volume"),
			CLIIfPresentThenValue("-space-guarantee", "none", "volume"),
		),
	}

	t.Run("valid create command", func(t *testing.T) {
		cmd := &CLICommand{
			Arguments: map[string]string{
				"-vserver":   "vs1",
				"-volume":    "vol1",
				"-size":      "100g",
				"-aggregate": "aggr1",
			},
		}

		allowed, reason := EvaluateRule(volumeCreateRule, cmd)

		if !allowed {
			t.Errorf("Expected allowed, got denied: %s", reason)
		}
	})

	t.Run("missing vserver", func(t *testing.T) {
		cmd := &CLICommand{
			Arguments: map[string]string{
				"-volume": "vol1",
			},
		}

		allowed, reason := EvaluateRule(volumeCreateRule, cmd)

		if allowed {
			t.Error("Expected denied for missing vserver")
		}
		if reason != "Missing required argument: -vserver" {
			t.Errorf("Reason = %q, want %q", reason, "Missing required argument: -vserver")
		}
	})

	t.Run("missing volume", func(t *testing.T) {
		cmd := &CLICommand{
			Arguments: map[string]string{
				"-vserver": "vs1",
			},
		}

		allowed, reason := EvaluateRule(volumeCreateRule, cmd)

		if allowed {
			t.Error("Expected denied for missing volume")
		}
		if reason != "Missing required argument: -volume" {
			t.Errorf("Reason = %q, want %q", reason, "Missing required argument: -volume")
		}
	})

	t.Run("valid space guarantee", func(t *testing.T) {
		cmd := &CLICommand{
			Arguments: map[string]string{
				"-vserver":         "vs1",
				"-volume":          "vol1",
				"-space-guarantee": "none",
			},
		}

		allowed, reason := EvaluateRule(volumeCreateRule, cmd)

		if !allowed {
			t.Errorf("Expected allowed, got denied: %s", reason)
		}
	})

	t.Run("invalid space guarantee", func(t *testing.T) {
		cmd := &CLICommand{
			Arguments: map[string]string{
				"-vserver":         "vs1",
				"-volume":          "vol1",
				"-space-guarantee": "invalid",
			},
		}

		allowed, _ := EvaluateRule(volumeCreateRule, cmd)

		if allowed {
			t.Error("Expected denied for invalid space guarantee")
		}
	})

	t.Run("CLIIfPresentThenValue passes when arg not present", func(t *testing.T) {
		cmd := &CLICommand{
			Arguments: map[string]string{
				"-vserver": "vs1",
				"-volume":  "vol1",
				// No -space-guarantee - should still pass
			},
		}

		allowed, reason := EvaluateRule(volumeCreateRule, cmd)

		if !allowed {
			t.Errorf("Expected allowed when optional args not present, got denied: %s", reason)
		}
	})

	t.Run("snaplock-type compliance allowed by rule condition", func(t *testing.T) {
		// Volume CLI rules no longer validate -snaplock-type; previously-denied values
		// (e.g. compliance) must not be rejected by the local rule condition.
		cmd := &CLICommand{
			Arguments: map[string]string{
				"-vserver":       "vs1",
				"-volume":        "vol1",
				"-snaplock-type": "compliance",
			},
		}

		allowed, reason := EvaluateRule(volumeCreateRule, cmd)

		if !allowed {
			t.Errorf("Expected allowed with -snaplock-type compliance (rule condition does not validate snaplock-type), got denied: %s", reason)
		}
	})
}

func TestVolumeCreateRule_SnaplockTypeNotValidated(t *testing.T) {
	// Asserts that the real volume create rule (from rules.go) does not reject
	// commands based on -snaplock-type. A previously-denied value like "compliance"
	// or an unknown value must pass the rule condition (EvaluateRule does not run
	// ExternalValidator).
	input := "volume create -vserver vs1 -volume vol1 -size 100g -aggregate aggr1 -snaplock-type compliance"
	cmd, err := ParseCLICommand(input)
	if err != nil {
		t.Fatalf("ParseCLICommand: %v", err)
	}

	rule, found := MatchCLIRule(cmd)
	if !found {
		t.Fatal("Expected to find a matching rule for volume create")
	}

	allowed, reason := EvaluateRule(rule, cmd)
	if !allowed {
		t.Errorf("Rule condition should allow -snaplock-type compliance (snaplock-type is no longer validated); got denied: %s", reason)
	}
}

func TestCLIConditionBuilders(t *testing.T) {
	t.Run("CLIHasArgs single", func(t *testing.T) {
		cond := CLIHasArgs("-vserver")

		// Argument present
		cmd := &CLICommand{Arguments: map[string]string{"-vserver": "vs1"}}
		if ok, _ := cond(cmd); !ok {
			t.Error("Expected condition to pass when argument is present")
		}

		// Argument missing
		cmd = &CLICommand{Arguments: map[string]string{}}
		if ok, _ := cond(cmd); ok {
			t.Error("Expected condition to fail when argument is missing")
		}
	})

	t.Run("CLIHasArgs multiple", func(t *testing.T) {
		cond := CLIHasArgs("-vserver", "-volume")

		// Both present
		cmd := &CLICommand{Arguments: map[string]string{"-vserver": "vs1", "-volume": "vol1"}}
		if ok, _ := cond(cmd); !ok {
			t.Error("Expected condition to pass when all arguments present")
		}

		// One missing
		cmd = &CLICommand{Arguments: map[string]string{"-vserver": "vs1"}}
		if ok, _ := cond(cmd); ok {
			t.Error("Expected condition to fail when one argument missing")
		}
	})

	t.Run("CLIIfPresentThenEquals", func(t *testing.T) {
		cond := CLIIfPresentThenEquals("-force", "true")

		// Argument not present - should pass
		cmd := &CLICommand{Arguments: map[string]string{}}
		if ok, _ := cond(cmd); !ok {
			t.Error("Expected condition to pass when argument is not present")
		}

		// Correct value
		cmd = &CLICommand{Arguments: map[string]string{"-force": "true"}}
		if ok, _ := cond(cmd); !ok {
			t.Error("Expected condition to pass for correct value")
		}

		// Wrong value
		cmd = &CLICommand{Arguments: map[string]string{"-force": "false"}}
		if ok, _ := cond(cmd); ok {
			t.Error("Expected condition to fail for wrong value")
		}
	})

	t.Run("CLIIfPresentThenValue", func(t *testing.T) {
		cond := CLIIfPresentThenValue("-space-guarantee", "none", "volume")

		// Argument not present - should pass
		cmd := &CLICommand{Arguments: map[string]string{}}
		if ok, _ := cond(cmd); !ok {
			t.Error("Expected condition to pass when argument is not present")
		}

		// Argument with valid value
		cmd = &CLICommand{Arguments: map[string]string{"-space-guarantee": "none"}}
		if ok, _ := cond(cmd); !ok {
			t.Error("Expected condition to pass for valid value")
		}

		// Argument with invalid value
		cmd = &CLICommand{Arguments: map[string]string{"-space-guarantee": "invalid"}}
		if ok, _ := cond(cmd); ok {
			t.Error("Expected condition to fail for invalid value")
		}
	})

	t.Run("CLIAnd", func(t *testing.T) {
		cond := CLIAnd(
			CLIHasArgs("-vserver"),
			CLIHasArgs("-volume"),
		)

		// Both present
		cmd := &CLICommand{Arguments: map[string]string{"-vserver": "vs1", "-volume": "vol1"}}
		if ok, _ := cond(cmd); !ok {
			t.Error("Expected condition to pass when both arguments present")
		}

		// One missing
		cmd = &CLICommand{Arguments: map[string]string{"-vserver": "vs1"}}
		if ok, _ := cond(cmd); ok {
			t.Error("Expected condition to fail when one argument missing")
		}
	})

	t.Run("CLIOr", func(t *testing.T) {
		cond := CLIOr(
			CLIHasArgs("-vserver"),
			CLIHasArgs("-svm"),
		)

		// First present
		cmd := &CLICommand{Arguments: map[string]string{"-vserver": "vs1"}}
		if ok, _ := cond(cmd); !ok {
			t.Error("Expected condition to pass when first argument present")
		}

		// Second present
		cmd = &CLICommand{Arguments: map[string]string{"-svm": "vs1"}}
		if ok, _ := cond(cmd); !ok {
			t.Error("Expected condition to pass when second argument present")
		}

		// Neither present
		cmd = &CLICommand{Arguments: map[string]string{}}
		if ok, _ := cond(cmd); ok {
			t.Error("Expected condition to fail when neither argument present")
		}
	})

	t.Run("CLIHasFlag", func(t *testing.T) {
		cond := CLIHasFlag("-force")

		// Flag present
		cmd := &CLICommand{Flags: []string{"-force"}}
		if ok, _ := cond(cmd); !ok {
			t.Error("Expected condition to pass when flag is present")
		}

		// Flag missing
		cmd = &CLICommand{Flags: []string{}}
		if ok, _ := cond(cmd); ok {
			t.Error("Expected condition to fail when flag is missing")
		}
	})
}

func TestGetCLIRules(t *testing.T) {
	rules := GetCLIRules()

	if len(rules) == 0 {
		t.Error("Expected at least one rule")
	}

	// Check that volume show rule exists and has RemoveFields
	found := false
	for _, rule := range rules {
		if rule.Pattern == "volume show" {
			found = true
			if len(rule.RemoveFields) == 0 {
				t.Error("Expected volume show rule to have RemoveFields")
			}
			break
		}
	}
	if !found {
		t.Error("Expected to find volume show rule")
	}
}

func TestVolumeShowRemoveFields(t *testing.T) {
	// Assert exact RemoveFields for volume show / vol show (physical and efficiency fields).
	// Corresponds to rule_map.go GET /api/storage/volumes and /api/private/cli/volume response filtering.
	expectedFields := []string{
		"Used Size",
		"Used Percentage",
		"Physical Used",
		"Storage Efficiency",
		"Deduplication",
		"Compression",
		"Sis Space Saved",
		"Dedupe Space Saved",
		"Dedupe Space Shared",
		"Compression Space Saved",
		"Efficiency",
	}

	rules := GetCLIRules()

	t.Run("WhenVolumeShowRule_ShouldHaveExpectedRemoveFields", func(t *testing.T) {
		var volumeShowRule *CLIRule
		for i := range rules {
			if rules[i].Pattern == "volume show" {
				volumeShowRule = &rules[i]
				break
			}
		}
		if volumeShowRule == nil {
			t.Fatal("volume show rule not found")
		}
		if len(volumeShowRule.RemoveFields) != len(expectedFields) {
			t.Errorf("volume show RemoveFields length = %d, want %d", len(volumeShowRule.RemoveFields), len(expectedFields))
		}
		for i, want := range expectedFields {
			if i >= len(volumeShowRule.RemoveFields) {
				break
			}
			if volumeShowRule.RemoveFields[i] != want {
				t.Errorf("volume show RemoveFields[%d] = %q, want %q", i, volumeShowRule.RemoveFields[i], want)
			}
		}
	})

	t.Run("WhenVolShowRule_ShouldHaveSameRemoveFieldsAsVolumeShow", func(t *testing.T) {
		var volShowRule *CLIRule
		for i := range rules {
			if rules[i].Pattern == "vol show" {
				volShowRule = &rules[i]
				break
			}
		}
		if volShowRule == nil {
			t.Fatal("vol show rule not found")
		}
		if len(volShowRule.RemoveFields) != len(expectedFields) {
			t.Errorf("vol show RemoveFields length = %d, want %d", len(volShowRule.RemoveFields), len(expectedFields))
		}
		for i, want := range expectedFields {
			if i >= len(volShowRule.RemoveFields) {
				break
			}
			if volShowRule.RemoveFields[i] != want {
				t.Errorf("vol show RemoveFields[%d] = %q, want %q", i, volShowRule.RemoveFields[i], want)
			}
		}
	})
}

func TestEvaluateRule_DefaultReason(t *testing.T) {
	// Test that denied rule with empty reason gets default message
	t.Run("WhenDeniedRuleHasEmptyReason_ShouldReturnDefaultMessage", func(t *testing.T) {
		rule := &CLIRule{
			Pattern: "test command",
			Allow:   false,
			Reason:  "", // Empty reason
		}
		cmd := &CLICommand{FullCommand: "test command"}

		allowed, reason := EvaluateRule(rule, cmd)

		if allowed {
			t.Error("Expected denied, got allowed")
		}
		if reason != "Command not allowed" {
			t.Errorf("Reason = %q, want %q", reason, "Command not allowed")
		}
	})
}

func TestEvaluateExternalValidator(t *testing.T) {
	t.Run("WhenRuleIsNil_ShouldReturnAllowed", func(t *testing.T) {
		allowed, reason := EvaluateExternalValidator(nil, nil, &CLICommand{})

		if !allowed {
			t.Error("Expected allowed for nil rule")
		}
		if reason != "" {
			t.Errorf("Expected empty reason, got %q", reason)
		}
	})

	t.Run("WhenRuleHasNoExternalValidator_ShouldReturnAllowed", func(t *testing.T) {
		rule := &CLIRule{
			Pattern:           "volume show",
			Allow:             true,
			ExternalValidator: nil,
		}
		cmd := &CLICommand{FullCommand: "volume show"}

		allowed, reason := EvaluateExternalValidator(nil, rule, cmd)

		if !allowed {
			t.Error("Expected allowed when no external validator")
		}
		if reason != "" {
			t.Errorf("Expected empty reason, got %q", reason)
		}
	})

	t.Run("WhenExternalValidatorPasses_ShouldReturnAllowed", func(t *testing.T) {
		rule := &CLIRule{
			Pattern: "volume create",
			Allow:   true,
			ExternalValidator: func(ctx context.Context, cmd *CLICommand) (bool, string) {
				return true, ""
			},
		}
		cmd := &CLICommand{FullCommand: "volume create"}

		allowed, reason := EvaluateExternalValidator(nil, rule, cmd)

		if !allowed {
			t.Error("Expected allowed")
		}
		if reason != "" {
			t.Errorf("Expected empty reason, got %q", reason)
		}
	})

	t.Run("WhenExternalValidatorFails_ShouldReturnDenied", func(t *testing.T) {
		rule := &CLIRule{
			Pattern: "volume create",
			Allow:   true,
			ExternalValidator: func(ctx context.Context, cmd *CLICommand) (bool, string) {
				return false, "External validation failed"
			},
		}
		cmd := &CLICommand{FullCommand: "volume create"}

		allowed, reason := EvaluateExternalValidator(nil, rule, cmd)

		if allowed {
			t.Error("Expected denied")
		}
		if reason != "External validation failed" {
			t.Errorf("Reason = %q, want %q", reason, "External validation failed")
		}
	})
}

func TestHasExternalValidator(t *testing.T) {
	t.Run("WhenRuleIsNil_ShouldReturnFalse", func(t *testing.T) {
		if HasExternalValidator(nil) {
			t.Error("Expected false for nil rule")
		}
	})

	t.Run("WhenRuleHasNoValidator_ShouldReturnFalse", func(t *testing.T) {
		rule := &CLIRule{
			Pattern:           "volume show",
			Allow:             true,
			ExternalValidator: nil,
		}

		if HasExternalValidator(rule) {
			t.Error("Expected false for rule without validator")
		}
	})

	t.Run("WhenRuleHasValidator_ShouldReturnTrue", func(t *testing.T) {
		rule := &CLIRule{
			Pattern: "volume create",
			Allow:   true,
			ExternalValidator: func(ctx context.Context, cmd *CLICommand) (bool, string) {
				return true, ""
			},
		}

		if !HasExternalValidator(rule) {
			t.Error("Expected true for rule with validator")
		}
	})
}

func TestHasInjectArguments(t *testing.T) {
	t.Run("WhenRuleIsNil_ShouldReturnFalse", func(t *testing.T) {
		if HasInjectArguments(nil) {
			t.Error("Expected false for nil rule")
		}
	})

	t.Run("WhenRuleHasNoInjectArguments_ShouldReturnFalse", func(t *testing.T) {
		rule := &CLIRule{
			Pattern:         "volume show",
			Allow:           true,
			InjectArguments: nil,
		}

		if HasInjectArguments(rule) {
			t.Error("Expected false for rule without inject arguments")
		}
	})

	t.Run("WhenRuleHasEmptyInjectArguments_ShouldReturnFalse", func(t *testing.T) {
		rule := &CLIRule{
			Pattern:         "volume show",
			Allow:           true,
			InjectArguments: map[string]string{},
		}

		if HasInjectArguments(rule) {
			t.Error("Expected false for rule with empty inject arguments")
		}
	})

	t.Run("WhenRuleHasInjectArguments_ShouldReturnTrue", func(t *testing.T) {
		rule := &CLIRule{
			Pattern: "volume create",
			Allow:   true,
			InjectArguments: map[string]string{
				"-is-space-enforcement-logical": "true",
			},
		}

		if !HasInjectArguments(rule) {
			t.Error("Expected true for rule with inject arguments")
		}
	})
}

func TestApplyInjectArguments(t *testing.T) {
	t.Run("WhenRuleIsNil_ShouldReturnOriginalCommand", func(t *testing.T) {
		cmd := &CLICommand{
			RawInput:  "volume create -vserver vs1",
			Arguments: map[string]string{"-vserver": "vs1"},
		}

		result := ApplyInjectArguments(cmd, nil)

		if result != cmd.RawInput {
			t.Errorf("Result = %q, want %q", result, cmd.RawInput)
		}
	})

	t.Run("WhenRuleHasEmptyInjectArguments_ShouldReturnOriginalCommand", func(t *testing.T) {
		cmd := &CLICommand{
			RawInput:  "volume create -vserver vs1",
			Arguments: map[string]string{"-vserver": "vs1"},
		}
		rule := &CLIRule{
			Pattern:         "volume create",
			Allow:           true,
			InjectArguments: map[string]string{},
		}

		result := ApplyInjectArguments(cmd, rule)

		if result != cmd.RawInput {
			t.Errorf("Result = %q, want %q", result, cmd.RawInput)
		}
	})

	t.Run("WhenArgumentsMissing_ShouldInjectThem", func(t *testing.T) {
		cmd := &CLICommand{
			RawInput:  "volume create -vserver vs1 -volume vol1 -size 100g",
			Arguments: map[string]string{"-vserver": "vs1", "-volume": "vol1", "-size": "100g"},
		}
		rule := &CLIRule{
			Pattern: "volume create",
			Allow:   true,
			InjectArguments: map[string]string{
				"-is-space-enforcement-logical": "true",
				"-is-space-reporting-logical":   "true",
			},
		}

		result := ApplyInjectArguments(cmd, rule)

		if result == cmd.RawInput {
			t.Error("Expected arguments to be injected")
		}
		if !contains(result, "-is-space-enforcement-logical true") {
			t.Errorf("Expected -is-space-enforcement-logical true to be injected, got %q", result)
		}
		if !contains(result, "-is-space-reporting-logical true") {
			t.Errorf("Expected -is-space-reporting-logical true to be injected, got %q", result)
		}
	})

	t.Run("WhenArgumentsAlreadyExist_ShouldNotInjectThem", func(t *testing.T) {
		cmd := &CLICommand{
			RawInput: "volume create -vserver vs1 -is-space-enforcement-logical true",
			Arguments: map[string]string{
				"-vserver":                      "vs1",
				"-is-space-enforcement-logical": "true",
			},
		}
		rule := &CLIRule{
			Pattern: "volume create",
			Allow:   true,
			InjectArguments: map[string]string{
				"-is-space-enforcement-logical": "true",
				"-is-space-reporting-logical":   "true",
			},
		}

		result := ApplyInjectArguments(cmd, rule)

		// Should only inject the missing argument
		if !contains(result, "-is-space-reporting-logical true") {
			t.Errorf("Expected -is-space-reporting-logical true to be injected, got %q", result)
		}
		// Should not duplicate the existing argument
		count := countOccurrences(result, "-is-space-enforcement-logical")
		if count > 1 {
			t.Errorf("Expected -is-space-enforcement-logical to appear once, got %d times", count)
		}
	})

	t.Run("WhenAllArgumentsAlreadyPresent_ShouldReturnUnchanged", func(t *testing.T) {
		cmd := &CLICommand{
			RawInput: "volume create -vserver vs1 -is-space-enforcement-logical true -is-space-reporting-logical true",
			Arguments: map[string]string{
				"-vserver":                      "vs1",
				"-is-space-enforcement-logical": "true",
				"-is-space-reporting-logical":   "true",
			},
		}
		rule := &CLIRule{
			Pattern: "volume create",
			Allow:   true,
			InjectArguments: map[string]string{
				"-is-space-enforcement-logical": "true",
				"-is-space-reporting-logical":   "true",
			},
		}

		result := ApplyInjectArguments(cmd, rule)

		// When all arguments are already present, result should be unchanged
		if result != cmd.RawInput {
			t.Errorf("Result = %q, want %q (no changes)", result, cmd.RawInput)
		}
	})
}

func TestGetInjectedArguments(t *testing.T) {
	t.Run("WhenRuleIsNil_ShouldReturnNil", func(t *testing.T) {
		cmd := &CLICommand{
			Arguments: map[string]string{"-vserver": "vs1"},
		}

		result := GetInjectedArguments(cmd, nil)

		if result != nil {
			t.Errorf("Expected nil, got %v", result)
		}
	})

	t.Run("WhenRuleHasEmptyInjectArguments_ShouldReturnNil", func(t *testing.T) {
		cmd := &CLICommand{
			Arguments: map[string]string{"-vserver": "vs1"},
		}
		rule := &CLIRule{
			Pattern:         "volume create",
			Allow:           true,
			InjectArguments: map[string]string{},
		}

		result := GetInjectedArguments(cmd, rule)

		if result != nil {
			t.Errorf("Expected nil, got %v", result)
		}
	})

	t.Run("WhenArgumentsWouldBeInjected_ShouldReturnThem", func(t *testing.T) {
		cmd := &CLICommand{
			Arguments: map[string]string{"-vserver": "vs1", "-volume": "vol1"},
		}
		rule := &CLIRule{
			Pattern: "volume create",
			Allow:   true,
			InjectArguments: map[string]string{
				"-is-space-enforcement-logical": "true",
				"-is-space-reporting-logical":   "true",
			},
		}

		result := GetInjectedArguments(cmd, rule)

		if len(result) != 2 {
			t.Errorf("Expected 2 injected arguments, got %d", len(result))
		}
		if result["-is-space-enforcement-logical"] != "true" {
			t.Error("Expected -is-space-enforcement-logical to be injected")
		}
		if result["-is-space-reporting-logical"] != "true" {
			t.Error("Expected -is-space-reporting-logical to be injected")
		}
	})

	t.Run("WhenArgumentsAlreadyPresent_ShouldExcludeThem", func(t *testing.T) {
		cmd := &CLICommand{
			Arguments: map[string]string{
				"-vserver":                      "vs1",
				"-is-space-enforcement-logical": "true", // Already present
			},
		}
		rule := &CLIRule{
			Pattern: "volume create",
			Allow:   true,
			InjectArguments: map[string]string{
				"-is-space-enforcement-logical": "true",
				"-is-space-reporting-logical":   "true",
			},
		}

		result := GetInjectedArguments(cmd, rule)

		if len(result) != 1 {
			t.Errorf("Expected 1 injected argument, got %d", len(result))
		}
		if _, exists := result["-is-space-enforcement-logical"]; exists {
			t.Error("Should not include already present argument")
		}
		if result["-is-space-reporting-logical"] != "true" {
			t.Error("Expected -is-space-reporting-logical to be in result")
		}
	})

	t.Run("WhenAllArgumentsAlreadyPresent_ShouldReturnNil", func(t *testing.T) {
		cmd := &CLICommand{
			Arguments: map[string]string{
				"-vserver":                      "vs1",
				"-is-space-enforcement-logical": "true",
				"-is-space-reporting-logical":   "true",
			},
		}
		rule := &CLIRule{
			Pattern: "volume create",
			Allow:   true,
			InjectArguments: map[string]string{
				"-is-space-enforcement-logical": "true",
				"-is-space-reporting-logical":   "true",
			},
		}

		result := GetInjectedArguments(cmd, rule)

		if result != nil {
			t.Errorf("Expected nil when all arguments present, got %v", result)
		}
	})
}

// Helper functions for tests
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsSubstring(s, substr))
}

func containsSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func countOccurrences(s, substr string) int {
	count := 0
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			count++
		}
	}
	return count
}
