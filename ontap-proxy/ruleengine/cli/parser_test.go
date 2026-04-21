package cli

import (
	"strings"
	"testing"
)

func TestParseCLICommand(t *testing.T) {
	t.Run("WhenBasicCommands_ShouldParse", func(t *testing.T) {
		tests := []struct {
			name        string
			input       string
			wantCommand string
			wantSubcmd  string
			wantFull    string
			wantArgs    map[string]string
			wantFlags   []string
			wantErr     bool
		}{
			{
				name:        "WhenSimpleShowCommand_ShouldParse",
				input:       "volume show",
				wantCommand: "volume",
				wantSubcmd:  "show",
				wantFull:    "volume show",
				wantArgs:    map[string]string{},
				wantFlags:   []string{},
			},
			{
				name:        "WhenCommandWithArguments_ShouldParseArgs",
				input:       "volume show -vserver vs1 -volume vol1",
				wantCommand: "volume",
				wantSubcmd:  "show",
				wantFull:    "volume show",
				wantArgs:    map[string]string{"-vserver": "vs1", "-volume": "vol1"},
				wantFlags:   []string{},
			},
			{
				name:        "WhenCommandWithFlags_ShouldParseFlags",
				input:       "volume show -vserver vs1 -root",
				wantCommand: "volume",
				wantSubcmd:  "show",
				wantFull:    "volume show",
				wantArgs:    map[string]string{"-vserver": "vs1"},
				wantFlags:   []string{"-root"},
			},
			{
				name:        "WhenMultiWordCommand_ShouldParseFullCommand",
				input:       "storage aggregate show -aggregate aggr1",
				wantCommand: "storage aggregate",
				wantSubcmd:  "show",
				wantFull:    "storage aggregate show",
				wantArgs:    map[string]string{"-aggregate": "aggr1"},
				wantFlags:   []string{},
			},
			{
				name:        "WhenCreateCommandWithMultipleArgs_ShouldParseAll",
				input:       "volume create -vserver vs1 -volume vol1 -size 100g -aggregate aggr1",
				wantCommand: "volume",
				wantSubcmd:  "create",
				wantFull:    "volume create",
				wantArgs: map[string]string{
					"-vserver":   "vs1",
					"-volume":    "vol1",
					"-size":      "100g",
					"-aggregate": "aggr1",
				},
				wantFlags: []string{},
			},
			{
				name:        "WhenSingleCommandWithoutSubcommand_ShouldParse",
				input:       "help",
				wantCommand: "help",
				wantSubcmd:  "",
				wantFull:    "help",
				wantArgs:    map[string]string{},
				wantFlags:   []string{},
			},
			{
				name:        "WhenCommandWithDoubleQuotedValue_ShouldStripQuotes",
				input:       `volume create -vserver vs1 -comment "my test volume"`,
				wantCommand: "volume",
				wantSubcmd:  "create",
				wantFull:    "volume create",
				wantArgs:    map[string]string{"-vserver": "vs1", "-comment": "my test volume"},
				wantFlags:   []string{},
			},
			{
				name:        "WhenCommandWithSingleQuotedValue_ShouldStripQuotes",
				input:       `volume create -vserver vs1 -comment 'my test volume'`,
				wantCommand: "volume",
				wantSubcmd:  "create",
				wantFull:    "volume create",
				wantArgs:    map[string]string{"-vserver": "vs1", "-comment": "my test volume"},
				wantFlags:   []string{},
			},
			{
				name:        "WhenCommandWithExtraWhitespace_ShouldNormalize",
				input:       "  volume   show   -vserver   vs1  ",
				wantCommand: "volume",
				wantSubcmd:  "show",
				wantFull:    "volume show",
				wantArgs:    map[string]string{"-vserver": "vs1"},
				wantFlags:   []string{},
			},
			{
				name:        "WhenSnapshotCommand_ShouldParseAll",
				input:       "snapshot create -vserver vs1 -volume vol1 -snapshot snap1",
				wantCommand: "snapshot",
				wantSubcmd:  "create",
				wantFull:    "snapshot create",
				wantArgs: map[string]string{
					"-vserver":  "vs1",
					"-volume":   "vol1",
					"-snapshot": "snap1",
				},
				wantFlags: []string{},
			},
			{
				name:        "WhenNetworkInterfaceCommand_ShouldParseMultiWord",
				input:       "network interface show -vserver vs1",
				wantCommand: "network interface",
				wantSubcmd:  "show",
				wantFull:    "network interface show",
				wantArgs:    map[string]string{"-vserver": "vs1"},
				wantFlags:   []string{},
			},
			{
				name:        "volume clone create multi-word command",
				input:       "volume clone create -vserver vs1 -flexclone clone1 -parent-volume src1",
				wantCommand: "volume clone",
				wantSubcmd:  "create",
				wantFull:    "volume clone create",
				wantArgs: map[string]string{
					"-vserver":       "vs1",
					"-flexclone":     "clone1",
					"-parent-volume": "src1",
				},
				wantFlags: []string{},
			},
			{
				name:        "volume clone split start multi-word command",
				input:       "volume clone split start -vserver vs1 -flexclone clone1",
				wantCommand: "volume clone split",
				wantSubcmd:  "start",
				wantFull:    "volume clone split start",
				wantArgs: map[string]string{
					"-vserver":   "vs1",
					"-flexclone": "clone1",
				},
				wantFlags: []string{},
			},
			{
				name:    "WhenEmptyCommand_ShouldReturnError",
				input:   "",
				wantErr: true,
			},
			{
				name:    "WhenWhitespaceOnly_ShouldReturnError",
				input:   "   ",
				wantErr: true,
			},
			{
				name:    "WhenUnclosedQuote_ShouldReturnError",
				input:   `volume create -comment "unclosed`,
				wantErr: true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				cmd, err := ParseCLICommand(tt.input)

				if tt.wantErr {
					if err == nil {
						t.Errorf("ParseCLICommand() expected error, got nil")
					}
					return
				}

				if err != nil {
					t.Fatalf("ParseCLICommand() unexpected error: %v", err)
				}

				if cmd.Command != tt.wantCommand {
					t.Errorf("Command = %q, want %q", cmd.Command, tt.wantCommand)
				}
				if cmd.Subcommand != tt.wantSubcmd {
					t.Errorf("Subcommand = %q, want %q", cmd.Subcommand, tt.wantSubcmd)
				}
				if cmd.FullCommand != tt.wantFull {
					t.Errorf("FullCommand = %q, want %q", cmd.FullCommand, tt.wantFull)
				}

				// Check arguments
				if len(cmd.Arguments) != len(tt.wantArgs) {
					t.Errorf("Arguments count = %d, want %d", len(cmd.Arguments), len(tt.wantArgs))
				}
				for key, wantVal := range tt.wantArgs {
					if gotVal, ok := cmd.Arguments[key]; !ok {
						t.Errorf("Missing argument %q", key)
					} else if gotVal != wantVal {
						t.Errorf("Arguments[%q] = %q, want %q", key, gotVal, wantVal)
					}
				}

				// Check flags
				if len(cmd.Flags) != len(tt.wantFlags) {
					t.Errorf("Flags count = %d, want %d", len(cmd.Flags), len(tt.wantFlags))
				}
				for i, wantFlag := range tt.wantFlags {
					if i < len(cmd.Flags) && cmd.Flags[i] != wantFlag {
						t.Errorf("Flags[%d] = %q, want %q", i, cmd.Flags[i], wantFlag)
					}
				}
			})
		}
	})
}

func TestCLICommand_HasArgument(t *testing.T) {
	cmd := &CLICommand{
		Arguments: map[string]string{
			"-vserver": "vs1",
			"-volume":  "vol1",
		},
	}

	tests := []struct {
		name string
		arg  string
		want bool
	}{
		{"WhenExistingArgument_ShouldReturnTrue", "-vserver", true},
		{"WhenAnotherExisting_ShouldReturnTrue", "-volume", true},
		{"WhenNonExisting_ShouldReturnFalse", "-aggregate", false},
		{"WhenEmptyString_ShouldReturnFalse", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := cmd.HasArgument(tt.arg); got != tt.want {
				t.Errorf("HasArgument(%q) = %v, want %v", tt.arg, got, tt.want)
			}
		})
	}
}

func TestCLICommand_GetArgument(t *testing.T) {
	cmd := &CLICommand{
		Arguments: map[string]string{
			"-vserver": "vs1",
			"-size":    "100g",
		},
	}

	tests := []struct {
		name string
		arg  string
		want string
	}{
		{"WhenExistingArgument_ShouldReturnValue", "-vserver", "vs1"},
		{"WhenAnotherExisting_ShouldReturnValue", "-size", "100g"},
		{"WhenNonExisting_ShouldReturnEmpty", "-aggregate", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := cmd.GetArgument(tt.arg); got != tt.want {
				t.Errorf("GetArgument(%q) = %q, want %q", tt.arg, got, tt.want)
			}
		})
	}
}

func TestCLICommand_HasFlag(t *testing.T) {
	cmd := &CLICommand{
		Flags: []string{"-root", "-verbose"},
	}

	tests := []struct {
		name string
		flag string
		want bool
	}{
		{"WhenExistingFlag_ShouldReturnTrue", "-root", true},
		{"WhenAnotherExisting_ShouldReturnTrue", "-verbose", true},
		{"WhenNonExisting_ShouldReturnFalse", "-quiet", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := cmd.HasFlag(tt.flag); got != tt.want {
				t.Errorf("HasFlag(%q) = %v, want %v", tt.flag, got, tt.want)
			}
		})
	}
}

func TestCLICommand_IsCommand(t *testing.T) {
	cmd := &CLICommand{
		Command:    "volume",
		Subcommand: "show",
	}

	tests := []struct {
		name    string
		command string
		subcmd  string
		want    bool
	}{
		{"WhenExactMatch_ShouldReturnTrue", "volume", "show", true},
		{"WhenCaseInsensitiveCommand_ShouldReturnTrue", "Volume", "show", true},
		{"WhenCaseInsensitiveSubcommand_ShouldReturnTrue", "volume", "Show", true},
		{"WhenBothCaseInsensitive_ShouldReturnTrue", "VOLUME", "SHOW", true},
		{"WhenWrongCommand_ShouldReturnFalse", "snapshot", "show", false},
		{"WhenWrongSubcommand_ShouldReturnFalse", "volume", "create", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := cmd.IsCommand(tt.command, tt.subcmd); got != tt.want {
				t.Errorf("IsCommand(%q, %q) = %v, want %v", tt.command, tt.subcmd, got, tt.want)
			}
		})
	}
}

func TestCLICommand_MatchesPattern(t *testing.T) {
	tests := []struct {
		name    string
		cmd     *CLICommand
		pattern string
		want    bool
	}{
		{
			name: "WhenExactMatch_ShouldReturnTrue",
			cmd: &CLICommand{
				Command:     "volume",
				Subcommand:  "show",
				FullCommand: "volume show",
			},
			pattern: "volume show",
			want:    true,
		},
		{
			name: "WhenExactMatchCaseInsensitive_ShouldReturnTrue",
			cmd: &CLICommand{
				Command:     "volume",
				Subcommand:  "show",
				FullCommand: "volume show",
			},
			pattern: "Volume Show",
			want:    true,
		},
		{
			name: "WhenWildcardMatch_ShouldReturnTrue",
			cmd: &CLICommand{
				Command:     "volume",
				Subcommand:  "show",
				FullCommand: "volume show",
			},
			pattern: "volume *",
			want:    true,
		},
		{
			name: "WhenWildcardMatchCreate_ShouldReturnTrue",
			cmd: &CLICommand{
				Command:     "volume",
				Subcommand:  "create",
				FullCommand: "volume create",
			},
			pattern: "volume *",
			want:    true,
		},
		{
			name: "WhenMultiWordCommandWildcard_ShouldReturnTrue",
			cmd: &CLICommand{
				Command:     "storage aggregate",
				Subcommand:  "show",
				FullCommand: "storage aggregate show",
			},
			pattern: "storage aggregate *",
			want:    true,
		},
		{
			name: "WhenDifferentCommand_ShouldReturnFalse",
			cmd: &CLICommand{
				Command:     "snapshot",
				Subcommand:  "show",
				FullCommand: "snapshot show",
			},
			pattern: "volume *",
			want:    false,
		},
		{
			name: "WhenDifferentSubcommand_ShouldReturnFalse",
			cmd: &CLICommand{
				Command:     "volume",
				Subcommand:  "show",
				FullCommand: "volume show",
			},
			pattern: "volume create",
			want:    false,
		},
		{
			name: "WhenEmptyPattern_ShouldReturnFalse",
			cmd: &CLICommand{
				Command:     "volume",
				Subcommand:  "show",
				FullCommand: "volume show",
			},
			pattern: "",
			want:    false,
		},
		{
			name: "WhenSystemWildcard_ShouldReturnTrue",
			cmd: &CLICommand{
				Command:     "system",
				Subcommand:  "node",
				FullCommand: "system node",
			},
			pattern: "system *",
			want:    true,
		},
		{
			name: "WhenPrefixMatchWithPositionalArg_ShouldReturnTrue",
			cmd: &CLICommand{
				Command:     "volume show",
				Subcommand:  "vol3",
				FullCommand: "volume show vol3",
			},
			pattern: "volume show",
			want:    true,
		},
		{
			name: "WhenPrefixMatchCaseInsensitive_ShouldReturnTrue",
			cmd: &CLICommand{
				Command:     "volume show",
				Subcommand:  "Vol3",
				FullCommand: "volume show Vol3",
			},
			pattern: "Volume Show",
			want:    true,
		},
		{
			name: "WhenShowFootprint_ShouldNotMatchShowPrefix",
			cmd: &CLICommand{
				Command:     "volume",
				Subcommand:  "show-footprint",
				FullCommand: "volume show-footprint",
			},
			pattern: "volume show",
			want:    false,
		},
		{
			name: "WhenDifferentCommandWithPrefix_ShouldReturnFalse",
			cmd: &CLICommand{
				Command:     "volume shower",
				Subcommand:  "vol3",
				FullCommand: "volume shower vol3",
			},
			pattern: "volume show",
			want:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.cmd.MatchesPattern(tt.pattern); got != tt.want {
				t.Errorf("MatchesPattern(%q) = %v, want %v", tt.pattern, got, tt.want)
			}
		})
	}
}

func TestTokenize(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    []string
		wantErr bool
	}{
		{
			name:  "WhenSimpleTokens_ShouldSplitCorrectly",
			input: "volume show -vserver vs1",
			want:  []string{"volume", "show", "-vserver", "vs1"},
		},
		{
			name:  "WhenDoubleQuotedString_ShouldStripQuotes",
			input: `volume create -comment "my volume"`,
			want:  []string{"volume", "create", "-comment", "my volume"},
		},
		{
			name:  "WhenSingleQuotedString_ShouldStripQuotes",
			input: `volume create -comment 'my volume'`,
			want:  []string{"volume", "create", "-comment", "my volume"},
		},
		{
			name:  "WhenMultipleSpaces_ShouldNormalize",
			input: "volume   show    -vserver   vs1",
			want:  []string{"volume", "show", "-vserver", "vs1"},
		},
		{
			name:    "WhenUnclosedDoubleQuote_ShouldReturnError",
			input:   `volume create -comment "unclosed`,
			wantErr: true,
		},
		{
			name:    "WhenUnclosedSingleQuote_ShouldReturnError",
			input:   `volume create -comment 'unclosed`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tokenize(tt.input)

			if tt.wantErr {
				if err == nil {
					t.Error("tokenize() expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("tokenize() unexpected error: %v", err)
			}

			if len(got) != len(tt.want) {
				t.Errorf("tokenize() returned %d tokens, want %d", len(got), len(tt.want))
				return
			}

			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("tokenize()[%d] = %q, want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestParseCLICommand_EdgeCases(t *testing.T) {
	t.Run("WhenCommandStartsWithDash_ShouldReturnError", func(t *testing.T) {
		// Input that starts with arguments only (no command tokens) returns error
		_, err := ParseCLICommand("-vserver vs1")
		if err == nil {
			t.Error("ParseCLICommand() expected error for input starting with dash, got nil")
		}
	})

	t.Run("WhenPositionalArgumentsPresent_ShouldSkipThem", func(t *testing.T) {
		// Test command with positional arguments (values without preceding flag)
		cmd, err := ParseCLICommand("volume show -vserver vs1 positional_value -volume vol1")
		if err != nil {
			t.Fatalf("ParseCLICommand() unexpected error: %v", err)
		}

		// Named arguments should be captured
		if cmd.Arguments["-vserver"] != "vs1" {
			t.Errorf("Expected -vserver=vs1, got %q", cmd.Arguments["-vserver"])
		}
		if cmd.Arguments["-volume"] != "vol1" {
			t.Errorf("Expected -volume=vol1, got %q", cmd.Arguments["-volume"])
		}
		// Positional argument should be skipped (not treated as flag or argument)
		if len(cmd.Flags) != 0 {
			t.Errorf("Expected no flags, positional should be skipped, got %v", cmd.Flags)
		}
	})

	t.Run("WhenConsecutiveFlagsPresent_ShouldParseAsFlags", func(t *testing.T) {
		cmd, err := ParseCLICommand("volume delete -vserver vs1 -force -verbose -volume vol1")
		if err != nil {
			t.Fatalf("ParseCLICommand() unexpected error: %v", err)
		}

		// -force and -verbose should be flags (no value after them)
		if !cmd.HasFlag("-force") {
			t.Error("Expected -force to be a flag")
		}
		if !cmd.HasFlag("-verbose") {
			t.Error("Expected -verbose to be a flag")
		}
		// -volume should be an argument with value
		if cmd.Arguments["-volume"] != "vol1" {
			t.Errorf("Expected -volume=vol1, got %q", cmd.Arguments["-volume"])
		}
	})

	t.Run("WhenFlagAtEndOfCommand_ShouldParseAsFlag", func(t *testing.T) {
		cmd, err := ParseCLICommand("volume show -vserver vs1 -force")
		if err != nil {
			t.Fatalf("ParseCLICommand() unexpected error: %v", err)
		}

		if !cmd.HasFlag("-force") {
			t.Error("Expected -force to be a flag")
		}
	})

	// ParseCLICommand does not split on semicolons (use ParseCLIChain for that).
	t.Run("WhenSemicolonInSingleCommand_ShouldTreatAsOneToken", func(t *testing.T) {
		cmd, err := ParseCLICommand("set diag; volume create -vserver vs1 -volume vol1 -size 100g")
		if err != nil {
			t.Fatalf("ParseCLICommand() unexpected error: %v", err)
		}
		if cmd.FullCommand != "set diag; volume create" {
			t.Errorf("FullCommand = %q, want %q", cmd.FullCommand, "set diag; volume create")
		}
	})
}

func TestParseCLIChain(t *testing.T) {
	t.Run("WhenSingleCommandWithoutSemicolon_ShouldParseSingleCommand", func(t *testing.T) {
		chain, err := ParseCLIChain("volume show -vserver vs1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(chain.Commands) != 1 {
			t.Fatalf("expected 1 command, got %d", len(chain.Commands))
		}
		if chain.PrimaryCommand.FullCommand != "volume show" {
			t.Errorf("PrimaryCommand.FullCommand = %q, want %q", chain.PrimaryCommand.FullCommand, "volume show")
		}
		if chain.SetPrefix != "" {
			t.Errorf("SetPrefix = %q, want empty", chain.SetPrefix)
		}
	})

	t.Run("WhenSetDiagPrefix_ShouldParseChainedCommand", func(t *testing.T) {
		chain, err := ParseCLIChain("set diag; volume show -vserver vs1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(chain.Commands) != 2 {
			t.Fatalf("expected 2 commands, got %d", len(chain.Commands))
		}
		if chain.Commands[0].Command != "set" {
			t.Errorf("first command = %q, want %q", chain.Commands[0].Command, "set")
		}
		if chain.PrimaryCommand.FullCommand != "volume show" {
			t.Errorf("PrimaryCommand.FullCommand = %q, want %q", chain.PrimaryCommand.FullCommand, "volume show")
		}
		if chain.SetPrefix != "set diag" {
			t.Errorf("SetPrefix = %q, want %q", chain.SetPrefix, "set diag")
		}
	})

	t.Run("WhenSetAdvancedPrefix_ShouldParseChainedCommand", func(t *testing.T) {
		chain, err := ParseCLIChain("set advanced; volume create -vserver vs1 -volume vol1 -size 100g")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(chain.Commands) != 2 {
			t.Fatalf("expected 2 commands, got %d", len(chain.Commands))
		}
		if chain.SetPrefix != "set advanced" {
			t.Errorf("SetPrefix = %q, want %q", chain.SetPrefix, "set advanced")
		}
		if chain.PrimaryCommand.Command != "volume" {
			t.Errorf("PrimaryCommand.Command = %q, want %q", chain.PrimaryCommand.Command, "volume")
		}
		if chain.PrimaryCommand.Subcommand != "create" {
			t.Errorf("PrimaryCommand.Subcommand = %q, want %q", chain.PrimaryCommand.Subcommand, "create")
		}
	})

	t.Run("WhenSetWithPrivilegeFlag_ShouldParseChainedCommand", func(t *testing.T) {
		chain, err := ParseCLIChain("set -privilege diagnostic; volume show -instance")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if chain.SetPrefix != "set -privilege diagnostic" {
			t.Errorf("SetPrefix = %q, want %q", chain.SetPrefix, "set -privilege diagnostic")
		}
		if chain.PrimaryCommand.FullCommand != "volume show" {
			t.Errorf("PrimaryCommand.FullCommand = %q, want %q", chain.PrimaryCommand.FullCommand, "volume show")
		}
	})

	t.Run("WhenMoreThan2Commands_ShouldReturnError", func(t *testing.T) {
		_, err := ParseCLIChain("set diag; volume show; snapshot show")
		if err == nil {
			t.Fatal("expected error for 3 commands, got nil")
		}
		if want := "at most 2 commands can be chained, got 3"; err.Error() != want {
			t.Errorf("error = %q, want %q", err.Error(), want)
		}
	})

	t.Run("WhenFourCommands_ShouldReturnError", func(t *testing.T) {
		_, err := ParseCLIChain("set diag; a; b; c")
		if err == nil {
			t.Fatal("expected error for 4 commands, got nil")
		}
		if want := "at most 2 commands can be chained, got 4"; err.Error() != want {
			t.Errorf("error = %q, want %q", err.Error(), want)
		}
	})

	t.Run("WhenFirstCommandNotSet_ShouldReturnError", func(t *testing.T) {
		_, err := ParseCLIChain("volume show; snapshot show")
		if err == nil {
			t.Fatal("expected error when first command is not 'set', got nil")
		}
		expected := `first command in a chain must be 'set', got "volume"`
		if err.Error() != expected {
			t.Errorf("error = %q, want %q", err.Error(), expected)
		}
	})

	t.Run("WhenEmptyInput_ShouldReturnError", func(t *testing.T) {
		_, err := ParseCLIChain("")
		if err == nil {
			t.Fatal("expected error for empty input, got nil")
		}
	})

	t.Run("WhenEmptySegmentAfterSemicolon_ShouldReturnError", func(t *testing.T) {
		_, err := ParseCLIChain("set diag; ")
		if err == nil {
			t.Fatal("expected error for empty segment, got nil")
		}
	})

	t.Run("WhenEmptySegmentBeforeSemicolon_ShouldReturnError", func(t *testing.T) {
		_, err := ParseCLIChain("; volume show")
		if err == nil {
			t.Fatal("expected error for empty segment, got nil")
		}
	})

	t.Run("WhenUnclosedQuoteInChainedCommand_ShouldReturnError", func(t *testing.T) {
		_, err := ParseCLIChain(`set diag; volume create -comment "unclosed`)
		if err == nil {
			t.Fatal("expected error for unclosed quote in chained command, got nil")
		}
	})

	t.Run("WhenRejectedSetPrefixVariants_ShouldReturnError", func(t *testing.T) {
		rejected := []struct {
			name  string
			input string
		}{
			{"WhenSetAdmin_ShouldReject", "set admin; volume show"},
			{"WhenSetPrivilegeAdmin_ShouldReject", "set -privilege admin; volume show"},
			{"WhenAbbreviatedFlagPriv_ShouldReject", "set -priv diag; volume show"},
			{"WhenSetWithExtraArgs_ShouldReject", "set diag -rows 50; volume show"},
			{"WhenSetPrivilegeWithExtraArgs_ShouldReject", "set -privilege diagnostic -rows 50; volume show"},
			{"WhenSetRowsUnrelatedOption_ShouldReject", "set -rows 50; volume show"},
			{"WhenSetUnknownSubcommand_ShouldReject", "set foo; volume show"},
			{"WhenSetPrivilegeUnknownLevel_ShouldReject", "set -privilege readonly; volume show"},
			{"WhenBareSetNoSubcommand_ShouldReject", "set; volume show"},
		}
		for _, tt := range rejected {
			t.Run(tt.name, func(t *testing.T) {
				_, err := ParseCLIChain(tt.input)
				if err == nil {
					t.Fatalf("expected error for %q, got nil", tt.input)
				}
				if !strings.Contains(err.Error(), "unsupported set prefix") {
					t.Errorf("error = %q, want it to contain 'unsupported set prefix'", err.Error())
				}
			})
		}
	})

	t.Run("WhenAcceptedSetPrefixVariants_ShouldParse", func(t *testing.T) {
		accepted := []struct {
			name  string
			input string
		}{
			{"WhenSetDiag_ShouldAccept", "set diag; volume show"},
			{"WhenSetDiagnostic_ShouldAccept", "set diagnostic; volume show"},
			{"WhenSetAdvanced_ShouldAccept", "set advanced; volume show"},
			{"WhenSetPrivilegeDiag_ShouldAccept", "set -privilege diag; volume show"},
			{"WhenSetPrivilegeDiagnostic_ShouldAccept", "set -privilege diagnostic; volume show"},
			{"WhenSetPrivilegeAdvanced_ShouldAccept", "set -privilege advanced; volume show"},
			{"WhenCaseInsensitiveSetDIAG_ShouldAccept", "set DIAG; volume show"},
			{"WhenCaseInsensitiveSetPrivilegeADVANCED_ShouldAccept", "set -privilege ADVANCED; volume show"},
		}
		for _, tt := range accepted {
			t.Run(tt.name, func(t *testing.T) {
				chain, err := ParseCLIChain(tt.input)
				if err != nil {
					t.Fatalf("unexpected error for %q: %v", tt.input, err)
				}
				if chain.PrimaryCommand.FullCommand != "volume show" {
					t.Errorf("PrimaryCommand.FullCommand = %q, want %q", chain.PrimaryCommand.FullCommand, "volume show")
				}
			})
		}
	})

	t.Run("WhenSemicolonInsideDoubleQuotes_ShouldNotSplit", func(t *testing.T) {
		chain, err := ParseCLIChain(`volume create -vserver vs1 -comment "a;b"`)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(chain.Commands) != 1 {
			t.Fatalf("expected 1 command, got %d", len(chain.Commands))
		}
		if chain.PrimaryCommand.Arguments["-comment"] != "a;b" {
			t.Errorf("comment = %q, want %q", chain.PrimaryCommand.Arguments["-comment"], "a;b")
		}
	})

	t.Run("WhenSemicolonInsideSingleQuotes_ShouldNotSplit", func(t *testing.T) {
		chain, err := ParseCLIChain(`volume create -vserver vs1 -comment 'a;b'`)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(chain.Commands) != 1 {
			t.Fatalf("expected 1 command, got %d", len(chain.Commands))
		}
		if chain.PrimaryCommand.Arguments["-comment"] != "a;b" {
			t.Errorf("comment = %q, want %q", chain.PrimaryCommand.Arguments["-comment"], "a;b")
		}
	})

	t.Run("WhenQuotedSemicolonInSecondCommand_ShouldNotSplit", func(t *testing.T) {
		chain, err := ParseCLIChain(`set diag; volume create -vserver vs1 -comment "x;y"`)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(chain.Commands) != 2 {
			t.Fatalf("expected 2 commands, got %d", len(chain.Commands))
		}
		if chain.PrimaryCommand.Arguments["-comment"] != "x;y" {
			t.Errorf("comment = %q, want %q", chain.PrimaryCommand.Arguments["-comment"], "x;y")
		}
	})

	t.Run("WhenUnclosedQuoteWithSemicolon_ShouldReturnError", func(t *testing.T) {
		_, err := ParseCLIChain(`volume create -comment "a;b`)
		if err == nil {
			t.Fatal("expected error for unclosed quote, got nil")
		}
	})
}

func TestCLIChain_BuildCommand(t *testing.T) {
	t.Run("WhenSingleCommand_ShouldReturnPrimaryInputAsIs", func(t *testing.T) {
		chain := &CLIChain{SetPrefix: ""}
		got := chain.BuildCommand("volume show -vserver vs1")
		if got != "volume show -vserver vs1" {
			t.Errorf("BuildCommand() = %q, want %q", got, "volume show -vserver vs1")
		}
	})

	t.Run("WhenChainedCommand_ShouldPrependSetPrefix", func(t *testing.T) {
		chain := &CLIChain{SetPrefix: "set diag"}
		got := chain.BuildCommand("volume show -vserver vs1")
		want := "set diag; volume show -vserver vs1"
		if got != want {
			t.Errorf("BuildCommand() = %q, want %q", got, want)
		}
	})

	t.Run("WhenChainedCommandWithInjectedArgs_ShouldPrependSetPrefix", func(t *testing.T) {
		chain := &CLIChain{SetPrefix: "set advanced"}
		got := chain.BuildCommand("volume create -vserver vs1 -volume vol1 -size 100g -is-space-enforcement-logical true")
		want := "set advanced; volume create -vserver vs1 -volume vol1 -size 100g -is-space-enforcement-logical true"
		if got != want {
			t.Errorf("BuildCommand() = %q, want %q", got, want)
		}
	})
}

func TestParseCLIChain_RuleMatchingOnPrimaryCommand(t *testing.T) {
	t.Run("WhenSetPrefix_ShouldNotAffectRuleMatching", func(t *testing.T) {
		chain, err := ParseCLIChain("set diag; volume show -vserver vs1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		rule, matched := MatchCLIRule(chain.PrimaryCommand)
		if !matched {
			t.Fatal("expected a matching rule for 'volume show'")
		}
		if !rule.Allow {
			t.Error("expected 'volume show' to be allowed")
		}
	})

	t.Run("WhenDeniedCommandChainedAfterSet_ShouldStillBeDenied", func(t *testing.T) {
		chain, err := ParseCLIChain("set diag; security certificate delete -vserver vs1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		rule, matched := MatchCLIRule(chain.PrimaryCommand)
		if !matched {
			t.Fatal("expected a matching rule for 'security certificate delete'")
		}
		if rule.Allow {
			t.Error("expected 'security certificate delete' to be denied even with set prefix")
		}
	})

	t.Run("WhenChainedVolumeShow_ShouldApplyRemoveFields", func(t *testing.T) {
		chain, err := ParseCLIChain("set diag; volume show -instance")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		rule, matched := MatchCLIRule(chain.PrimaryCommand)
		if !matched {
			t.Fatal("expected a matching rule for 'volume show'")
		}
		if len(rule.RemoveFields) == 0 {
			t.Error("expected RemoveFields to be configured for 'volume show'")
		}

		output := "Volume Name: vol1\nUsed Size: 50GB\nAvailable: 100GB"
		filtered := RemoveFieldsFromCLIOutput(output, rule.RemoveFields)
		if strings.Contains(filtered, "Used Size") {
			t.Error("expected 'Used Size' to be removed from output")
		}
		if !strings.Contains(filtered, "Volume Name") {
			t.Error("expected 'Volume Name' to be preserved")
		}
	})
}

func TestCLIChain_IsDiagMode(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{"WhenSingleCommand_ShouldNotBeDiag", "volume show", false},
		{"WhenSetDiagPrefix_ShouldBeDiag", "set diag; volume show", true},
		{"WhenSetDiagnosticPrefix_ShouldBeDiag", "set diagnostic; volume show", true},
		{"WhenSetPrivilegeDiagnostic_ShouldBeDiag", "set -privilege diagnostic; volume show", true},
		{"WhenSetPrivilegeDiag_ShouldBeDiag", "set -privilege diag; volume show", true},
		{"WhenSetAdvanced_ShouldNotBeDiag", "set advanced; volume show", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chain, err := ParseCLIChain(tt.input)
			if err != nil {
				t.Fatalf("unexpected parse error: %v", err)
			}
			if got := chain.IsDiagMode(); got != tt.want {
				t.Errorf("IsDiagMode() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCLIChain_IsAdvancedMode(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{"WhenSingleCommand_ShouldNotBeAdvanced", "volume show", false},
		{"WhenSetAdvancedPrefix_ShouldBeAdvanced", "set advanced; volume show", true},
		{"WhenSetPrivilegeAdvanced_ShouldBeAdvanced", "set -privilege advanced; volume show", true},
		{"WhenSetDiag_ShouldNotBeAdvanced", "set diag; volume show", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chain, err := ParseCLIChain(tt.input)
			if err != nil {
				t.Fatalf("unexpected parse error: %v", err)
			}
			if got := chain.IsAdvancedMode(); got != tt.want {
				t.Errorf("IsAdvancedMode() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMatchAdvancedRule(t *testing.T) {
	t.Run("WhenStatisticsShow_ShouldBeInAdvancedAllowlist", func(t *testing.T) {
		cmd, err := ParseCLICommand("statistics show")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		rule, matched := MatchPrivilegedRule(cmd, GetAdvancedAllowedRules())
		if !matched {
			t.Fatal("expected statistics show to be in advanced allowlist")
		}
		if !rule.Allow {
			t.Error("expected advanced statistics show to be allowed")
		}
	})

	t.Run("WhenVolumeCheckMetadata_ShouldNotBeInAdvancedAllowlist", func(t *testing.T) {
		cmd, err := ParseCLICommand("volume check metadata")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		_, matched := MatchPrivilegedRule(cmd, GetAdvancedAllowedRules())
		if matched {
			t.Error("expected volume check metadata to NOT be in advanced allowlist")
		}
	})

	t.Run("WhenVolumeShow_ShouldBeInAdvancedAllowlist", func(t *testing.T) {
		cmd, err := ParseCLICommand("volume show -vserver vs1 -instance")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		rule, matched := MatchPrivilegedRule(cmd, GetAdvancedAllowedRules())
		if !matched {
			t.Error("expected volume show to be in advanced allowlist")
		}
		if !rule.Allow {
			t.Error("volume show should be allowed in advanced mode")
		}
	})

	t.Run("WhenVolumeCreate_ShouldNotBeInAdvancedAllowlist", func(t *testing.T) {
		cmd, err := ParseCLICommand("volume create -vserver vs1 -volume vol1 -size 100g")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		_, matched := MatchPrivilegedRule(cmd, GetAdvancedAllowedRules())
		if matched {
			t.Error("expected volume create to NOT be in advanced allowlist")
		}
	})

	t.Run("WhenSecurityCertificateDelete_ShouldNotBeInAdvancedAllowlist", func(t *testing.T) {
		cmd, err := ParseCLICommand("security certificate delete -vserver vs1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		_, matched := MatchPrivilegedRule(cmd, GetAdvancedAllowedRules())
		if matched {
			t.Error("expected security certificate delete to NOT be in advanced allowlist")
		}
	})

	t.Run("WhenNilCommand_ShouldReturnFalse", func(t *testing.T) {
		_, matched := MatchPrivilegedRule(nil, GetAdvancedAllowedRules())
		if matched {
			t.Error("expected nil command to not match")
		}
	})
}

func TestNormalRemoveFields_IncludesPhysicalUsedPercent(t *testing.T) {
	cmd, _ := ParseCLICommand("volume show")
	rule, matched := MatchCLIRule(cmd)
	if !matched {
		t.Fatal("expected volume show to match a normal rule")
	}

	found := false
	for _, f := range rule.RemoveFields {
		if f == "Physical Used Percent" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected normal volume show RemoveFields to include 'Physical Used Percent'")
	}
}

func TestExtractCommandTokens(t *testing.T) {
	tests := []struct {
		name        string
		tokens      []string
		wantCommand []string
		wantArgs    []string
	}{
		{
			name:        "WhenSimpleCommandWithArgs_ShouldSplitCorrectly",
			tokens:      []string{"volume", "show", "-vserver", "vs1"},
			wantCommand: []string{"volume", "show"},
			wantArgs:    []string{"-vserver", "vs1"},
		},
		{
			name:        "WhenNoArguments_ShouldReturnCommandOnly",
			tokens:      []string{"volume", "show"},
			wantCommand: []string{"volume", "show"},
			wantArgs:    nil,
		},
		{
			name:        "WhenStartsWithDash_ShouldReturnEmptyCommand",
			tokens:      []string{"-vserver", "vs1"},
			wantCommand: []string{},
			wantArgs:    []string{"-vserver", "vs1"},
		},
		{
			name:        "WhenMultiWordCommand_ShouldExtractAll",
			tokens:      []string{"storage", "aggregate", "show", "-aggregate", "aggr1"},
			wantCommand: []string{"storage", "aggregate", "show"},
			wantArgs:    []string{"-aggregate", "aggr1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotCmd, gotArgs := extractCommandTokens(tt.tokens)

			if len(gotCmd) != len(tt.wantCommand) {
				t.Errorf("command tokens = %v, want %v", gotCmd, tt.wantCommand)
			}
			for i := range gotCmd {
				if i < len(tt.wantCommand) && gotCmd[i] != tt.wantCommand[i] {
					t.Errorf("command[%d] = %q, want %q", i, gotCmd[i], tt.wantCommand[i])
				}
			}

			if len(gotArgs) != len(tt.wantArgs) {
				t.Errorf("arg tokens = %v, want %v", gotArgs, tt.wantArgs)
			}
		})
	}
}
