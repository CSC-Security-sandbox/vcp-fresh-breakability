package cli

import (
	"testing"
)

func TestParseCLICommand(t *testing.T) {
	t.Run("basic commands", func(t *testing.T) {
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
				name:        "simple show command",
				input:       "volume show",
				wantCommand: "volume",
				wantSubcmd:  "show",
				wantFull:    "volume show",
				wantArgs:    map[string]string{},
				wantFlags:   []string{},
			},
			{
				name:        "command with arguments",
				input:       "volume show -vserver vs1 -volume vol1",
				wantCommand: "volume",
				wantSubcmd:  "show",
				wantFull:    "volume show",
				wantArgs:    map[string]string{"-vserver": "vs1", "-volume": "vol1"},
				wantFlags:   []string{},
			},
			{
				name:        "command with flags",
				input:       "volume show -vserver vs1 -root",
				wantCommand: "volume",
				wantSubcmd:  "show",
				wantFull:    "volume show",
				wantArgs:    map[string]string{"-vserver": "vs1"},
				wantFlags:   []string{"-root"},
			},
			{
				name:        "multi-word command",
				input:       "storage aggregate show -aggregate aggr1",
				wantCommand: "storage aggregate",
				wantSubcmd:  "show",
				wantFull:    "storage aggregate show",
				wantArgs:    map[string]string{"-aggregate": "aggr1"},
				wantFlags:   []string{},
			},
			{
				name:        "create command with multiple args",
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
				name:        "single command without subcommand",
				input:       "help",
				wantCommand: "help",
				wantSubcmd:  "",
				wantFull:    "help",
				wantArgs:    map[string]string{},
				wantFlags:   []string{},
			},
			{
				name:        "command with quoted value",
				input:       `volume create -vserver vs1 -comment "my test volume"`,
				wantCommand: "volume",
				wantSubcmd:  "create",
				wantFull:    "volume create",
				wantArgs:    map[string]string{"-vserver": "vs1", "-comment": "my test volume"},
				wantFlags:   []string{},
			},
			{
				name:        "command with single quoted value",
				input:       `volume create -vserver vs1 -comment 'my test volume'`,
				wantCommand: "volume",
				wantSubcmd:  "create",
				wantFull:    "volume create",
				wantArgs:    map[string]string{"-vserver": "vs1", "-comment": "my test volume"},
				wantFlags:   []string{},
			},
			{
				name:        "command with extra whitespace",
				input:       "  volume   show   -vserver   vs1  ",
				wantCommand: "volume",
				wantSubcmd:  "show",
				wantFull:    "volume show",
				wantArgs:    map[string]string{"-vserver": "vs1"},
				wantFlags:   []string{},
			},
			{
				name:        "snapshot command",
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
				name:        "network interface command",
				input:       "network interface show -vserver vs1",
				wantCommand: "network interface",
				wantSubcmd:  "show",
				wantFull:    "network interface show",
				wantArgs:    map[string]string{"-vserver": "vs1"},
				wantFlags:   []string{},
			},
			{
				name:    "empty command",
				input:   "",
				wantErr: true,
			},
			{
				name:    "whitespace only",
				input:   "   ",
				wantErr: true,
			},
			{
				name:    "unclosed quote",
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
		{"existing argument", "-vserver", true},
		{"another existing", "-volume", true},
		{"non-existing", "-aggregate", false},
		{"empty string", "", false},
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
		{"existing argument", "-vserver", "vs1"},
		{"another existing", "-size", "100g"},
		{"non-existing returns empty", "-aggregate", ""},
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
		{"existing flag", "-root", true},
		{"another existing", "-verbose", true},
		{"non-existing", "-quiet", false},
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
		{"exact match", "volume", "show", true},
		{"case insensitive command", "Volume", "show", true},
		{"case insensitive subcommand", "volume", "Show", true},
		{"both case insensitive", "VOLUME", "SHOW", true},
		{"wrong command", "snapshot", "show", false},
		{"wrong subcommand", "volume", "create", false},
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
			name: "exact match",
			cmd: &CLICommand{
				Command:     "volume",
				Subcommand:  "show",
				FullCommand: "volume show",
			},
			pattern: "volume show",
			want:    true,
		},
		{
			name: "exact match case insensitive",
			cmd: &CLICommand{
				Command:     "volume",
				Subcommand:  "show",
				FullCommand: "volume show",
			},
			pattern: "Volume Show",
			want:    true,
		},
		{
			name: "wildcard match",
			cmd: &CLICommand{
				Command:     "volume",
				Subcommand:  "show",
				FullCommand: "volume show",
			},
			pattern: "volume *",
			want:    true,
		},
		{
			name: "wildcard match create",
			cmd: &CLICommand{
				Command:     "volume",
				Subcommand:  "create",
				FullCommand: "volume create",
			},
			pattern: "volume *",
			want:    true,
		},
		{
			name: "multi-word command wildcard",
			cmd: &CLICommand{
				Command:     "storage aggregate",
				Subcommand:  "show",
				FullCommand: "storage aggregate show",
			},
			pattern: "storage aggregate *",
			want:    true,
		},
		{
			name: "no match different command",
			cmd: &CLICommand{
				Command:     "snapshot",
				Subcommand:  "show",
				FullCommand: "snapshot show",
			},
			pattern: "volume *",
			want:    false,
		},
		{
			name: "no match different subcommand",
			cmd: &CLICommand{
				Command:     "volume",
				Subcommand:  "show",
				FullCommand: "volume show",
			},
			pattern: "volume create",
			want:    false,
		},
		{
			name: "empty pattern",
			cmd: &CLICommand{
				Command:     "volume",
				Subcommand:  "show",
				FullCommand: "volume show",
			},
			pattern: "",
			want:    false,
		},
		{
			name: "system wildcard",
			cmd: &CLICommand{
				Command:     "system",
				Subcommand:  "node",
				FullCommand: "system node",
			},
			pattern: "system *",
			want:    true,
		},
		// Prefix matching: positional args after the command should still match
		{
			name: "prefix match with positional arg",
			cmd: &CLICommand{
				Command:     "volume show",
				Subcommand:  "vol3",
				FullCommand: "volume show vol3",
			},
			pattern: "volume show",
			want:    true,
		},
		{
			name: "prefix match with positional arg case insensitive",
			cmd: &CLICommand{
				Command:     "volume show",
				Subcommand:  "Vol3",
				FullCommand: "volume show Vol3",
			},
			pattern: "Volume Show",
			want:    true,
		},
		{
			name: "prefix match must not match show-footprint",
			cmd: &CLICommand{
				Command:     "volume",
				Subcommand:  "show-footprint",
				FullCommand: "volume show-footprint",
			},
			pattern: "volume show",
			want:    false,
		},
		{
			name: "prefix match must not match different command",
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
			name:  "simple tokens",
			input: "volume show -vserver vs1",
			want:  []string{"volume", "show", "-vserver", "vs1"},
		},
		{
			name:  "double quoted string",
			input: `volume create -comment "my volume"`,
			want:  []string{"volume", "create", "-comment", "my volume"},
		},
		{
			name:  "single quoted string",
			input: `volume create -comment 'my volume'`,
			want:  []string{"volume", "create", "-comment", "my volume"},
		},
		{
			name:  "multiple spaces",
			input: "volume   show    -vserver   vs1",
			want:  []string{"volume", "show", "-vserver", "vs1"},
		},
		{
			name:    "unclosed double quote",
			input:   `volume create -comment "unclosed`,
			wantErr: true,
		},
		{
			name:    "unclosed single quote",
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

	// Parser does not split on semicolon; composite "set diag; volume create" is one command.
	// The endpoint rejects any input containing ";" outright (composite commands not allowed).
	t.Run("WhenCompositeWithSemicolon_ShouldParseAsSingleCommand", func(t *testing.T) {
		cmd, err := ParseCLICommand("set diag; volume create -vserver vs1 -volume vol1 -size 100g")
		if err != nil {
			t.Fatalf("ParseCLICommand() unexpected error: %v", err)
		}
		// Semicolon is not a token separator; "diag;" is one token, so FullCommand includes the rest
		if cmd.FullCommand != "set diag; volume create" {
			t.Errorf("FullCommand = %q, want %q", cmd.FullCommand, "set diag; volume create")
		}
		// "set diag" pattern does not match (no prefix "set diag "); endpoint rejects composites by containing ";"
		rule, found := MatchCLIRule(cmd)
		if found && rule.Pattern == "set diag" {
			t.Error("Parsed composite should not match 'set diag' rule")
		}
	})
}

func TestExtractCommandTokens(t *testing.T) {
	tests := []struct {
		name           string
		tokens         []string
		wantCommand    []string
		wantArgs       []string
	}{
		{
			name:        "simple command with args",
			tokens:      []string{"volume", "show", "-vserver", "vs1"},
			wantCommand: []string{"volume", "show"},
			wantArgs:    []string{"-vserver", "vs1"},
		},
		{
			name:        "no arguments",
			tokens:      []string{"volume", "show"},
			wantCommand: []string{"volume", "show"},
			wantArgs:    nil,
		},
		{
			name:        "starts with dash",
			tokens:      []string{"-vserver", "vs1"},
			wantCommand: []string{},
			wantArgs:    []string{"-vserver", "vs1"},
		},
		{
			name:        "multi-word command",
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
