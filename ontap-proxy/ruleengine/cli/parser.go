// Package cli provides CLI command parsing, rule matching, and response modification
// for the ONTAP private/cli endpoint.
package cli

import (
	"fmt"
	"strings"
	"unicode"
)

// CLICommand represents a parsed ONTAP CLI command.
type CLICommand struct {
	// Command is the primary command (e.g., "volume", "storage aggregate")
	Command string

	// Subcommand is the action (e.g., "show", "create", "delete")
	Subcommand string

	// FullCommand is the combined command for rule matching (e.g., "volume show")
	FullCommand string

	// Arguments contains named arguments with their values (e.g., {"-vserver": "vs1"})
	Arguments map[string]string

	// Flags contains boolean flags without values (e.g., ["-root", "-verbose"])
	Flags []string

	// RawInput is the original command string
	RawInput string
}

// ParseCLICommand parses an ONTAP CLI command string into a structured CLICommand.
// It handles:
// - Command hierarchy (e.g., "volume show", "storage aggregate show")
// - Named arguments with "-" prefix (e.g., "-vserver vs1")
// - Boolean flags (arguments without values)
// - Quoted strings for values containing spaces
//
// Example:
//
//	"volume show -vserver vs1 -volume vol1" ->
//	  Command: "volume", Subcommand: "show",
//	  Arguments: {"-vserver": "vs1", "-volume": "vol1"}
func ParseCLICommand(input string) (*CLICommand, error) {
	input = strings.TrimSpace(input)
	if input == "" {
		return nil, fmt.Errorf("empty command")
	}

	tokens, err := tokenize(input)
	if err != nil {
		return nil, fmt.Errorf("failed to tokenize command: %w", err)
	}

	if len(tokens) == 0 {
		return nil, fmt.Errorf("no tokens found in command")
	}

	cmd := &CLICommand{
		Arguments: make(map[string]string),
		Flags:     []string{},
		RawInput:  input,
	}

	// Extract command and subcommand from the beginning of tokens
	// Commands can be multi-word (e.g., "storage aggregate show")
	commandTokens, argTokens := extractCommandTokens(tokens)

	if len(commandTokens) == 0 {
		return nil, fmt.Errorf("no command found")
	}

	// Last token of command tokens is the subcommand (action)
	if len(commandTokens) == 1 {
		cmd.Command = commandTokens[0]
		cmd.Subcommand = ""
		cmd.FullCommand = cmd.Command
	} else {
		// Join all but the last token as the command
		cmd.Command = strings.Join(commandTokens[:len(commandTokens)-1], " ")
		cmd.Subcommand = commandTokens[len(commandTokens)-1]
		cmd.FullCommand = strings.Join(commandTokens, " ")
	}

	// Parse arguments and flags
	if err := parseArguments(cmd, argTokens); err != nil {
		return nil, err
	}

	return cmd, nil
}

// tokenize splits the input string into tokens, respecting quoted strings.
func tokenize(input string) ([]string, error) {
	var tokens []string
	var current strings.Builder
	inQuote := false
	quoteChar := rune(0)

	for i, r := range input {
		switch {
		case (r == '"' || r == '\'') && !inQuote:
			// Start of quoted string
			inQuote = true
			quoteChar = r
		case r == quoteChar && inQuote:
			// End of quoted string
			inQuote = false
			quoteChar = 0
		case unicode.IsSpace(r) && !inQuote:
			// End of token (if not in quotes)
			if current.Len() > 0 {
				tokens = append(tokens, current.String())
				current.Reset()
			}
		default:
			current.WriteRune(r)
		}

		// Check for unclosed quote at end
		if i == len(input)-1 && inQuote {
			return nil, fmt.Errorf("unclosed quote in command")
		}
	}

	// Add final token
	if current.Len() > 0 {
		tokens = append(tokens, current.String())
	}

	return tokens, nil
}

// extractCommandTokens separates command tokens from argument tokens.
// Command tokens are all tokens before the first token starting with "-".
// Multi-word commands like "storage aggregate show" are supported.
func extractCommandTokens(tokens []string) (commandTokens, argTokens []string) {
	for i, token := range tokens {
		if strings.HasPrefix(token, "-") {
			return tokens[:i], tokens[i:]
		}
	}
	// No arguments found, all tokens are command tokens
	return tokens, nil
}

// parseArguments extracts named arguments and flags from argument tokens.
func parseArguments(cmd *CLICommand, tokens []string) error {
	i := 0
	for i < len(tokens) {
		token := tokens[i]

		if !strings.HasPrefix(token, "-") {
			// Positional argument (not starting with -)
			// For now, we skip these as ONTAP CLI typically uses named arguments
			i++
			continue
		}

		// Check if this is a flag (no value) or an argument (has value)
		if i+1 < len(tokens) && !strings.HasPrefix(tokens[i+1], "-") {
			// Named argument with value
			cmd.Arguments[token] = tokens[i+1]
			i += 2
		} else {
			// Boolean flag (no value)
			cmd.Flags = append(cmd.Flags, token)
			i++
		}
	}

	return nil
}

// HasArgument checks if the command has a specific argument.
func (c *CLICommand) HasArgument(name string) bool {
	_, exists := c.Arguments[name]
	return exists
}

// GetArgument returns the value of an argument, or empty string if not found.
func (c *CLICommand) GetArgument(name string) string {
	return c.Arguments[name]
}

// HasFlag checks if the command has a specific flag.
func (c *CLICommand) HasFlag(name string) bool {
	for _, flag := range c.Flags {
		if flag == name {
			return true
		}
	}
	return false
}

// IsCommand checks if the command matches the given command and subcommand.
func (c *CLICommand) IsCommand(command, subcommand string) bool {
	return strings.EqualFold(c.Command, command) && strings.EqualFold(c.Subcommand, subcommand)
}

// CLIChain represents a chain of CLI commands separated by ";".
// A chain has at most 2 commands; if chained, the first must be a "set" command.
type CLIChain struct {
	// Commands contains all parsed commands in the chain.
	Commands []*CLICommand

	// PrimaryCommand is the command to evaluate rules against.
	// For a single command, this is that command.
	// For a chain, this is the second (non-set) command.
	PrimaryCommand *CLICommand

	// SetPrefix is the raw "set ..." prefix when chaining; empty for single commands.
	SetPrefix string
}

// IsDiagMode returns true if the chain uses a diagnostic privilege prefix.
// Recognises "set diag", "set diagnostic", "set -privilege diagnostic",
// and "set -privilege diag".
func (c *CLIChain) IsDiagMode() bool {
	if len(c.Commands) < 2 {
		return false
	}
	first := c.Commands[0]
	sub := strings.ToLower(first.Subcommand)
	if sub == "diag" || sub == "diagnostic" {
		return true
	}
	privArg := strings.ToLower(first.GetArgument("-privilege"))
	return privArg == "diag" || privArg == "diagnostic"
}

// IsAdvancedMode returns true if the chain uses an advanced privilege prefix.
// Recognises "set advanced" and "set -privilege advanced".
func (c *CLIChain) IsAdvancedMode() bool {
	if len(c.Commands) < 2 {
		return false
	}
	first := c.Commands[0]
	if strings.EqualFold(first.Subcommand, "advanced") {
		return true
	}
	return strings.EqualFold(first.GetArgument("-privilege"), "advanced")
}

// BuildCommand reconstructs the full command string to send to ONTAP.
// If a set prefix exists, it is prepended with "; ".
func (c *CLIChain) BuildCommand(primaryInput string) string {
	if c.SetPrefix == "" {
		return primaryInput
	}
	return c.SetPrefix + "; " + primaryInput
}

// allowedSetPrivileges enumerates the privilege levels accepted as chain
// prefixes. Any "set" variant not matching one of these is rejected.
var allowedSetPrivileges = map[string]bool{
	"diag":       true,
	"diagnostic": true,
	"advanced":   true,
}

// isAllowedSetPrefix returns true when the parsed "set" command is one of:
//   - set {diag|diagnostic|advanced}          (subcommand form, no extra args)
//   - set -privilege {diag|diagnostic|advanced} (flag form, no extra args)
func isAllowedSetPrefix(cmd *CLICommand) bool {
	if len(cmd.Flags) > 0 {
		return false
	}
	if cmd.Subcommand != "" && len(cmd.Arguments) == 0 {
		return allowedSetPrivileges[strings.ToLower(cmd.Subcommand)]
	}
	if cmd.Subcommand == "" && len(cmd.Arguments) == 1 {
		return allowedSetPrivileges[strings.ToLower(cmd.GetArgument("-privilege"))]
	}
	return false
}

// splitOnUnquotedSemicolons splits input on ';' that are not inside single- or
// double-quoted regions. Quote characters are preserved in the output.
func splitOnUnquotedSemicolons(input string) ([]string, error) {
	var (
		parts   []string
		current strings.Builder
		quote   rune
	)
	for _, r := range input {
		switch {
		case quote == 0 && (r == '"' || r == '\''):
			quote = r
			current.WriteRune(r)
		case r == quote:
			quote = 0
			current.WriteRune(r)
		case r == ';' && quote == 0:
			parts = append(parts, current.String())
			current.Reset()
		default:
			current.WriteRune(r)
		}
	}
	if quote != 0 {
		return nil, fmt.Errorf("unclosed quote in command")
	}
	parts = append(parts, current.String())
	return parts, nil
}

// ParseCLIChain parses CLI input that may contain chained commands separated by ";".
//   - A single command (no ";") is always allowed
//   - At most 2 commands can be chained
//   - The first command in a chain must be an allowed "set" prefix
//     (set {diag|diagnostic|advanced} or set -privilege {diag|diagnostic|advanced})
//   - Each command is individually parsed with ParseCLICommand
//   - Semicolons inside quoted strings are not treated as delimiters
func ParseCLIChain(input string) (*CLIChain, error) {
	input = strings.TrimSpace(input)
	if input == "" {
		return nil, fmt.Errorf("empty command")
	}

	parts, err := splitOnUnquotedSemicolons(input)
	if err != nil {
		return nil, err
	}
	if len(parts) > 2 {
		return nil, fmt.Errorf("at most 2 commands can be chained, got %d", len(parts))
	}

	chain := &CLIChain{}

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, fmt.Errorf("empty command in chain")
		}
		cmd, err := ParseCLICommand(part)
		if err != nil {
			return nil, fmt.Errorf("failed to parse chained command %q: %w", part, err)
		}
		chain.Commands = append(chain.Commands, cmd)
	}

	if len(chain.Commands) == 2 {
		first := chain.Commands[0]
		if !strings.EqualFold(first.Command, "set") {
			return nil, fmt.Errorf("first command in a chain must be 'set', got %q", first.Command)
		}
		if !isAllowedSetPrefix(first) {
			return nil, fmt.Errorf("unsupported set prefix %q; only set {diag|diagnostic|advanced} or set -privilege {diag|diagnostic|advanced} are allowed", first.RawInput)
		}
		chain.SetPrefix = first.RawInput
		chain.PrimaryCommand = chain.Commands[1]
	} else {
		chain.PrimaryCommand = chain.Commands[0]
	}

	return chain, nil
}

// MatchesPattern checks if the command matches a pattern.
// Patterns support wildcards (*) for the subcommand.
// Examples:
//   - "volume show" matches exactly "volume show"
//   - "volume *" matches "volume show", "volume create", etc.
//   - "storage aggregate *" matches "storage aggregate show", etc.
//   - "system *" matches "system node show", "system health", etc.
func (c *CLICommand) MatchesPattern(pattern string) bool {
	pattern = strings.TrimSpace(pattern)
	if pattern == "" {
		return false
	}

	// Check for wildcard pattern
	if strings.HasSuffix(pattern, " *") {
		prefix := strings.TrimSuffix(pattern, " *")
		// Check if the command starts with the prefix
		// This handles both "volume *" matching "volume show"
		// and "system *" matching "system node show"
		return strings.EqualFold(c.Command, prefix) ||
			strings.HasPrefix(strings.ToLower(c.FullCommand), strings.ToLower(prefix+" "))
	}

	// Exact match or prefix match (handles positional arguments like "volume show vol3")
	return strings.EqualFold(c.FullCommand, pattern) ||
		strings.HasPrefix(strings.ToLower(c.FullCommand), strings.ToLower(pattern+" "))
}
