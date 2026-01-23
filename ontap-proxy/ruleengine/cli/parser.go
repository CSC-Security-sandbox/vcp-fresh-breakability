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

	// Exact match
	return strings.EqualFold(c.FullCommand, pattern)
}
