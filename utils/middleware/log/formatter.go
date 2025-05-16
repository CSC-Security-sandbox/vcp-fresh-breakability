package log

import (
	"fmt"
	"regexp"
)

// PasswordMask defines the mask used when logging out a password
const PasswordMask = "******************"

// Secret is a type that represents a secret value, such as a password
type Secret string

// Secret defines a type that outputs the password mask when called with String()
func (s Secret) String() string {
	return PasswordMask
}

// IpMask defines the mask used when logging out a ip address
const IpMask = "***.***.***.***"

var Sanitize = _sanitize

func _sanitize(val string) string {
	for _, sanitation := range sanitations {
		val = sanitation.pattern.ReplaceAllString(val, sanitation.replacement)
	}
	return val
}

type sanitation struct {
	pattern     *regexp.Regexp
	replacement string
}

var sanitations = []*sanitation{
	{
		pattern: regexp.MustCompile(
			`(\\?"[^\\"]*[Cc]lient_?[Cc]ertificate\\?":[^\\"]*)(\\?")[^\\"]*(\\?")|([Cc]lient_?[Cc]ertificate=)[^ ]+`,
		),
		replacement: fmt.Sprintf(`$4$1$2%s$3`, PasswordMask),
	},
	{
		pattern:     regexp.MustCompile(`(<[^/>]*[Cc]lient_?[Cc]ertificate[^>]*>)[^<]*</`),
		replacement: fmt.Sprintf(`$1%s</`, PasswordMask),
	},
	{
		pattern: regexp.MustCompile(
			`(\\?"[^\\"]*[Pp]assword\\?":[^\\"]*)(\\?")[^\\"]*(\\?")|([Pp]assword=)[^ ]+`,
		),
		replacement: fmt.Sprintf(`$4$1$2%s$3`, PasswordMask),
	},
	{
		pattern:     regexp.MustCompile(`(<[^/>]*[Pp]assword[^>]*>)[^<]*</`),
		replacement: fmt.Sprintf(`$1%s</`, PasswordMask),
	},
	{
		pattern:     regexp.MustCompile(`("password":)"[^"]*"`),
		replacement: fmt.Sprintf(`$1"%s"`, PasswordMask),
	},
	{
		pattern:     regexp.MustCompile(".*Api-Key: (.*)"),
		replacement: fmt.Sprintf("Api-Key: %s\r", PasswordMask)},
	{
		pattern:     regexp.MustCompile(".*Secret-Key: (.*)"),
		replacement: fmt.Sprintf("Secret-Key: %s\r", PasswordMask),
	},
	{
		pattern:     regexp.MustCompile(".*Authorization: (.*)"),
		replacement: fmt.Sprintf("Authorization: %s\r", PasswordMask),
	},
	{
		pattern:     regexp.MustCompile(`("passphrase":)"[^"]*"`),
		replacement: fmt.Sprintf(`$1"%s"`, PasswordMask),
	},
	{
		pattern:     regexp.MustCompile(`("peerIpAddresses":)\s*\[\s*("[^"]*"\s*,\s*)*("[^"]*"\s*)]`),
		replacement: fmt.Sprintf(`$1["%s"]`, IpMask),
	},
	{
		pattern:     regexp.MustCompile(`("peerAddresses":)\s*\[\s*("[^"]*"\s*,\s*)*("[^"]*"\s*)]`),
		replacement: fmt.Sprintf(`$1["%s"]`, IpMask),
	},
}
