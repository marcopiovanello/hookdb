package sqlutil

import (
	"regexp"
	"strings"
)

var validIdentifier = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

func ValidateIdentifier(s string) string {
	s = strings.ReplaceAll(s, "\x00", "")
	return strings.ReplaceAll(s, "'", "''")
}

func QuoteStringSlice(s []string) string {
	quoted := make([]string, len(s))
	for i, f := range s {
		escaped := strings.ReplaceAll(f, "\x00", "")
		quoted[i] = "'" + strings.ReplaceAll(escaped, "'", "''") + "'"
	}

	return strings.Join(quoted, ", ")
}
