package sqlutil

import (
	"fmt"
	"strings"
)

// var validIdentifier = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

func ValidateIdentifier(s string) string {
	s = strings.ReplaceAll(s, "\x00", "")
	return strings.ReplaceAll(s, "'", "''")
}

func QuoteLiteral(s string) (string, error) {
	if strings.ContainsRune(s, 0) {
		return "", fmt.Errorf("string contains null byte: %q", s)
	}
	escaped := strings.ReplaceAll(s, "'", "''")
	return "'" + escaped + "'", nil
}

func QuoteStringSlice(s []string) string {
	quoted := make([]string, len(s))
	for i, f := range s {
		escaped := strings.ReplaceAll(f, "\x00", "")
		quoted[i] = "'" + strings.ReplaceAll(escaped, "'", "''") + "'"
	}

	return strings.Join(quoted, ", ")
}
