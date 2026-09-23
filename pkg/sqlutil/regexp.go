package sqlutil

import (
	"regexp"
	"strings"
)

func LikeToRegexp(pattern string) (*regexp.Regexp, error) {
	var b strings.Builder

	b.WriteByte('^')

	for _, r := range pattern {
		switch r {
		case '%':
			b.WriteString(".*")
		case '_':
			b.WriteByte('.')
		default:
			b.WriteString(regexp.QuoteMeta(string(r)))
		}
	}

	b.WriteByte('$')

	return regexp.Compile(b.String())
}
