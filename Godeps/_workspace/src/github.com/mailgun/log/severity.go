package log

import (
	"fmt"
	"strings"
)

type Severity int32

// Supported severities.
const (
	SeverityInfo Severity = iota
	SeverityWarning
	SeverityError
)

var severityNames = []string{"INFO", "WARN", "ERROR"}

func (s Severity) String() string {
	return severityNames[s]
}

func severityFromString(s string) (Severity, error) {
	s = strings.ToUpper(s)
	for idx, name := range severityNames {
		if name == s {
			return Severity(idx), nil
		}
	}
	return -1, fmt.Errorf("unsupported severity: %s", s)
}
