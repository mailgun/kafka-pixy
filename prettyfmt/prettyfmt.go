package prettyfmt

import (
	"fmt"
	"unicode"
)

// Bytes returns human friendly string representation of the number of bytes.
func Bytes(bytes int64) string {
	kilo := bytes / 1024
	if kilo == 0 {
		return fmt.Sprintf("%d", bytes)
	}
	mega := kilo / 1024
	if mega == 0 {
		return fmt.Sprintf("%dK", kilo)
	}
	giga := mega / 1024
	if giga == 0 {
		return fmt.Sprintf("%dM", mega)
	}
	return fmt.Sprintf("%dG", giga)
}

const (
	collapseStateOutside = iota
	collapseStateSkip
	collapseStateID
)

// CollapseJSON takes as input the output of json.MarshalIndent function and
// puts all lists to the same line. It was not intended to solve this problem
// in general case and works on a very limited set of inputs. Specifically to
// make the output of the `GET /topics/<>/consumers` API method look compact.
func CollapseJSON(bytes []byte) []byte {
	state := collapseStateOutside
	j := 0
	for i := 0; i < len(bytes); i++ {
		c := rune(bytes[i])
		switch state {
		case collapseStateOutside:
			if c == '[' {
				state = collapseStateSkip
			}

		case collapseStateSkip:
			if c == ']' {
				state = collapseStateOutside
				break
			}
			if unicode.IsDigit(c) {
				state = collapseStateID
				break
			}
			continue

		case collapseStateID:
			if unicode.IsDigit(c) {
				break
			}
			if c == ']' {
				state = collapseStateOutside
				break
			}
			if c == ',' {
				state = collapseStateSkip
				break
			}
			continue
		}
		bytes[j] = bytes[i]
		j++
	}
	return bytes[:j]
}
