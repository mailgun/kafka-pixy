package args

import "strings"

// Given a string a `key=value,key=value` and
// successive calls to Next() will return `key` then `=`
// then `value` then `,` etc, etc....
type KeyValueTokenizer struct {
	Buffer   string
	Pos      int
	nextRune rune
}

func NewKeyValueTokenizer(source string) *KeyValueTokenizer {
	return &KeyValueTokenizer{
		Buffer: source,
	}
}

// Return the next token found
func (t *KeyValueTokenizer) Next() string {
	// No more tokens to find
	if t.Pos >= len(t.Buffer) {
		return ""
	}
	for i := t.Pos; i < len(t.Buffer); i++ {
		char := t.Buffer[i]
		if char == '=' || char == ',' {
			// It's an escaped delimiter
			if !(i-1 <= 0) && t.Buffer[i-1] == '\\' {
				// As long as the escape was not escaped =)
				if !((i - 2) <= 0) && t.Buffer[i-2] != '\\' {
					// Remove the escape
					t.Buffer = t.Buffer[:i-1] + t.Buffer[i:]
					// Account for the deleted escape
					i -= 1
					// And skip this delimiter
					continue
				}
			}
			var token string
			if i == t.Pos {
				token = string(t.Buffer[i])
				t.Pos += 1
			} else {
				token = strings.TrimSpace(string(t.Buffer[t.Pos:i]))
				t.Pos = i
			}
			return token
		}
	}
	// If we get here, we are at the end of our buffer. Nothing more to tokenize
	token := strings.TrimSpace(t.Buffer[t.Pos:])
	t.Pos = len(t.Buffer)
	return token
}
