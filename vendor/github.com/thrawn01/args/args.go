package args

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

const (
	DefaultTerminator  string = "--"
	DefaultOptionGroup string = ""
)

var DefaultLogger *NullLogger = &NullLogger{}

// We only need part of the standard logging functions
type StdLogger interface {
	Print(...interface{})
	Printf(string, ...interface{})
	Println(...interface{})
}

type NullLogger struct{}

func (self *NullLogger) Print(...interface{})          {}
func (self *NullLogger) Printf(string, ...interface{}) {}
func (self *NullLogger) Println(...interface{})        {}

// ***********************************************
// PUBLIC FUNCTIONS
// ***********************************************
func Name(name string) ParseModifier {
	return func(parser *ArgParser) {
		parser.Name = name
	}
}

func Desc(desc string, flags ...int64) ParseModifier {
	return func(parser *ArgParser) {
		// Set any flags if provided
		for _, flag := range flags {
			SetFlags(&parser.flags, flag)
		}
		parser.Description = desc
	}
}

func WrapLen(length int) ParseModifier {
	return func(parser *ArgParser) {
		parser.WordWrap = length
	}
}

func EnvPrefix(prefix string) ParseModifier {
	return func(parser *ArgParser) {
		parser.EnvPrefix = prefix
	}
}

func NoHelp() ParseModifier {
	return func(parser *ArgParser) {
		parser.AddHelpOption = false
	}
}

// ***********************************************
// Public Word Formatting Functions
// ***********************************************

// Mixing Spaces and Tabs will have undesired effects
func Dedent(input string) string {
	text := []byte(input)

	// find the first \n::space:: combo
	leadingWhitespace := regexp.MustCompile(`(?m)^[ \t]+`)
	idx := leadingWhitespace.FindIndex(text)
	if idx == nil {
		fmt.Printf("Unable to find \\n::space:: combo\n")
		return input
	}
	//fmt.Printf("idx: '%d:%d'\n", idx[0], idx[1])

	// Create a regex to match any the number of spaces we first found
	gobbleRegex := fmt.Sprintf("(?m)^[ \t]{%d}?", (idx[1] - idx[0]))
	//fmt.Printf("gobbleRegex: '%s'\n", gobbleRegex)
	gobbleIndents := regexp.MustCompile(gobbleRegex)
	// Find any identical spaces and remove them
	dedented := gobbleIndents.ReplaceAll(text, []byte{})
	return string(dedented)
}

func DedentTrim(input string, cutset string) string {
	return strings.Trim(Dedent(input), cutset)
}

func WordWrap(msg string, indent int, wordWrap int) string {
	// Remove any previous formatting
	regex, _ := regexp.Compile(" {2,}|\n|\t")
	msg = regex.ReplaceAllString(msg, "")

	wordWrapLen := wordWrap - indent
	if wordWrapLen <= 0 {
		panic(fmt.Sprintf("Flag indent spacing '%d' exceeds wordwrap length '%d'\n", indent, wordWrap))
	}

	if len(msg) < wordWrapLen {
		return msg
	}

	// Split the msg into lines
	var lines []string
	var eol int
	for i := 0; i < len(msg); {
		eol = i + wordWrapLen
		// If the End Of Line exceeds the message length + our peek at the next character
		if (eol + 1) >= len(msg) {
			// Slice until the end of the message
			lines = append(lines, msg[i:len(msg)])
			i = len(msg)
			break
		}
		// Slice this portion of the message into a single line
		line := msg[i:eol]
		// If the next character past eol is not a space
		// (Usually means we are in the middle of a word)
		if msg[eol+1] != ' ' {
			// Find the last space before the word wrap
			idx := strings.LastIndex(line, " ")
			eol = i + idx
		}
		lines = append(lines, msg[i:eol])
		i = eol
	}
	var spacer string
	if indent <= 0 {
		spacer = fmt.Sprintf("\n%%s")
	} else {
		spacer = fmt.Sprintf("\n%%-%ds", indent-1)
	}

	//fmt.Print("fmt: %s\n", spacer)
	seperator := fmt.Sprintf(spacer, "")
	return strings.Join(lines, seperator)
}

func castString(name string, dest interface{}, value interface{}) (interface{}, error) {
	// If value is nil, return the type default
	if value == nil {
		return "", nil
	}

	// If value is not an string
	if reflect.TypeOf(value).Kind() != reflect.String {
		return 0, errors.New(fmt.Sprintf("Invalid value for '%s' - '%s' is not a String", name, value))
	}
	return value, nil
}

func castInt(name string, dest interface{}, value interface{}) (interface{}, error) {
	// If value is nil, return the type default
	if value == nil {
		return 0, nil
	}

	// If it's already an integer of some sort
	kind := reflect.TypeOf(value).Kind()
	switch kind {
	case reflect.Int:
		return value, nil
	case reflect.Int8:
		return int(value.(int8)), nil
	case reflect.Int16:
		return int(value.(int16)), nil
	case reflect.Int32:
		return int(value.(int32)), nil
	case reflect.Int64:
		return int(value.(int64)), nil
	case reflect.Uint8:
		return int(value.(uint8)), nil
	case reflect.Uint16:
		return int(value.(uint16)), nil
	case reflect.Uint32:
		return int(value.(uint32)), nil
	case reflect.Uint64:
		return int(value.(uint64)), nil
	}
	// If it's not an integer, it better be a string that we can cast
	if kind != reflect.String {
		return 0, errors.New(fmt.Sprintf("Invalid value for '%s' - '%s' is not a Integer or Castable string", name, value))
	}
	strValue := value.(string)

	intValue, err := strconv.ParseInt(strValue, 10, 64)
	if err != nil {
		return 0, errors.New(fmt.Sprintf("Invalid value for '%s' - '%s' is not an Integer", name, strValue))
	}
	return int(intValue), nil
}

func castBool(name string, dest interface{}, value interface{}) (interface{}, error) {
	// If value is nil, return the type default
	if value == nil {
		return false, nil
	}

	kind := reflect.TypeOf(value).Kind()
	if kind == reflect.Bool {
		return value, nil
	}
	// If it's not a boolean, it better be a string that we can cast
	if kind != reflect.String {
		return 0, errors.New(fmt.Sprintf("Invalid value for '%s' - '%s' is not a Boolean or Castable string", name, value))
	}
	strValue := value.(string)

	boolValue, err := strconv.ParseBool(strValue)
	if err != nil {
		return false, errors.New(fmt.Sprintf("Invalid value for '%s' - '%s' is not a Boolean", name, strValue))
	}
	return bool(boolValue), nil
}

func castStringSlice(name string, dest interface{}, value interface{}) (interface{}, error) {
	// If our destination is nil, init a new slice
	if dest == nil {
		dest = make([]string, 0)
	}

	// If value is nil, return the type default
	if value == nil {
		return dest, nil
	}

	// value could already be a slice
	kind := reflect.TypeOf(value).Kind()
	if kind == reflect.Slice {
		sliceKind := reflect.TypeOf(value).Elem().Kind()
		// Is already a []string
		if sliceKind == reflect.String {
			return append(dest.([]string), value.([]string)...), nil
		}
		return dest, errors.New(fmt.Sprintf("Invalid slice type for '%s' - '%s'  not a String",
			name, value))
	}

	// or it could be a string
	if kind != reflect.String {
		return dest, errors.New(fmt.Sprintf("Invalid slice type for '%s' - '%s' is not a "+
			"[]string or parsable comma delimited string", name, value))
	}

	// Assume the value must be a parsable string
	return append(dest.([]string), StringToSlice(value.(string), strings.TrimSpace)...), nil
}

// Given a comma separated string, return a slice of string items.
// Return the entire string as the first item if no comma is found.
//	// Returns []string{"one"}
// 	result := args.StringToSlice("one")
//
//	// Returns []string{"one", "two", "three"}
// 	result := args.StringToSlice("one, two, three", strings.TrimSpace)
//
//	// Returns []string{"ONE", "TWO", "THREE"}
// 	result := args.StringToSlice("one, two, three", strings.ToUpper, strings.TrimSpace)
func StringToSlice(value string, modifiers ...func(s string) string) []string {
	result := strings.Split(value, ",")
	// Apply the modifiers
	for _, modifier := range modifiers {
		for idx, item := range result {
			result[idx] = modifier(item)
		}
	}
	return result
}

func mergeStringMap(src, dest map[string]string) map[string]string {
	for key, value := range src {
		dest[key] = value
	}
	return dest
}

func isMapString(value interface{}) bool {
	kind := reflect.TypeOf(value).Kind()
	if kind == reflect.Map {
		kind := reflect.TypeOf(value).Elem().Kind()
		// Is already a string
		if kind == reflect.String {
			return true
		}
	}
	return false
}

func castStringMap(name string, dest interface{}, value interface{}) (interface{}, error) {
	// If our destination is nil, init a new slice
	if dest == nil {
		dest = make(map[string]string, 0)
	}

	// Don't attempt to cast a nil value
	if value == nil {
		return nil, nil
	}

	// could already be a map[string]string
	if isMapString(value) {
		return mergeStringMap(dest.(map[string]string), value.(map[string]string)), nil
	}

	// value should be a string
	if reflect.TypeOf(value).Kind() != reflect.String {
		return dest, errors.New(fmt.Sprintf("Invalid map type for '%s' - '%s' is not a "+
			"map[string]string or parsable key=value string", name, reflect.TypeOf(value)))
	}

	// Assume the value is a parsable string
	strValue := value.(string)
	// Parse the string
	result, err := StringToMap(strValue)
	if err != nil {
		return dest, errors.New(fmt.Sprintf("Invalid map type for '%s' - %s", name, err))
	}

	return mergeStringMap(dest.(map[string]string), result), nil
}

func containsString(needle string, haystack []string) bool {
	for _, item := range haystack {
		if item == needle {
			return true
		}
	}
	return false
}

func copyStringSlice(src []string) (dest []string) {
	dest = make([]string, len(src))
	for idx, value := range src {
		dest[idx] = value
	}
	return
}

func JSONToMap(value string) (map[string]string, error) {
	result := make(map[string]string)
	err := json.Unmarshal([]byte(value), &result)
	if err != nil {
		return result, errors.New(fmt.Sprintf("JSON map decoding for '%s' failed with '%s'; "+
			`JSON map values should be in form '{"key":"value", "foo":"bar"}'`, value, err))
	}
	return result, nil
}

func StringToMap(value string) (map[string]string, error) {
	tokenizer := NewKeyValueTokenizer(value)
	result := make(map[string]string)

	var lvalue, rvalue, expression string
	for {
		lvalue = tokenizer.Next()
		if lvalue == "" {
			return result, errors.New(fmt.Sprintf("Expected key at pos '%d' but found none; "+
				"map values should be 'key=value' separated by commas", tokenizer.Pos))
		}
		if strings.HasPrefix(lvalue, "{") {
			// Assume this is JSON format and attempt to un-marshal
			return JSONToMap(value)
		}

		expression = tokenizer.Next()
		if expression != "=" {
			return result, errors.New(fmt.Sprintf("Expected '=' after '%s' but found '%s'; "+
				"map values should be 'key=value' separated by commas", lvalue, expression))
		}
		rvalue = tokenizer.Next()
		if rvalue == "" {
			return result, errors.New(fmt.Sprintf("Expected value after '%s' but found none; "+
				"map values should be 'key=value' separated by commas", expression))
		}
		result[lvalue] = rvalue

		// Are there anymore tokens?
		delimiter := tokenizer.Next()
		if delimiter == "" {
			break
		}

		// Should be a comma next
		if delimiter != "," {
			return result, errors.New(fmt.Sprintf("Expected ',' after '%s' but found '%s'; "+
				"map values should be 'key=value' separated by commas", rvalue, delimiter))
		}
	}
	return result, nil
}

// Returns true if the error was because help message was printed
func IsHelpError(err error) bool {
	obj, ok := err.(isHelpError)
	return ok && obj.IsHelpError()
}

// Returns true if the error was because help message was printed
func AskedForHelp(err error) bool {
	obj, ok := err.(isHelpError)
	return ok && obj.IsHelpError()
}

type isHelpError interface {
	IsHelpError() bool
}

type HelpError struct{}

func (e *HelpError) Error() string {
	return "User asked for help; Inspect this error with args.AskedForHelp(err)"
}

func (e *HelpError) IsHelpError() bool {
	return true
}
