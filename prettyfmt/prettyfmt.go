package prettyfmt

import (
	"bytes"
	"fmt"
	"reflect"
	"sort"
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

func Val(some interface{}) string {
	return fmtVal(reflect.ValueOf(some))
}

func fmtVal(someRV reflect.Value) string {
	var buf bytes.Buffer
	writeVal(&buf, someRV)
	return buf.String()
}

func writeVal(buf *bytes.Buffer, someRV reflect.Value) {
	// If the value has String method then use it.
	stringMethodRV := someRV.MethodByName("String")
	if stringMethodRV.IsValid() {
		buf.WriteString(stringMethodRV.Call(nil)[0].String())
		return
	}
	// If the value is not a pointer then check if it has a String method with
	// a pointer receiver.
	if someRV.Kind() != reflect.Ptr {
		somePtrRV := reflect.New(someRV.Type())
		somePtrRV.Elem().Set(someRV)
		stringMethodRV = somePtrRV.MethodByName("String")
		if stringMethodRV.IsValid() {
			buf.WriteString(stringMethodRV.Call(nil)[0].String())
			return
		}
	}

	switch someRV.Kind() {
	case reflect.String:
		buf.WriteString(someRV.String())

	case reflect.Map:
		writeMap(buf, someRV)

	case reflect.Slice:
		writeSlice(buf, someRV)

	case reflect.Int8:
		fallthrough
	case reflect.Int16:
		fallthrough
	case reflect.Int32:
		fallthrough
	case reflect.Int64:
		fallthrough
	case reflect.Int:
		buf.WriteString(fmt.Sprintf("%d", someRV.Int()))

	default:
		buf.WriteString(fmt.Sprintf("%v", someRV.Interface()))
	}
}

type valRepr struct {
	rv  reflect.Value
	str string
}

func writeMap(buf *bytes.Buffer, mapRV reflect.Value) {
	mapKeyRVs := mapRV.MapKeys()
	if len(mapKeyRVs) == 0 {
		buf.WriteString("{}")
	}

	mapKeys := make([]valRepr, len(mapKeyRVs))
	for i, mapKeyRV := range mapKeyRVs {
		mapKeys[i] = valRepr{mapKeyRV, fmtVal(mapKeyRV)}
	}
	sort.Slice(mapKeys, func(i, j int) bool {
		return mapKeys[i].str < mapKeys[j].str
	})

	buf.WriteString("{")
	firstKey := true
	for _, mapKey := range mapKeys {
		if firstKey {
			buf.WriteString("\n")
			firstKey = false
		}
		buf.WriteString("    ")
		buf.WriteString(mapKey.str)
		buf.WriteString(": ")
		writeVal(buf, mapRV.MapIndex(mapKey.rv))
		buf.WriteString("\n")
	}
	buf.WriteString("}")
}

func writeSlice(buf *bytes.Buffer, sliceRV reflect.Value) {
	buf.WriteString("[")
	firstElem := true
	for i := 0; i < sliceRV.Len(); i++ {
		if firstElem {
			firstElem = false
		} else {
			buf.WriteString(" ")
		}
		sliceElemRV := sliceRV.Index(i)
		writeVal(buf, sliceElemRV)
	}
	buf.WriteString("]")
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
