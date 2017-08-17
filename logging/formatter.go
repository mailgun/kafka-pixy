package logging

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	log "github.com/sirupsen/logrus"
)

const quoteCharacter = "\""

type textFormatter struct {
}

func (f *textFormatter) Format(entry *log.Entry) ([]byte, error) {
	var b *bytes.Buffer
	keys := make([]string, 0, len(entry.Data))
	for k := range entry.Data {
		keys = append(keys, k)
	}

	sort.Strings(keys)
	if entry.Buffer != nil {
		b = entry.Buffer
	} else {
		b = &bytes.Buffer{}
	}

	prefixFieldClashes(entry.Data)

	timestampFormat := "2006-01-02 15:04:05.000000 Z07"
	b.WriteString(entry.Time.Format(timestampFormat))
	b.WriteByte(' ')
	f.appendValue(b, entry.Level.String())
	b.WriteByte(' ')

	tid := entry.Data["tid"]
	if tid != nil {
		b.WriteByte('<')
		b.WriteString(tid.(string))
		b.WriteString("> ")
	}

	if entry.Message != "" {
		f.appendValue(b, entry.Message)
	}
	b.WriteByte(' ')
	for _, key := range keys {
		if key == "tid" {
			continue
		}
		f.appendKeyValue(b, key, entry.Data[key])
	}
	b.WriteByte('\n')
	return b.Bytes(), nil
}

func (f *textFormatter) needsQuoting(text string) bool {
	for _, ch := range text {
		if !((ch >= 'a' && ch <= 'z') ||
			(ch >= 'A' && ch <= 'Z') ||
			(ch >= '0' && ch <= '9') ||
			ch == '-' || ch == '.') {
			return true
		}
	}
	return false
}

func (f *textFormatter) appendKeyValue(b *bytes.Buffer, key string, value interface{}) {
	b.WriteString(key)
	b.WriteByte('=')
	f.appendValue(b, value)
	b.WriteByte(' ')
}

func (f *textFormatter) appendValue(b *bytes.Buffer, value interface{}) {
	switch value := value.(type) {
	case string:
		if !f.needsQuoting(value) {
			b.WriteString(value)
		} else {
			b.WriteString(f.quoteString(value))
		}
	case error:
		errmsg := value.Error()
		if !f.needsQuoting(errmsg) {
			b.WriteString(errmsg)
		} else {
			b.WriteString(f.quoteString(errmsg))
		}
	default:
		fmt.Fprint(b, value)
	}
}

func (f *textFormatter) quoteString(v string) string {
	escapedQuote := fmt.Sprintf("\\%s", quoteCharacter)
	escapedValue := strings.Replace(v, quoteCharacter, escapedQuote, -1)

	return fmt.Sprintf("%s%v%s", quoteCharacter, escapedValue, quoteCharacter)
}

// This is to not silently overwrite `time`, `msg` and `level` fields when
// dumping it. If this code wasn't there doing:
//
//  logrus.WithField("level", 1).Info("hello")
//
// Would just silently drop the user provided level. Instead with this code
// it'll logged as:
//
//  {"level": "info", "fields.level": 1, "msg": "hello", "time": "..."}
//
// It's not exported because it's still using Data in an opinionated way. It's to
// avoid code duplication between the two default formatters.
func prefixFieldClashes(data log.Fields) {
	if t, ok := data["time"]; ok {
		data["fields.time"] = t
	}

	if m, ok := data["msg"]; ok {
		data["fields.msg"] = m
	}

	if l, ok := data["level"]; ok {
		data["fields.level"] = l
	}
}
