package logging

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/mailgun/holster/v3/callstack"
	"github.com/mailgun/holster/v3/errors"
	"github.com/sirupsen/logrus"
)

//easyjson:json
type LogRecord struct {
	Context   map[string]interface{} `json:"context,omitempty"`
	Category  string                 `json:"category,omitempty"`
	AppName   string                 `json:"appname"`
	HostName  string                 `json:"hostname"`
	LogLevel  string                 `json:"logLevel"`
	FileName  string                 `json:"filename"`
	FuncName  string                 `json:"funcName"`
	LineNo    int                    `json:"lineno"`
	Message   string                 `json:"message"`
	Timestamp number                 `json:"timestamp"`
	CID       string                 `json:"cid,omitempty"`
	PID       int                    `json:"pid,omitempty"`
	TID       string                 `json:"tid,omitempty"`
	ExcType   string                 `json:"excType,omitempty"`
	ExcText   string                 `json:"excText,omitempty"`
	ExcValue  string                 `json:"excValue,omitempty"`
}

func (r *LogRecord) FromFields(fields logrus.Fields) {
	if len(fields) == 0 {
		return
	}
	r.Context = make(map[string]interface{})
	for k, v := range fields {
		switch k {
		// logrus.WithError adds a field with name error.
		case "error":
			fallthrough
		case "err":
			// Record details of the error
			if v, ok := v.(error); ok {
				r.ExcValue = v.Error()
				r.ExcType = fmt.Sprintf("%T", errors.Cause(v))
				r.ExcText = fmt.Sprintf("%+v", v)

				// Extract the stack info if provided
				if v, ok := v.(callstack.HasStackTrace); ok {
					trace := v.StackTrace()
					caller := callstack.GetLastFrame(trace)
					r.FuncName = caller.Func
					r.LineNo = caller.LineNo
					r.FileName = caller.File
				}

				// Extract context if provided
				if ctx, ok := v.(errors.HasContext); ok {
					for ck, cv := range ctx.Context() {
						r.Context[ck] = cv
					}
				}
				continue
			}
		case "tid":
			if v, ok := v.(string); ok {
				r.TID = v
				continue
			}
		case "excValue":
			if v, ok := v.(string); ok {
				r.ExcValue = v
				continue
			}
		case "excType":
			if v, ok := v.(string); ok {
				r.ExcType = v
				continue
			}
		case "excText":
			if v, ok := v.(string); ok {
				r.ExcText = v
				continue
			}
		case "excFuncName":
			if v, ok := v.(string); ok {
				r.FuncName = v
				continue
			}
		case "excLineno":
			if v, ok := v.(int); ok {
				r.LineNo = v
				continue
			}
		case "excFileName":
			if v, ok := v.(string); ok {
				r.FileName = v
				continue
			}
		case "category":
			if v, ok := v.(string); ok {
				r.Category = v
				continue
			}
		}
		expandNested(k, v, r.Context)
	}
}

func expandNested(key string, value interface{}, dest map[string]interface{}) {
	if strings.ContainsRune(key, '.') {
		parts := strings.SplitN(key, ".", 2)
		// This nested value might already exist
		nested, isMap := dest[parts[0]].(map[string]interface{})
		if !isMap {
			// if not a map, overwrite current entry and make it a map
			nested = make(map[string]interface{})
			dest[parts[0]] = nested
		}
		expandNested(parts[1], value, nested)
		return
	}
	switch value.(type) {
	case *http.Request:
		dest[key] = requestToMap(value.(*http.Request))
	default:
		dest[key] = value
	}
}

func requestToMap(req *http.Request) map[string]interface{} {
	var form []byte
	var err error

	// Scrub auth information
	headers := req.Header
	headers.Del("Authorization")
	headers.Del("Cookie")

	if len(req.Form) != 0 {
		form, err = json.MarshalIndent(req.Form, "", "  ")
		if err != nil {
			form = []byte(fmt.Sprintf("JSON Encode Error: %s", err))
		}
	}

	headersJSON, err := json.MarshalIndent(headers, "", "  ")
	if err != nil {
		headersJSON = []byte(fmt.Sprintf("JSON Encode Error: %s", err))
	}

	return map[string]interface{}{
		"headers-json": string(headersJSON),
		"ip":           req.RemoteAddr,
		"method":       req.Method,
		"params-json":  string(form),
		"size":         req.ContentLength,
		"url":          req.URL.String(),
		"useragent":    req.Header.Get("User-Agent"),
	}
}

type number float64

func (n number) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("%f", n)), nil
}
