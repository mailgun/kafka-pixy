package logging

import (
	"bufio"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/mailgun/holster/v3/callstack"
	"github.com/mailru/easyjson/jwriter"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type jsonFormater struct {
	appName  string
	hostName string
	cid      string
	pid      int
}

func newJSONFormatter() *jsonFormater {
	f := &jsonFormater{
		appName:  "kafka-pixy",
		pid:      os.Getpid(),
	}

	var err error
	if f.hostName, err = os.Hostname(); err != nil {
		f.hostName = "unknown"
	}
	f.appName = filepath.Base(os.Args[0])
	if f.pid = os.Getpid(); f.pid == 1 {
		f.pid = 0
	}
	f.cid = getDockerCID()
	return f
}

func (f *jsonFormater) Format(entry *logrus.Entry) ([]byte, error) {
	var caller *callstack.FrameInfo

	caller = getLogrusCaller()

	rec := &LogRecord{
		Category:  "logrus",
		AppName:   f.appName,
		HostName:  f.hostName,
		LogLevel:  strings.ToUpper(entry.Level.String()),
		FileName:  caller.File,
		FuncName:  caller.Func,
		LineNo:    caller.LineNo,
		Message:   entry.Message,
		Context:   nil,
		Timestamp: number(float64(entry.Time.UnixNano()) / 1000000000),
		CID:       f.cid,
		PID:       f.pid,
	}
	rec.FromFields(entry.Data)

	var w jwriter.Writer
	rec.MarshalEasyJSON(&w)
	if w.Error != nil {
		return nil, errors.Wrap(w.Error, "while marshalling json")
	}
	w.Buffer.AppendBytes([]byte("\n"))
	buf := w.Buffer.BuildBytes()
	return buf, nil
}

// Returns the file, function and line number of the function that called logrus
func getLogrusCaller() *callstack.FrameInfo {
	var frames [32]uintptr

	// iterate until we find non logrus function
	length := runtime.Callers(5, frames[:])
	for idx := 0; idx < (length - 1); idx++ {
		pc := uintptr(frames[idx]) - 1
		fn := runtime.FuncForPC(pc)
		funcName := fn.Name()
		if strings.Contains(strings.ToLower(funcName), "sirupsen/logrus") {
			continue
		}
		filePath, lineNo := fn.FileLine(pc)
		return &callstack.FrameInfo{
			Func:   callstack.FuncName(fn),
			File:   filePath,
			LineNo: lineNo,
		}
	}
	return &callstack.FrameInfo{}
}

func getDockerCID() string {
	f, err := os.Open("/proc/self/cgroup")
	if err != nil {
		return ""
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, "/docker/")
		if len(parts) != 2 {
			continue
		}

		fullDockerCID := parts[1]
		return fullDockerCID[:12]
	}
	return ""
}
