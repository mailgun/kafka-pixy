package args

import (
	"io/ioutil"

	"os"

	"fmt"
	"net/http"
	"strings"

	"github.com/pkg/errors"
)

// A collection of CLI build helpers

// Load contents from the specified file
func LoadFile(fileName string) ([]byte, error) {
	content, err := ioutil.ReadFile(fileName)
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to read '%s'", fileName)
	}
	return content, nil
}

// Returns true if the file has ModeCharDevice set. This is useful when determining if
// a CLI is receiving piped data.
//
//   var contents string
//   var err error
//
//   // If stdin is getting piped data, read from stdin
//   if args.IsCharDevice(os.Stdin) {
//       contents, err = ioutil.ReadAll(os.Stdin)
//   }
//
func IsCharDevice(file *os.File) bool {
	stat, err := file.Stat()
	if err != nil {
		return false
	}
	return (stat.Mode() & os.ModeCharDevice) == 0
}

// Returns true if the flags given are set on src
func HasFlags(src, flag int64) bool {
	return src&flag != 0
}

// Sets all the flags given on dest
func SetFlags(dest *int64, flag int64) {
	*dest = (*dest | flag)
}

// Returns a curl command representation of the passed http.Request
func CurlString(req *http.Request, payload *[]byte) string {
	parts := []string{"curl", "-i", "-X", req.Method, req.URL.String()}
	for key, value := range req.Header {
		parts = append(parts, fmt.Sprintf("-H \"%s: %s\"", key, value[0]))
	}

	if payload != nil {
		parts = append(parts, fmt.Sprintf(" -d '%s'", string(*payload)))
	}
	return strings.Join(parts, " ")
}
