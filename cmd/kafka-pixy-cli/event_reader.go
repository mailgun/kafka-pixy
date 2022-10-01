package main

import (
	"bufio"
	"bytes"
	"io"
)

type EventReader struct {
	scanner *bufio.Scanner
	buffer  []byte
	idx     int
}

func NewEventReader(source io.Reader) *EventReader {
	scanner := bufio.NewScanner(source)
	split := func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		if atEOF && len(data) == 0 {
			return 0, nil, nil
		}
		if i := bytes.IndexByte(data, '\r'); i >= 0 {
			// We have a full event
			return i + 1, data[0:i], nil
		}
		// If we're at EOF, we have a final event
		if atEOF {
			return len(data), data, nil
		}
		// Request more data.
		return 0, nil, nil
	}
	// Set the split function for the scanning operation.
	scanner.Split(split)

	return &EventReader{
		scanner: scanner,
	}
}

// ReadEvent reads the next event from the source. It returns io.EOF if no
// more events are available.
func (r *EventReader) ReadEvent() ([]byte, error) {
	if r.scanner.Scan() {
		event := r.scanner.Bytes()
		return event, nil
	}
	if err := r.scanner.Err(); err != nil {
		return nil, err
	}
	return nil, io.EOF
}
