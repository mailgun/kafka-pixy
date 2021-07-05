package logging

import (
	"fmt"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

// Only levels with higher or the same severity are advertised by a level
// filtering hook.
func TestLevels(t *testing.T) {
	for i, tc := range []struct {
		original []logrus.Level
		level    logrus.Level
		filtered []logrus.Level
	}{
		0: {
			original: logrus.AllLevels,
			level:    logrus.PanicLevel,
			filtered: []logrus.Level{logrus.PanicLevel},
		}, 1: {
			original: logrus.AllLevels,
			level:    logrus.FatalLevel,
			filtered: []logrus.Level{logrus.PanicLevel, logrus.FatalLevel},
		}, 2: {
			original: logrus.AllLevels,
			level:    logrus.ErrorLevel,
			filtered: []logrus.Level{logrus.PanicLevel, logrus.FatalLevel, logrus.ErrorLevel},
		}, 3: {
			original: logrus.AllLevels,
			level:    logrus.WarnLevel,
			filtered: []logrus.Level{logrus.PanicLevel, logrus.FatalLevel, logrus.ErrorLevel, logrus.WarnLevel},
		}, 4: {
			original: logrus.AllLevels,
			level:    logrus.InfoLevel,
			filtered: []logrus.Level{logrus.PanicLevel, logrus.FatalLevel, logrus.ErrorLevel, logrus.WarnLevel, logrus.InfoLevel},
		}, 5: {
			original: logrus.AllLevels,
			level:    logrus.DebugLevel,
			filtered: []logrus.Level{logrus.PanicLevel, logrus.FatalLevel, logrus.ErrorLevel, logrus.WarnLevel, logrus.InfoLevel, logrus.DebugLevel},
		}, 6: {
			// Original missing levels stay missing when filtered.
			original: []logrus.Level{logrus.PanicLevel, logrus.WarnLevel, logrus.InfoLevel, logrus.DebugLevel},
			level:    logrus.InfoLevel,
			filtered: []logrus.Level{logrus.PanicLevel, logrus.WarnLevel, logrus.InfoLevel},
		}, 7: {
			// It is ok to specify a level missing in the original list for filtering.
			original: []logrus.Level{logrus.PanicLevel, logrus.WarnLevel, logrus.DebugLevel},
			level:    logrus.InfoLevel,
			filtered: []logrus.Level{logrus.PanicLevel, logrus.WarnLevel},
		}} {
		fmt.Printf("Test case #%d\n", i)

		fakeHook := newFakeHook(tc.original)
		lf := newLevelFilter(fakeHook, tc.level)

		require.Equal(t, tc.filtered, lf.Levels())
	}
}

type fakeHook struct {
	levels  []logrus.Level
	entries []*logrus.Entry
}

func newFakeHook(levels []logrus.Level) *fakeHook {
	return &fakeHook{levels: levels}
}

func (h *fakeHook) Levels() []logrus.Level {
	return h.levels
}

func (h *fakeHook) Fire(entry *logrus.Entry) error {
	h.entries = append(h.entries, entry)
	return nil
}
