package logging

import "github.com/sirupsen/logrus"

type levelFilter struct {
	hook   logrus.Hook
	levels []logrus.Level
}

func newLevelFilter(hook logrus.Hook, level logrus.Level) *levelFilter {
	levels := make([]logrus.Level, 0, len(logrus.AllLevels))
	for _, l := range hook.Levels() {
		if l <= level {
			levels = append(levels, l)
		}
	}

	return &levelFilter{
		hook:   hook,
		levels: levels,
	}
}

func (lf *levelFilter) Levels() []logrus.Level {
	return lf.levels
}

func (lf *levelFilter) Fire(entry *logrus.Entry) error {
	return lf.hook.Fire(entry)
}
