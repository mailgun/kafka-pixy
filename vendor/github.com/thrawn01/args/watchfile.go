package args

import (
	"time"

	"sync"

	"github.com/fsnotify/fsnotify"
)

func WatchFile(path string, interval time.Duration, callBack func(error)) (WatchCancelFunc, error) {
	var isRunning sync.WaitGroup
	fsWatch, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	if err := fsWatch.Add(path); err != nil {
		return nil, err
	}

	// Check for write events at this interval
	tick := time.Tick(interval)
	done := make(chan struct{}, 1)
	once := sync.Once{}

	isRunning.Add(1)
	go func() {
		var lastWriteEvent *fsnotify.Event
		var checkFile *fsnotify.Event
		for {
			once.Do(func() { isRunning.Done() }) // Notify we are watching
			select {
			case event := <-fsWatch.Events:
				//fmt.Printf("Event %s\n", event.String())
				// If it was a write event
				if event.Op&fsnotify.Write == fsnotify.Write {
					lastWriteEvent = &event
				}
				// VIM apparently renames a file before writing
				if event.Op&fsnotify.Rename == fsnotify.Rename {
					checkFile = &event
				}
				// If we see a Remove event, This is probably ConfigMap updating the config symlink
				if event.Op&fsnotify.Remove == fsnotify.Remove {
					checkFile = &event
				}
			case <-tick:
				// If the file was renamed or removed; maybe it re-appears after our duration?
				if checkFile != nil {
					// Since the file was removed, we must
					// re-register the file to be watched
					fsWatch.Remove(checkFile.Name)
					if err := fsWatch.Add(checkFile.Name); err != nil {
						// Nothing left to watch
						callBack(err)
						return
					}
					lastWriteEvent = checkFile
					checkFile = nil
					continue
				}

				// No events during this interval
				if lastWriteEvent == nil {
					continue
				}
				// Execute the callback
				callBack(nil)
				// Reset the last event
				lastWriteEvent = nil

			case <-done:
				return
			}
		}
	}()

	// Wait until the go-routine is running before we return, this ensures we
	// pickup any file changes after we leave this function
	isRunning.Wait()

	return func() {
		close(done)
		fsWatch.Close()
	}, err
}
