package args_test

import (
	"fmt"
	"time"

	"io/ioutil"

	"os"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	"github.com/thrawn01/args"
)

func saveFile(fileName string, content []byte) error {
	err := ioutil.WriteFile(fileName, content, os.ModePerm)
	if err != nil {
		return errors.Wrapf(err, "Failed to write '%s'", fileName)
	}
	return nil
}

var _ = Describe("args.WatchFile()", func() {
	var iniFile *os.File
	var log *TestLogger
	iniVersion1 := []byte(`
		value=my-value
		version=1
	`)
	iniVersion2 := []byte(`
		value=new-value
		version=2
	`)

	BeforeEach(func() {
		log = NewTestLogger()
	})

	It("Should reload a config file when watched file is modified", func() {
		parser := args.NewParser()
		parser.SetLog(log)

		parser.AddConfig("value")
		parser.AddConfig("version").IsInt()

		opt, err := parser.Parse(nil)
		Expect(err).To(BeNil())
		Expect(log.GetEntry()).To(Equal(""))
		Expect(opt.String("value")).To(Equal(""))
		Expect(opt.Int("version")).To(Equal(0))

		iniFile, err = ioutil.TempFile("/tmp", "args-test")
		if err != nil {
			Fail(err.Error())
		}
		defer os.Remove(iniFile.Name())

		// Write version 1 of the ini file
		if err := saveFile(iniFile.Name(), iniVersion1); err != nil {
			Fail(err.Error())
		}

		// Load the INI file
		content, err := args.LoadFile(iniFile.Name())
		if err != nil {
			Fail(err.Error())
		}
		// Parse the ini file
		opt, err = parser.FromINI(content)
		Expect(err).To(BeNil())
		Expect(log.GetEntry()).To(Equal(""))
		Expect(opt.String("value")).To(Equal("my-value"))
		Expect(opt.Int("version")).To(Equal(1))

		done := make(chan struct{})
		cancelWatch, err := args.WatchFile(iniFile.Name(), time.Second, func(err error) {
			parser.FromINIFile(iniFile.Name())
			// Tell the test to continue, Change event was handled
			close(done)
		})
		if err != nil {
			Fail(err.Error())
		}

		if err := saveFile(iniFile.Name(), iniVersion2); err != nil {
			Fail(err.Error())
		}
		// Wait until the new file was loaded
		<-done
		// Stop the watch
		cancelWatch()
		// Get the updated options
		opts := parser.GetOpts()

		Expect(log.GetEntry()).To(Equal(""))
		Expect(opts.String("value")).To(Equal("new-value"))
		Expect(opts.Int("version")).To(Equal(2))
	})

	It("Should signal a modification if the file is deleted and re-created", func() {
		testFile, err := ioutil.TempFile("/tmp", "args-test")
		if err != nil {
			Fail(err.Error())
		}
		testFile.Close()

		var watchErr error
		done := make(chan struct{})
		cancelWatch, err := args.WatchFile(testFile.Name(), time.Second, func(err error) {
			if err != nil {
				fmt.Printf("Watch Error %s\n", err.Error())
			}
			// Tell the test to continue, Change event was handled
			close(done)
		})
		if err != nil {
			Fail(err.Error())
		}

		// Quickly Remove the file and replace it
		os.Remove(testFile.Name())
		testFile, err = os.OpenFile(testFile.Name(), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
		testFile.Close()
		defer os.Remove(testFile.Name())

		// Wait until the new file was loaded
		<-done
		Expect(watchErr).To(BeNil())

		// Stop the watch
		cancelWatch()
	})

	// Apparently VIM does this to files on OSX
	It("Should signal a modification if the file is renamed and renamed back", func() {
		testFile, err := ioutil.TempFile("/tmp", "args-test")
		if err != nil {
			Fail(err.Error())
		}
		testFile.Close()

		var watchErr error
		done := make(chan struct{})
		cancelWatch, err := args.WatchFile(testFile.Name(), time.Second, func(err error) {
			if err != nil {
				fmt.Printf("Watch Error %s\n", err.Error())
			}
			// Tell the test to continue, Change event was handled
			close(done)
		})
		if err != nil {
			Fail(err.Error())
		}

		// Quickly Remove the file and replace it
		os.Rename(testFile.Name(), testFile.Name()+"-new")
		os.Rename(testFile.Name()+"-new", testFile.Name())
		defer os.Remove(testFile.Name())

		// Wait until the new file was loaded
		<-done
		Expect(watchErr).To(BeNil())
		// Stop the watch
		cancelWatch()
	})
})
