package args_test

import (
	"fmt"
	"strings"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/thrawn01/args"
)

func TestArgs(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Args Parser")
}

type TestLogger struct {
	result string
}

func NewTestLogger() *TestLogger {
	return &TestLogger{""}
}

func (self *TestLogger) Print(stuff ...interface{}) {
	self.result = self.result + fmt.Sprint(stuff...) + "|"
}

func (self *TestLogger) Printf(format string, stuff ...interface{}) {
	self.result = self.result + fmt.Sprintf(format, stuff...) + "|"
}

func (self *TestLogger) Println(stuff ...interface{}) {
	self.result = self.result + fmt.Sprintln(stuff...) + "|"
}

func (self *TestLogger) GetEntry() string {
	return self.result
}

var _ = Describe("args", func() {
	Describe("args.WordWrap()", func() {
		It("Should wrap the line including the indent length", func() {
			// Should show the message with the indentation of 10 characters on the next line
			msg := args.WordWrap(`Lorem ipsum dolor sit amet, consectetur
				adipiscing elit, sed do eiusmod tempor incididunt ut labore et
				mollit anim id est laborum.`, 10, 80)
			Expect(msg).To(Equal("Lorem ipsum dolor sit amet, consecteturadipiscing elit, sed do\n          eiusmod tempor incididunt ut labore etmollit anim id est laborum."))
		})
		It("Should wrap the line without the indent length", func() {
			// Should show the message with no indentation
			msg := args.WordWrap(`Lorem ipsum dolor sit amet, consectetur
				adipiscing elit, sed do eiusmod tempor incididunt ut labore et
				mollit anim id est laborum.`, 0, 80)
			Expect(msg).To(Equal("Lorem ipsum dolor sit amet, consecteturadipiscing elit, sed do eiusmod tempor\n incididunt ut labore etmollit anim id est laborum."))
		})
	})
	Describe("args.Dedent()", func() {
		It("Should un-indent a simple string", func() {
			text := args.Dedent(`Lorem ipsum dolor sit amet, consecteturadipiscing elit, sed
			 do eiusmod tempor incididunt ut labore etmollit anim id
			 est laborum.`)
			Expect(text).To(Equal("Lorem ipsum dolor sit amet, consecteturadipiscing elit, sed\ndo eiusmod tempor incididunt ut labore etmollit anim id\nest laborum."))
		})
		It("Should un-indent a string starting with a new line", func() {
			text := args.Dedent(`
			 Lorem ipsum dolor sit amet, consecteturadipiscing elit, sed
			 do eiusmod tempor incididunt ut labore etmollit anim id
			 est laborum.`)
			Expect(text).To(Equal("\nLorem ipsum dolor sit amet, consecteturadipiscing elit, sed\ndo eiusmod tempor incididunt ut labore etmollit anim id\nest laborum."))
		})
	})
	Describe("args.DedentTrim()", func() {
		It("Should un-indent a simple string and trim the result", func() {
			text := args.DedentTrim(`
		    Lorem ipsum dolor sit amet, consecteturadipiscing elit, sed
		    do eiusmod tempor incididunt ut labore etmollit anim id
		    est laborum.
		    `, "\n")
			Expect(text).To(Equal("Lorem ipsum dolor sit amet, consecteturadipiscing elit, sed\ndo eiusmod tempor incididunt ut labore etmollit anim id\nest laborum."))
		})
	})
	Describe("args.StringSlice()", func() {
		It("Should parse a simple comma separated line", func() {
			result := args.StringToSlice("one,two,three")
			Expect(result).To(Equal([]string{"one", "two", "three"}))

			result = args.StringToSlice("one, two, three", strings.TrimSpace)
			Expect(result).To(Equal([]string{"one", "two", "three"}))

			result = args.StringToSlice("one, two, three", strings.TrimSpace, strings.ToUpper)
			Expect(result).To(Equal([]string{"ONE", "TWO", "THREE"}))

			result = args.StringToSlice("one", strings.TrimSpace, strings.ToUpper)
			Expect(result).To(Equal([]string{"ONE"}))
		})
	})
})
