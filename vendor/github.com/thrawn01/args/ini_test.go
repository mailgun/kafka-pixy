package args_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/thrawn01/args"
)

var _ = Describe("ArgParser", func() {
	var log *TestLogger

	BeforeEach(func() {
		log = NewTestLogger()
	})

	Describe("FromIni()", func() {
		It("Should provide arg values from INI file", func() {
			parser := args.NewParser()
			parser.AddOption("--one").IsString()
			input := []byte("one=this is one value\ntwo=this is two value\n")
			opt, err := parser.FromINI(input)
			Expect(err).To(BeNil())
			Expect(opt.String("one")).To(Equal("this is one value"))
		})

		It("Should provide arg values from INI file after parsing the command line", func() {
			parser := args.NewParser()
			parser.AddOption("--one").IsString()
			parser.AddOption("--two").IsString()
			parser.AddOption("--three").IsString()
			cmdLine := []string{"--three", "this is three value"}
			opt, err := parser.Parse(&cmdLine)
			input := []byte("one=this is one value\ntwo=this is two value\n")
			opt, err = parser.FromINI(input)
			Expect(err).To(BeNil())
			Expect(opt.String("one")).To(Equal("this is one value"))
			Expect(opt.String("three")).To(Equal("this is three value"))
		})

		It("Should not overide options supplied via the command line", func() {
			parser := args.NewParser()
			parser.AddOption("--one").IsString()
			parser.AddOption("--two").IsString()
			parser.AddOption("--three").IsString()
			cmdLine := []string{"--three", "this is three value", "--one", "this is from the cmd line"}
			opt, err := parser.Parse(&cmdLine)
			input := []byte("one=this is one value\ntwo=this is two value\n")
			opt, err = parser.FromINI(input)
			Expect(err).To(BeNil())
			Expect(opt.String("one")).To(Equal("this is from the cmd line"))
			Expect(opt.String("three")).To(Equal("this is three value"))
		})

		It("Should clear any pre existing slices in the struct before assignment", func() {
			parser := args.NewParser()
			var list []string
			parser.AddOption("--list").StoreStringSlice(&list).Default("foo,bar,bit")

			opt, err := parser.Parse(nil)
			Expect(err).To(BeNil())
			Expect(opt.StringSlice("list")).To(Equal([]string{"foo", "bar", "bit"}))
			Expect(list).To(Equal([]string{"foo", "bar", "bit"}))

			input := []byte("list=six,five,four\n")
			opt, err = parser.FromINI(input)
			Expect(err).To(BeNil())
			Expect(opt.StringSlice("list")).To(Equal([]string{"six", "five", "four"}))
			Expect(list).To(Equal([]string{"six", "five", "four"}))
		})
		It("Should raise an error if a Config is required but not provided", func() {
			parser := args.NewParser()
			parser.AddConfig("one").Required()
			input := []byte("two=this is one value\nthree=this is two value\n")
			_, err := parser.FromINI(input)
			Expect(err).To(Not(BeNil()))
			Expect(err.Error()).To(Equal("config 'one' is required"))
		})
		It("Should not raise if options and configs share the same name, but are in diff groups", func() {
			parser := args.NewParser()
			parser.AddOption("--debug").IsTrue()
			parser.AddConfig("debug").InGroup("database").IsBool()

			cmdLine := []string{"--debug"}
			opts, err := parser.Parse(&cmdLine)

			iniFile := []byte(`
				[database]
				debug=false
			`)
			opts, err = parser.FromINI(iniFile)
			Expect(err).To(BeNil())
			Expect(opts.Bool("debug")).To(Equal(true))
			Expect(opts.Group("database").Bool("debug")).To(Equal(false))
		})
		It("Should report IsSet properly", func() {
			parser := args.NewParser()
			parser.AddOption("--one")
			parser.AddOption("--two")
			parser.AddConfig("three")
			parser.AddOption("four")
			parser.AddConfig("five")

			// 'two' is missing from the command line
			cmdLine := []string{"--one", "this is one"}
			opt, err := parser.Parse(&cmdLine)
			Expect(opt.String("one")).To(Equal("this is one"))
			Expect(opt.IsSet("one")).To(Equal(true))
			Expect(opt.IsSet("two")).To(Equal(false))
			Expect(opt.IsSet("three")).To(Equal(false))
			Expect(opt.IsSet("four")).To(Equal(false))
			Expect(opt.IsSet("five")).To(Equal(false))

			input := []byte("two=this is two value\nthree=yes")
			opt, err = parser.FromINI(input)
			Expect(err).To(BeNil())
			Expect(opt.IsSet("two")).To(Equal(true))
			Expect(opt.IsSet("one")).To(Equal(true))
			Expect(opt.IsSet("three")).To(Equal(true))
			Expect(opt.IsSet("four")).To(Equal(false))
			Expect(opt.IsSet("five")).To(Equal(false))

			err = opt.Required([]string{"two", "one", "three"})
			Expect(err).To(BeNil())
			err = opt.Required([]string{"two", "one", "four"})
			Expect(err).To(Not(BeNil()))
		})
	})
	Describe("ArgParser.AddConfigGroup()", func() {
		It("Should Parser an adhoc group from the ini file", func() {
			cmdLine := []string{"--one", "one-thing"}
			parser := args.NewParser()
			parser.SetLog(log)
			parser.AddOption("--one").IsString()
			parser.AddConfigGroup("candy-bars")

			opt, err := parser.Parse(&cmdLine)
			Expect(err).To(BeNil())
			Expect(log.GetEntry()).To(Equal(""))
			Expect(opt.String("one")).To(Equal("one-thing"))

			iniFile := []byte(`
				one=true

				[candy-bars]
				snickers=300 Cals
				fruit-snacks=100 Cals
				m&ms=400 Cals
			`)
			opts, err := parser.FromINI(iniFile)
			Expect(err).To(BeNil())
			Expect(opts.Group("candy-bars").ToMap()).To(Equal(map[string]interface{}{
				"snickers":     "300 Cals",
				"fruit-snacks": "100 Cals",
				"m&ms":         "400 Cals",
			}))

		})
	})
})
