package args_test

import (
	"os"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/thrawn01/args"
)

var _ = Describe("Options", func() {
	var opts *args.Options
	var log *TestLogger
	BeforeEach(func() {
		log = NewTestLogger()
		parser := args.NewParser()
		parser.SetLog(log)

		opts = parser.NewOptionsFromMap(
			map[string]interface{}{
				"int":    1,
				"bool":   true,
				"string": "one",
				"path":   "~/.myrc",
				"endpoints": map[string]interface{}{
					"endpoint1": "host1",
					"endpoint2": "host2",
					"endpoint3": "host3",
				},
				"deeply": map[string]interface{}{
					"nested": map[string]interface{}{
						"thing": "foo",
					},
					"foo": "bar",
				},
			},
		)
	})
	Describe("log", func() {
		It("Should log to StdLogger when cast fails", func() {
			result := opts.Int("string")
			Expect(log.GetEntry()).To(Equal(`unable to cast "one" of type string to int for key 'string'|`))
			Expect(result).To(Equal(0))
		})
	})
	Describe("Int()", func() {
		It("Should convert values to integers", func() {
			result := opts.Int("int")
			Expect(log.GetEntry()).To(Equal(""))
			Expect(result).To(Equal(1))
		})
		It("Should return default value if key doesn't exist", func() {
			result := opts.Int("none")
			Expect(log.GetEntry()).To(Equal(""))
			Expect(result).To(Equal(0))
		})

	})
	Describe("Bool()", func() {
		It("Should convert values to boolean", func() {
			result := opts.Bool("bool")
			Expect(log.GetEntry()).To(Equal(""))
			Expect(result).To(Equal(true))
		})
		It("Should return default value if key doesn't exist", func() {
			result := opts.Bool("none")
			Expect(result).To(Equal(false))
		})
	})
	Describe("String()", func() {
		It("Should return values as string", func() {
			result := opts.String("string")
			Expect(log.GetEntry()).To(Equal(""))
			Expect(result).To(Equal("one"))
		})
		It("Should return default value if key doesn't exist", func() {
			result := opts.String("none")
			Expect(result).To(Equal(""))
		})
	})
	Describe("FilePath()", func() {
		It("Should return values as string", func() {
			result := opts.FilePath("string")
			Expect(log.GetEntry()).To(Equal(""))
			Expect(result).To(Equal("one"))
		})
		It("Should return default value if key doesn't exist", func() {
			result := opts.FilePath("none")
			Expect(result).To(Equal(""))
		})
		It("Should return expanded path if value contains a tilde", func() {
			result := opts.FilePath("path")
			if len(result) == 7 {
				Fail("Tilde was not expanded")
			}
		})
	})
	Describe("NoArgs()", func() {
		It("Should return true if no arguments on the command line", func() {
			parser := args.NewParser()
			parser.AddOption("--power-level").IsInt().Default("1")

			opt, err := parser.Parse(nil)
			Expect(err).To(BeNil())
			Expect(opt.Int("power-level")).To(Equal(1))
			Expect(opt.NoArgs()).To(Equal(true))
		})
		It("Should return false if arguments on the command line", func() {
			parser := args.NewParser()
			parser.AddOption("--power-level").IsInt().Default("1")

			opt, err := parser.Parse(&[]string{"--power-level", "2"})
			Expect(err).To(BeNil())
			Expect(opt.Int("power-level")).To(Equal(2))
			Expect(opt.NoArgs()).To(Equal(false))
		})
	})
	Describe("ToMap()", func() {
		It("Should return a map of the group options", func() {
			Expect(opts.Group("endpoints").ToMap()).To(Equal(map[string]interface{}{
				"endpoint1": "host1",
				"endpoint2": "host2",
				"endpoint3": "host3",
			}))
		})
		It("Should return an empty map if the group doesn't exist", func() {
			Expect(opts.Group("no-group").ToMap()).To(Equal(map[string]interface{}{}))
		})
	})

	Describe("IsSet()", func() {
		It("Should return true if the value is not a cast default", func() {
			parser := args.NewParser()
			parser.AddOption("--is-set").IsInt().Default("1")
			parser.AddOption("--not-set")
			opt, err := parser.Parse(nil)
			Expect(err).To(BeNil())
			Expect(opt.IsSet("is-set")).To(Equal(true))
			Expect(opt.IsSet("not-set")).To(Equal(false))

		})
	})
	Describe("IsArg()", func() {
		It("Should return true if the option as set via the command line", func() {
			cmdLine := []string{"--two", "2"}
			parser := args.NewParser()
			parser.AddOption("--one").IsInt().Default("1")
			parser.AddOption("--two").IsInt().Default("0")

			opt, err := parser.Parse(&cmdLine)
			Expect(err).To(BeNil())
			Expect(opt.Int("one")).To(Equal(1))
			Expect(opt.Int("two")).To(Equal(2))
			Expect(opt.IsArg("one")).To(Equal(false))
			Expect(opt.IsArg("two")).To(Equal(true))
		})
	})
	Describe("IsEnv()", func() {
		It("Should return true if the option was set via an environment variable", func() {
			parser := args.NewParser()
			parser.AddOption("--one").IsInt().Default("1").Env("ONE")
			parser.AddOption("--two").IsInt().Default("0").Env("TWO")

			os.Setenv("TWO", "2")
			opt, err := parser.Parse(nil)
			Expect(err).To(BeNil())
			Expect(opt.Int("one")).To(Equal(1))
			Expect(opt.Int("two")).To(Equal(2))
			Expect(opt.IsEnv("one")).To(Equal(false))
			Expect(opt.IsEnv("two")).To(Equal(true))
		})
	})
	Describe("IsDefault()", func() {
		It("Should return true if the option used the default value", func() {
			cmdLine := []string{"--two", "2"}
			parser := args.NewParser()
			parser.AddOption("--one").IsInt().Default("1")
			parser.AddOption("--two").IsInt().Default("0")
			opt, err := parser.Parse(&cmdLine)
			Expect(err).To(BeNil())
			Expect(opt.Int("one")).To(Equal(1))
			Expect(opt.Int("two")).To(Equal(2))
			Expect(opt.IsDefault("one")).To(Equal(true))
			Expect(opt.IsDefault("two")).To(Equal(false))
		})
	})

	Describe("WasSeen()", func() {
		It("Should return true if the option was seen on the commandline", func() {
			cmdLine := []string{"--is-seen"}
			parser := args.NewParser()
			parser.AddOption("--is-set").IsInt().Default("1")
			parser.AddOption("--is-seen").IsTrue()
			parser.AddOption("--not-set")
			opt, err := parser.Parse(&cmdLine)
			Expect(err).To(BeNil())
			Expect(opt.WasSeen("is-set")).To(Equal(false))
			Expect(opt.WasSeen("not-set")).To(Equal(false))
			Expect(opt.WasSeen("is-seen")).To(Equal(true))

		})
		It("Should return true if the option was seen in the environment", func() {
			parser := args.NewParser()
			parser.AddOption("--is-set").IsInt().Default("1")
			parser.AddOption("--is-seen").IsTrue().Env("IS_SEEN")
			parser.AddOption("--not-set")

			os.Setenv("IS_SEEN", "true")
			opt, err := parser.Parse(nil)
			Expect(err).To(BeNil())
			Expect(opt.WasSeen("is-set")).To(Equal(false))
			Expect(opt.WasSeen("not-set")).To(Equal(false))
			Expect(opt.WasSeen("is-seen")).To(Equal(true))
		})
	})

	Describe("InspectOpt()", func() {
		It("Should return the option object requested", func() {
			parser := args.NewParser()
			parser.AddOption("--is-set").IsInt().Default("1")
			parser.AddOption("--not-set")
			opt, err := parser.Parse(nil)
			Expect(err).To(BeNil())
			option := opt.InspectOpt("is-set")
			Expect(option.GetValue().(int)).To(Equal(1))
			Expect(option.GetRule().Flags).To(Equal(int64(544)))
		})
	})

	Describe("Required()", func() {
		It("Should return nil if all values are provided", func() {
			parser := args.NewParser()
			parser.AddOption("--is-set").IsInt().Default("1")
			parser.AddOption("--is-provided")
			parser.AddOption("--not-set")
			opt, err := parser.Parse(&[]string{"--is-provided", "foo"})
			Expect(err).To(BeNil())

			// All options required have values
			Expect(opt.Required([]string{"is-set", "is-provided"})).To(BeNil())

			// Option 'not-set' is missing is not provided
			err = opt.Required([]string{"is-set", "is-provided", "not-set"})
			Expect(err).To(Not(BeNil()))
			Expect(err.Error()).To(Equal("not-set"))
		})
	})

	Describe("ToString()", func() {
		It("Should return a string representation of the options", func() {
			output := opts.ToString()
			Expect(output).To(Equal(`{
  'bool' = true
  'deeply' = {
    'foo' = bar
    'nested' = {
      'thing' = foo
    }
  }
  'endpoints' = {
    'endpoint1' = host1
    'endpoint2' = host2
    'endpoint3' = host3
  }
  'int' = 1
  'path' = ~/.myrc
  'string' = one
}`))
		})
	})

})
