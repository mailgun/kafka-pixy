package args_test

import (
	"os"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/thrawn01/args"
)

var _ = Describe("RuleModifier", func() {
	Describe("RuleModifier.AddConfig()", func() {
		cmdLine := []string{"--power-level", "--power-level", "--user"}
		It("Should add new config only rule", func() {
			parser := args.NewParser()
			parser.AddConfig("power-level").Count().Help("My help message")

			db := parser.InGroup("database")
			db.AddConfig("user").Help("database user")
			db.AddConfig("pass").Help("database password")

			// Should ignore command line options
			opt, err := parser.Parse(&cmdLine)
			Expect(err).To(BeNil())
			Expect(opt.Int("power-level")).To(Equal(0))

			// But Apply() a config file
			options := parser.NewOptionsFromMap(
				map[string]interface{}{
					"power-level": 3,
					"database": map[string]interface{}{
						"user": "my-user",
						"pass": "my-pass",
					},
				})
			newOpt, _ := parser.Apply(options)
			// The new config has the value applied
			Expect(newOpt.Int("power-level")).To(Equal(3))
			Expect(newOpt.Group("database").String("user")).To(Equal("my-user"))
			Expect(newOpt.Group("database").String("pass")).To(Equal("my-pass"))
		})
	})
	Describe("RuleModifier.InGroup()", func() {
		cmdLine := []string{"--power-level", "--hostname", "mysql.com"}
		It("Should add a new group", func() {
			parser := args.NewParser()
			parser.AddOption("--power-level").Count()
			parser.AddOption("--hostname").InGroup("database")
			opt, err := parser.Parse(&cmdLine)
			Expect(err).To(BeNil())
			Expect(opt.Int("power-level")).To(Equal(1))
			Expect(opt.Group("database").String("hostname")).To(Equal("mysql.com"))
		})
		It("Regression: Should not modify the original group rule, but a new group rule", func() {
			parser := args.NewParser()
			db := parser.InGroup("database")
			db.AddOption("--host").Alias("-dH").Default("localhost")
			db.AddConfig("debug").IsTrue()

			_, err := parser.Parse(nil)
			Expect(err).To(BeNil())

			rule := parser.GetRules()[0]
			Expect(rule.Name).To(Equal("host"))
			rule = parser.GetRules()[1]
			Expect(rule.Name).To(Equal("debug"))
			Expect(len(rule.Aliases)).To(Equal(0))
		})
	})
	Describe("RuleModifier.AddConfigGroup()", func() {
		iniFile := []byte(`
		power-level=20000

		[endpoints]
		endpoint1=http://thrawn01.org/1
		endpoint2=http://thrawn01.org/2
		endpoint3=http://thrawn01.org/3
		`)

		It("Should add a new group", func() {
			parser := args.NewParser()
			parser.AddOption("--power-level").IsInt()
			parser.AddConfigGroup("endpoints").Help("List of http endpoints")
			opt, err := parser.FromINI(iniFile)
			Expect(err).To(BeNil())
			Expect(opt.Int("power-level")).To(Equal(20000))
			Expect(opt.Group("endpoints").ToMap()).To(Equal(map[string]interface{}{
				"endpoint1": "http://thrawn01.org/1",
				"endpoint2": "http://thrawn01.org/2",
				"endpoint3": "http://thrawn01.org/3",
			}))
			Expect(opt.Group("endpoints").String("endpoint1")).To(Equal("http://thrawn01.org/1"))
		})
	})
	Describe("RuleModifier.Count()", func() {
		It("Should count one", func() {
			parser := args.NewParser()
			cmdLine := []string{"--verbose"}
			parser.AddOption("--verbose").Count()
			opt, err := parser.Parse(&cmdLine)
			Expect(err).To(BeNil())
			Expect(opt.Int("verbose")).To(Equal(1))
		})
		It("Should count three times", func() {
			parser := args.NewParser()
			cmdLine := []string{"--verbose", "--verbose", "--verbose"}
			parser.AddOption("--verbose").Count()
			opt, err := parser.Parse(&cmdLine)
			Expect(err).To(BeNil())
			Expect(opt.Int("verbose")).To(Equal(3))
		})
	})
	Describe("RuleModifier.IsInt()", func() {
		It("Should ensure value supplied is an integer", func() {
			parser := args.NewParser()
			parser.AddOption("--power-level").IsInt()

			cmdLine := []string{"--power-level", "10000"}
			opt, err := parser.Parse(&cmdLine)
			Expect(err).To(BeNil())
			Expect(opt.Int("power-level")).To(Equal(10000))
		})

		It("Should set err if the option value is not parsable as an integer", func() {
			parser := args.NewParser()
			cmdLine := []string{"--power-level", "over-ten-thousand"}
			parser.AddOption("--power-level").IsInt()
			_, err := parser.Parse(&cmdLine)
			Expect(err).ToNot(BeNil())
			Expect(err.Error()).To(Equal("Invalid value for '--power-level' - 'over-ten-thousand' is not an Integer"))
			//Expect(opt.Int("power-level")).To(Equal(0))
		})

		It("Should set err if no option value is provided", func() {
			parser := args.NewParser()
			cmdLine := []string{"--power-level"}
			parser.AddOption("--power-level").IsInt()
			_, err := parser.Parse(&cmdLine)
			Expect(err).ToNot(BeNil())
			Expect(err.Error()).To(Equal("Expected '--power-level' to have an argument"))
			//Expect(opt.Int("power-level")).To(Equal(0))
		})
	})
	Describe("RuleModifier.StoreInt()", func() {
		It("Should ensure value supplied is assigned to passed value", func() {
			parser := args.NewParser()
			var value int
			parser.AddOption("--power-level").StoreInt(&value)

			cmdLine := []string{"--power-level", "10000"}
			opt, err := parser.Parse(&cmdLine)
			Expect(err).To(BeNil())
			Expect(opt.Int("power-level")).To(Equal(10000))
			Expect(value).To(Equal(10000))
		})
	})
	Describe("RuleModifier.IsString()", func() {
		It("Should provide string value", func() {
			parser := args.NewParser()
			parser.AddOption("--power-level").IsString()

			cmdLine := []string{"--power-level", "over-ten-thousand"}
			opt, err := parser.Parse(&cmdLine)
			Expect(err).To(BeNil())
			Expect(opt.String("power-level")).To(Equal("over-ten-thousand"))
		})

		It("Should set err if no option value is provided", func() {
			parser := args.NewParser()
			cmdLine := []string{"--power-level"}
			parser.AddOption("--power-level").IsString()
			_, err := parser.Parse(&cmdLine)
			Expect(err).ToNot(BeNil())
			Expect(err.Error()).To(Equal("Expected '--power-level' to have an argument"))
		})
	})
	Describe("RuleModifier.StoreString()", func() {
		It("Should ensure value supplied is assigned to passed value", func() {
			parser := args.NewParser()
			var value string
			parser.AddOption("--power-level").StoreString(&value)

			cmdLine := []string{"--power-level", "over-ten-thousand"}
			opt, err := parser.Parse(&cmdLine)
			Expect(err).To(BeNil())
			Expect(opt.String("power-level")).To(Equal("over-ten-thousand"))
			Expect(value).To(Equal("over-ten-thousand"))
		})
	})
	Describe("RuleModifier.StoreStr()", func() {
		It("Should ensure value supplied is assigned to passed value", func() {
			parser := args.NewParser()
			var value string
			parser.AddOption("--power-level").StoreStr(&value)

			cmdLine := []string{"--power-level", "over-ten-thousand"}
			opt, err := parser.Parse(&cmdLine)
			Expect(err).To(BeNil())
			Expect(opt.String("power-level")).To(Equal("over-ten-thousand"))
			Expect(value).To(Equal("over-ten-thousand"))
		})
	})
	Describe("RuleModifier.StoreTrue()", func() {
		It("Should ensure value supplied is true when argument is seen", func() {
			parser := args.NewParser()
			var debug bool
			parser.AddOption("--debug").StoreTrue(&debug)

			cmdLine := []string{"--debug"}
			opt, err := parser.Parse(&cmdLine)
			Expect(err).To(BeNil())
			Expect(opt.Bool("debug")).To(Equal(true))
			Expect(debug).To(Equal(true))
		})

		It("Should ensure value supplied is false when argument is NOT seen", func() {
			parser := args.NewParser()
			var debug bool
			parser.AddOption("--debug").StoreTrue(&debug)

			cmdLine := []string{"--something-else"}
			opt, err := parser.Parse(&cmdLine)
			Expect(err).To(BeNil())
			Expect(opt.Bool("debug")).To(Equal(false))
			Expect(debug).To(Equal(false))
		})
	})
	Describe("RuleModifier.IsTrue()", func() {
		It("Should set true value when seen", func() {
			parser := args.NewParser()
			parser.AddOption("--help").IsTrue()

			cmdLine := []string{"--help"}
			opt, err := parser.Parse(&cmdLine)
			Expect(err).To(BeNil())
			Expect(opt.Bool("help")).To(Equal(true))
		})

		It("Should set false when NOT seen", func() {
			parser := args.NewParser()
			cmdLine := []string{"--something-else"}
			parser.AddOption("--help").IsTrue()
			opt, err := parser.Parse(&cmdLine)
			Expect(err).To(BeNil())
			Expect(opt.Bool("help")).To(Equal(false))
		})
	})
	Describe("RuleModifier.IsStringSlice()", func() {
		It("Should ensure []string provided is set when a comma separated list is provided", func() {
			parser := args.NewParser()
			parser.AddOption("--list").IsStringSlice()

			cmdLine := []string{"--list", "one,two,three"}
			opt, err := parser.Parse(&cmdLine)
			Expect(err).To(BeNil())
			Expect(opt.StringSlice("list")).To(Equal([]string{"one", "two", "three"}))
		})
	})
	Describe("RuleModifier.StoreStringSlice()", func() {
		It("Should ensure []string provided is set when a comma separated list is provided", func() {
			parser := args.NewParser()
			var list []string
			parser.AddOption("--list").StoreStringSlice(&list)

			cmdLine := []string{"--list", "one,two,three"}
			opt, err := parser.Parse(&cmdLine)
			Expect(err).To(BeNil())
			Expect(opt.StringSlice("list")).To(Equal([]string{"one", "two", "three"}))
			Expect(list).To(Equal([]string{"one", "two", "three"}))
		})

		It("Should ensure []string provided is set when a comma separated list is provided", func() {
			parser := args.NewParser()
			var list []string
			parser.AddOption("--list").StoreStringSlice(&list)

			cmdLine := []string{"--list", "one,two,three"}
			opt, err := parser.Parse(&cmdLine)
			Expect(err).To(BeNil())
			Expect(opt.StringSlice("list")).To(Equal([]string{"one", "two", "three"}))
			Expect(list).To(Equal([]string{"one", "two", "three"}))
		})

		It("Should ensure []interface{} provided is set if a single value is provided", func() {
			parser := args.NewParser()
			var list []string
			parser.AddOption("--list").StoreStringSlice(&list)

			cmdLine := []string{"--list", "one"}
			opt, err := parser.Parse(&cmdLine)
			Expect(err).To(BeNil())
			Expect(opt.StringSlice("list")).To(Equal([]string{"one"}))
			Expect(list).To(Equal([]string{"one"}))
		})

		It("Should set err if no list value is provided", func() {
			parser := args.NewParser()
			var list []string
			parser.AddOption("--list").StoreStringSlice(&list)

			cmdLine := []string{"--list"}
			_, err := parser.Parse(&cmdLine)
			Expect(err).ToNot(BeNil())
			Expect(err.Error()).To(Equal("Expected '--list' to have an argument"))
		})

	})
	Describe("RuleModifier.Default()", func() {
		It("Should ensure default values is supplied if no matching argument is found", func() {
			parser := args.NewParser()
			var value int
			parser.AddOption("--power-level").StoreInt(&value).Default("10")

			opt, err := parser.Parse(nil)
			Expect(err).To(BeNil())
			Expect(opt.Int("power-level")).To(Equal(10))
			Expect(value).To(Equal(10))
		})

		It("Should return err if default value does not match AddOption() type", func() {
			parser := args.NewParser()
			parser.AddOption("--power-level").IsInt().Default("over-ten-thousand")
			_, err := parser.Parse(nil)
			Expect(err).ToNot(BeNil())
			Expect(err.Error()).To(ContainSubstring("Bad default value"))
		})
	})
	Describe("RuleModifier.Choices()", func() {
		It("Should err if not provided", func() {
			parser := args.NewParser()
			parser.AddOption("--choices").Choices([]string{"one", "two", "three"})

			_, err := parser.Parse(nil)
			Expect(err).To(Not(BeNil()))
			Expect(err.Error()).To(Equal("option '--choices' is required"))
		})
		It("Should allow valid choices", func() {
			parser := args.NewParser()
			parser.AddOption("--choices").Choices([]string{"one", "two", "three"})

			cmdLine := []string{"--choices", "one"}
			opt, err := parser.Parse(&cmdLine)
			Expect(err).To(BeNil())
			Expect(opt.String("choices")).To(Equal("one"))
		})
		It("Should err if not valid choice", func() {
			parser := args.NewParser()
			parser.AddOption("--choices").Choices([]string{"one", "two", "three"})

			cmdLine := []string{"--choices", "five"}
			_, err := parser.Parse(&cmdLine)
			Expect(err).To(Not(BeNil()))
			Expect(err.Error()).To(Equal("'five' is an invalid argument for 'choices' " +
				"choose from (one, two, three)"))
		})
		It("Should work with default value", func() {
			parser := args.NewParser()
			parser.AddOption("--choices").Default("two").Choices([]string{"one", "two", "three"})

			opt, err := parser.Parse(nil)
			Expect(err).To(BeNil())
			Expect(opt.String("choices")).To(Equal("two"))
		})
		It("Should work with non string values", func() {
			parser := args.NewParser()
			parser.AddOption("--choices").IsInt().Choices([]string{"1", "2", "3"})

			cmdLine := []string{"--choices", "5"}
			_, err := parser.Parse(&cmdLine)
			Expect(err).To(Not(BeNil()))
			Expect(err.Error()).To(Equal("'5' is an invalid argument for 'choices' " +
				"choose from (1, 2, 3)"))
		})
	})
	Describe("RuleModifier.Env()", func() {
		AfterEach(func() {
			os.Unsetenv("POWER_LEVEL")
		})

		It("Should supply the environ value if argument was not passed", func() {
			parser := args.NewParser()
			var value int
			parser.AddOption("--power-level").StoreInt(&value).Env("POWER_LEVEL")

			os.Setenv("POWER_LEVEL", "10")

			opt, err := parser.Parse(nil)
			Expect(err).To(BeNil())
			Expect(opt.Int("power-level")).To(Equal(10))
			Expect(value).To(Equal(10))
		})

		It("Should return an error if the environ value does not match the AddOption() type", func() {
			parser := args.NewParser()
			var value int
			parser.AddOption("--power-level").StoreInt(&value).Env("POWER_LEVEL")

			os.Setenv("POWER_LEVEL", "over-ten-thousand")

			_, err := parser.Parse(nil)
			Expect(err).ToNot(BeNil())
			Expect(err.Error()).To(Equal("Invalid value for 'POWER_LEVEL' - 'over-ten-thousand' is not an Integer"))
		})

		It("Should use the default value if argument was not passed and environment var was not set", func() {
			parser := args.NewParser()
			var value int
			parser.AddOption("--power-level").StoreInt(&value).Env("POWER_LEVEL").Default("1")

			opt, err := parser.Parse(nil)
			Expect(err).To(BeNil())
			Expect(opt.Int("power-level")).To(Equal(1))
			Expect(value).To(Equal(1))
		})
	})
})
