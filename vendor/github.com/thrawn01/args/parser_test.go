package args_test

import (
	"os"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/thrawn01/args"
)

var _ = Describe("ArgParser", func() {
	var log *TestLogger

	BeforeEach(func() {
		log = NewTestLogger()
	})

	Describe("ArgParser.Parse(nil)", func() {
		It("Should return error if AddOption() was never called", func() {
			parser := args.NewParser(args.NoHelp())
			_, err := parser.Parse(nil)
			Expect(err).ToNot(BeNil())
			Expect(err.Error()).To(Equal("Must create some options to match with args.AddOption() before calling arg.Parse()"))
		})
		It("Should add Help option if none provided", func() {
			parser := args.NewParser()
			_, err := parser.Parse(nil)
			Expect(err).To(BeNil())
			rule := parser.GetRules()[0]
			Expect(rule.Name).To(Equal("help"))
		})
	})
	Describe("ArgParser.AddOption()", func() {
		cmdLine := []string{"--one", "-two", "++three", "+four", "--power-level"}

		It("Should create optional rule --one", func() {
			parser := args.NewParser()
			parser.AddOption("--one").Count()
			rule := parser.GetRules()[0]
			Expect(rule.Name).To(Equal("one"))
			Expect(rule.Order).To(Equal(0))
		})
		It("Should create optional rule ++one", func() {
			parser := args.NewParser()
			parser.AddOption("++one").Count()
			rule := parser.GetRules()[0]
			Expect(rule.Name).To(Equal("one"))
			Expect(rule.Order).To(Equal(0))
		})

		It("Should create optional rule -one", func() {
			parser := args.NewParser()
			parser.AddOption("-one").Count()
			rule := parser.GetRules()[0]
			Expect(rule.Name).To(Equal("one"))
			Expect(rule.Order).To(Equal(0))
		})

		It("Should create optional rule +one", func() {
			parser := args.NewParser()
			parser.AddOption("+one").Count()
			rule := parser.GetRules()[0]
			Expect(rule.Name).To(Equal("one"))
			Expect(rule.Order).To(Equal(0))
		})

		It("Should match --one", func() {
			parser := args.NewParser()
			parser.AddOption("--one").Count()
			opt, err := parser.Parse(&cmdLine)
			Expect(err).To(BeNil())
			Expect(opt.Int("one")).To(Equal(1))
		})
		It("Should match -two", func() {
			parser := args.NewParser()
			parser.AddOption("-two").Count()
			opt, err := parser.Parse(&cmdLine)
			Expect(err).To(BeNil())
			Expect(opt.Int("two")).To(Equal(1))
		})
		It("Should match ++three", func() {
			parser := args.NewParser()
			parser.AddOption("++three").Count()
			opt, err := parser.Parse(&cmdLine)
			Expect(err).To(BeNil())
			Expect(opt.Int("three")).To(Equal(1))
		})
		It("Should match +four", func() {
			parser := args.NewParser()
			parser.AddOption("+four").Count()
			opt, err := parser.Parse(&cmdLine)
			Expect(err).To(BeNil())
			Expect(opt.Int("four")).To(Equal(1))
		})
		It("Should match --power-level", func() {
			parser := args.NewParser()
			parser.AddOption("--power-level").Count()
			opt, err := parser.Parse(&cmdLine)
			Expect(err).To(BeNil())
			Expect(opt.Int("power-level")).To(Equal(1))
		})
		It("Should match 'no-docker'", func() {
			cmdLine := []string{"--no-docker"}

			parser := args.NewParser()
			parser.AddOption("no-docker").Count()
			opt, err := parser.Parse(&cmdLine)
			Expect(err).To(BeNil())
			Expect(opt.Bool("no-docker")).To(Equal(true))
		})
		It("Should raise an error if a option is required but not provided", func() {
			parser := args.NewParser()
			parser.AddOption("--power-level").Required()
			cmdLine := []string{""}
			_, err := parser.Parse(&cmdLine)

			Expect(err).To(Not(BeNil()))
			Expect(err.Error()).To(Equal("option '--power-level' is required"))
		})
	})

	Describe("ArgParser.IsStringSlice()", func() {
		It("Should allow slices in a comma delimited string", func() {
			parser := args.NewParser()
			parser.AddOption("--list").IsStringSlice().Default("foo,bar,bit")

			// Test Default Value
			opt, err := parser.Parse(nil)
			Expect(err).To(BeNil())
			Expect(opt.StringSlice("list")).To(Equal([]string{"foo", "bar", "bit"}))

			// Provided on the command line
			cmdLine := []string{"--list", "belt,car,table"}
			opt, err = parser.Parse(&cmdLine)
			Expect(err).To(BeNil())
			Expect(opt.StringSlice("list")).To(Equal([]string{"belt", "car", "table"}))
		})
		It("Should allow slices in a comma delimited string saved to a variable", func() {
			parser := args.NewParser()
			var list []string
			parser.AddOption("--list").StoreStringSlice(&list).Default("foo,bar,bit")

			cmdLine := []string{"--list", "belt,car,table"}
			opt, err := parser.Parse(&cmdLine)
			Expect(err).To(BeNil())
			Expect(opt.StringSlice("list")).To(Equal([]string{"belt", "car", "table"}))
			Expect(list).To(Equal([]string{"belt", "car", "table"}))
		})
		It("Should allow multiple iterations of the same option to create a slice", func() {
			parser := args.NewParser()
			parser.AddOption("--list").IsStringSlice()

			cmdLine := []string{"--list", "bee", "--list", "cat", "--list", "dad"}
			opt, err := parser.Parse(&cmdLine)
			Expect(err).To(BeNil())
			Expect(opt.StringSlice("list")).To(Equal([]string{"bee", "cat", "dad"}))
		})
		It("Should allow multiple iterations of the same option to create a slice - with var", func() {
			parser := args.NewParser()
			var list []string
			parser.AddOption("--list").StoreStringSlice(&list)

			cmdLine := []string{"--list", "bee", "--list", "cat", "--list", "dad"}
			opt, err := parser.Parse(&cmdLine)
			Expect(err).To(BeNil())
			Expect(opt.StringSlice("list")).To(Equal([]string{"bee", "cat", "dad"}))
			Expect(list).To(Equal([]string{"bee", "cat", "dad"}))
		})
		It("Should allow multiple iterations of the same argument to create a slice", func() {
			parser := args.NewParser()
			parser.AddArgument("list").IsStringSlice()

			cmdLine := []string{"bee", "cat", "dad"}
			opt, err := parser.Parse(&cmdLine)
			Expect(err).To(BeNil())
			Expect(opt.StringSlice("list")).To(Equal([]string{"bee", "cat", "dad"}))
		})
	})
	Describe("ArgParser.IsStringMap()", func() {
		It("Should handle slice apply from alternate sources", func() {
			parser := args.NewParser()
			parser.AddOption("--list").IsStringSlice()

			options := parser.NewOptionsFromMap(
				map[string]interface{}{
					"list": []string{"bee", "cat", "dad"},
				})
			opt, err := parser.Apply(options)
			Expect(err).To(BeNil())
			Expect(opt.StringSlice("list")).To(Equal([]string{"bee", "cat", "dad"}))
		})
		It("Should error if apply on map is invalid type", func() {
			parser := args.NewParser()
			parser.AddOption("--list").IsStringSlice()

			options := parser.NewOptionsFromMap(
				map[string]interface{}{
					"list": 1,
				})
			_, err := parser.Apply(options)
			Expect(err).To(Not(BeNil()))
		})
		It("Should not error if key contains non alpha char", func() {
			parser := args.NewParser()
			parser.AddOption("--map").IsStringMap()
			parser.AddOption("--foo")

			cmdLine := []string{"--map", "http.ip=192.168.1.1"}
			opt, err := parser.Parse(&cmdLine)
			Expect(opt.StringMap("map")).To(Equal(map[string]string{"http.ip": "192.168.1.1"}))
			Expect(err).To(BeNil())
		})
		It("Should not error if key or value contains an escaped equal or comma", func() {
			parser := args.NewParser()
			parser.AddOption("--map").IsStringMap()
			parser.AddOption("--foo")

			cmdLine := []string{"--map", `http\=ip=192.168.1.1`}
			opt, err := parser.Parse(&cmdLine)
			Expect(err).To(BeNil())
			Expect(opt.StringMap("map")).To(Equal(map[string]string{"http=ip": "192.168.1.1"}))
		})
		It("Should error not error if no map value is supplied", func() {
			parser := args.NewParser()
			parser.AddOption("--list").IsStringMap()
			parser.AddOption("--foo")

			_, err := parser.Parse(nil)
			Expect(err).To(BeNil())
		})
		It("Should allow string map with '=' expression in a comma delimited string", func() {
			parser := args.NewParser()
			parser.AddOption("--map").IsStringMap().Default("foo=bar,bar=foo")

			// Test Default Value
			opt, err := parser.Parse(nil)
			Expect(opt).To(Not(BeNil()))

			Expect(err).To(BeNil())
			Expect(opt.StringMap("map")).To(Equal(map[string]string{"foo": "bar", "bar": "foo"}))

			// Provided on the command line
			cmdLine := []string{"--map", "belt=car,table=cloth"}
			opt, err = parser.Parse(&cmdLine)
			Expect(err).To(BeNil())
			Expect(opt.StringMap("map")).To(Equal(map[string]string{"belt": "car", "table": "cloth"}))
		})
		It("Should store string map into a struct", func() {
			parser := args.NewParser()
			var destMap map[string]string
			parser.AddOption("--map").StoreStringMap(&destMap).Default("foo=bar,bar=foo")

			// Test Default Value
			opt, err := parser.Parse(nil)
			Expect(opt).To(Not(BeNil()))

			Expect(err).To(BeNil())
			Expect(destMap).To(Equal(map[string]string{"foo": "bar", "bar": "foo"}))

			// Provided on the command line
			cmdLine := []string{"--map", "belt=car,table=cloth"}
			opt, err = parser.Parse(&cmdLine)
			Expect(err).To(BeNil())
			Expect(destMap).To(Equal(map[string]string{"belt": "car", "table": "cloth"}))
		})
		It("Should allow string map with JSON string", func() {
			parser := args.NewParser()
			parser.AddOption("--map").IsStringMap().Default(`{"foo":"bar", "bar":"foo"}`)

			// Test Default Value
			opt, err := parser.Parse(nil)
			Expect(err).To(BeNil())
			Expect(opt.StringMap("map")).To(Equal(map[string]string{"foo": "bar", "bar": "foo"}))

			// Provided on the command line
			cmdLine := []string{"--map", `{"belt":"car","table":"cloth"}`}
			opt, err = parser.Parse(&cmdLine)
			Expect(err).To(BeNil())
			Expect(opt.StringMap("map")).To(Equal(map[string]string{"belt": "car", "table": "cloth"}))
		})
		It("Should allow multiple iterations of the same argument to create a map", func() {
			parser := args.NewParser()
			parser.AddOption("--map").IsStringMap()

			cmdLine := []string{"--map", "blue=bell", "--map", "cat=dog", "--map", "dad=boy"}
			opt, err := parser.Parse(&cmdLine)
			Expect(err).To(BeNil())
			Expect(opt.StringMap("map")).To(Equal(map[string]string{
				"blue": "bell",
				"cat":  "dog",
				"dad":  "boy",
			}))
		})
		It("Should handle map apply from alternate sources", func() {
			parser := args.NewParser()
			parser.AddOption("--map").IsStringMap()

			options := parser.NewOptionsFromMap(
				map[string]interface{}{
					"map": map[string]string{"key": "value"},
				})
			opt, err := parser.Apply(options)
			Expect(err).To(BeNil())
			Expect(opt.StringMap("map")).To(Equal(map[string]string{
				"key": "value",
			}))
		})
		It("Should error if apply on map is invalid type", func() {
			parser := args.NewParser()
			parser.AddOption("--map").IsStringMap()

			options := parser.NewOptionsFromMap(
				map[string]interface{}{
					"map": 1,
				})
			_, err := parser.Apply(options)
			Expect(err).To(Not(BeNil()))
		})
		It("Should fail with incomplete key=values", func() {
			parser := args.NewParser()
			parser.AddOption("--map").IsStringMap()

			cmdLine := []string{"--map", "belt"}
			opt, err := parser.Parse(&cmdLine)
			Expect(err).To(Not(BeNil()))

			cmdLine = []string{"--map", "belt="}
			opt, err = parser.Parse(&cmdLine)
			Expect(err).To(Not(BeNil()))

			cmdLine = []string{"--map", "belt=blue;,"}
			opt, err = parser.Parse(&cmdLine)
			Expect(err).To(Not(BeNil()))

			cmdLine = []string{"--map", "belt=car,table=cloth"}
			opt, err = parser.Parse(&cmdLine)
			Expect(err).To(BeNil())
			Expect(opt.StringMap("map")).To(Equal(map[string]string{"belt": "car", "table": "cloth"}))
		})

		It("Should allow multiple iterations of the same argument to create a map with JSON", func() {
			parser := args.NewParser()
			parser.AddOption("--map").IsStringMap()

			cmdLine := []string{
				"--map", `{"blue":"bell"}`,
				"--map", `{"cat":"dog"}`,
				"--map", `{"dad":"boy"}`,
			}
			opt, err := parser.Parse(&cmdLine)
			Expect(err).To(BeNil())
			Expect(opt.StringMap("map")).To(Equal(map[string]string{
				"blue": "bell",
				"cat":  "dog",
				"dad":  "boy",
			}))
		})

	})

	// Here until we complete deprecation of AddPositional()
	Describe("ArgParser.AddPositional()", func() {
		cmdLine := []string{"one", "two", "three", "four"}
		It("Should create argument rule first", func() {
			parser := args.NewParser()
			parser.AddPositional("first").IsString()
			rule := parser.GetRules()[0]
			Expect(rule.Name).To(Equal("first"))
			Expect(rule.Order).To(Equal(1))
		})
		It("Should match first argument 'one'", func() {
			parser := args.NewParser()
			parser.AddArgument("first").IsString()
			opt, err := parser.Parse(&cmdLine)
			Expect(err).To(BeNil())
			Expect(opt.String("first")).To(Equal("one"))
		})
	})
	Describe("ArgParser.ModifyRule()", func() {
		It("Should return allow user to modify an existing rule ", func() {
			parser := args.NewParser()
			parser.AddOption("first").IsString().Default("one")
			opt, err := parser.Parse(nil)
			Expect(err).To(BeNil())
			Expect(opt.String("first")).To(Equal("one"))
			// Modify the rule and parse again
			parser.ModifyRule("first").Default("two")
			opt, err = parser.Parse(nil)
			Expect(err).To(BeNil())
			Expect(opt.String("first")).To(Equal("two"))
		})
	})
	Describe("ArgParser.GetRule()", func() {
		It("Should return allow user to modify an existing rule ", func() {
			parser := args.NewParser()
			parser.AddOption("first").IsString().Default("one")
			opt, err := parser.Parse(nil)
			Expect(err).To(BeNil())
			Expect(opt.String("first")).To(Equal("one"))
			// Get the rule
			rule := parser.GetRule("first")
			Expect(rule.Name).To(Equal("first"))
		})
	})

	Describe("ArgParser.AddArgument()", func() {
		cmdLine := []string{"one", "two", "three", "four"}

		It("Should create argument rule first", func() {
			parser := args.NewParser()
			parser.AddArgument("first").IsString()
			rule := parser.GetRules()[0]
			Expect(rule.Name).To(Equal("first"))
			Expect(rule.Order).To(Equal(1))
		})
		It("Should match first argument 'one'", func() {
			parser := args.NewParser()
			parser.AddArgument("first").IsString()
			opt, err := parser.Parse(&cmdLine)
			Expect(err).To(BeNil())
			Expect(opt.String("first")).To(Equal("one"))
		})
		It("Should match first argument in order of declaration", func() {
			parser := args.NewParser()
			parser.AddArgument("first").IsString()
			parser.AddArgument("second").IsString()
			parser.AddArgument("third").IsString()
			opt, err := parser.Parse(&cmdLine)
			Expect(err).To(BeNil())
			Expect(opt.String("first")).To(Equal("one"))
			Expect(opt.String("second")).To(Equal("two"))
			Expect(opt.String("third")).To(Equal("three"))
		})
		It("Should handle no arguments if declared", func() {
			parser := args.NewParser()
			parser.AddArgument("first").IsString()
			parser.AddArgument("second").IsString()
			parser.AddArgument("third").IsString()

			cmdLine := []string{"one", "two"}
			opt, err := parser.Parse(&cmdLine)
			Expect(err).To(BeNil())
			Expect(opt.String("first")).To(Equal("one"))
			Expect(opt.String("second")).To(Equal("two"))
			Expect(opt.String("third")).To(Equal(""))
		})
		It("Should mixing optionals and arguments", func() {
			parser := args.NewParser()
			parser.AddOption("--verbose").IsTrue()
			parser.AddOption("--first").IsString()
			parser.AddArgument("second").IsString()
			parser.AddArgument("third").IsString()

			cmdLine := []string{"--first", "one", "two", "--verbose"}
			opt, err := parser.Parse(&cmdLine)
			Expect(err).To(BeNil())
			Expect(opt.String("first")).To(Equal("one"))
			Expect(opt.String("second")).To(Equal("two"))
			Expect(opt.String("third")).To(Equal(""))
			Expect(opt.Bool("verbose")).To(Equal(true))
		})
		It("Should raise an error if an optional and an argument share the same name", func() {
			parser := args.NewParser()
			parser.AddOption("--first").IsString()
			parser.AddArgument("first").IsString()

			cmdLine := []string{"--first", "one", "one"}
			_, err := parser.Parse(&cmdLine)
			Expect(err).To(Not(BeNil()))
			Expect(err.Error()).To(Equal("Duplicate option 'first' defined"))
		})
		It("Should raise if options and configs share the same name", func() {
			parser := args.NewParser()
			parser.AddOption("--debug").IsTrue()
			parser.AddConfig("debug").IsBool()

			_, err := parser.Parse(nil)
			Expect(err).To(Not(BeNil()))
			Expect(err.Error()).To(Equal("Duplicate option 'debug' defined"))
		})
		It("Should raise an error if a argument is required but not provided", func() {
			parser := args.NewParser()
			parser.AddArgument("first").Required()
			parser.AddArgument("second").Required()

			cmdLine := []string{"one"}
			_, err := parser.Parse(&cmdLine)
			Expect(err).To(Not(BeNil()))
			Expect(err.Error()).To(Equal("argument 'second' is required"))
		})
		It("Should raise an error if a slice argument is followed by another argument", func() {
			parser := args.NewParser()
			parser.AddArgument("first").IsStringSlice()
			parser.AddArgument("second")

			cmdLine := []string{"one"}
			_, err := parser.Parse(&cmdLine)
			Expect(err).To(Not(BeNil()))
			Expect(err.Error()).To(Equal("'second' is ambiguous when " +
				"following greedy argument 'first'"))
		})
		It("Should raise an error if a slice argument is followed by another slice argument", func() {
			parser := args.NewParser()
			parser.AddArgument("first").IsStringSlice()
			parser.AddArgument("second").IsStringSlice()

			cmdLine := []string{"one"}
			_, err := parser.Parse(&cmdLine)
			Expect(err).To(Not(BeNil()))
			Expect(err.Error()).To(Equal("'second' is ambiguous when " +
				"following greedy argument 'first'"))
		})
	})
	Describe("ArgParser.AddConfig()", func() {
		cmdLine := []string{"--power-level", "--power-level"}
		It("Should add new config only rule", func() {
			parser := args.NewParser()
			parser.AddConfig("power-level").Count().Help("My help message")

			// Should ignore command line options
			opt, err := parser.Parse(&cmdLine)
			Expect(err).To(BeNil())
			Expect(opt.Int("power-level")).To(Equal(0))

			// But Apply() a config file
			options := parser.NewOptionsFromMap(
				map[string]interface{}{
					"power-level": 3,
				})
			newOpt, _ := parser.Apply(options)
			// The old config still has the original non config applied version
			Expect(opt.Int("power-level")).To(Equal(0))
			// The new config has the value applied
			Expect(newOpt.Int("power-level")).To(Equal(3))
		})
	})
	Describe("ArgParser.InGroup()", func() {
		cmdLine := []string{"--power-level", "--hostname", "mysql.com"}
		It("Should add a new group", func() {
			parser := args.NewParser()
			parser.AddOption("--power-level").Count()
			parser.InGroup("database").AddOption("--hostname")
			opt, err := parser.Parse(&cmdLine)
			Expect(err).To(BeNil())
			Expect(opt.Int("power-level")).To(Equal(1))
			Expect(opt.Group("database").String("hostname")).To(Equal("mysql.com"))
		})
	})
	Describe("ArgParser.AddRule()", func() {
		cmdLine := []string{"--power-level", "--power-level"}
		It("Should add new rules", func() {
			parser := args.NewParser()
			rule := args.NewRuleModifier(parser).Count().Help("My help message")
			parser.AddRule("--power-level", rule)
			opt, err := parser.Parse(&cmdLine)
			Expect(err).To(BeNil())
			Expect(opt.Int("power-level")).To(Equal(2))
		})
	})
	Describe("ArgParser.GenerateOptHelp()", func() {
		It("Should generate help messages given a set of rules", func() {
			parser := args.NewParser(args.WrapLen(80))
			parser.AddOption("--power-level").Alias("-p").Help("Specify our power level")
			parser.AddOption("--cat-level").
				Alias("-c").
				Help(`Lorem ipsum dolor sit amet, consectetur
			mollit anim id est laborum.`)
			msg := parser.GenerateHelpSection(args.IsOption)
			Expect(msg).To(Equal("  -p, --power-level   Specify our power level" +
				"\n  -c, --cat-level     Lorem ipsum dolor sit amet, consecteturmollit anim id est" +
				"\n                      laborum.\n"))
		})
	})
	Describe("ArgParser.GenerateHelp()", func() {
		It("Should generate help messages given a set of rules", func() {
			parser := args.NewParser(args.EnvPrefix("APP_"), args.Desc("Small Description"),
				args.Name("dragon-ball"), args.WrapLen(80))
			parser.AddOption("--environ").Default("1").Alias("-e").Env("ENV").Help("Default thing")
			parser.AddOption("--default").Default("0").Alias("-d").Help("Default thing")
			parser.AddOption("--power-level").Alias("-p").Help("Specify our power level")
			parser.AddOption("--cat-level").Alias("-c").Help(`Lorem ipsum dolor sit amet, consectetur
				adipiscing elit, sed do eiusmod tempor incididunt ut labore et
				mollit anim id est laborum.`)
			msg := parser.GenerateHelp()
			Expect(msg).To(ContainSubstring("Usage: dragon-ball [OPTIONS]"))
			Expect(msg).To(ContainSubstring("-e, --environ       Default thing (Default=1, Env=APP_ENV)"))
			Expect(msg).To(ContainSubstring("-d, --default       Default thing (Default=0)"))
			Expect(msg).To(ContainSubstring("-p, --power-level   Specify our power level"))
			Expect(msg).To(ContainSubstring("Small Description"))
		})
		It("Should generate formated description if flag is set", func() {
			desc := `
			Custom formated description ----------------------------------------------------------- over 80

			With lots of new lines
			`
			parser := args.NewParser(args.Desc(desc, args.IsFormated),
				args.Name("dragon-ball"), args.WrapLen(80))
			parser.AddOption("--environ").Default("1").Alias("-e").Help("Default thing")
			msg := parser.GenerateHelp()
			Expect(msg).To(ContainSubstring("Custom formated description --------------------" +
				"--------------------------------------- over 80"))
		})
	})
	Describe("ArgParser.AddCommand()", func() {
		It("Should run a command if seen on the command line", func() {
			parser := args.NewParser()
			called := false
			parser.AddCommand("command1", func(parent *args.ArgParser, data interface{}) (int, error) {
				called = true
				return 0, nil
			})
			cmdLine := []string{"command1"}
			retCode, err := parser.ParseAndRun(&cmdLine, nil)
			Expect(err).To(BeNil())
			Expect(retCode).To(Equal(0))
			Expect(called).To(Equal(true))
		})
		It("Should not confuse a command with a following argument", func() {
			parser := args.NewParser()
			called := 0
			parser.AddCommand("set", func(parent *args.ArgParser, data interface{}) (int, error) {
				called++
				return 0, nil
			})
			cmdLine := []string{"set", "set"}
			retCode, err := parser.ParseAndRun(&cmdLine, nil)
			Expect(err).To(BeNil())
			Expect(retCode).To(Equal(0))
			Expect(called).To(Equal(1))
		})
		It("Should provide a sub parser with that will not confuse a following argument", func() {
			parser := args.NewParser()
			called := 0
			parser.AddCommand("set", func(parent *args.ArgParser, data interface{}) (int, error) {
				parent.AddArgument("first").Required()
				parent.AddArgument("second").Required()
				opts, err := parent.Parse(nil)
				Expect(err).To(BeNil())
				Expect(opts.String("first")).To(Equal("foo"))
				Expect(opts.String("second")).To(Equal("bar"))

				called++
				return 0, nil
			})
			cmdLine := []string{"set", "foo", "bar"}
			retCode, err := parser.ParseAndRun(&cmdLine, nil)
			Expect(err).To(BeNil())
			Expect(retCode).To(Equal(0))
			Expect(called).To(Equal(1))
		})
		It("Should allow sub commands to be a thing", func() {
			parser := args.NewParser()
			called := 0
			parser.AddCommand("volume", func(parent *args.ArgParser, data interface{}) (int, error) {
				parent.AddCommand("create", func(subParent *args.ArgParser, data interface{}) (int, error) {
					subParent.AddArgument("volume-name").Required()
					opts, err := subParent.Parse(nil)
					Expect(err).To(BeNil())
					Expect(opts.String("volume-name")).To(Equal("my-new-volume"))

					called++
					return 0, nil
				})
				retCode, err := parent.ParseAndRun(nil, nil)
				Expect(err).To(BeNil())
				Expect(retCode).To(Equal(0))
				return retCode, nil
			})
			cmdLine := []string{"volume", "create", "my-new-volume"}
			retCode, err := parser.ParseAndRun(&cmdLine, nil)
			Expect(err).To(BeNil())
			Expect(retCode).To(Equal(0))
			Expect(called).To(Equal(1))
		})
		It("Should respect auto added help option in commands", func() {
			parser := args.NewParser()
			// Capture the help message via Pipe()
			_, ioWriter, _ := os.Pipe()
			parser.HelpIO = ioWriter

			called := 0
			parser.AddCommand("set", func(parent *args.ArgParser, data interface{}) (int, error) {
				parent.AddArgument("first").Required()
				_, err := parent.Parse(nil)
				Expect(err).To(Not(BeNil()))
				Expect(err.Error()).To(Equal("User asked for help; Inspect this error " +
					"with args.AskedForHelp(err)"))
				Expect(args.IsHelpError(err)).To(Equal(true))
				Expect(args.AskedForHelp(err)).To(Equal(true))

				called++
				return 0, nil
			})
			cmdLine := []string{"set", "-h"}
			retCode, err := parser.ParseAndRun(&cmdLine, nil)
			Expect(err).To(BeNil())
			Expect(retCode).To(Equal(0))
			Expect(called).To(Equal(1))
		})
		It("Should root parser should ignore help option if a sub command was provided", func() {
			// ignoring help gives a sub command a chance to provide help

			parser := args.NewParser()
			// Capture the help message via Pipe()
			_, ioWriter, _ := os.Pipe()
			parser.HelpIO = ioWriter

			called := 0
			parser.AddCommand("set", func(parent *args.ArgParser, data interface{}) (int, error) {
				parent.AddArgument("first").Required()
				_, err := parent.Parse(nil)
				Expect(err).To(Not(BeNil()))
				Expect(err.Error()).To(Equal("User asked for help; Inspect this error " +
					"with args.AskedForHelp(err)"))
				Expect(args.IsHelpError(err)).To(Equal(true))
				Expect(args.AskedForHelp(err)).To(Equal(true))

				called++
				return 0, nil
			})

			// Parse at this point will indicate `--help` is false because the
			// sub command `set` was provided
			cmdLine := []string{"set", "-h"}
			opt := parser.ParseSimple(&cmdLine)
			Expect(opt).To(Not(BeNil()))
			Expect(opt.Bool("help")).To(Equal(false))
			// We can still tell if the help option `WasSeen` if root the parser
			// needs to know if help was requested by the user
			Expect(opt.WasSeen("help")).To(Equal(true))

			// Thus allowing the sub command `set` to provide help
			retCode, err := parser.RunCommand(nil)
			Expect(err).To(BeNil())
			Expect(retCode).To(Equal(0))
			Expect(called).To(Equal(1))
		})
	})
	Describe("ArgParser.GetArgs()", func() {
		It("Should return all un-matched arguments and options", func() {
			parser := args.NewParser()
			parser.AddArgument("image")
			parser.AddOption("-output").Alias("-o").Required()
			parser.AddOption("-runtime").Default("docker")

			cmdLine := []string{"golang:1.6", "build", "-o",
				"amd64-my-prog", "-installsuffix", "static", "./..."}
			opt, err := parser.Parse(&cmdLine)
			Expect(err).To(BeNil())
			Expect(opt.String("output")).To(Equal("amd64-my-prog"))
			Expect(opt.String("image")).To(Equal("golang:1.6"))
			Expect(parser.GetArgs()).To(Equal([]string{"build", "-installsuffix", "static", "./..."}))
		})
		It("Should return all empty if all arguments and options matched", func() {
			parser := args.NewParser()
			parser.AddOption("--list").IsStringSlice()

			cmdLine := []string{"--list", "bee", "--list", "cat", "--list", "dad"}
			opt, err := parser.Parse(&cmdLine)
			Expect(err).To(BeNil())
			Expect(opt.StringSlice("list")).To(Equal([]string{"bee", "cat", "dad"}))
			Expect(parser.GetArgs()).To(Equal([]string{}))
		})
	})
})
