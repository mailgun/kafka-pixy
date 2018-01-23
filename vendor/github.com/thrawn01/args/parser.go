package args

import (
	"bytes"
	"fmt"
	"os"
	"regexp"
	"sort"
	"sync"

	"strings"

	"github.com/fatih/structs"
	"github.com/pkg/errors"
)

var regexIsOptional = regexp.MustCompile(`^(\W+)([\w|-]*)$`)

type ParseModifier func(*ArgParser)

type ArgParser struct {
	Command              *Rule
	EnvPrefix            string
	Description          string
	Name                 string
	WordWrap             int
	IsSubParser          bool
	StopParsingOnCommand bool
	HelpIO               *os.File
	helpAdded            bool
	mutex                sync.Mutex
	AddHelpOption        bool
	args                 []string
	options              *Options
	rules                Rules
	err                  error
	idx                  int
	posCount             int
	attempts             int
	log                  StdLogger
	flags                int64
}

// Creates a new instance of the argument parser
func NewParser(modifiers ...ParseModifier) *ArgParser {
	parser := &ArgParser{
		WordWrap:      200,
		mutex:         sync.Mutex{},
		log:           DefaultLogger,
		AddHelpOption: true,
		HelpIO:        os.Stdout,
	}
	for _, modify := range modifiers {
		modify(parser)
	}
	return parser
}

// Takes the current parser and return a new parser
// appropriate for use within a command function
func (self *ArgParser) SubParser() *ArgParser {
	parser := NewParser()
	src := structs.New(self)
	dest := structs.New(parser)

	// Copy all the public values
	for _, field := range src.Fields() {
		if field.IsExported() {
			dest.Field(field.Name()).Set(field.Value())
		}
	}

	parser.args = self.args
	parser.rules = self.rules
	parser.log = self.log
	parser.helpAdded = self.helpAdded
	parser.AddHelpOption = self.AddHelpOption
	parser.options = self.options
	parser.flags = self.flags

	// Remove all Commands from our rules
	for i := len(parser.rules) - 1; i >= 0; i-- {
		if parser.rules[i].HasFlag(IsCommand) {
			// Remove the rule
			parser.rules = append(parser.rules[:i], parser.rules[i+1:]...)
		}
	}
	// Clear the selected Commands
	parser.Command = nil
	parser.IsSubParser = true
	return parser
}

func (self *ArgParser) SetLog(logger StdLogger) {
	self.log = logger
}

func (self *ArgParser) GetLog() StdLogger {
	return self.log
}

func (self *ArgParser) SetDesc(desc string) {
	self.Description = desc
}

func (self *ArgParser) info(format string, args ...interface{}) {
	if self.log != nil {
		self.log.Printf(format, args...)
	}
}

func (self *ArgParser) ValidateRules() error {
	var greedyRule *Rule
	for idx, rule := range self.rules {
		// Duplicate rule check
		next := idx + 1
		if next < len(self.rules) {
			for ; next < len(self.rules); next++ {
				// If the name and groups are the same
				if rule.Name == self.rules[next].Name && rule.Group == self.rules[next].Group {
					return errors.Errorf("Duplicate option '%s' defined", rule.Name)
				}
			}
		}
		// Ensure user didn't set a bad default value
		if rule.Cast != nil && rule.Default != nil {
			_, err := rule.Cast(rule.Name, nil, *rule.Default)
			if err != nil {
				return errors.Wrap(err, "Bad default value")
			}
		}
		if !rule.HasFlag(IsArgument) {
			continue
		}
		// If we already found a greedy rule, no other argument should follow
		if greedyRule != nil {
			return errors.Errorf("'%s' is ambiguous when following greedy argument '%s'",
				rule.Name, greedyRule.Name)
		}
		// Check for ambiguous greedy arguments
		if rule.HasFlag(IsGreedy) {
			if greedyRule == nil {
				greedyRule = rule
			}
		}
	}
	return nil
}

func (self *ArgParser) InGroup(group string) *RuleModifier {
	return NewRuleModifier(self).InGroup(group)
}

func (self *ArgParser) AddConfigGroup(group string) *RuleModifier {
	return NewRuleModifier(self).AddConfigGroup(group)
}

func (self *ArgParser) AddOption(name string) *RuleModifier {
	rule := newRule()
	rule.SetFlag(IsOption)
	return self.AddRule(name, newRuleModifier(rule, self))
}

func (self *ArgParser) AddConfig(name string) *RuleModifier {
	rule := newRule()
	rule.SetFlag(IsConfig)
	return self.AddRule(name, newRuleModifier(rule, self))
}

// Deprecated use AddArgument instead
func (self *ArgParser) AddPositional(name string) *RuleModifier {
	return self.AddArgument(name)
}

func (self *ArgParser) AddArgument(name string) *RuleModifier {
	rule := newRule()
	self.posCount++
	rule.Order = self.posCount
	rule.SetFlag(IsArgument)
	return self.AddRule(name, newRuleModifier(rule, self))
}

func (self *ArgParser) AddCommand(name string, cmdFunc CommandFunc) *RuleModifier {
	rule := newRule()
	rule.SetFlag(IsCommand)
	rule.CommandFunc = cmdFunc
	rule.Action = func(rule *Rule, alias string, args []string, idx *int) error {
		return nil
	}
	// Make a new RuleModifier using self as the template
	return self.AddRule(name, newRuleModifier(rule, self))
}

func (self *ArgParser) AddRule(name string, modifier *RuleModifier) *RuleModifier {
	rule := modifier.GetRule()

	// Apply the Environment Prefix to all new rules
	rule.EnvPrefix = self.EnvPrefix

	// If name begins with a non word character, assume it's an optional argument
	if regexIsOptional.MatchString(name) {
		// Attempt to extract the name
		group := regexIsOptional.FindStringSubmatch(name)
		if group == nil {
			panic(fmt.Sprintf("Invalid optional argument name '%s'", name))
		} else {
			rule.Aliases = append(rule.Aliases, name)
			rule.Name = group[2]
		}
	} else {
		switch true {
		case rule.HasFlag(IsCommand):
			rule.Aliases = append(rule.Aliases, name)
			rule.Name = fmt.Sprintf("!cmd-%s", name)
		case rule.HasFlag(IsOption):
			rule.Aliases = append(rule.Aliases, fmt.Sprintf("--%s", name))
			rule.Aliases = append(rule.Aliases, fmt.Sprintf("-%s", name))
			rule.Name = name
		default:
			rule.Name = name
		}
	}
	// Append the rule our list of rules
	self.rules = append(self.rules, rule)
	return modifier
}

// Returns the current list of rules for this parser. If you want to modify a rule
// use GetRule() instead.
func (self *ArgParser) GetRules() Rules {
	return self.rules
}

// Allow the user to modify an existing parser rule
//	parser := args.NewParser()
//	parser.AddOption("--endpoint").Default("http://localhost:19092")
//	parser.AddOption("--grpc").IsTrue()
// 	opts := parser.`ParseSimple(nil)
//
//	if opts.Bool("grpc") && !opts.WasSeen("endpoint") {
//		parser.ModifyRule("endpoint").SetDefault("localhost:19091")
//		opts = parser.ParseSimple(nil)
//	}
func (self *ArgParser) ModifyRule(name string) *RuleModifier {
	for _, rule := range self.rules {
		if rule.Name == name {
			return newRuleModifier(rule, self)
		}
	}
	return nil
}

// Allow the user to inspect a parser rule
func (self *ArgParser) GetRule(name string) *Rule {
	for _, rule := range self.rules {
		if rule.Name == name {
			return rule
		}
	}
	return nil
}

func (self *ArgParser) ParseAndRun(args *[]string, data interface{}) (int, error) {
	_, err := self.Parse(args)
	if err != nil {
		if IsHelpError(err) {
			self.PrintHelp()
			return 1, nil
		}
		return 1, err
	}
	return self.RunCommand(data)
}

// Run the command chosen via the command line, err != nil
// if no command was found on the commandline
func (self *ArgParser) RunCommand(data interface{}) (int, error) {
	// If user didn't provide a command via the commandline
	if self.Command == nil {
		self.PrintHelp()
		return 1, nil
	}

	parser := self.SubParser()
	retCode, err := self.Command.CommandFunc(parser, data)
	return retCode, err
}

func (self *ArgParser) HasHelpOption() bool {
	for _, rule := range self.rules {
		if rule.Name == "help" {
			return true
		}
		for _, alias := range rule.Aliases {
			if alias == "-h" {
				return true
			}
		}
	}
	return false
}

// Parses the command line and prints errors and help if needed
// if user asked for --help print the help message and return nil.
// if there was an error parsing, print the error to stderr and return ni
//	opts := parser.ParseSimple(nil)
//	if opts != nil {
//		return 0, nil
//	}
func (self *ArgParser) ParseSimple(args *[]string) *Options {
	opt, err := self.Parse(args)

	// We could have a non critical error, in addition to the user asking for help
	if opt != nil && opt.Bool("help") {
		self.PrintHelp()
		return nil
	}
	// Print errors to stderr and include our help message
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return nil
	}
	return opt
}

// Parse the commandline, but also print help and exit if the user asked for --help
func (self *ArgParser) ParseOrExit(args *[]string) *Options {
	opt, err := self.Parse(args)

	// We could have a non critical error, in addition to the user asking for help
	if opt != nil && opt.Bool("help") {
		self.PrintHelp()
		os.Exit(1)
	}
	// Print errors to stderr and include our help message
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
	return opt
}

// Parses command line arguments using os.Args if 'args' is nil
func (self *ArgParser) Parse(args *[]string) (*Options, error) {
	if args != nil {
		self.args = copyStringSlice(*args)
	} else if args == nil && !self.IsSubParser {
		// If args is nil and we are not a subparser
		self.args = copyStringSlice(os.Args[1:])
	}

	if self.AddHelpOption && !self.HasHelpOption() {
		// Add help option if --help or -h are not already taken by other options
		self.AddOption("--help").Alias("-h").IsTrue().Help("Display this help message and exit")
		self.helpAdded = true
	}
	return self.parseUntil("--")
}

func (self *ArgParser) parseUntil(terminator string) (*Options, error) {
	self.idx = 0

	// Sanity Check
	if len(self.rules) == 0 {
		return nil, errors.New("Must create some options to match with args.AddOption()" +
			" before calling arg.Parse()")
	}

	if err := self.ValidateRules(); err != nil {
		return nil, err
	}

	// Sort the rules so positional rules are parsed last
	sort.Sort(self.rules)

	// Process command line arguments until we find our terminator
	for ; self.idx < len(self.args); self.idx++ {

		if self.args[self.idx] == terminator {
			goto Apply
		}
		// Match our arguments with rules expected
		//fmt.Printf("====== Attempting to match: %d:%s - ", self.idx, self.args[self.idx])

		// Some options have arguments, this is the idx of the option if it matches
		startIdx := self.idx
		rule, err := self.matchRules(self.rules)
		if err != nil {
			return nil, err
		}
		if rule == nil {
			continue
		}
		//fmt.Printf("Found rule - %+v\n", rule)

		// Remove the argument so a sub processor won't process it again, this avoids confusing behavior
		// for sub parsers. IE: [prog -o option sub-command -o option] the first -o will not
		// be confused with the second -o since we remove it from args here
		self.args = append(self.args[:startIdx], self.args[self.idx+1:]...)
		self.idx += startIdx - (self.idx + 1)

		// If we matched a command
		if rule.HasFlag(IsCommand) {
			// If we already found a command token on the commandline
			if self.Command != nil {
				// Ignore this match, it must be a sub command or a positional argument
				rule.ClearFlag(Seen)
			}
			self.Command = rule
			// If user asked us to stop parsing arguments after finding a command
			// This might be useful if the user wants arguments found before the command
			// to apply only to the parent processor
			if self.StopParsingOnCommand {
				goto Apply
			}
		}
	}
Apply:
	opts, err := self.Apply(nil)
	// TODO: Wrap post parsing validation stuff into a method
	// TODO: This should include the isRequired check
	// return self.PostValidation(self.Apply(nil))

	// When the user asks for --help
	if self.helpAdded && opts.Bool("help") {
		// Ignore the --help request if we see a sub command so the
		// sub command gets a change to process the --help request
		if self.Command != nil {
			// root parsers that want to know if the -h option was provided
			// can still ask if the option was `WasSeen("help")`
			rule := opts.InspectOpt("help").GetRule()
			return opts.SetWithRule("help", false, rule), err
		}
		return opts, &HelpError{}
	}
	return opts, err
}

// Gather all the values from our rules, then apply the passed in options to any rules that don't have a computed value.
func (self *ArgParser) Apply(values *Options) (*Options, error) {
	results := self.NewOptions()

	// for each of the rules
	for _, rule := range self.rules {
		// Get the computed value
		value, err := rule.ComputedValue(values)
		if err != nil {
			self.err = err
			continue
		}

		// If we have a Store() for this rule apply it here
		if rule.StoreValue != nil {
			rule.StoreValue(value)
		}

		// Special Case here for Config Groups
		if rule.HasFlag(IsConfigGroup) && values != nil {
			for _, key := range values.Group(rule.Group).Keys() {
				value := values.Group(rule.Group).Get(key)
				results.Group(rule.Group).SetWithRule(key, value, rule)
			}
		} else {
			results.Group(rule.Group).SetWithRule(rule.Name, value, rule)

			// Choices check
			if rule.Choices != nil {
				strValue := results.Group(rule.Group).String(rule.Name)
				if !containsString(strValue, rule.Choices) {
					err := errors.Errorf("'%s' is an invalid argument for '%s' choose from (%s)",
						strValue, rule.Name, strings.Join(rule.Choices, ", "))
					return results, err
				}
			}
		}
	}

	self.SetOpts(results)
	return self.GetOpts(), self.err
}

func (self *ArgParser) SetOpts(options *Options) {
	self.mutex.Lock()
	self.options = options
	self.mutex.Unlock()
}

func (self *ArgParser) GetOpts() *Options {
	self.mutex.Lock()
	defer func() {
		self.mutex.Unlock()
	}()
	return self.options
}

// Return the un-parsed portion of the argument array. These are arguments that where not
// matched by any AddOption() or AddArgument() rules defined by the user.
func (self *ArgParser) GetArgs() []string {
	return copyStringSlice(self.args)
}

func (self *ArgParser) matchRules(rules Rules) (*Rule, error) {
	// Find a Rule that matches this argument
	for _, rule := range rules {
		matched, err := rule.Match(self.args, &self.idx)
		// If no rule was matched
		if !matched {
			continue
		}
		return rule, err
	}
	// No Rules matched our arguments and there was no error
	return nil, nil
}

func (self *ArgParser) PrintRules() {
	for _, rule := range self.rules {
		fmt.Printf("Rule: %s - '%+v'\n", rule.Name, rule)
	}
}

func (self *ArgParser) PrintHelp() {
	fmt.Fprintln(self.HelpIO, self.GenerateHelp())
}

func (self *ArgParser) GenerateHelp() string {
	var result bytes.Buffer
	// TODO: Improve this once we have arguments
	// Super generic usage message
	result.WriteString(fmt.Sprintf("Usage: %s %s %s\n", self.Name,
		self.GenerateUsage(IsOption),
		self.GenerateUsage(IsArgument)))

	if self.Description != "" {
		result.WriteString("\n")
		if HasFlags(self.flags, IsFormated) {
			result.WriteString(self.Description)
		} else {
			result.WriteString(WordWrap(self.Description, 0, 80))
		}
		result.WriteString("\n")
	}

	commands := self.GenerateHelpSection(IsCommand)
	if commands != "" {
		result.WriteString("\nCommands:\n")
		result.WriteString(commands)
	}

	argument := self.GenerateHelpSection(IsArgument)
	if argument != "" {
		result.WriteString("\nArguments:\n")
		result.WriteString(argument)
	}

	options := self.GenerateHelpSection(IsOption)
	if options != "" {
		result.WriteString("\nOptions:\n")
		result.WriteString(options)
	}
	return result.String()
}

func (self *ArgParser) GenerateUsage(flags int64) string {
	var result bytes.Buffer

	// TODO: Should only return [OPTIONS] if there are too many options
	// TODO: to display on a single line
	if flags == IsOption {
		return "[OPTIONS]"
	}

	for _, rule := range self.rules {
		if !rule.HasFlag(flags) {
			continue
		}
		result.WriteString(" " + rule.GenerateUsage())
	}
	return result.String()
}

type HelpMsg struct {
	Flags   string
	Message string
}

func (self *ArgParser) GenerateHelpSection(flags int64) string {
	var result bytes.Buffer
	var options []HelpMsg

	// Ask each rule to generate a Help message for the options
	maxLen := 0
	for _, rule := range self.rules {
		if !rule.HasFlag(flags) {
			continue
		}
		flags, message := rule.GenerateHelp()
		if len(flags) > maxLen {
			maxLen = len(flags)
		}
		options = append(options, HelpMsg{flags, message})
	}

	// Set our indent length
	indent := maxLen + 3
	flagFmt := fmt.Sprintf("%%-%ds%%s\n", indent)

	for _, opt := range options {
		message := WordWrap(opt.Message, indent, self.WordWrap)
		result.WriteString(fmt.Sprintf(flagFmt, opt.Flags, message))
	}
	return result.String()
}
