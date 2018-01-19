package args

import (
	"fmt"
	"os"
	"path"
	"sort"
	"strings"

	"github.com/pkg/errors"
)

// ***********************************************
// Rule Object
// ***********************************************

type CastFunc func(string, interface{}, interface{}) (interface{}, error)
type ActionFunc func(*Rule, string, []string, *int) error
type StoreFunc func(interface{})
type CommandFunc func(*ArgParser, interface{}) (int, error)

const (
	IsCommand int64 = 1 << iota
	IsArgument
	IsConfig
	IsConfigGroup
	IsRequired
	IsOption
	IsFormated
	IsGreedy
	NoValue
	DefaultValue
	EnvValue
	Seen
)

type Rule struct {
	Count       int
	Order       int
	Name        string
	RuleDesc    string
	Value       interface{}
	Default     *string
	Aliases     []string
	EnvVars     []string
	Choices     []string
	EnvPrefix   string
	Cast        CastFunc
	Action      ActionFunc
	StoreValue  StoreFunc
	CommandFunc CommandFunc
	Group       string
	Key         string
	NotGreedy   bool
	Flags       int64
}

func newRule() *Rule {
	return &Rule{Cast: castString, Group: DefaultOptionGroup}
}

func (self *Rule) HasFlag(flag int64) bool {
	return self.Flags&flag != 0
}

func (self *Rule) SetFlag(flag int64) {
	self.Flags = (self.Flags | flag)
}

func (self *Rule) ClearFlag(flag int64) {
	mask := (self.Flags ^ flag)
	self.Flags &= mask
}

func (self *Rule) Validate() error {
	return nil
}

func (self *Rule) GenerateUsage() string {
	switch {
	case self.Flags&IsOption != 0:
		if self.HasFlag(IsRequired) {
			return fmt.Sprintf("%s", self.Aliases[0])
		}
		return fmt.Sprintf("[%s]", self.Aliases[0])
	case self.Flags&IsArgument != 0:
		if self.HasFlag(IsRequired) {
			return fmt.Sprintf("<%s>", self.Name)
		}
		return fmt.Sprintf("[%s]", self.Name)
	}
	return ""
}

func (self *Rule) GenerateHelp() (string, string) {
	var parens []string
	paren := ""

	if !self.HasFlag(IsCommand) {
		if self.Default != nil {
			parens = append(parens, fmt.Sprintf("Default=%s", *self.Default))
		}
		if len(self.EnvVars) != 0 {
			envs := strings.Join(self.EnvVars, ",")
			parens = append(parens, fmt.Sprintf("Env=%s", envs))
		}
		if len(parens) != 0 {
			paren = fmt.Sprintf(" (%s)", strings.Join(parens, ", "))
		}
	}

	if self.HasFlag(IsArgument) {
		return ("  " + self.Name), self.RuleDesc
	}
	// TODO: This sort should happen when we validate rules
	sort.Sort(sort.Reverse(sort.StringSlice(self.Aliases)))
	return ("  " + strings.Join(self.Aliases, ", ")), (self.RuleDesc + paren)
}

func (self *Rule) MatchesAlias(args []string, idx *int) (bool, string) {
	for _, alias := range self.Aliases {
		if args[*idx] == alias {
			return true, args[*idx]
		}
	}
	return false, ""
}

func (self *Rule) Match(args []string, idx *int) (bool, error) {
	name := self.Name
	var matched bool

	if self.HasFlag(IsConfig) {
		return false, nil
	}

	// If this is an argument
	if self.HasFlag(IsArgument) {
		// And we have already seen this argument and it's not greedy
		if self.HasFlag(Seen) && !self.HasFlag(IsGreedy) {
			return false, nil
		}
	} else {
		// Match any known aliases
		matched, name = self.MatchesAlias(args, idx)
		if !matched {
			return false, nil
		}
	}
	self.SetFlag(Seen)

	// If user defined an action
	if self.Action != nil {
		return true, self.Action(self, name, args, idx)
	}

	// If no actions are specified assume a value follows this argument
	if !self.HasFlag(IsArgument) {
		*idx++
		if len(args) <= *idx {
			return true, errors.New(fmt.Sprintf("Expected '%s' to have an argument", name))
		}
	}

	// If we get here, this argument is associated with either an option value or an positional argument
	value, err := self.Cast(name, self.Value, args[*idx])
	if err != nil {
		return true, err
	}
	self.Value = value
	return true, nil
}

// Returns the appropriate required warning to display to the user
func (self *Rule) RequiredMessage() string {
	switch {
	case self.Flags&IsArgument != 0:
		return fmt.Sprintf("argument '%s' is required", self.Name)
	case self.Flags&IsConfig != 0:
		return fmt.Sprintf("config '%s' is required", self.Name)
	default:
		return fmt.Sprintf("option '%s' is required", self.Aliases[0])
	}
}

func (self *Rule) ComputedValue(values *Options) (interface{}, error) {
	if self.Count != 0 {
		self.Value = self.Count
	}

	// If rule matched argument on command line
	if self.HasFlag(Seen) {
		return self.Value, nil
	}

	// If rule matched environment variable
	value, err := self.GetEnvValue()
	if err != nil {
		return nil, err
	}

	if value != nil {
		self.SetFlag(EnvValue)
		return value, nil
	}

	// TODO: Move this logic from here, This method should be all about getting the value
	if self.HasFlag(IsConfigGroup) {
		return nil, nil
	}

	// If provided our map of values, use that
	if values != nil {
		group := values.Group(self.Group)
		if group.HasKey(self.Name) {
			self.ClearFlag(NoValue)
			return self.Cast(self.Name, self.Value, group.Get(self.Name))
		}
	}

	// Apply default if available
	if self.Default != nil {
		self.SetFlag(DefaultValue)
		return self.Cast(self.Name, self.Value, *self.Default)
	}

	// TODO: Move this logic from here, This method should be all about getting the value
	if self.HasFlag(IsRequired) {
		return nil, errors.New(self.RequiredMessage())
	}

	// Flag that we found no value for this rule
	self.SetFlag(NoValue)

	// Return the default value for our type choice
	value, _ = self.Cast(self.Name, self.Value, nil)
	return value, nil
}

func (self *Rule) GetEnvValue() (interface{}, error) {
	if self.EnvVars == nil {
		return nil, nil
	}

	for _, varName := range self.EnvVars {
		//if value, ok := os.LookupEnv(varName); ok {
		if value := os.Getenv(varName); value != "" {
			return self.Cast(varName, self.Value, value)
		}
	}
	return nil, nil
}

func (self *Rule) BackendKey(rootPath string) string {
	// Do this so users are not surprised root isn't prefixed with "/"
	rootPath = "/" + strings.TrimPrefix(rootPath, "/")
	// If the user provided their own key used that instead
	if self.Key != "" {
		return path.Join("/", rootPath, self.Key)
	}

	if self.HasFlag(IsConfigGroup) {
		return path.Join("/", rootPath, self.Group)
	}

	// This might cause a key collision if a group shares the same name as an argument
	// This can be avoided by assigning a custom key name to the group or argument.
	if self.Group == DefaultOptionGroup {
		return path.Join("/", rootPath, self.Name)
	}
	return path.Join("/", rootPath, self.Group, self.Name)
}

// ***********************************************
// Rules Object
// ***********************************************
type Rules []*Rule

func (self Rules) Len() int {
	return len(self)
}

func (self Rules) Less(left, right int) bool {
	return self[left].Order < self[right].Order
}

func (self Rules) Swap(left, right int) {
	self[left], self[right] = self[right], self[left]
}
