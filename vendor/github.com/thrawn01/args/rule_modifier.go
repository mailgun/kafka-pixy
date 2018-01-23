package args

import "reflect"

type RuleModifier struct {
	rule   *Rule
	parser *ArgParser
}

func NewRuleModifier(parser *ArgParser) *RuleModifier {
	return &RuleModifier{newRule(), parser}
}

func newRuleModifier(rule *Rule, parser *ArgParser) *RuleModifier {
	return &RuleModifier{rule, parser}
}

func (self *RuleModifier) GetRule() *Rule {
	return self.rule
}

func (self *RuleModifier) IsString() *RuleModifier {
	self.rule.Cast = castString
	return self
}

// If the option is seen on the command line, the value is 'true'
func (self *RuleModifier) IsTrue() *RuleModifier {
	self.rule.Action = func(rule *Rule, alias string, args []string, idx *int) error {
		rule.Value = true
		return nil
	}
	self.rule.Cast = castBool
	return self
}

func (self *RuleModifier) IsBool() *RuleModifier {
	self.rule.Cast = castBool
	self.rule.Value = false
	return self
}

func (self *RuleModifier) Default(value string) *RuleModifier {
	self.rule.Default = &value
	return self
}

func (self *RuleModifier) StoreInt(dest *int) *RuleModifier {
	// Implies IsInt()
	self.rule.Cast = castInt
	self.rule.StoreValue = func(value interface{}) {
		*dest = value.(int)
	}
	return self
}

func (self *RuleModifier) IsInt() *RuleModifier {
	self.rule.Cast = castInt
	return self
}

func (self *RuleModifier) StoreTrue(dest *bool) *RuleModifier {
	self.rule.Action = func(rule *Rule, alias string, args []string, idx *int) error {
		rule.Value = true
		return nil
	}
	self.rule.Cast = castBool
	self.rule.StoreValue = func(value interface{}) {
		*dest = value.(bool)
	}
	return self
}

func (self *RuleModifier) IsStringSlice() *RuleModifier {
	self.rule.Cast = castStringSlice
	self.rule.SetFlag(IsGreedy)
	return self
}

func (self *RuleModifier) IsStringMap() *RuleModifier {
	self.rule.Cast = castStringMap
	self.rule.SetFlag(IsGreedy)
	return self
}

// TODO: Make this less horribad, and use more reflection to make the interface simpler
// It should also take more than just []string but also []int... etc...
func (self *RuleModifier) StoreStringSlice(dest *[]string) *RuleModifier {
	self.rule.Cast = castStringSlice
	self.rule.StoreValue = func(src interface{}) {
		// First clear the current slice if any
		*dest = nil
		// This should never happen if we validate the types
		srcType := reflect.TypeOf(src)
		if srcType.Kind() != reflect.Slice {
			self.parser.GetLog().Printf("Attempted to store '%s' which is not a slice", srcType.Kind())
		}
		for _, value := range src.([]string) {
			*dest = append(*dest, value)
		}
	}
	return self
}

func (self *RuleModifier) StoreStringMap(dest *map[string]string) *RuleModifier {
	self.rule.Cast = castStringMap
	self.rule.StoreValue = func(src interface{}) {
		// clear the current before assignment
		*dest = nil
		*dest = src.(map[string]string)
	}
	return self
}

// Indicates this option has an alias it can go by
func (self *RuleModifier) Alias(aliasName string) *RuleModifier {
	self.rule.Aliases = append(self.rule.Aliases, aliasName)
	return self
}

// Makes this option or positional argument required
func (self *RuleModifier) Required() *RuleModifier {
	self.rule.SetFlag(IsRequired)
	return self
}

// Value of this option can only be one of the provided choices; Required() is implied
func (self *RuleModifier) Choices(choices []string) *RuleModifier {
	self.rule.SetFlag(IsRequired)
	self.rule.Choices = choices
	return self
}

func (self *RuleModifier) StoreStr(dest *string) *RuleModifier {
	return self.StoreString(dest)
}

func (self *RuleModifier) StoreString(dest *string) *RuleModifier {
	// Implies IsString()
	self.rule.Cast = castString
	self.rule.StoreValue = func(value interface{}) {
		*dest = value.(string)
	}
	return self
}

func (self *RuleModifier) Count() *RuleModifier {
	self.rule.Action = func(rule *Rule, alias string, args []string, idx *int) error {
		// If user asked us to count the instances of this argument
		rule.Count = rule.Count + 1
		return nil
	}
	self.rule.Cast = castInt
	return self
}

func (self *RuleModifier) Env(varName string) *RuleModifier {
	self.rule.EnvVars = append(self.rule.EnvVars, self.parser.EnvPrefix+varName)
	return self
}

func (self *RuleModifier) Help(message string) *RuleModifier {
	self.rule.RuleDesc = message
	return self
}

func (self *RuleModifier) InGroup(group string) *RuleModifier {
	self.rule.Group = group
	return self
}

func (self *RuleModifier) AddConfigGroup(group string) *RuleModifier {
	var newRule Rule
	newRule = *self.rule
	newRule.SetFlag(IsConfigGroup)
	newRule.Group = group
	// Make a new RuleModifier using self as the template
	return self.parser.AddRule(group, newRuleModifier(&newRule, self.parser))
}

func (self *RuleModifier) AddOption(name string) *RuleModifier {
	var newRule Rule
	newRule = *self.rule
	newRule.SetFlag(IsOption)
	// Make a new RuleModifier using self as the template
	return self.parser.AddRule(name, newRuleModifier(&newRule, self.parser))
}

func (self *RuleModifier) AddConfig(name string) *RuleModifier {
	var newRule Rule
	newRule = *self.rule
	// Make a new Rule using self.rule as the template
	newRule.SetFlag(IsConfig)
	return self.parser.AddRule(name, newRuleModifier(&newRule, self.parser))
}

func (self *RuleModifier) Key(key string) *RuleModifier {
	self.rule.Key = key
	return self
}
