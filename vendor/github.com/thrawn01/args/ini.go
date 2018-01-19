package args

import (
	"fmt"

	"github.com/go-ini/ini"
	"github.com/pkg/errors"
)

// Key Value Pairs found in this section will be considered as if they had no section. You can overide this section name
// by including a list of sections in the FromINI() call.
const DefaultSection string = ""

// Parse the INI file and the Apply() the values to the parser
func (self *ArgParser) FromINI(input []byte, defaultSections ...string) (*Options, error) {
	// If no default sections where provided, use our default section
	options, err := self.ParseINI(input)
	if err != nil {
		return options, err
	}
	// Apply the ini file values to the commandline and environment variables
	return self.Apply(options)
}

func (self *ArgParser) FromINIFile(fileName string) (*Options, error) {
	content, err := LoadFile(fileName)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("'%s'", fileName))
	}
	return self.FromINI(content)
}

// Parse the INI file and return the raw parsed options
func (self *ArgParser) ParseINI(input []byte) (*Options, error) {
	// Parse the file return a map of the contents
	cfg, err := ini.Load(input)
	if err != nil {
		return nil, err
	}
	values := self.NewOptions()
	for _, section := range cfg.Sections() {
		group := cfg.Section(section.Name())
		for _, key := range group.KeyStrings() {
			// Always use our default option group name for the DEFAULT section
			name := section.Name()
			// TODO: This should be user configurable
			if name == "DEFAULT" {
				name = DefaultOptionGroup
			}
			values.Group(name).Set(key, group.Key(key).String())
		}

	}
	return values, nil
}
