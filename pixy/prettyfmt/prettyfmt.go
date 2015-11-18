package prettyfmt

import (
	"fmt"
)

func Bytes(bytes int64) string {
	kilo := bytes / 1024
	if kilo == 0 {
		return fmt.Sprintf("%db", bytes)
	}
	mega := kilo / 1024
	if mega == 0 {
		return fmt.Sprintf("%dKb", kilo)
	}
	giga := mega / 1024
	if giga == 0 {
		return fmt.Sprintf("%dMb", mega)
	}
	return fmt.Sprintf("%dGb", giga)
}
