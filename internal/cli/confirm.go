package cli

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func (application *app) confirm(prompt string, yes bool) (bool, error) {
	if yes || application.output != outputText ||
		!isTerminal(os.Stdin) || !isTerminal(application.stderr) {
		return true, nil
	}
	fmt.Fprintf(application.stderr, "%s Type yes to continue: ", prompt)
	input, err := bufio.NewReader(os.Stdin).ReadString('\n')
	if err != nil {
		return false, err
	}
	return strings.EqualFold(strings.TrimSpace(input), "yes"), nil
}
