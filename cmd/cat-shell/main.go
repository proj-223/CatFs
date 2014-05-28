package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

const cmdHelp = `Command List:
  help
  exit
`

func main() {
	runPrompt()
}

func runPrompt() {
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Print("> ")
	for scanner.Scan() {
		line := scanner.Text()
		args := strings.Fields(line)
		if len(args) > 0 {
			if runCmd(args) {
				break
			}
		}
		fmt.Print("> ")
	}
}

func runCmd(args []string) bool {
	cmd := args[0]
	switch cmd {
	case "exit":
		return true
	case "help":
		fmt.Println(cmdHelp)
	default:
		logError(fmt.Errorf("bad command, try \"help\"."))
	}
	return false
}

func logError(e error) {
	if e != nil {
		fmt.Fprintln(os.Stderr, e)
	}
}
