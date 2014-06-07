package main

import (
	"bufio"
	"fmt"
	"github.com/proj-223/CatFs/client"
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

func printError(err error) {
	fmt.Printf("Error: %s\n", err.Error())
}

func runCmd(args []string) bool {
	cmd := args[0]
	switch cmd {
	case "mkdir":
		dirname := args[1]
		err := client.Mkdir(dirname, 0)
		if err != nil {
			printError(err)
			return false
		}
		fmt.Println("Success")
	case "ls":
		files, err := client.ListDir("")
		if err != nil {
			printError(err)
			return false
		}
		for _, file := range files {
			fmt.Println(file)
		}
	case "exit":
		return true
	case "help":
		fmt.Println(cmdHelp)
	default:
		logError(fmt.Errorf("bad command, try \"help\"."))
	}
	fmt.Println()
	return false
}

func logError(e error) {
	if e != nil {
		fmt.Fprintln(os.Stderr, e)
	}
}
