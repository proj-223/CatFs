package main

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/proj-223/CatFs/client"
	"os"
	"strings"
)

const cmdHelp = `Command List:
  rm [a]
  mv [a] [b]
  pwd
  mkdir [a]
  cd [a]
  ls or ls [a]
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
	if len(args) < 1 {
		printError(errors.New("Need command"))
		return false
	}
	cmd := args[0]
	switch cmd {
	case "mv":
		if len(args) <= 2 {
			printError(errors.New("Need 2 argument"))
			return false
		}
		src := args[1]
		dst := args[2]
		err := client.Rename(src, dst)
		if err != nil {
			printError(err)
			return false
		}
		fmt.Println("Success")
	case "rm":
		if len(args) <= 1 {
			printError(errors.New("Need argument"))
			return false
		}
		path := args[1]
		err := client.Remove(path)
		if err != nil {
			printError(err)
			return false
		}
		fmt.Println("Success")
	case "cd":
		if len(args) <= 1 {
			printError(errors.New("Need argument"))
			return false
		}
		path := args[1]
		err := client.Chdir(path)
		if err != nil {
			printError(err)
			return false
		}
		fmt.Println("Success")
	case "mkdir":
		if len(args) <= 1 {
			printError(errors.New("Need argument"))
			return false
		}
		dirname := args[1]
		err := client.Mkdir(dirname, 0)
		if err != nil {
			printError(err)
			return false
		}
		fmt.Println("Success")
	case "ls":
		dirname := ""
		if len(args) > 1 {
			dirname = args[1]
		}
		files, err := client.ListDir(dirname)
		if err != nil {
			printError(err)
			return false
		}
		for _, file := range files {
			fmt.Println(file)
		}
	case "pwd":
		cur := client.CurrentDir()
		fmt.Println(cur)
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
