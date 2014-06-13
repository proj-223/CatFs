package main

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/proj-223/CatFs/client"
	proc "github.com/proj-223/CatFs/protocols"
	"io"
	"os"
	"strings"
)

const cmdHelp = `Command List:
  touch [a]
  rm [a]
  mv [a] [b]
  pwd
  mkdir [a]
  cd [a]
  ls or ls [a]
  write_new [name] [data]
  cat [name]
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
	case "cat":
		if len(args) <= 1 {
			printError(errors.New("Need argument"))
			return false
		}
		filename := args[1]
		fi, err := client.Open(filename, proc.OPEN_MODE_READ)
		if err != nil {
			printError(err)
			return false
		}
		buf := make([]byte, 100)
		n, err := fi.Read(buf)
		if err != nil && err != io.EOF {
			printError(err)
			return false
		}
		println(string(buf[:n]))
	case "write_new":
		if len(args) <= 2 {
			printError(errors.New("Need 2 argument"))
			return false
		}
		filename := args[1]
		fi, err := client.Create(filename)
		if err != nil {
			printError(err)
			return false
		}
		_, err = fi.Write([]byte(args[2]))
		if err != nil {
			printError(err)
			return false
		}
		err = fi.Close()
		if err != nil {
			printError(err)
			return false
		}
		fmt.Println("success")
	case "touch":
		if len(args) <= 1 {
			printError(errors.New("Need argument"))
			return false
		}
		filename := args[1]
		fi, err := client.Create(filename)
		if err != nil {
			printError(err)
			return false
		}
		err = fi.Close()
		if err != nil {
			printError(err)
			return false
		}
		fmt.Println("success")
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
