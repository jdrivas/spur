package main 

import (
	"errors"
	"fmt"
	"strings"
)

var (
	commandMap = make(map[string]func(string, []string, *KinesisStream)(), 20)
)

func init() {
	commandMap["list"] = listCommand
}

func doICommand(line string, s *KinesisStream) (error) {

	line = strings.TrimRight(line,"\n")
	fields := strings.Fields(line)
	if len(fields) <= 0 { return nil }

	commandName := fields[0]
	command := commandMap[commandName]
	if command != nil {
		command(line, fields, s)
	} else {
		return errors.New(fmt.Sprintf("Unknown command: \"%s\"", commandName))
	}

	return nil
}

func listCommand(line string, args []string, s *KinesisStream) {
	fmt.Printf("Doing list command.\n")
}


