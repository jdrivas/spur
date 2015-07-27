package main 

import (
	"errors"
	"fmt"
	"strings"
	"strconv"
)

type ICommandFunc func(string, []string, *KinesisStream)(error)

type ICommand struct {
	Name string
	HelpText string
	Func ICommandFunc
}

var (
	commandMap = make(map[string]*ICommand, 20)
)

const (
	defaultIterations = 10
	defaultTestString = "This is a test"
)


func init() {
	newICommand("list", "list,\t list all streams.", listCommand)
	newICommand("iterate", "iterate [<iterations>=10] [<test-string>=\"this is a test\"]", iterateCommand)
}

func newICommand(name string, help string, icf ICommandFunc) (*ICommand){
	ic := &ICommand{name, help, icf}
	commandMap[name] = ic
	return ic
}

func doICommand(line string, s *KinesisStream) (err error) {

	line = strings.TrimRight(line,"\n")
	fields := strings.Fields(line)
	if len(fields) <= 0 { return nil }

	commandName := fields[0]
	command := commandMap[commandName]
	if command != nil {
		err = command.Func(line, fields, s)
	} else {
		err = errors.New(fmt.Sprintf("Unknown command: \"%s\"", commandName))
	}

	return err
}


// Commands

func listCommand(line string, args []string, s *KinesisStream) (error){
	streams, err := s.ListStreams()
	if err != nil {
		return err
	}
	for i, name := range streams {
		fmt.Printf("%d. \"%s\"\n", i, name)
	}
	return nil
}

func iterateCommand(line string, args []string, s *KinesisStream) (err error){
	var numberOfIterations int
	if len(args) > 1 {
		numberOfIterations, err = strconv.Atoi(args[1])
		if err != nil {
			return errors.New(fmt.Sprintf("Bad iterator argument: \"%s\"", args[1]))
		}
	} else {
		numberOfIterations = defaultIterations
	}

	testString := defaultTestString
	for i:= 0; i < numberOfIterations; i++ {
		line := fmt.Sprintf("%s: %d", testString, i)
		_, err := s.PutLogLine(line)
		if err != nil {return err}
	}

	return nil
}

