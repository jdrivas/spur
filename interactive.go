package main

import (
	// "errors"
	"fmt"
	// "strconv"
	"strings"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	interApp *kingpin.Application

	interIterate *kingpin.CmdClause
	iterateCount int

	interList *kingpin.CmdClause

	interCreate *kingpin.CmdClause
	interStreamName string

	interDelete *kingpin.CmdClause
)

func init() {
	interApp = kingpin.New("", "Spur interactive mode.")
	interIterate = interApp.Command("iterate", "Push a number of log entires to the Kinesis stream.")
	interIterate.Arg("numberOfIterations", "Number of log entires to push to the Kinesis stream").Required().IntVar(&iterateCount)

	interList = interApp.Command("list", "List the available Kinesis streams.")

	interCreate = interApp.Command("create", "Create a new Kinesis stream.")
	interCreate.Arg("stream", "Name of Kinesis stream to create").Required().StringVar(&interStreamName)
	interDelete = interApp.Command("delete", "Delete a specific Kinesis stream.")
	interDelete.Arg("stream", "Name of Kinesis stream to delete").Required().StringVar(&interStreamName)

}

const (
	defaultIterations = 10
	defaultTestString = "Testing the stream."
)


func doICommand(line string, g *KinesisStreamGroup) (err error) {
	line = strings.TrimRight(line, "\n")
	fields := []string{}
	fields = append(fields, strings.Fields(line)...)
	if len(fields) <= 0 {
		return nil
	}

	command, err := interApp.Parse(fields)
	if err != nil {
		fmt.Printf("Parse error: %s\n", err)
		return nil
	} else {
		switch command {
			case interList.FullCommand(): err = doListStreams(g)
			case interDelete.FullCommand(): err = doDeleteStream(g)
			case interCreate.FullCommand(): err = doCreateStream(g)
			case interIterate.FullCommand(): err = doIterateWrite(g)
			default: fmt.Printf("(Shoudn't get here) - Unknown Command: %s\n", command)
		}
	}

	return err

}

func doCreateStream(g *KinesisStreamGroup) (err error) {
	shards := int64(2)
	err = g.CreateKinesisStream(interStreamName, shards)
	if err == nil {
		fmt.Printf("Created stream: %s with %d shards.\n", interStreamName, shards)
	}
	return err
}

func doDeleteStream(g *KinesisStreamGroup) (error) {

	// name := args[1]
	err := g.DeleteKinesisStream(interStreamName)
	if err == nil {
		fmt.Printf("Deleted sttream: %s.\n", interStreamName)
	}
	return err
}

func doListStreams(g *KinesisStreamGroup) (error) {

	streams, err := g.ListStreams()
	if err == nil {
		for i, description := range streams {
			fmt.Printf("%d. %s\n", i+1, description.Name)
			sd := description.Description
			fmt.Printf("%20s %s\n", "Stream Status:", *sd.StreamStatus)
			fmt.Printf("%20s %s\n", "ARN:", *sd.StreamARN)
			fmt.Printf("%20s %d\n", "Number of Shards:", len(sd.Shards))
			fmt.Printf("%20s [", "ShardNames:")
			for j, shard := range sd.Shards {
				fmt.Printf("%s", *shard.ShardID)
				if j < (len(sd.Shards) - 1) {
					fmt.Printf(", ")
				}
				// Put three shardIDs to a line, then 'tab' over
				if ((j + 1) % 3) == 0 {
					fmt.Printf("%20s ", "")
				}
			}
			fmt.Printf("]\n")
		}
	}
	return err
}

// func iterateCommand(line string, args []string, g *KinesisStreamGroup) (err error) {
func doIterateWrite(g *KinesisStreamGroup) (err error) {

	testString := defaultTestString
	for i := 0; i < iterateCount; i++ {
		line := fmt.Sprintf("%s: %d", testString, i)
		_, err := g.CurrentStream.PutLogLine(line)
		if err != nil {
			return err
		}
	}

	return nil
}
