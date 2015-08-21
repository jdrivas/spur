package main
// TODO: pull this out as it's own package.

import (
  "fmt"
  "log"
  "strings"
  "time"
  "io"
  "gopkg.in/alecthomas/kingpin.v2"
)

var (
  interApp     *kingpin.Application

  interExit *kingpin.CmdClause
  interQuit *kingpin.CmdClause
  interVerbose *kingpin.CmdClause
  iVerbose bool

  interIterate *kingpin.CmdClause
  iterateCount int
  interTestString []string

  interPrompt *kingpin.CmdClause

  interRead *kingpin.CmdClause
  interTailCmd *kingpin.CmdClause
  interReadType *string
  interTail bool

  interShow *kingpin.CmdClause
  interUse *kingpin.CmdClause
  interList   *kingpin.CmdClause
  interListType *string
  interCreate *kingpin.CmdClause
  interDelete *kingpin.CmdClause
  interStreamName string

)

func init() {

  interApp = kingpin.New("", "Spur interactive mode.").Terminate(doTerminate)

  // state 
  interVerbose = interApp.Command("verbose", "toggle verbose mode")
  interExit = interApp.Command("exit", "exit the program. <ctrl-D> works too.")
  interQuit = interApp.Command("quit", "exit the program.")

  // Write to stream
  interIterate = interApp.Command("iterate", "Push a number of log entires to the Kinesis stream.")
  interIterate.Arg("numberOfIterations", "Number of log entires to push to the Kinesis stream.").Required().IntVar(&iterateCount)
  interIterate.Arg("test-string", "string to use in the log message.").Default("Testing the stream.").StringsVar(&interTestString)

  interPrompt = interApp.Command("prompt", "Prompt for lines of text to log to stream.")

  // Read from streams
  interRead = interApp.Command("read", "Read from the stream.")
  interReadType = interRead.Arg("read type", "How to read from the stream <latest|all|tail>.").Required().Enum("latest", "all", "tail")


  // Manage streams
  interList = interApp.Command("list", "List the available Kinesis streams.")
  interListType = interList.Arg("type", "List all the arguments. ").Required().Enum("aws", "group")
  interCreate = interApp.Command("create", "Create a new Kinesis stream.")
  interCreate.Arg("stream", "Name of Kinesis stream to create").Required().StringVar(&interStreamName)
  interDelete = interApp.Command("delete", "Delete a specific Kinesis stream.")
  interDelete.Arg("stream", "Name of Kinesis stream to delete").Required().StringVar(&interStreamName)

  // Manage current stream
  interShow = interApp.Command("show", "Display details of the current Kinesis Stream.")
  interUse = interApp.Command("use", "Set the named stream as the current Kinesis Stream for future commands.")
  interUse.Arg("stream", "Name of KinesisStream to use.").Required().StringVar(&interStreamName)
}


func DoICommand(line string, g *KinesisStreamGroup) (err error) {

  // This is due to a 'peculiarity' kingpin, it collects strings as arguments across parses.
  interTestString = []string{}

  // Prepare the line for parsing.
  line = strings.TrimRight(line, "\n")
  fields := []string{}
  fields = append(fields, strings.Fields(line)...)
  if len(fields) <= 0 {                                                                                                                                                                                                                                                                                       
    return nil
  }

  command, err := interApp.Parse(fields)
  if err != nil {
    fmt.Printf("Command error: %s.\n", err)
    return nil
  } else {
    switch command {
      case interList.FullCommand(): err = doListStreams(g)
      case interDelete.FullCommand(): err = doDeleteStream(g)
      case interCreate.FullCommand(): err = doCreateStream(g)
      case interIterate.FullCommand(): err = doIterateWrite(g)
      case interPrompt.FullCommand(): err = doPromptWrite(g)
      case interRead.FullCommand(): err = doReadStream(g)
      case interShow.FullCommand(): err = doShowStream(g)
      case interUse.FullCommand(): err = doUseStream(g)
      case interVerbose.FullCommand(): err = doVerbose()
      case interExit.FullCommand(): err = doQuit()
      case interQuit.FullCommand(): err = doQuit() 
      case "help": // do nothing in the case we fall through to help.
      default: fmt.Printf("(Shoudn't get here) - Unknown Command: %s\n", command)
    }
  }
  return err
}

func toggleVerbose() (bool) {
  iVerbose = !iVerbose
  return iVerbose
}

func doVerbose() (error){
  if toggleVerbose() {
    fmt.Println("Verbose is on.")
  } else {
    fmt.Println("Verbose is off.")    
  }
  return nil
}

func doQuit() (error) {
  return io.EOF
}


func doCreateStream(g *KinesisStreamGroup) (err error) {
  shards := int64(2)
  stream, err := g.CreateKinesisStream(interStreamName, shards)
  if err == nil {
    stream.WaitForStateChange(10, 1, "CREATING", func(stateName string, err error) {
      if err == nil {
        fmt.Printf("\nStream %s is now in the %s state.\n", stream.Name, stateName)
      }
      })
    fmt.Printf("Created stream: %s with %d shards.\n", stream.Name, shards)
  }
  return err
}

func doDeleteStream(g *KinesisStreamGroup) (error) {
  stream, err := g.DeleteKinesisStream(interStreamName)
  if err == nil {
    stream.WaitForStateChange(10, 1, "DELETING", func(stateName string, err error) {
      if err == nil {
        if stateName == "" {
          stateName = "DELETED"
        }
        fmt.Printf("\nStream %s, is now in the %s state.\n", stream.Name, stateName)
      }
    })
    fmt.Printf("Deleted sttream: %s.\n", interStreamName)
  }
  return err
}

func doListStreams(g *KinesisStreamGroup) (err error) {

  if *interListType == "aws" {
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
      }
    }
  } else {
    fmt.Printf("The local group is: %s.\n", g.Description())
  }
  return err
}

func doShowStream(g *KinesisStreamGroup) (err error) {
  fmt.Printf("Current stream is: \n%s", g.CurrentStream.Description())
  return nil
}

func doUseStream(g *KinesisStreamGroup) (err error) {
  err = g.SetCurrentStream(interStreamName)
  if err != nil {
    fmt.Printf("Error: %s\n", err)
  }
  fmt.Printf("Now using %s.\n", g.CurrentStream.Name)
  return nil
}

func doPromptWrite(g *KinesisStreamGroup) (err error) {
  xPRCommand := func(line string) (err error) { 
    _, e := g.CurrentStream.PutLogLine(line)
    return e
  }

  prompt := g.CurrentStream.Name + "(write) >"
  err = promptLoop(prompt, xPRCommand)
  fmt.Println("")
  return err
}

func doIterateWrite(g *KinesisStreamGroup) (err error) {
  testString := ""
  for i := range interTestString {
    testString += " " + interTestString[i]
  }
  if iVerbose {
    fmt.Printf("Using \"%s\" as the test string for %d iterations.\n", testString, iterateCount)
  }

  for i := 0; i < iterateCount; i++ {
    line := fmt.Sprintf("%s: %d", testString, i)
    _, err := g.CurrentStream.PutLogLine(line)
    if err != nil {
      return err
    }
  }
  return nil
}

func doReadStreamTail(g *KinesisStreamGroup) (err error) {
  interTail = true
  g.CurrentStream.ShardIteratorType = "LATEST"
  return doReadStream(g)
}

func configureFromReadType(readType string) (shardIteeator string, tail bool) {
  switch readType {
    case "tail": return "LATEST", true
    case "latest": return "LATEST", false
    case "all": return "TRIM_HORIZON", false
    default: log.Fatal(fmt.Sprintf("Unkonown ReadType: %s", readType))
  }
  return "", false
}

func doReadStream(g *KinesisStreamGroup) (err error) {

  const sleepMilli = 500
  var lastDelay, msecBehind int64 = 0,0
  s := g.CurrentStream
  s.ShardIteratorType, interTail = configureFromReadType(*interReadType)
  
  if iVerbose {
    fmt.Printf("Reading from shard: %s\n", s.ShardID)
    fmt.Printf("With iterator type: %s\n", s.ShardIteratorType)
  }

  emptyReads := 0
  s.ReadReset()
  for moreData := true; moreData; {

    output, err := s.GetRecords()
    if err != nil {
      printAWSError(err)
      return err
    }

    // Read until we're caught up, or sleep if we're tailing.
    // TODO: Tail needs fixing. Currently, we can only control-c out of it
    // need to set something up that allows us to tail and listen
    // and so stop taliing with a control-d.
    msecBehind = *output.MillisBehindLatest
    if msecBehind == 0 {
      if interTail {
        time.Sleep(time.Duration(sleepMilli) * time.Millisecond)
      } else {
        moreData = false
      }
    }

    // Count empty reads, and only report on them when we finally get data.
    if len(output.Records) > 0 {
      if iVerbose {
        if emptyReads != 0 {
          fmt.Printf("%d empty responses (no records).")
        } 
        fmt.Printf("Got %d data records\n", len(output.Records))
      }
      emptyReads = 0 
    } else {
      emptyReads++
    }

    // Report if the lag from end of stream has changed.
    if iVerbose && (lastDelay != msecBehind) {
      lastDelay = msecBehind 
      if msecBehind == 0 {
        fmt.Printf("We are now at the top of the stream.\n")
      } else {
        fmt.Printf("The response is now %s behind the top of stream.\n", fmtMilliseconds(msecBehind))
      }
    }

    for i, record := range output.Records {
      if iVerbose {
        fmt.Printf("Data record: %d\n", i+1)
        fmt.Printf("Parittion: %s\n", *record.PartitionKey)
        fmt.Printf("Sequence number: %s\n", *record.SequenceNumber)
        fmt.Printf("Data: ")
      }
      fmt.Println(string(record.Data))
      if iVerbose {fmt.Println()}
    }
  }

  return nil
}

// This is used to catch the termiation on help
// that is the default kingpin behavior.
func doTerminate(i int) {
}
