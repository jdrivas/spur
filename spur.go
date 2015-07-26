// spur.go

package main

import (
	"bufio"
	"os"
	"io"
	"log"
	"fmt"
	"strings"
	"path/filepath"
	"time"
	"gopkg.in/alecthomas/kingpin.v2"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/awsutil"
	"github.com/bobappleyard/readline"
 )

var (

	app = kingpin.New("spur","A command-line AWS Kinesis demo.")
	verbose bool
	region, stream, partition,shardID string
	iType string
	iteratorType = &iType

	// Prompt for Commands
	interactive *kingpin.CmdClause

	// Generate data.
	gen *kingpin.CmdClause
	genLog bool

	genFile *kingpin.CmdClause
	// fileName string
	file *os.File

	genItr *kingpin.CmdClause
	numberOfIterations int
	testString string	

	genPrompt *kingpin.CmdClause

	// Read data.
	read *kingpin.CmdClause
	showEmptyReads bool
	tail bool
	sleepMilli int

)

func init() {
	kingpin.New("spur", "A command-line AWS Kinesis application.")
	kingpin.Flag("verbose", "Describe what is happening, as it happens.").Short('v').BoolVar(&verbose)

	kingpin.Flag("region", "Find the kinsesis stream in this AWS region.").Default("us-west-1").StringVar(&region)
	kingpin.Flag("stream", "Use this Kinesis stream name").Default("JDR_TestStream_1").StringVar(&stream)
	kingpin.Flag("partition", "Identify as this Kinesis stream partition").Default("PARTITION").StringVar(&partition)
	// TODO: Accommodate the other two ShardIterator types (two flags?, Arg, Flag?)
	// ShardIteratorType
	// - "AT_SEQUENCE_NUMBER" start reading at a particular sequence numner
	// - "AFTER_SEQUENCE_NUMBER" start reading right after the position indicated by the sequence number.
	// - "TIME_HORIZON" start reading at the last untrimmed record (oldest).
	// - "LATEST"  start reading just after hte most recent record in the shard.
	kingpin.Flag("iterator-type", "Where to start reading the stream.").Default("LATEST").EnumVar(&iteratorType, "TRIM_HORIZON", "LATEST")


	interactive = kingpin.Command("interactive", "Prompt for commands.")

	gen = kingpin.Command("gen", "Put data into the Kinesis stream. This is inefficiently done a record at a time, no batching.")
	gen.Flag("log", "Generate a log style prefix for each message including the current time. Default on, use --no-log to turn it off.").BoolVar(&genLog)

	genFile = gen.Command("file", "Put data into the Kinesis stream from a file or stdin.")
	genFile.Arg("file-name", "Name of file for reading newline separeted records, each record is sent to the Kinesis stream.").OpenFileVar(&file, os.O_RDONLY, 0666)


	genItr = gen.Command("iterate", "Generate <iterations> log entires to the stream, using <test-string>.")
	genItr.Arg("iterations", "Number of test string entries to send to the kinesis stream.").Required().IntVar(&numberOfIterations)
	genItr.Arg("test-string", "String to send to the kinsesis stream.").Default("this is a test string").StringVar(&testString)

	genPrompt = gen.Command("prompt", "Prompt for strings to send to the kinesis stream.")

	read = kingpin.Command("read", "Read from a kinesis stream.")
	read.Flag("tail", "Continue waiting for records to read from the stream, will set latest unless -all specificed").Short('t').BoolVar(&tail)
	read.Flag("sleep", "Delay in milliseconds for sleep between polls in tail mode.").Default("500").IntVar(&sleepMilli)
	read.Flag("shard-id", "The Shard to read on the kinesis stream.").Default("shardId-000000000001").StringVar(&shardID)
	read.Flag("log-empty-reads", "Print out the empty reads and delay stats. This will happen with verbose as well.").BoolVar(&showEmptyReads)

	kingpin.CommandLine.Help = `A command-line AWS Kinesis application.
	Spur reads from the environment or ~/.aws/credentials for AWS credentials in the usual way. Unfortunately
	it doesn't read out the ~/.aws/configuration file for other informaiton (e.g. region).
	`
}

func main() {

	command := kingpin.Parse()

	// Flag messiness.
	shardIteratorType := *iteratorType

	if tail {
		// read = true;
		shardIteratorType = "LATEST"
	}

	// Say Hello
	if verbose {
		// fmt.Println(aws.DefaultConfig.Credentials.Get())
		fmt.Println("\nOpening up kinesies stream:", stream)
		fmt.Println("To partition:", partition)
		fmt.Println("In region:", region)
	}

	// The AWS library doesn't read configuariton information 
	// out of .aws/config, just the credentials from .aws/credentials.
	if aws.DefaultConfig.Region == "" {
		aws.DefaultConfig.Region = region
	}

	// Set up Kinesis.
	kinesisStream := NewStream(aws.DefaultConfig, stream, partition, shardIteratorType, shardID)

	// List of commands as parsed matched against functions to execute the commands.
	commandMap := map[string]func(*KinesisStream)(){
		interactive.FullCommand(): doInteractive,
		genFile.FullCommand(): doPutFile,
		genItr.FullCommand(): doIterate,
		genPrompt.FullCommand(): doPrompt,
		read.FullCommand(): doRead,
	}

	// Execute the command.
	commandMap[command](kinesisStream)

}

func doPutFile(s *KinesisStream) {

	if file == nil {
		if verbose {
			fmt.Println("Reading from stdin, and putting the lines into the Kinesis stream.")
		}
		file = os.Stdin
	} else {
		if verbose {
			// This is pretty close to disgusting.
			absolutePath, _ := filepath.Abs(filepath.Join(filepath.Dir(file.Name()), file.Name()))
			fmt.Printf("Putting file \"%s\" into the Kinesis stream.\n", absolutePath)
		}
		defer file.Close()
	}

	scanner := bufio.NewScanner(file)
	i := 0
	for scanner.Scan() {
		// resp, err := kPutLine(kStream, scanner.Text())
		resp, err := s.PutLogLine(scanner.Text())
		if err != nil {
			printAWSError(err)
		}
		if err = scanner.Err(); err != nil {
			log.Fatal(err)
		}
		if verbose {
			fmt.Printf("Put line %d\n", i)
			fmt.Printf("Resp: %s\n", resp)
		}
		i++
	}
}


// Generate data by iterating a test string out to the stream.
func doIterate(s *KinesisStream) {

	if verbose {
		fmt.Printf("Will push %v enties into the stream.\n", numberOfIterations)
		fmt.Printf("Using the string: %s\n", testString)
	}

	for i := 0; i < numberOfIterations; i++ {
		line := fmt.Sprintf("%s %d", testString, i)
		// resp, err := kPutLine(svc, line, partition, stream)
		resp, err := s.PutLogLine(line)
		if err != nil {
			log.Fatal(err)
		}

		if verbose {
			if i % 100 == 0 {
				fmt.Printf("%d iteration %s\n", i, resp)
			}
		}
	}
}

func doPrompt(s *KinesisStream) {

	moreToRead := true
	for moreToRead {
		line, err := readline.String("Send to Kinesis <crtl-d> to end: ")
		if(err == io.EOF) {
			moreToRead = false
		} else if err !=  nil {
			log.Fatal(err)
		} else {
			// eat the trailing new line, before you send it off to the stream.
			resp, err := s.PutLogLine(strings.TrimRight(line, "\n"))
			if err != nil {
				log.Fatal(err)
			}
			if verbose {
				fmt.Println("Response:", awsutil.StringValue(resp))
			}
			readline.AddHistory(line)
		}
	}
}


// Read string and print them fromt he stream.
func doRead(s *KinesisStream) {

	if verbose {
		fmt.Println("\nReading from shard: ", s.ShardID)
		fmt.Println("With iterator type:", s.ShardIteratorType)
	}

	var msecBehind int64 = 0
	var lastDelay int64 = 0
	emptyReads := 0

	s.ReadReset();
	for moreData := true; moreData; {

		output, err := s.GetRecords()
		if err != nil {
			printAWSError(err)
			log.Fatal(err)
		}

		// Keep reading until we're caught up or catchup and sleep if we're tailling.
		msecBehind = *output.MillisBehindLatest
		if msecBehind <= 0 {
			if tail {
				time.Sleep(time.Duration(sleepMilli) * time.Millisecond)
			} else {
				moreData = false
			}
		}

		// Share what you got.
		if showEmptyReads || verbose {
			if len(output.Records) > 0 {
				if (emptyReads != 0 ) {
					fmt.Println(emptyReads, "empty responses (no records) and delay is now:", fmtMilliseconds(msecBehind), "behind the tip of the stream.")
				}
				if(verbose) {
					fmt.Println("Got ", len(output.Records), " data records.")
					fmt.Println("The response is  ", fmtMilliseconds(msecBehind), " behind the tip of the stream.")
				}
				emptyReads = 0
			} else {
				if emptyReads == 0 {
					if lastDelay != msecBehind {
						lastDelay = msecBehind
						fmt.Printf("The response is %s behind the top of the stream.\n", fmtMilliseconds(msecBehind))
					}
				}
				emptyReads++
			}
		}

		for i, record := range output.Records {
			if verbose {
				fmt.Println("Data record: ", i + 1)
				fmt.Println("Partition: ", *record.PartitionKey)
				fmt.Println("SequenceNumber: ", *record.SequenceNumber)
				fmt.Printf("Data: ")
			}
			fmt.Println(string(record.Data))
		}
	}
}

func doInteractive(s *KinesisStream) {

	for moreCommands := true; moreCommands; {
		line, err := readline.String("&> ")
		if(err == io.EOF) {
			moreCommands = false
		} else if err !=  nil {
			log.Fatal(err)
		} else {
			doICommand(line, s)
			readline.AddHistory(line)
		}
	}
}

func doICommand(line string, s *KinesisStream) {
	// fmt.Printf("Doing command: \"%s\"\n", line)
	fields := strings.Fields(line)	
	switch command := fields[0]; command {
		case "list": {
			fmt.Printf("Doing list.\n")
		}
	}
}


func fmtMilliseconds(msec int64) (string) {
	hours := (msec / (1000*60*60)) % 24
	minutes := (msec / (1000*60)) % 60
	var seconds float32 = float32(msec - int64(hours * 1000*60*60) - int64(minutes *1000*60)) / 1000.0

	if hours > 0 {
		return fmt.Sprintf("%d hours %d minutes", hours, minutes)
	} else if minutes > 0 {
		return fmt.Sprintf("%d minutes and %g seconds", minutes, seconds)
	} else if seconds >= 1.0 {
		return fmt.Sprintf("%g seconds", seconds)
	} else {
		return fmt.Sprintf("%d milliseconds", msec)
	}
}

func printAWSError(err error) {
	awsErr, _ := err.(awserr.Error)
	fmt.Println("awsError:")
	fmt.Println(awsErr.Code(), awsErr.Message(), awsErr.OrigErr())
	if reqErr, ok := err.(awserr.RequestFailure); ok {
		fmt.Println("reqErr:")
		fmt.Println(reqErr.Code(), reqErr.Message(), reqErr.StatusCode(), reqErr.RequestID())
	}
}
