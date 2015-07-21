// spur.go

package main

import (
	"bufio"
	"os"
	"io"
	"log"
	"fmt"
	"strings"
	"time"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/awsutil"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"gopkg.in/alecthomas/kingpin.v2"
 )

var (

	app = kingpin.New("spur","A command-line AWS Kinesis demo.")
	verbose bool
	region, stream, partition,shardID string
	iType string
	iteratorType = &iType

	// Generate data.
	gen *kingpin.CmdClause
	genLog bool

	genItr *kingpin.CmdClause
	numberOfIterations int
	testString string	

	genPrompt *kingpin.CmdClause

	// Read data.
	read *kingpin.CmdClause
	showEmptyReads bool
	tail bool
	
)

func init() {
	kingpin.New("spur", "A command-line AWS Kinesis application.")
	kingpin.Flag("verbose", "Describe what is happening, as it happens.").Short('v').BoolVar(&verbose)

	kingpin.Flag("region", "Find the kinsesis stream in this AWS region.").Default("us-west-1").StringVar(&region)
	kingpin.Flag("stream", "Use this Kinesis stream name").Default("JDR_TestStream_1").StringVar(&stream)
	kingpin.Flag("partition", "Identify as this Kinesis stream partition").Default("PARTITION").StringVar(&partition)

	gen = kingpin.Command("gen", "Put data into the Kinesis stream.")
	gen.Flag("log", "Generate a log style prefix for each message including the current time. Default on, use --no-log to turn it off.").BoolVar(&genLog)

	genItr = gen.Command("iterate", "Generate <iterations> log entires to the stream, using <test-string>.")
	genItr.Arg("iterations", "Number of test string entries to send to the kinesis stream.").Required().IntVar(&numberOfIterations)
	genItr.Arg("test-string", "String to send to the kinsesis stream.").Default("this is a test string").StringVar(&testString)

	genPrompt = gen.Command("prompt", "Prompt for strings to send to the kinesis stream.")

	read = kingpin.Command("read", "Read from a kinesis stream.")
	read.Flag("tail", "Continue waiting for records to read from the stream, will set latest unless -all specificed").Short('t').BoolVar(&tail)
	read.Flag("shard-id", "The Shard to read on the kinesis stream.").Default("shardId-000000000001").StringVar(&shardID)
	// TODO: Accommodate the other two ShardIterator types (two flags?, Arg, Flag?)
	// ShardIteratorType
	// - "AT_SEQUENCE_NUMBER" start reading at a particular sequence numner
	// - "AFTER_SEQUENCE_NUMBER" start reading right after the position indicated by the sequence number.
	// - "TIME_HORIZONG" start reading at the last untrimmed record (oldest).
	// - "LATEST"  start reading just after hte most recent record in the shard.
	read.Flag("iterator-type", "Where to start reading the stream.").Default("LATEST").EnumVar(&iteratorType, "TRIM_HORIZON", "LATEST")
	read.Flag("log-empty-reads", "Print out the empty reads and delay stats. This will happen with verbose as well.").BoolVar(&showEmptyReads)

	kingpin.CommandLine.Help = "A command-line AWS Kinesis applicaiton."
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
	// out of .aws/config, just the credentials.
	if aws.DefaultConfig.Region == "" {
		aws.DefaultConfig.Region = region
	}

	// Set up Kinesis.
	kinesis_svc := kinesis.New(aws.DefaultConfig)

	// Do something.
	switch command {

		case genItr.FullCommand(): {
			doIterate(kinesis_svc)
		}

		case genPrompt.FullCommand(): {
			doPrompt(kinesis_svc)
		}

		case read.FullCommand(): {
			doRead(kinesis_svc, shardIteratorType)
		}

	}
}

// TODO: refactor into two objects/files read and generate, plus a util file.

// Generate data by iterating a test string out to the stream.
func doIterate(svc *kinesis.Kinesis) {

	if verbose {
		fmt.Printf("Will push %v enties into the stream.\n", numberOfIterations)
		fmt.Printf("Using the string: %s\n", testString)
	}

	for i := 0; i < numberOfIterations; i++ {
		line := fmt.Sprintf("%s %d", testString, i)
		resp, err := kPutLine(svc, line, partition, stream)
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


// Prompt for strings to send to the Kinesis stream.
func doPrompt(svc *kinesis.Kinesis) {

	// TODO: add in a readline library here.
	reader := bufio.NewReader(os.Stdin)
	moreToRead := true
	for moreToRead {
		fmt.Printf("Text to send <ctrl-D> to end > ")
		line, err := reader.ReadString('\n')
		if(err == io.EOF) {
			moreToRead = false
		} else if err !=  nil {
			log.Fatal(err)
		} else {
			// eat the trailing new line, before you send it off to the stream.
			resp, err := kPutLine(svc, strings.TrimRight(line, "\n"), partition, stream)
			if err != nil {
				log.Fatal(err)
			}
			if verbose {
				fmt.Println("Response:", awsutil.StringValue(resp))
			}
		}
	}
}

func doRead(svc *kinesis.Kinesis, shardIteratorType string) {

	if verbose {
		fmt.Println("\nReading from shard: ", shardID)
		fmt.Println("With iterator type:", shardIteratorType)
	}

	// The read records funciton needs a shardIterator to determine
	// which records to read. This first one can either be told
	// to start with the oldest available (TRIM_HORIZON), the latest
	// LATEST, or relative to an actual point AT_SEQUENCE_NUMBER", "AFTER_SEQUENCE_NUMBER"
	// Have to add more UI to get the sequence AT/AFTER Sequence number adeed in.
	//
	// Each read will generate a next iterator to use.
	// siOutput := getFirstSharedIterator(svc, shardIteratorType)
	// shardIteratorName := siOutput.ShardIterator
	var msecBehind int64 = 0
	var lastDelay int64 = 0
	emptyReads := 0
	shardIteratorName := getFirstSharedIterator(svc, shardIteratorType).ShardIterator
	for moreData := true; moreData; {

		output := getRecords(svc, shardIteratorName)
		shardIteratorName = output.NextShardIterator

		// Keep reading until we're caught up or catchup and sleep if we're tailling.
		msecBehind = *output.MillisBehindLatest
		if msecBehind <= 0 {
			if tail {
				time.Sleep(500 * time.Millisecond)
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

func getRecords(svc *kinesis.Kinesis, shardIteratorName *string) (output *kinesis.GetRecordsOutput) {
	gr_params := &kinesis.GetRecordsInput {
		ShardIterator: aws.String(*shardIteratorName),
		// Limit: aws.Long(1),
	}
	output, err := svc.GetRecords(gr_params)

	if err != nil {
		printAWSError(err)
		log.Fatal(err)
	}

	return output
}

func getFirstSharedIterator(svc *kinesis.Kinesis, shardIteratorType string) (*kinesis.GetShardIteratorOutput) {
	// Get the first shard iterator
	siParams := &kinesis.GetShardIteratorInput {
		ShardID: aws.String(shardID),
		ShardIteratorType: aws.String(shardIteratorType),
		StreamName: aws.String(stream),
		// StartingSequenceNumber:  aws.String(startingSequenceNumber)
	}
	siOutput, err := svc.GetShardIterator(siParams)

	if err != nil {
		printAWSError(err)
		log.Fatal("Couldn't get the ShardIterator for shard: ", shardID)
	}

	return siOutput
}
 
func kPutLine(svc *kinesis.Kinesis,  line, partition, stream string) (resp *kinesis.PutRecordOutput, err error) {

	// Add log data to the line we've been given 
	logString := fmt.Sprintf("[ %s ] %s", time.Now().UTC().Format(time.RFC1123Z), line)
	logData := []byte(logString)

	record := &kinesis.PutRecordInput {
		Data: logData,
		PartitionKey: aws.String(partition),
		StreamName: aws.String(stream),
		// ExplicitHashKey
		// SequenceNUmberForORdering
	}
	
	resp, err = svc.PutRecord(record)

	if err != nil {
		printAWSError(err)
	}

	return resp, err
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
