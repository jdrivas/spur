// spur.go

package main

import (
	"bufio"
	"os"
	"io"
	"log"
	// "flag"
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

	// TODO think about setting this up as a command like UI with "generate", "prompt", "read" as commands.

	app = kingpin.New("spur","A command-line AWS Kinesis applicaiton.")
	verbose bool
	region, stream, partition,shardID string
	iType string
	iteratorType = &iType

	// Put data into the stream.
	gen *kingpin.CmdClause
	numberOfIterations int
	testString string

	prompt *kingpin.CmdClause

	read *kingpin.CmdClause
	tail bool

	// region = kingpin.Flag("region", "find the kinsesis stream in this AWS region.").Default("us-west-1").String()
	// stream = kingpin.Flag("stream", "use this Kinesis stream name").Default("JDR_TestStream_1").String()
	// partition = kingpin.Flag("partition", "identify as this Kinesis stream partition").Default("PARTITION").String()
	// shardID = kingpin.Flag("shard-id", "the Shard to read on the kinesis stream.").Default("shardId-000000000001").String()
	// iteratorType = kingpin.Flag("iterator-type", "where to read the stream from").Default("LATEST").Enum("TRIM_HORIZON", "LATEST")

	// latest = kingpin.Flag("latest", "start reading just after the most recent record in the stream").Short('l').Default("true").Bool()
	// all = kingpin.Flag("all", "read from the last untrimmed record (oldest).").Default("false").Bool()

	// kingpin.Flag("iterations", "number of test string entries to send to the kinesis stream.").Default("0").IntVar(&numberOfIterations)
	// kingpin.Flag("test-string", "string to send to the kinsesis stream.").Default("this is a test string").StringVar(&testString)
	// prompt = kingpin.Flag("prompt", "prompt for strings to send to the kinesis string.").Default("true").Bool()

	// read = kingpin.Flag("read", "read from a kinesis stream. Will read after writing, if any.").Default("false").Bool()
	// tail = kingpin.Flag("tail", "Continue waiting for records to read from the stream, will set latest unless -all specificed").Short('t').Default("false").Bool()

)

func init() {
	kingpin.New("spur", "A command-line AWS Kinesis application.")
	kingpin.Flag("verbose", "Describe what is going to happen, as it happens.").Short('v').BoolVar(&verbose)

	kingpin.Flag("region", "find the kinsesis stream in this AWS region.").Default("us-west-1").StringVar(&region)
	kingpin.Flag("stream", "use this Kinesis stream name").Default("JDR_TestStream_1").StringVar(&stream)
	kingpin.Flag("partition", "identify as this Kinesis stream partition").Default("PARTITION").StringVar(&partition)

	gen = kingpin.Command("gen", "generate <iterattions> log entries to the stream, using <test-string>.")
	gen.Arg("iterations", "number of test string entries to send to the kinesis stream.").Required().IntVar(&numberOfIterations)
	gen.Arg("test-string", "string to send to the kinsesis stream.").Default("this is a test string").StringVar(&testString)

	prompt = kingpin.Command("prompt", "prompt for strings to send to the kinesis string.")

	read = kingpin.Command("read", "read from a kinesis stream.")
	read.Flag("tail", "Continue waiting for records to read from the stream, will set latest unless -all specificed").Short('t').BoolVar(&tail)
	read.Flag("shard-id", "the Shard to read on the kinesis stream.").Default("shardId-000000000001").StringVar(&shardID)
	read.Flag("iterator-type", "where to read the stream from").Default("LATEST").EnumVar(&iteratorType, "TRIM_HORIZON", "LATEST")

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

	// Set up Kinesis.
	if aws.DefaultConfig.Region == "" {
		aws.DefaultConfig.Region = region
	}
	kinesis_svc := kinesis.New(aws.DefaultConfig)

	// do something.
	switch command {

		case gen.FullCommand(): {
			doGen(kinesis_svc)
		}

		case prompt.FullCommand(): {
			doPrompt(kinesis_svc)
		}

		case read.FullCommand(): {
			doRead(kinesis_svc, shardIteratorType)
		}

	}

	// TODO: Build this out into a command line setting.
	// ShardIteratorType
	// - "AT_SEQUENCE_NUMBER" start reading at a particular sequence numner
	// - "AFTER_SEQUENCE_NUMBER" start reading right after the position indicated by the sequence number.
	// - "TIME_HORIZONG" start reading at the last untrimmed record (oldest).
	// - "LATEST"  start reading just after hte most recent record in the shard.
	// Figure out where in the stream to read from.
	// shardIteratorType := "TRIM_HORIZON"
	// shardIteratorType := iteratorType

	// This map is  because searching isn't built into array.
	// flagStatus := make(map[string]bool)
	// mark the variables set on the command line.
	// flag.Visit(func(f *flag.Flag) {
	// 	flagStatus[f.Name] = true
	// })




	// Write a bunch of strings to Kinesis
	// TODO: Consider putting JSON onto the stream, instead of raw text.
	// {
	//		"logtag": "[ Thu, 16 JUL 2015 17:08:03 +0000 ]"
	//    "localSequence": "10"	// An integer that is a local sequence number.
	// }


	// TODO: add support for github.com/peterh/liner or 
	// a readline equivelant.
	// Read from STDIN and lines to Kinesis stream
	// if prompt {
	// 	reader := bufio.NewReader(os.Stdin)
	// 	moreToRead := true
	// 	for moreToRead {
	// 		fmt.Printf("Text to send <ctrl-D> to end > ")
	// 		line, err := reader.ReadString('\n')
	// 		if(err == io.EOF) {
	// 			moreToRead = false
	// 		} else if err !=  nil {
	// 			log.Fatal(err)
	// 		} else {
	// 			// eat the trailing new line, before you send it off to the stream.
	// 			resp, err := kPutLine(kinesis_svc, strings.TrimRight(line, "\n"), partition, stream)
	// 			if err != nil {
	// 				log.Fatal(err)
	// 			}
	// 			if verbose {
	// 				fmt.Println("Response:", awsutil.StringValue(resp))
	// 			}
	// 		}
	// 	}
	// }

	// Read records from the stream
	// if read {
	// 	if verbose {
	// 		fmt.Println("\nReading from shard: ", shardID)
	// 		fmt.Println("With iterator type:", shardIteratorType)
	// 	}

	// 	// Get the first shard iterator
	// 	siParams := &kinesis.GetShardIteratorInput {
	// 		ShardID: aws.String(shardID),
	// 		ShardIteratorType: aws.String(shardIteratorType),
	// 		StreamName: aws.String(stream),
	// 		// StartingSequenceNumber:  aws.String(startingSequenceNumber)
	// 	}
	// 	siOutput, err := kinesis_svc.GetShardIterator(siParams)
	// 	if err != nil {
	// 		printAWSError(err)
	// 		log.Fatal("Couldn't get the ShardIterator for shard: ", shardID)
	// 	}

	// 	// Get data from the stream and print them to stdout.
	// 	shardIteratorName := siOutput.ShardIterator
	// 	for moreData := true; moreData; {
	// 		gr_params := &kinesis.GetRecordsInput {
	// 			ShardIterator: aws.String(*shardIteratorName),
	// 			// Limit: aws.Long(1),
	// 		}
	// 		output, err := kinesis_svc.GetRecords(gr_params)
	// 		if err != nil {
	// 			printAWSError(err)
	// 			log.Fatal(err)
	// 		}
	// 		shardIteratorName = output.NextShardIterator

	// 		// Keep reading until we're caught up or sleep if we're tailling.
	// 		msecBehind := *output.MillisBehindLatest
	// 		if msecBehind <= 0 {
	// 			if tail {
	// 					time.Sleep(500 * time.Millisecond)
	// 				} else {
	// 					moreData = false
	// 				}
	// 		}

	// 		// Share what you got.
	// 		if verbose {
	// 			fmt.Println("Got ", len(output.Records), " data records.")
	// 			fmt.Println("The response is  ", fmtMilliseconds(msecBehind), " behind the tip of the stream.")
	// 		}
	// 		for i, record := range output.Records {
	// 			if verbose {
	// 				fmt.Println("Data record: ", i + 1)
	// 				fmt.Println("Partition: ", *record.PartitionKey)
	// 				fmt.Println("SequenceNumber: ", *record.SequenceNumber)
	// 				fmt.Printf("Data: ")
	// 			}
	// 			fmt.Println(string(record.Data))
	// 		}
	// 	}
	// }
}

func doGen(svc *kinesis.Kinesis) {
	if verbose {
		fmt.Println("Will push %i enties into the stream.", numberOfIterations)
		fmt.Println("Using the string: %s", testString)
	}
	for i := 0; i < numberOfIterations; i++ {
		line := fmt.Sprintf("%s %d", testString, i)
		resp, err := kPutLine(svc, line, partition, stream)
		if err != nil {
			log.Fatal(err)
		}

		if verbose {
			if i % 100 == 0 {
				fmt.Printf("%i iteration\n%s", i, resp)
			}
		}
	}
}

func doRead(svc *kinesis.Kinesis, shardIteratorType string) {
	if verbose {
		fmt.Println("\nReading from shard: ", shardID)
		fmt.Println("With iterator type:", shardIteratorType)
	}

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

	// Get data from the stream and print them to stdout.
	shardIteratorName := siOutput.ShardIterator
	for moreData := true; moreData; {
		gr_params := &kinesis.GetRecordsInput {
			ShardIterator: aws.String(*shardIteratorName),
			// Limit: aws.Long(1),
		}
		output, err := svc.GetRecords(gr_params)
		if err != nil {
			printAWSError(err)
			log.Fatal(err)
		}
		shardIteratorName = output.NextShardIterator

		// Keep reading until we're caught up or sleep if we're tailling.
		msecBehind := *output.MillisBehindLatest
		if msecBehind <= 0 {
			if tail {
					time.Sleep(500 * time.Millisecond)
				} else {
					moreData = false
				}
		}

		// Share what you got.
		if verbose {
			fmt.Println("Got ", len(output.Records), " data records.")
			fmt.Println("The response is  ", fmtMilliseconds(msecBehind), " behind the tip of the stream.")
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

func doPrompt(svc *kinesis.Kinesis) {
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

func fmtMilliseconds(msec int64) (string) {
	return fmt.Sprintf("%d msec", msec)
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
		awsErr, _ := err.(awserr.Error)
		fmt.Println("awsError:")
		fmt.Println(awsErr.Code(), awsErr.Message(), awsErr.OrigErr())
		if reqErr, ok := err.(awserr.RequestFailure); ok {
			fmt.Println("reqErr:")
			fmt.Println(reqErr.Code(), reqErr.Message(), reqErr.StatusCode(), reqErr.RequestID())
		}
	}

	return resp, err
}


