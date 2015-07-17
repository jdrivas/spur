// spur.go

package main

import (
	"bufio"
	"os"
	"io"
	"log"
	"flag"
	"fmt"
	"strings"
	"time"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/awsutil"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

func main() {

	//
	// Configure and Init
	var verbose bool
	flag.BoolVar(&verbose, "verbose", false, "describe what is going on as it happens.")

	// AWS and Kinesis configuration
	var region string
	flag.StringVar(&region, "region", "us-west-1", "find the kinesis stream in this AWS region.")

	var stream string
	flag.StringVar(&stream, "stream", "JDR_TestStream_1", "use this Kinesis stream name.")

	// TODO: set up a read partition that filters out all but messages for the names partitions.
	var partition string
	flag.StringVar(&partition, "partition", "PARTITION", "idenitfy as this Kineses stream partiion.")

	var shardID string 
	flag.StringVar(&shardID, "shard-id", "shardId-000000000000", "the Shard to read on the Kinesis stream.")


	var latest bool
	flag.BoolVar(&latest, "latest", true, "start reading just after the most recent record in the stream." )

	var all bool
	flag.BoolVar(&all, "all", false, "read from the last untrimmed record in the stream (oldest).")


	// Write functions
	var numberOfIterations int 
	flag.IntVar(&numberOfIterations, "iterations", 0, "number of test string entries to send to the kinesis stream.")

	var testString string
	flag.StringVar(&testString, "test-string", "This is a test record", "string to send to the kinesis stream.")


	var prompt bool
	flag.BoolVar(&prompt, "prompt", true, "Prompt for strings to send to the kinesis stream, can't iterate and prompt.")

	// Read functions
	var read bool
	flag.BoolVar(&read, "read", false, "Read from the kinesis stream. Will read after writing if any.")

	var tail bool
	flag.BoolVar(&tail, "tail", false, "Continue waiting for records to read from the stream, will set -latest unless -all specified.")

	flag.Parse();

	// TODO: Build this out into a command line setting.
	// ShardIteratorType
	// - "AT_SEQUENCE_NUMBER" start reading at a particular sequence numner
	// - "AFTER_SEQUENCE_NUMBER" start reading right after the position indicated by the sequence number.
	// - "TIME_HORIZONG" start reading at the last untrimmed record (oldest).
	// - "LATEST"  start reading just after hte most recent record in the shard.
	// Figure out where in the stream to read from.
	shardIteratorType := "TRIM_HORIZON"

	// This map is  because searching isn't built into array.
	flagStatus := make(map[string]bool)
	// mark the variables set on the command line.
	flag.Visit(func(f *flag.Flag) {
		flagStatus[f.Name] = true
	})

	if latest {
		shardIteratorType = "LATEST"
	} else {
		shardIteratorType = "TRIM_HORIZON"
	}

	if tail {
		read = true;
		if flagStatus["all"] {
			shardIteratorType = "TRIM_HORIZON"
		} else {
			shardIteratorType = "LATEST"
		}
	}

	if false {
		fmt.Println("\nAll flag values:")
		flag.VisitAll( func(f *flag.Flag) { fmt.Println(f.Name, ": ", f.Value) } )
		fmt.Println("\nFlags set on command line:")
		flag.Visit( func(f *flag.Flag) { fmt.Println(f.Name, ": ", f.Value) } )
	}

	if aws.DefaultConfig.Region == "" {
		aws.DefaultConfig.Region = region
	}

	//
	// Say Hello
	if verbose {
		// fmt.Println(aws.DefaultConfig.Credentials.Get())
		fmt.Println("\nOpening up kinesies stream:", stream)
		fmt.Println("To partition:", partition)
		fmt.Println("In region:", region)
	}

	//
	// Do stuff with Kinesis.
	kinesis_svc := kinesis.New(aws.DefaultConfig)

	// Write a bunch of strings to Kinesis
	// TODO: Consider putting JSON onto the stream, instead of raw text.
	// {
	//		"logtag": "[ Thu, 16 JUL 2015 17:08:03 +0000 ]"
	//    "localSequence": "10"	// An integer that is a local sequence number.
	// }
	if numberOfIterations > 0 {
		if verbose {
			fmt.Println("Will push %i enties into the stream.", numberOfIterations)
			fmt.Println("Using the string: %s", testString)
		}
		for i := 0; i < numberOfIterations; i++ {
			line := fmt.Sprintf("%s %d", testString, i)
			resp, err := kPutLine(kinesis_svc, line, partition, stream)
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

	// Read from STDIN and lines to Kinesis stream
	if prompt {
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
				resp, err := kPutLine(kinesis_svc, strings.TrimRight(line, "\n"), partition, stream)
				if err != nil {
					log.Fatal(err)
				}
				if verbose {
					fmt.Println("Response:", awsutil.StringValue(resp))
				}
			}
		}
	}

	// Read records from the stream
	if read {
		if verbose {
			fmt.Println("\nReading from shard: ", shardID)
		}

		// Get the first shard iterator
		siParams := &kinesis.GetShardIteratorInput {
			ShardID: aws.String(shardID),
			ShardIteratorType: aws.String(shardIteratorType),
			StreamName: aws.String(stream),
			// StartingSequenceNumber:  aws.String(startingSequenceNumber)
		}
		siOutput, err := kinesis_svc.GetShardIterator(siParams)
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
			output, err := kinesis_svc.GetRecords(gr_params)
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
		PartitionKey: aws.String("PARTIION"),
		StreamName: aws.String("JDR_TestStream_1"),
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


