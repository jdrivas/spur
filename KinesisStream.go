package main 

import (
	"fmt"
	"time"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

type KinesisStream struct {
	Service *kinesis.Kinesis
	Name string
	Partition string
	ShardIteratorType string
	ShardID string
	NextShardIteratorName string
}

func NewStream(config *aws.Config, name, partition, iteratorType, shardID string) *KinesisStream {
	svc := kinesis.New(config)
	return &KinesisStream{svc, name, partition, iteratorType, shardID, ""}
}

func (s *KinesisStream) PutLogLine(line string) (*kinesis.PutRecordOutput, error) {
	logString := fmt.Sprintf("[ %s ] %s", time.Now().UTC().Format(time.RFC1123Z), line)
	logData := []byte(logString)
	record := &kinesis.PutRecordInput {
		Data: logData,
		PartitionKey: aws.String(s.Partition),
		StreamName: aws.String(s.Name),
	}

	// resp, err := s.Service.PutRecord(record)
	return s.Service.PutRecord(record)
}

func (s *KinesisStream) ReadReset() {
	s.NextShardIteratorName = ""
}

func (s *KinesisStream) GetRecords() (output *kinesis.GetRecordsOutput, err error) {

	// The read records funciton needs a ShardIterator to determine
	// which records to read. This first one can either be told
	// to start with the oldest available (TRIM_HORIZON), the latest
	// LATEST, or relative to an actual point AT_SEQUENCE_NUMBER", "AFTER_SEQUENCE_NUMBER"
	// Have to add more UI to get the sequence AT/AFTER Sequence number adeed in.

	// First time through, we need to get a Shard Iterator
	if s.NextShardIteratorName == "" {
		err = s.getFirstShardIteratorName()
		if err != nil {
			return nil, err
		}
	}

	params := &kinesis.GetRecordsInput {
		ShardIterator: aws.String(s.NextShardIteratorName),
		// Limit: aws.Long(1);
	}

	output, err = s.Service.GetRecords(params)
	s.NextShardIteratorName = *output.NextShardIterator

	return output, err

}

func (s *KinesisStream) getFirstShardIteratorName() (error) {

	params := &kinesis.GetShardIteratorInput {
		ShardID: aws.String(s.ShardID),
		ShardIteratorType: aws.String(s.ShardIteratorType),
		StreamName: aws.String(s.Name),
	}
	output, err := s.Service.GetShardIterator(params)
	if err == nil {
		s.NextShardIteratorName = *output.ShardIterator
	}
	return err
}