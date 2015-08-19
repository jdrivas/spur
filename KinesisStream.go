package main

import (
  "fmt"
  "github.com/aws/aws-sdk-go/aws"
  "github.com/aws/aws-sdk-go/service/kinesis"
  "time"
  "errors"
)

type KinesisStream struct {
  Service               *kinesis.Kinesis
  Name                  string
  Partition             string
  ShardIteratorType     string
  ShardID               string
  NextShardIteratorName string
}

type KinesisStreamGroup struct {
  Streams               map[string]*KinesisStream
  CurrentStream         *KinesisStream
  Service               *kinesis.Kinesis
  Region                string
}

func NewStream(config *aws.Config, name, partition, iteratorType, shardID string) *KinesisStream {
  svc := kinesis.New(config)
  return &KinesisStream{svc, name, partition, iteratorType, shardID, ""}
}

func NewStreamGroup(config *aws.Config) (g *KinesisStreamGroup, err error){
  g = &KinesisStreamGroup{Streams: make(map[string]*KinesisStream), Service: kinesis.New(config), Region: config.Region}
  err = g.init()
  return g, err
}

func (g *KinesisStreamGroup) init() (err error){
  streams, err := g.ListStreams()
  if err != nil {return err}

  for _, description := range streams {
    g.Streams[description.Name] = &KinesisStream{Service: g.Service, Name: description.Name}
  }

  return nil
}

func (g *KinesisStreamGroup) CreateKinesisStream(name string, shards int64) (*KinesisStream, error) {
  _, err := g.Service.CreateStream(&kinesis.CreateStreamInput{StreamName: &name, ShardCount: &shards})
  g.Streams[name] = &KinesisStream{Service: g.Service, Name: name}
  return g.Streams[name], err
}

func (g *KinesisStreamGroup) GetStream(name string) (stream *KinesisStream, err error) {
  stream = g.Streams[name]
  err = nil
  if stream == nil {
    err = errors.New(fmt.Sprintf("Coulnd't find the stream named: \"%s\".", name))
  }
  return stream, err
}

func (g *KinesisStreamGroup) DeleteKinesisStream(name string) (*KinesisStream, error) {
  stream, err := g.GetStream(name)
  if err == nil {
    delete(g.Streams, name)
    _, err = stream.Service.DeleteStream(&kinesis.DeleteStreamInput{StreamName: &name})
  }
  return stream, err
}

func (g* KinesisStreamGroup) SetCurrentStream(name string) (err error) {
  stream, err := g.GetStream(name)
  if err == nil {
    g.CurrentStream = stream
  }
  return err
}

type StreamDescription struct {
  Name        string
  Description *kinesis.StreamDescription
}

func (g *KinesisStreamGroup) ListStreams() (streams []*StreamDescription, err error) {

  for {

    output, err := g.Service.ListStreams(&kinesis.ListStreamsInput{})
    if err != nil {
      return streams, err
    }

    var name string
    for _, name := range output.StreamNames {
      descript, err := g.Service.DescribeStream(&kinesis.DescribeStreamInput{StreamName: name})
      if err != nil {
        return streams, err
      }
      streams = append(streams, &StreamDescription{Name: *name, Description: descript.StreamDescription})
    }

    if *output.HasMoreStreams {
      output, err = g.Service.ListStreams(&kinesis.ListStreamsInput{ExclusiveStartStreamName: &name})
      if err != nil {
        return streams, err
      }
    } else {
      break
    }
  }

  return streams, nil
}

func (g *KinesisStreamGroup) String() string {
  return fmt.Sprintf("In %s there are %d streams, current: %s",g.Region, len(g.Streams), g.CurrentStream.Name)
}

func (g *KinesisStreamGroup) Description() (s string) {
  s = fmt.Sprintf("%s.\n", g)
  for _, stream := range g.Streams {
    s += stream.Description() + "\n"
  }
  return s
}

func (s *KinesisStream) PutLogLine(line string) (*kinesis.PutRecordOutput, error) {
  logString := fmt.Sprintf("[ %s ] %s", time.Now().UTC().Format(time.RFC1123Z), line)
  logData := []byte(logString)
  record := &kinesis.PutRecordInput{
    Data:         logData,
    PartitionKey: aws.String(s.Partition),
    StreamName:   aws.String(s.Name),
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

  params := &kinesis.GetRecordsInput{
    ShardIterator: aws.String(s.NextShardIteratorName),
    // Limit: aws.Long(1);
  }

  output, err = s.Service.GetRecords(params)
  s.NextShardIteratorName = *output.NextShardIterator

  return output, err

}

func (s *KinesisStream) getFirstShardIteratorName() error {

  params := &kinesis.GetShardIteratorInput{
    ShardID:           aws.String(s.ShardID),
    ShardIteratorType: aws.String(s.ShardIteratorType),
    StreamName:        aws.String(s.Name),
  }
  output, err := s.Service.GetShardIterator(params)
  if err == nil {
    s.NextShardIteratorName = *output.ShardIterator
  }
  return err
}

func (s *KinesisStream) String() (string) {
  return fmt.Sprintf("%s - (%s)",  s.Name, s.Partition)
}

func (s *KinesisStream) Description() string {
  return fmt.Sprintf("Name: \"%s\"\n", s.Name) +
    fmt.Sprintf("Partition: \"%s\"\n", s.Partition) +
    fmt.Sprintf("ShardIteratorType: \"%s\"\n", s.ShardIteratorType) +
    fmt.Sprintf("ShardID: \"%s\"\n", s.ShardID) +
    fmt.Sprintf("NextShardIteratorName: \"%s\"\n", s.NextShardIteratorName)
}



func (s* KinesisStream) GetAWSDescription() (*kinesis.StreamDescription, error) {
  res, err := s.Service.DescribeStream(&kinesis.DescribeStreamInput{StreamName: &s.Name})
  return res.StreamDescription, err
}

func (s* KinesisStream) WaitForStateChange(delaySeconds, periodSeconds int, currentState string, cb func(string, error)) {
  go func() {
    time.Sleep(time.Second * time.Duration(delaySeconds))
    var e error
    var status string
    for sd, e := s.GetAWSDescription(); e == nil;  {
      if *sd.StreamStatus != currentState {
        status = *sd.StreamStatus
        break;
      }
      time.Sleep(time.Second * time.Duration(periodSeconds))
      sd, e = s.GetAWSDescription()
    }
    cb(status, e)
  }()
}
