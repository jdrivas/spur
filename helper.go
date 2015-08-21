package main

import (
  "fmt"
  "io"
  "github.com/bobappleyard/readline"
  "github.com/aws/aws-sdk-go/aws/awserr"
)

func fmtMilliseconds(msec int64) string {
  hours := (msec / (1000 * 60 * 60)) % 24
  minutes := (msec / (1000 * 60)) % 60
  var seconds float32 = float32(msec-int64(hours*1000*60*60)-int64(minutes*1000*60)) / 1000.0

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

func promptLoop(prompt string, process func(string) (error)) (err error) {

  errStr := "Error - %s.\n"
  for moreCommands := true; moreCommands; {
    line, err := readline.String(prompt)
    if err == io.EOF {
      moreCommands = false
    } else if err != nil {
      fmt.Printf(errStr, err)
    } else {
      readline.AddHistory(line)
      err = process(line)
      if err == io.EOF {
        moreCommands = false
      } else if err != nil {
        fmt.Printf(errStr, err)
      }
    }
  }
  return nil
}
