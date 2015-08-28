package main

import (
  "testing"
	"github.com/aws/aws-sdk-go/aws"
  . "github.com/smartystreets/goconvey/convey"
)

func TestSpec(t *testing.T) {

  Convey("Given the region is set in the aws.DefaultConfig", t, func() {
    testRegion := "us-west-1"
    aws.DefaultConfig.Region = "us-west-1"

    Convey("When a new Test group is created", func() {
      g, err := NewStreamGroup(aws.DefaultConfig)

      Convey("The new groups region should be the same as the aws.DefultConfig", func() {
        So(g.Region, ShouldEqual, testRegion)
        So(err, ShouldBeNil)
      }) 
    })

    Convey("When a new Stream is created", func() {
      s := NewStream(aws.DefaultConfig, "TestStream", "", "", "")
      Convey("The new stream should have the correct name.", func() {
        So(s.Name, ShouldEqual, "TestStream")
      })
    })
  })
}

