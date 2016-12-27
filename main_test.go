package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"
	"testing"
)

var systemTest *bool

func init() {
	systemTest = flag.Bool("systemTest", false, "Set to true when running ec2 tests")
}

func TestSystem(t *testing.T) {
	if *systemTest {
		notifier := make(chan os.Signal, 1)
		signal.Notify(notifier, syscall.SIGINT, syscall.SIGTERM)
		go main()
		<-notifier
	}
}
