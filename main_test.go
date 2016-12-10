package main

import (
	"os"
	"os/signal"
	"syscall"
	"testing"
)

var systemTest *bool

func TestSystem(t *testing.T) {
	notifier := make(chan os.Signal, 1)
	signal.Notify(notifier, syscall.SIGINT, syscall.SIGTERM)
	go main()
	<-notifier
}
