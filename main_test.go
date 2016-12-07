package main

import (
	"flag"
	"testing"
)

var systemTest *bool

func init() {
	systemTest = flag.Bool("systemTest", false, "Set to true to instrument binary for code coverage")
}

func TestMain(t *testing.T) {
	if *systemTest {
		main()
	}
}
