package main

import (
	"os"

	"github.com/lincyaw/ag/internal/cli"
)

const version = "0.2.0"

func main() {
	os.Exit(cli.Run(os.Args[1:], os.Stdout, os.Stderr, version))
}
