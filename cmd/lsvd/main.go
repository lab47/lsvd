package main

import (
	"flag"
	"os"

	"github.com/hashicorp/go-hclog"
	"github.com/lab47/lsvd/cli"
)

var (
	fAddr    = flag.String("addr", ":8989", "address to listen on")
	fProfile = flag.Bool("profile", false, "enable profiling")
	fConfig  = flag.String("config", "lsvd.hcl", "path to configuration")
	fMetrics = flag.String("metrics", ":2121", "path to serve metrics on")
	fName    = flag.String("name", "default", "name of the volume to attach to")
	fList    = flag.Bool("list", false, "list volumes")
	fInit    = flag.Bool("init", false, "initialize a volume")
	fSize    = flag.Int64("size", 0, "size of the volume")
)

func main() {
	level := hclog.Info

	if os.Getenv("LSVD_DEBUG") != "" {
		level = hclog.Trace
	}

	log := hclog.New(&hclog.LoggerOptions{
		Name:  "lsvd",
		Level: level,
		Color: hclog.AutoColor,

		IndependentLevels: true,

		ColorHeaderAndFields: true,
	})

	log.Debug("log level configured", "level", level)

	c, err := cli.NewCLI(log, os.Args[1:])
	if err != nil {
		log.Error("error creating CLI", "error", err)
		os.Exit(1)
		return
	}

	code, err := c.Run()
	if err != nil {
		log.Error("error running CLI", "error", err)
		os.Exit(1)
	}

	os.Exit(code)
}
