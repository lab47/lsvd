package main

import (
	"flag"
	"log/slog"
	"os"
	"time"

	"github.com/lab47/lsvd"
	"github.com/lab47/lsvd/cli"
	"github.com/lab47/lsvd/logger"
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
	level := slog.LevelInfo

	if os.Getenv("LSVD_DEBUG") != "" {
		level = slog.LevelDebug
	}

	log := logger.New(level)

	log.Debug("log level configured", "level", level)

	c, err := cli.NewCLI(log, os.Args[1:])
	if err != nil {
		log.Error("error creating CLI", "error", err)
		os.Exit(1)
		return
	}

	go func() {
		t := time.NewTicker(60 * time.Second)
		defer t.Stop()

		for {
			<-t.C
			lsvd.LogMetrics(log)
		}
	}()

	code, err := c.Run()
	if err != nil {
		log.Error("error running CLI", "error", err)
		os.Exit(1)
	}

	os.Exit(code)
}
