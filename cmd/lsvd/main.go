package main

import (
	"flag"
	"net"
	"os"
	"path/filepath"

	"github.com/hashicorp/go-hclog"
	"github.com/lab47/lsvd"
	"github.com/lab47/lsvd/pkg/nbd"
)

var (
	fPath = flag.String("path", "./data", "path to store objects in")
	fAddr = flag.String("addr", ":8989", "address to listen on")
)

func main() {
	flag.Parse()

	log := hclog.New(&hclog.LoggerOptions{
		Name:  "lsvd",
		Level: hclog.Info,
		Color: hclog.AutoColor,

		ColorHeaderAndFields: true,
	})

	path, err := filepath.Abs(*fPath)
	if err != nil {
		log.Error("error resolving path", "error", err)
		os.Exit(1)
	}

	d, err := lsvd.NewDisk(log, path)
	if err != nil {
		log.Error("error creating new disk", "error", err)
		os.Exit(1)
	}

	l, err := net.Listen("tcp", *fAddr)
	if err != nil {
		log.Error("error listening on addr", "error", err, "addr", *fAddr)
		os.Exit(1)
	}

	exports := []*nbd.Export{
		{
			Name:        "lsvd",
			Description: "disk",
			Backend:     lsvd.NBDWrapper(log, d),
		},
	}

	opts := &nbd.Options{
		MinimumBlockSize:   4096,
		PreferredBlockSize: 4096,
		MaximumBlockSize:   4096,
	}

	for {
		c, err := l.Accept()
		if err != nil {
			break
		}

		log.Info("connection to nbd server", "remote", c.RemoteAddr().String())

		err = nbd.Handle(log, c, exports, opts)
		if err != nil {
			log.Error("error handling nbd client", "error", err)
		}
	}
}
