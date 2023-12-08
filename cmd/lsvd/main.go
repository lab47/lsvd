package main

import (
	"context"
	"flag"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/pprof"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/hashicorp/go-hclog"
	"github.com/lab47/lsvd"
	"github.com/lab47/lsvd/pkg/nbd"
)

var (
	fAddr    = flag.String("addr", ":8989", "address to listen on")
	fProfile = flag.Bool("profile", false, "enable profiling")
	fConfig  = flag.String("config", "lsvd.hcl", "path to configuration")
)

func main() {
	flag.Parse()

	log := hclog.New(&hclog.LoggerOptions{
		Name:  "lsvd",
		Level: hclog.Trace,
		Color: hclog.AutoColor,

		ColorHeaderAndFields: true,
	})

	if *fProfile {
		f, err := os.Create("lsvd.profile")
		if err != nil {
			log.Error("unable to enable profiling", "error", err)
			os.Exit(1)
		}

		defer func() {
			pprof.StopCPUProfile()
			f.Close()
			log.Info("wrote profiling information", "path", "lsvd.profile")
		}()

		pprof.StartCPUProfile(f)
	}

	cfg, err := lsvd.LoadConfig(*fConfig)
	if err != nil {
		log.Error("error loading configuration", "error", err)
		os.Exit(1)
	}

	path, err := filepath.Abs(cfg.CachePath)
	if err != nil {
		log.Error("error resolving path", "error", err)
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer cancel()

	var sa lsvd.SegmentAccess

	if cfg.Storage.FilePath != "" {
		if cfg.Storage.S3.Bucket != "" {
			log.Error("storage is either filepath, or s3, not both")
			os.Exit(1)
		}

		storagePath, err := filepath.Abs(cfg.Storage.FilePath)
		if err != nil {
			log.Error("error resolving file path to store objects", "error", err)
			os.Exit(1)
		}

		sa = &lsvd.LocalFileAccess{Dir: storagePath}
	} else if cfg.Storage.S3.Bucket != "" {
		awsCfg, err := config.LoadDefaultConfig(ctx, func(lo *config.LoadOptions) error {
			lo.Region = cfg.Storage.S3.Region

			if cfg.Storage.S3.AccessKey != "" {
				lo.Credentials = credentials.NewStaticCredentialsProvider(
					cfg.Storage.S3.AccessKey, cfg.Storage.S3.SecretKey, "",
				)
			}
			return nil
		})
		if err != nil {
			log.Error("error initializing S3 configuration", "error", err)
			os.Exit(1)
		}

		sa, err = lsvd.NewS3Access(log, cfg.Storage.S3.URL, cfg.Storage.S3.Bucket, awsCfg)
		if err != nil {
			log.Error("error initializing S3 access", "error", err)
			os.Exit(1)
		}
	}

	d, err := lsvd.NewDisk(ctx, log, path, lsvd.WithSegmentAccess(sa))
	if err != nil {
		log.Error("error creating new disk", "error", err)
		os.Exit(1)
	}

	defer func() {
		log.Info("closing disk", "timeout", "5m")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		d.Close(ctx)
	}()

	l, err := net.Listen("tcp", *fAddr)
	if err != nil {
		log.Error("error listening on addr", "error", err, "addr", *fAddr)
		os.Exit(1)
	}

	go func() {
		<-ctx.Done()
		log.Info("shutting down")
		l.Close()
	}()

	exports := []*nbd.Export{
		{
			Name:        "lsvd",
			Description: "disk",
			Backend:     lsvd.NBDWrapper(ctx, log, d),
		},
	}

	log.Info("listening for connections", "addr", *fAddr)

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
