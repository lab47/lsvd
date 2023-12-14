package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/pprof"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/hashicorp/go-hclog"
	"github.com/lab47/lsvd"
	"github.com/lab47/lsvd/cli"
	"github.com/lab47/lsvd/pkg/nbd"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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

func oldmain() {
	flag.Parse()

	level := hclog.Info

	if os.Getenv("LSVD_DEBUG") != "" {
		level = hclog.Trace
	}

	log := hclog.New(&hclog.LoggerOptions{
		Name:  "lsvd",
		Level: level,
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

	if *fList {
		volumes, err := sa.ListVolumes(ctx)
		if err != nil {
			log.Error("error listing volumes", "error", err)
			os.Exit(1)
		}

		for _, vol := range volumes {
			fmt.Println(vol)
		}

		return
	}

	if *fInit {
		err := sa.InitVolume(ctx, &lsvd.VolumeInfo{
			Name: *fName,
			Size: *fSize,
		})
		if err != nil {
			log.Error("error listing volumes", "error", err)
			os.Exit(1)
		}

		fmt.Printf("volume '%s' created\n", *fName)

		return
	}

	d, err := lsvd.NewDisk(ctx, log, path,
		lsvd.WithSegmentAccess(sa),
		lsvd.WithVolumeName(*fName),
	)
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

	http.Handle("/metrics", promhttp.Handler())
	go http.ListenAndServe(*fMetrics, nil)

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
