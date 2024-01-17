package cli

import (
	"context"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/hashicorp/go-hclog"
	"github.com/lab47/cleo"
	"github.com/lab47/lsvd"
	"github.com/lab47/lsvd/pkg/nbd"
	"github.com/mitchellh/cli"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sys/unix"
)

type CLI struct {
	log hclog.Logger

	lc *cli.CLI
}

type Global struct {
	Config string `short:"c" long:"config" description:"storage configuration" required:"true"`
	Debug  bool   `short:"D" long:"debug" description:"enabel debug mode"`
}

func NewCLI(log hclog.Logger, args []string) (*CLI, error) {
	c := &CLI{
		log: log,
		lc:  cli.NewCLI("lsvd", "alpha"),
	}

	c.lc.Args = args

	err := c.setupCommands()
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (c *CLI) Run() (int, error) {
	return c.lc.Run()
}

func (c *CLI) setupCommands() error {
	c.lc.Commands = map[string]cli.CommandFactory{
		"volume list": func() (cli.Command, error) {
			return cleo.Infer("volume list", "list all volumes", c.volumeList), nil
		},
		"volume init": func() (cli.Command, error) {
			return cleo.Infer("volume init", "initialize a volume", c.volumeInit), nil
		},
		"volume inspect": func() (cli.Command, error) {
			return cleo.Infer("volume inspect", "inspect a volume", c.volumeInspect), nil
		},
		"nbd": func() (cli.Command, error) {
			return cleo.Infer("nbd", "service a volume over nbd", c.nbdServe), nil
		},
	}

	return nil
}

func (c *CLI) loadSegmentAccess(ctx context.Context, path string) (lsvd.SegmentAccess, error) {
	cfg, err := lsvd.LoadConfig(path)
	if err != nil {
		c.log.Error("error loading configuration", "error", err)
		os.Exit(1)
	}

	var sa lsvd.SegmentAccess

	if cfg.Storage.FilePath != "" {
		if cfg.Storage.S3.Bucket != "" {
			c.log.Error("storage is either filepath, or s3, not both")
			os.Exit(1)
		}

		storagePath, err := filepath.Abs(cfg.Storage.FilePath)
		if err != nil {
			c.log.Error("error resolving file path to store objects", "error", err)
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
			c.log.Error("error initializing S3 configuration", "error", err)
			os.Exit(1)
		}

		sa, err = lsvd.NewS3Access(c.log, cfg.Storage.S3.URL, cfg.Storage.S3.Bucket, awsCfg)
		if err != nil {
			c.log.Error("error initializing S3 access", "error", err)
			os.Exit(1)
		}
	}

	return sa, nil
}

func (c *CLI) volumeList(ctx context.Context, opts struct {
	Global
}) error {
	sa, err := c.loadSegmentAccess(ctx, opts.Config)
	if err != nil {
		return err
	}

	volumes, err := sa.ListVolumes(ctx)
	if err != nil {
		c.log.Error("error listing volumes", "error", err)
		os.Exit(1)
	}

	tr := tabwriter.NewWriter(os.Stdout, 2, 2, 1, ' ', 0)
	defer tr.Flush()

	fmt.Fprintf(tr, "NAME\tSIZE\n")

	for _, vol := range volumes {
		vi, err := sa.GetVolumeInfo(ctx, vol)
		if err != nil {
			return err
		}
		fmt.Fprintf(tr, "%s\t%s\n", vol, niceSize(vi.Size))
	}

	return nil
}

const (
	kilo = 1000
	mega = kilo * 1000
	giga = mega * 1000
	tera = giga * 1000
	peta = tera * 1000
)

var sizeSuffix = map[string]int{
	"k": kilo,
	"K": kilo,
	"m": mega,
	"M": mega,
	"g": giga,
	"G": giga,
	"t": tera,
	"T": tera,
	"p": peta,
	"P": peta,
}

func niceSize(sz int64) string {
	cases := []struct {
		f float64
		s string
	}{
		{peta, "PB"},
		{tera, "TB"},
		{giga, "GB"},
		{mega, "MB"},
		{kilo, "KB"},
	}

	x := float64(sz)

	for _, c := range cases {
		sub := x / c.f
		if sub >= 1.0 {
			return fmt.Sprintf("%.3f%s", sub, c.s)
		}
	}

	return fmt.Sprintf("%db", sz)
}

func (c *CLI) volumeInit(ctx context.Context, opts struct {
	Global
	Name string `short:"n" long:"name" description:"name of volume to create" required:"true"`
	Size string `short:"s" long:"size" description:"size to advertise the volume as" required:"true"`
}) error {
	sa, err := c.loadSegmentAccess(ctx, opts.Config)
	if err != nil {
		return err
	}

	var size int64

	for suf, factor := range sizeSuffix {
		if strings.HasSuffix(opts.Size, suf) {
			base, err := strconv.ParseInt(opts.Size[:len(opts.Size)-len(suf)], 10, 64)
			if err != nil {
				return errors.Wrapf(err, "parsing size")
			}

			size = base * int64(factor)
		}
	}

	if opts.Name == "" {
		return fmt.Errorf("name must not be empty")
	}

	err = sa.InitVolume(ctx, &lsvd.VolumeInfo{
		Name: opts.Name,
		Size: size,
	})
	if err != nil {
		c.log.Error("error listing volumes", "error", err)
		os.Exit(1)
	}

	fmt.Printf("volume '%s' created (%d bytes)\n", opts.Name, size)

	return nil
}

func (c *CLI) volumeInspect(ctx context.Context, opts struct {
	Global
	Name string `short:"n" long:"name" description:"name of volume to create" required:"true"`
}) error {
	sa, err := c.loadSegmentAccess(ctx, opts.Config)
	if err != nil {
		return err
	}

	info, err := sa.GetVolumeInfo(ctx, opts.Name)
	if err != nil {
		return err
	}

	fmt.Printf("%s: %d\n", info.Name, info.Size)

	entries, err := sa.ListSegments(ctx, opts.Name)
	if err != nil {
		return err
	}

	fmt.Printf("%d segments\n", len(entries))

	for _, ent := range entries {
		fmt.Printf("  %s\n", ent)
	}

	return nil
}

func (c *CLI) nbdServe(ctx context.Context, opts struct {
	Global
	Name        string `short:"n" long:"name" description:"name of volume to create" required:"true"`
	Path        string `short:"p" long:"path" description:"path for cached data" required:"true"`
	Addr        string `short:"a" long:"addr" default:":8989" description:"address to listen on"`
	MetricsAddr string `long:"metrics" default:":2121" description:"address to expose metrics on"`
}) error {
	sa, err := c.loadSegmentAccess(ctx, opts.Config)
	if err != nil {
		return err
	}

	log := c.log
	path := opts.Path
	name := opts.Name

	if opts.Debug {
		log.SetLevel(hclog.Trace)
	}

	d, err := lsvd.NewDisk(ctx, log, path,
		lsvd.WithSegmentAccess(sa),
		lsvd.WithVolumeName(name),
	)
	if err != nil {
		log.Error("error creating new disk", "error", err)
		os.Exit(1)
	}

	ch := make(chan os.Signal, 1)

	go func() {
		for range ch {
			log.Info("closing segment by signal request")
			d.CloseSegment(ctx)
		}
	}()

	signal.Notify(ch, unix.SIGHUP)

	defer func() {
		log.Info("closing disk", "timeout", "5m")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		d.Close(ctx)
	}()

	l, err := net.Listen("tcp", opts.Addr)
	if err != nil {
		log.Error("error listening on addr", "error", err, "addr", opts.Addr)
		os.Exit(1)
	}

	go func() {
		<-ctx.Done()
		log.Info("shutting down")
		l.Close()
	}()

	exports := []*nbd.Export{
		{
			Name:        name,
			Description: "disk",
			Backend:     lsvd.NBDWrapper(ctx, log, d),
		},
	}

	log.Info("listening for connections", "addr", opts.Addr)

	nbdOpts := &nbd.Options{
		MinimumBlockSize:   4096,
		PreferredBlockSize: 4096,
		MaximumBlockSize:   4096,
	}

	http.Handle("/metrics", promhttp.Handler())
	// Will also include pprof via the init() in net/http/pprof
	go http.ListenAndServe(opts.MetricsAddr, nil)

	for {
		c, err := l.Accept()
		if err != nil {
			break
		}

		log.Info("connection to nbd server", "remote", c.RemoteAddr().String())

		err = nbd.Handle(log, c, exports, nbdOpts)
		if err != nil {
			log.Error("error handling nbd client", "error", err)
		}
	}

	return nil
}
