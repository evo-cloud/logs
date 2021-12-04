package config

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"

	"google.golang.org/grpc"

	"github.com/evo-cloud/logs/go/emitters/blob"
	"github.com/evo-cloud/logs/go/emitters/console"
	"github.com/evo-cloud/logs/go/emitters/stackdriver"
	"github.com/evo-cloud/logs/go/logs"
	"github.com/evo-cloud/logs/go/streamers/elasticsearch"
	"github.com/evo-cloud/logs/go/streamers/jaeger"
	"github.com/evo-cloud/logs/go/streamers/remote"
)

// Config defines the configuration for logging.
type Config struct {
	ClientName     string
	ConsolePrinter string
	Color          bool

	// Blob file output.
	BlobFile      string
	BlobSync      bool
	BlobSizeLimit int64

	// ElasticSearch streamer.
	ESServerURL  string
	ESDataStream string

	// Jaeger streamer.
	JaegerAddr string

	// Remote streamer.
	RemoteAddr     string
	RemoteInsecure bool

	// Chunked streaming configurations.
	ChunkedMaxBuffer     int
	ChunkedMaxBatch      int
	ChunkedCollectPeriod time.Duration

	// EmitterVerbose allows emitter to write errors using emergent logger.
	EmitterVerbose bool
}

type FlagSet interface {
	StringVar(*string, string, string, string)
	BoolVar(*bool, string, bool, string)
	Int64Var(*int64, string, int64, string)
	IntVar(*int, string, int, string)
	DurationVar(*time.Duration, string, time.Duration, string)
}

// Default creates a default configuration.
func Default() *Config {
	return &Config{
		ChunkedMaxBuffer:     envOrInt("LOGS_CHUNKED_BUFFER_MAX", 1<<20), // 1M
		ChunkedMaxBatch:      envOrInt("LOGS_CHUNKED_BATCH_MAX", 1<<14),  // 16K
		ChunkedCollectPeriod: time.Second,
	}
}

// SetupFlags sets up commandline flags.
func (c *Config) SetupFlags() {
	c.SetupFlagsWith(flag.CommandLine)
}

func (c *Config) SetupFlagsWith(f FlagSet) {
	f.StringVar(&c.ClientName, "logs-client", os.Getenv("LOGS_CLIENT"), "Logs client name")
	f.StringVar(&c.ConsolePrinter, "logs-printer", os.Getenv("LOGS_PRINTER"), "Logs console printer")
	f.BoolVar(&c.Color, "logs-color", c.Color, "Enable color on console printer")
	f.StringVar(&c.BlobFile, "logs-blob-file", os.Getenv("LOGS_BLOB_FILE"), "Blob filename template for writing binary proto encoded logs to files")
	f.BoolVar(&c.BlobSync, "logs-blob-sync", c.BlobSync, "Blob file writes with sync")
	f.Int64Var(&c.BlobSizeLimit, "logs-blob-sizelimit", c.BlobSizeLimit, "Blob file size limit, 0 means no limit")
	f.StringVar(&c.ESServerURL, "logs-es-url", os.Getenv("LOGS_ES_URL"), "ElasticSearch server URL")
	f.StringVar(&c.ESDataStream, "logs-es-datastream", os.Getenv("LOGS_ES_DATASTREAM"), "ElasticSearch data stream")
	f.StringVar(&c.JaegerAddr, "logs-jaeger-addr", os.Getenv("LOGS_JAEGER_ADDR"), "Jaeger server address (host:port)")
	f.StringVar(&c.RemoteAddr, "logs-remote-addr", os.Getenv("LOGS_REMOTE_ADDR"), "Remote server address (host:port)")
	f.BoolVar(&c.RemoteInsecure, "logs-remote-insecure", false, "Remote server address is insecre")
	f.IntVar(&c.ChunkedMaxBuffer, "logs-chunked-buffer-max", c.ChunkedMaxBuffer, "Logs chunked emitter: max buffer of unstreamed logs")
	f.IntVar(&c.ChunkedMaxBatch, "logs-chunked-batch-max", c.ChunkedMaxBatch, "Logs chunked emitter: max size in one batch")
	f.DurationVar(&c.ChunkedCollectPeriod, "logs-chunked-collect-period", c.ChunkedCollectPeriod, "Logs chunked emitter: batch period")
	f.BoolVar(&c.EmitterVerbose, "logs-emitter-verbose", c.EmitterVerbose, "Allow emitters write error logs using emergent logger")
}

// Emitter creates LogEmitter based on the current configuration.
func (c *Config) Emitter() (logs.LogEmitter, error) {
	var emitters logs.MultiEmitter
	switch c.ConsolePrinter {
	case "", "default":
		printer := console.NewPrinter(os.Stderr)
		printer.UseColor(c.Color)
		emitters = append(emitters, printer)
	case "json":
		emitters = append(emitters, &console.Emitter{Printer: console.NewPrinter(os.Stderr), JSON: true})
	case "stackdriver":
		printer, err := stackdriver.NewJSONEmitter(os.Stderr, os.Getenv("LOGS_STACKDRIVER_PROJECTID"))
		if err != nil {
			return nil, fmt.Errorf("create Stackdriver emitter: %w", err)
		}
		if levelStr := os.Getenv("LOGS_STACKDRIVER_MIN_LEVEL"); levelStr != "" {
			if printer.MinLevel, err = logs.ParseLevel(levelStr); err != nil {
				return nil, err
			}
		}
		if valStr := os.Getenv("LOGS_STACKDRIVER_MAX_VALUE_SIZE"); valStr != "" {
			value, err := strconv.Atoi(valStr)
			if err == nil && value <= 0 {
				err = fmt.Errorf("non-positive")
			}
			if err != nil {
				return nil, fmt.Errorf("invalid LOGS_STACKDRIVER_MAX_VALUE_SIZE %q: %w", valStr, err)
			}
			printer.MaxValueSize = value
		}
		emitters = append(emitters, printer)
	default:
		return nil, fmt.Errorf("unknown console printer: %s", c.ConsolePrinter)
	}

	if c.BlobFile != "" {
		fn, err := blob.CreateFileWith(c.BlobFile)
		if err != nil {
			return nil, fmt.Errorf("blob filename template: %w", err)
		}
		emitters = append(emitters, &blob.Emitter{CreateFile: fn, Sync: c.BlobSync, SizeLimit: c.BlobSizeLimit})
	}

	if c.ESServerURL != "" {
		if c.ClientName == "" {
			return nil, fmt.Errorf("streamer ElasticSearch requires client name")
		}
		if c.ESDataStream == "" {
			return nil, fmt.Errorf("streamer ElasticSearch requires data stream name")
		}
		s := elasticsearch.NewStreamer(c.ClientName, c.ESDataStream, c.ESServerURL)
		s.Verbose = c.EmitterVerbose
		emitters = append(emitters, logs.NewStreamEmitter(s))
	}

	if c.JaegerAddr != "" {
		if c.ClientName == "" {
			return nil, fmt.Errorf("streamer Jaeger requires client name")
		}
		reporter, err := jaeger.New(c.ClientName, c.JaegerAddr, nil)
		if err != nil {
			return nil, fmt.Errorf("streamer Jaeger creation error: %w", err)
		}
		chunkedEmitter := logs.NewChunkedEmitter(reporter, c.ChunkedMaxBuffer, c.ChunkedMaxBatch)
		chunkedEmitter.CollectPeriod = c.ChunkedCollectPeriod
		emitters = append(emitters, chunkedEmitter)
	}

	if c.RemoteAddr != "" {
		if c.ClientName == "" {
			return nil, fmt.Errorf("streamer Remote requires client name")
		}
		var opts []grpc.DialOption
		if c.RemoteInsecure {
			opts = append(opts, grpc.WithInsecure())
		}
		streamer, err := remote.NewStreamer(c.ClientName, c.RemoteAddr, opts...)
		if err != nil {
			return nil, fmt.Errorf("streamer Remote creation error: %w", err)
		}
		streamer.Verbose = c.EmitterVerbose
		emitters = append(emitters, logs.NewStreamEmitter(streamer))
	}

	if len(emitters) == 1 {
		return emitters[0], nil
	}
	return emitters, nil
}

// SetupDefaultLogger sets up the default logger.
func (c *Config) SetupDefaultLogger() error {
	emitter, err := c.Emitter()
	if err != nil {
		return err
	}
	logs.Setup(emitter)
	return nil
}

// MustSetupDefaultLogger asserts the success of SetupDefaultLogger.
// If failed, Fatal will be called.
func (c *Config) MustSetupDefaultLogger() {
	if err := c.SetupDefaultLogger(); err != nil {
		logs.Emergent().Fatal(err).PrintErr("SetupDefaultLogger: ")
	}
}

func envOrInt(envVar string, defVal int) int {
	val := os.Getenv(envVar)
	if val == "" {
		return defVal
	}
	if intVal, err := strconv.Atoi(val); err == nil {
		return intVal
	}
	return defVal
}
