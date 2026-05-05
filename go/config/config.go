package config

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/evo-cloud/logs/go/emitters/blob"
	"github.com/evo-cloud/logs/go/emitters/console"
	"github.com/evo-cloud/logs/go/emitters/elasticsearch"
	"github.com/evo-cloud/logs/go/logs"
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

	// ElasticSearch emitter.
	ESServerURL  string
	ESDataStream string

	// Stream server.
	StreamAddr string

	// Remote RPC server.
	RemoteRPCAddr     string
	RemoteRPCInsecure bool

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
	f.StringVar(&c.StreamAddr, "logs-stream-addr", os.Getenv("LOGS_STREAM_ADDR"), "Remote stream server address (host:port or unix socket)")
	f.StringVar(&c.RemoteRPCAddr, "logs-remote-rpc-addr", os.Getenv("LOGS_REMOTE_RPC_ADDR"), "Remote RPC server address (host:port)")
	f.BoolVar(&c.RemoteRPCInsecure, "logs-remote-rpc-insecure", false, "Remote RPC server address is insecre")
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
		emitter := elasticsearch.NewEmitter(c.ClientName, c.ESDataStream, c.ESServerURL)
		emitter.Verbose = c.EmitterVerbose
		emitters = append(emitters, logs.NewAsyncBatchEmitter(emitter))
	}

	if c.StreamAddr != "" {
		if c.ClientName == "" {
			return nil, fmt.Errorf("streamer Remote requires client name")
		}
		emitter, err := logs.NewStreamBatchEmitter(c.ClientName, "", c.StreamAddr)
		if err != nil {
			return nil, fmt.Errorf("streamer Remote creation error: %w", err)
		}
		emitter.Verbose = c.EmitterVerbose
		emitters = append(emitters, logs.NewAsyncBatchEmitter(emitter))
	}

	if c.RemoteRPCAddr != "" {
		if c.ClientName == "" {
			return nil, fmt.Errorf("streamer Remote requires client name")
		}
		var opts []grpc.DialOption
		if c.RemoteRPCInsecure {
			opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
		}
		emitter, err := logs.NewRPCBatchEmitter(c.ClientName, c.RemoteRPCAddr, opts...)
		if err != nil {
			return nil, fmt.Errorf("streamer Remote creation error: %w", err)
		}
		emitter.Verbose = c.EmitterVerbose
		emitters = append(emitters, logs.NewAsyncBatchEmitter(emitter))
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
