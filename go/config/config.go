package config

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/evo-cloud/logs/go/emitters/blob"
	"github.com/evo-cloud/logs/go/emitters/console"
	"github.com/evo-cloud/logs/go/emitters/stackdriver"
	"github.com/evo-cloud/logs/go/logs"
	"github.com/evo-cloud/logs/go/streamers/elasticsearch"
	"github.com/evo-cloud/logs/go/streamers/jaeger"
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

	// Chunked streaming configurations.
	ChunkedMaxBuffer     int
	ChunkedMaxBatch      int
	ChunkedCollectPeriod time.Duration
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
	flag.StringVar(&c.ClientName, "logs-client", os.Getenv("LOGS_CLIENT"), "Logs client name")
	flag.StringVar(&c.ConsolePrinter, "logs-printer", os.Getenv("LOGS_PRINTER"), "Logs console printer")
	flag.BoolVar(&c.Color, "logs-color", c.Color, "Enable color on console printer")
	flag.StringVar(&c.BlobFile, "logs-blob-file", os.Getenv("LOGS_BLOB_FILE"), "Blob filename template for writing binary proto encoded logs to files")
	flag.BoolVar(&c.BlobSync, "logs-blob-sync", c.BlobSync, "Blob file writes with sync")
	flag.Int64Var(&c.BlobSizeLimit, "logs-blob-sizelimit", c.BlobSizeLimit, "Blob file size limit, 0 means no limit")
	flag.StringVar(&c.ESServerURL, "logs-es-url", os.Getenv("LOGS_ES_URL"), "ElasticSearch server URL")
	flag.StringVar(&c.ESDataStream, "logs-es-datastream", os.Getenv("LOGS_ES_DATASTREAM"), "ElasticSearch data stream")
	flag.StringVar(&c.JaegerAddr, "logs-jaeger-addr", os.Getenv("LOGS_JAEGER_ADDR"), "Jaeger server address (host:port)")
	flag.IntVar(&c.ChunkedMaxBuffer, "logs-chunked-buffer-max", c.ChunkedMaxBuffer, "Logs chunked emitter: max buffer of unstreamed logs")
	flag.IntVar(&c.ChunkedMaxBatch, "logs-chunked-batch-max", c.ChunkedMaxBatch, "Logs chunked emitter: max size in one batch")
	flag.DurationVar(&c.ChunkedCollectPeriod, "logs-chunked-collect-period", c.ChunkedCollectPeriod, "Logs chunked emitter: batch period")
}

// SetupDefaultLogger sets up the default logger.
func (c *Config) SetupDefaultLogger() error {
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
			return fmt.Errorf("create Stackdriver emitter: %w", err)
		}
		if levelStr := os.Getenv("LOGS_STACKDRIVER_MIN_LEVEL"); levelStr != "" {
			if printer.MinLevel, err = logs.ParseLevel(levelStr); err != nil {
				return err
			}
		}
		if valStr := os.Getenv("LOGS_STACKDRIVER_MAX_VALUE_SIZE"); valStr != "" {
			value, err := strconv.Atoi(valStr)
			if err == nil && value <= 0 {
				err = fmt.Errorf("non-positive")
			}
			if err != nil {
				return fmt.Errorf("invalid LOGS_STACKDRIVER_MAX_VALUE_SIZE %q: %w", valStr, err)
			}
			printer.MaxValueSize = value
		}
		emitters = append(emitters, printer)
	default:
		return fmt.Errorf("unknown console printer: %s", c.ConsolePrinter)
	}

	if c.BlobFile != "" {
		fn, err := blob.CreateFileWith(c.BlobFile)
		if err != nil {
			return fmt.Errorf("blob filename template: %w", err)
		}
		emitters = append(emitters, &blob.Emitter{CreateFile: fn, Sync: c.BlobSync, SizeLimit: c.BlobSizeLimit})
	}

	if c.ESServerURL != "" {
		if c.ClientName == "" {
			return fmt.Errorf("ElasticSearch streamer requires client name")
		}
		if c.ESDataStream == "" {
			return fmt.Errorf("ElasticSearch streamer requires data stream name")
		}
		s := elasticsearch.NewStreamer(c.ClientName, c.ESDataStream, c.ESServerURL)
		emitters = append(emitters, logs.NewStreamEmitter(s))
	}

	if c.JaegerAddr != "" {
		if c.ClientName == "" {
			return fmt.Errorf("Jaeger streamer requires client name")
		}
		reporter, err := jaeger.New(c.ClientName, c.JaegerAddr, nil)
		if err != nil {
			return fmt.Errorf("Jaeger creation error: %w", err)
		}
		chunkedEmitter := logs.NewChunkedEmitter(reporter, c.ChunkedMaxBuffer, c.ChunkedMaxBatch)
		chunkedEmitter.CollectPeriod = c.ChunkedCollectPeriod
		emitters = append(emitters, chunkedEmitter)
	}

	if len(emitters) == 1 {
		logs.Setup(emitters[0])
	} else {
		logs.Setup(emitters)
	}
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
