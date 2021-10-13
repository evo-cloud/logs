package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"golang.org/x/crypto/ssh/terminal"

	"github.com/evo-cloud/logs/go/emitters/console"
	"github.com/evo-cloud/logs/go/source"
)

var (
	catInput    string
	catColorful bool
	fullTraceID bool

	maxStrAttrLen = intFromEnv("LOGS_CAT_MAX_STR_ATTR", 80)
	maxBinAttrLen = intFromEnv("LOGS_CAT_MAX_BIN_ATTR", 8)
	maxPathLen    = intFromEnv("LOGS_CAT_MAX_PATH", 20)
)

func cmdCat() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cat FILTERS...",
		Short: "Cat logs with filters.",
		RunE:  runCat,
	}
	cmd.Flags().StringVarP(
		&catInput,
		"in", "i",
		"",
		"Specify the input of logs, filename or - for STDIN.",
	)
	cmd.Flags().BoolVar(
		&catColorful,
		"color",
		true,
		"Print with color.",
	)
	cmd.Flags().IntVar(
		&maxStrAttrLen,
		"max-str-attr",
		maxStrAttrLen,
		"Max length of string attributes.",
	)
	cmd.Flags().IntVar(
		&maxBinAttrLen,
		"max-bin-attr",
		maxBinAttrLen,
		"Max length of binary attributes.",
	)
	cmd.Flags().IntVar(
		&maxPathLen,
		"max-path",
		maxPathLen,
		"Max length of paths.",
	)
	cmd.Flags().BoolVar(
		&fullTraceID,
		"full-traceid",
		false,
		"Display full trace IDs.",
	)
	return cmd
}

func runCat(cmd *cobra.Command, args []string) error {
	filters, err := source.ParseFilters(args...)
	if err != nil {
		return err
	}
	var in io.Reader = os.Stdin
	if catInput != "" && catInput != "-" {
		f, err := os.Open(catInput)
		if err != nil {
			return fmt.Errorf("open %q: %w", catInput, err)
		}
		defer f.Close()
		in = f
	}
	reader := &source.StreamReader{In: in, SkipErrors: true}
	printer := console.NewPrinter(os.Stdout)
	printer.MaxStrAttrLen = maxStrAttrLen
	printer.MaxBinAttrLen = maxBinAttrLen
	printer.MaxPathLen = maxPathLen
	if fullTraceID {
		printer.ShortenTraceID = false
	}
	if catColorful {
		if terminal.IsTerminal(int(os.Stdout.Fd())) {
			printer.UseColor(true)
		}
	}
	printer.DisplaySpanNames()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	for {
		entry, err := reader.Read(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		if entry == nil {
			break
		}
		spanRec := printer.RecordSpanEvent(entry)
		if filters == nil || filters.FilterLogEntry(entry) {
			printer.EmitLogEntry(entry)
		}
		spanRec.Done()
	}
	return nil
}
