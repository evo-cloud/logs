package main

import (
	"errors"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/icrowley/fake"
	"github.com/spf13/cobra"

	"github.com/evo-cloud/logs/go/logs"
)

var (
	genRatePerMinute = 80
	genMaxSpanDepth  = 3
	genNumWords      = 10
	genNumAttrSets   = 8
	genNumAttrs      = 6
	genExitInstantly = false
)

func genLogAttrs(r *rand.Rand, attrSet map[string]interface{}) logs.AttributeSetter {
	var setters logs.AttributeSetters
	for key, val := range attrSet {
		switch val.(type) {
		case string:
			setters = append(setters, logs.Str(key, fake.Word()))
		case int64:
			setters = append(setters, logs.Int(key, int64(r.Intn(100))))
		case bool:
			setters = append(setters, logs.Bool(key, r.Intn(2) > 0))
		}
	}
	return setters
}

func runGen(cmd *cobra.Command, args []string) error {
	logsConfig.MustSetupDefaultLogger()

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	avgDelayNS := int64(6e10) / int64(genRatePerMinute)
	avgDelayDrift := avgDelayNS / 2

	attrSets := make([]map[string]interface{}, genNumAttrSets)
	for n := range attrSets {
		attrSet := make(map[string]interface{})
		attrSets[n] = attrSet
		for m := 0; m < genNumAttrs+r.Intn(genNumAttrs*2/10)-genNumAttrs/10; m++ {
			key := fake.Word()
			switch r.Intn(3) {
			case 0:
				attrSet[key] = ""
			case 1:
				attrSet[key] = int64(0)
			case 2:
				attrSet[key] = false
			}
		}
	}
	spanStack := make([]*logs.Logger, genMaxSpanDepth+1)
	var spanAt int
	spanStack[spanAt] = logs.Default()

	ctx, cancel := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			if !genExitInstantly {
				logs.Warningf("EXITING")
				for ; spanAt > 1; spanAt-- {
					spanStack[spanAt].EndSpan()
				}
			}
			return nil
		case <-time.After(time.Duration(avgDelayNS+r.Int63n(avgDelayDrift*2)-avgDelayDrift) * time.Nanosecond):
		}

		logger := spanStack[spanAt]
		if r.Intn(10) > 6 {
			if spanAt+1 < len(spanStack) && r.Intn(2) > 0 {
				spanAt++
				spanStack[spanAt] = logger.StartSpan(logs.SpanInfo{Name: strings.ReplaceAll(fake.WordsN(2), " ", "/")}, genLogAttrs(r, attrSets[r.Intn(len(attrSets))]))
				continue
			}
			if spanAt > 1 {
				logger.EndSpan()
				spanAt--
				continue
			}
		}
		printer := logger.Printer(0)
		switch r.Intn(10) {
		case 2, 3, 4:
			printer = printer.Info()
		case 5, 6:
			printer = printer.Warning(errors.New(fake.WordsN(3)))
		case 7, 8:
			printer = printer.Error(errors.New(fake.WordsN(3)))
		case 9:
			printer = printer.Critical(errors.New(fake.WordsN(3)))
		}

		printer.Print(fake.WordsN(genNumWords))
	}
}

func cmdGen() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "gen",
		Short: "Generate random logs",
		RunE:  runGen,
	}

	cmd.Flags().IntVar(&genRatePerMinute, "rate", genRatePerMinute, "Generate rate: logs/min")
	cmd.Flags().IntVar(&genMaxSpanDepth, "max-span-depth", genMaxSpanDepth, "Maximum span depth")
	cmd.Flags().IntVar(&genNumWords, "num-words", genNumWords, "Number of words per log")
	cmd.Flags().IntVar(&genNumAttrSets, "num-attrsets", genNumAttrSets, "Number of attribute sets")
	cmd.Flags().IntVar(&genNumAttrs, "num-attrs", genNumAttrs, "Number of attributes per set")
	cmd.Flags().BoolVar(&genExitInstantly, "instant-exit", genExitInstantly, "Exit instantly without completing spans")
	return cmd
}
