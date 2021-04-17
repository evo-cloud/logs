package main

import (
	"context"
	"flag"

	"github.com/evo-cloud/logs/go/config"
	"github.com/evo-cloud/logs/go/logs"
)

var (
	logConfig = config.Default()
)

func init() {
	logConfig.SetupFlags()
}

func main() {
	flag.Parse()
	logConfig.MustSetupDefaultLogger()

	logs.Printf("Hello")

	ctx := context.Background()
	_, log := logs.StartSpan(ctx, "span1")
	log.Infof("Some event")
	log.Errorf("Go an error")
	log.Warningf("This is a warning")
	log.EndSpan()

	ctx1, log := logs.StartSpan(ctx, "span2")
	log.Infof("Some work starts")
	_, log2 := logs.StartSpan(ctx1, "sub task1", logs.Str("id", "sub"))
	log2.Criticalf("This is critical situation!")
	log2.EndSpan()
	log.Infof("Let's continue")
	log.EndSpan()
}
