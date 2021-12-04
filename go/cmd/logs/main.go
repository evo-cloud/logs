package main

import (
	"os"
	"strconv"

	"github.com/spf13/cobra"

	"github.com/evo-cloud/logs/go/config"
)

var logsConfig config.Config

func intFromEnv(name string, defaultVal int) int {
	str := os.Getenv(name)
	if str == "" {
		return defaultVal
	}
	if val, err := strconv.Atoi(str); err == nil {
		return val
	}
	return defaultVal
}

func main() {
	cmd := cobra.Command{
		Use:          "logs COMMAND ...",
		SilenceUsage: true,
	}
	logsConfig.SetupFlagsWith(cmd.PersistentFlags())
	cmd.AddCommand(cmdCat(), cmdHub(), cmdGen())
	cmd.Execute()
}
