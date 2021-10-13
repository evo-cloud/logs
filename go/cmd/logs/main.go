package main

import (
	"os"
	"strconv"

	"github.com/spf13/cobra"
)

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
		Use: "logs COMMAND ...",
	}
	cmd.AddCommand(cmdCat())
	cmd.Execute()
}
