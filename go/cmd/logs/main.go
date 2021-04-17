package main

import "github.com/spf13/cobra"

func main() {
	cmd := cobra.Command{
		Use: "logs COMMAND ...",
	}
	cmd.AddCommand(cmdCat())
	cmd.Execute()
}
