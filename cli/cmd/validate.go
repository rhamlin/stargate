package cmd

import (
	"os"

	"github.com/spf13/cobra"
)

// applyCmd represents the apply command
var validateCmd = &cobra.Command{
	Short:   "Validate schema",
	Long:    `Validate schema`,
	Use:     "validate [path] [host]",
	Example: "stargate validate ./todo.conf http://server.stargate.com:8080",
	Args:    cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		path := args[0]
		var url string
		if len(args) == 2 {
			url = args[1]
		}
		err := Apply(cmd, "validate", path, url)
		if err != nil {
			cmd.PrintErrln(err)
			os.Exit(1)
		}
		cmd.Println("No errors found! 🎉")
	},
}

func init() {
	rootCmd.AddCommand(validateCmd)
}
