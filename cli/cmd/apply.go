package cmd

import (
	"os"

	"github.com/PuerkitoBio/purell"
	"github.com/datastax/stargate/cli/pkg/upload"

	"github.com/spf13/cobra"
)

// Apply sends a schema to server and print output for a user
func Apply(cmd *cobra.Command, name string, path string, url string, showDate bool) bool {
	if url == "" {
		url = upload.Host
	}

	err := upload.Upload(path, url+"/"+name)

	errored := err != nil

	if errored {
		cmd.PrintErrln("Failed to apply schema!")
		cmd.PrintErrln(err.Error())
	} else {
		endpointURL := purell.MustNormalizeURLString(url+"/"+name, purell.FlagsUnsafeGreedy)
		cmd.Println("Endpoint created at", endpointURL)
	}
	return !errored
}

// applyCmd represents the apply command
var applyCmd = &cobra.Command{
	Short:   "Apply schema to a stargate server",
	Long:    `Apply schema to a stargate server`,
	Use:     "apply [name] [path] [host]",
	Example: "stargate apply todo ./todo.conf http://server.stargate.com:8080",
	Args:    cobra.MinimumNArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		name, path := args[0], args[1]
		var url string
		if len(args) == 3 {
			url = args[2]
		}
		if !Apply(cmd, name, path, url, false) {
			os.Exit(1)
		}
	},
}

func init() {
	rootCmd.AddCommand(applyCmd)
}
