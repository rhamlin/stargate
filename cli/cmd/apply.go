/*
Copyright © 2020 NAME HERE <EMAIL ADDRESS>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"log"
	"os"

	"github.com/PuerkitoBio/purell"
	"github.com/datastax/stargate/cli/pkg/upload"

	"github.com/spf13/cobra"
)

// Apply sends a schema to server and print output for a user
func Apply(name string, path string, url string, showDate bool) bool {
	if !showDate {
		log.SetPrefix("")
	}

	if url == "" {
		url = upload.Host
	}

	_, err := upload.Upload(path, url+"/"+name)

	errored := err != nil

	if errored {
		log.Println("Failed to apply schema!")
		log.Println(err.Error())
	} else {
		endpointURL := purell.MustNormalizeURLString(url+"/"+name, purell.FlagsUnsafeGreedy)
		log.Println("Endpoint created at", endpointURL)
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
		if !Apply(name, path, url, false) {
			os.Exit(1)
		}
	},
}

func init() {
	rootCmd.AddCommand(applyCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// uploadCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// uploadCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
