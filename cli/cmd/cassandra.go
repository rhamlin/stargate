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
	"fmt"
	"strings"

	"github.com/datastax/stargate/cli/pkg/docker"

	"github.com/spf13/cobra"
)

// cassandraCmd represents the apply command
var cassandraCmd = &cobra.Command{
	Short:   "Start a local, dockerized cassandra server",
	Long:    `Start a local, dockerized cassandra server`,
	Use:     "cassandra (start|stop|remove)",
	Example: "stargate cassandra start",
	Args:    cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		command := strings.ToLower(args[0])
		var err error
		switch command {
		case "start":
			err = docker.Start("docker.io/library/", "cassandra", "")
		case "stop":
			err = docker.Stop("cassandra")
		case "remove":
			err = docker.Remove("cassandra")
		}
		if err == nil {
			fmt.Println("Success!")
		} else {
			fmt.Println(err)
		}
	},
}

func init() {
	rootCmd.AddCommand(cassandraCmd)
}
