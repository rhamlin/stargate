// Copyright DataStax, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"github.com/datastax/stargate/cli/pkg/docker"

	"github.com/spf13/cobra"
)

// devCmd represents the apply command
var devCmd = &cobra.Command{
	Short:   "Start a local, dockerized service",
	Long:    `Start a local, dockerized service`,
	Use:     "dev (start|stop|remove)",
	Example: "stargate service start",
}

var stopDevCmd = &cobra.Command{
	Short:   "Stop a local stargate service",
	Long:    `Stop a local stargate service`,
	Use:     "stop",
	Example: "stargate service stop",
	Args:    cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		client, err := docker.NewClient()
		if err != nil {
			cmd.PrintErrln(err)
			return
		}
		if withCassandra {
			err = client.Stop("cassandra")
			if err != nil {
				cmd.PrintErrln(err)
				return
			}
		}
		err = client.Stop("service")
		if err != nil {
			cmd.PrintErrln(err)
		} else {
			cmd.Println("Success!")
		}
	},
}

var removeDevCmd = &cobra.Command{
	Short:   "Force remove a local stargate service",
	Long:    `Force remove a local stargate service`,
	Use:     "remove",
	Example: "stargate service remove",
	Args:    cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		client, err := docker.NewClient()
		if err != nil {
			cmd.PrintErrln(err)
			return
		}
		if withCassandra {
			err = client.Remove("cassandra")
			if err != nil {
				cmd.PrintErrln(err)
				return
			}
		}
		err = client.Remove("service")
		if err != nil {
			cmd.PrintErrln(err)
		} else {
			cmd.Println("Success!")
		}
	},
}

var startDevCmd = &cobra.Command{
	Short:   "Start a local, dockerized service server",
	Long:    `Start a local, dockerized service server`,
	Use:     "start [NAME] [PATH]",
	Example: "stargate service start",
	Args:    cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		client, err := docker.NewClient()
		if err != nil {
			cmd.PrintErrln(err)
			return
		}

		err = client.StartCassandra(&docker.StartCassandraOptions{
			DockerImageHost: "docker.io/library/",
			ImageName:       "cassandra",
		})
		if err != nil {
			cmd.PrintErrln(err)
			return
		}

		err = client.StartService(&docker.StartServiceOptions{
			CassandraURL:    "stargate-cassandra",
			ExposedPorts:    []string{"8080"},
			DockerImageHost: "docker.io/",
			ImageName:       "service",
		})

		if err != nil {
			cmd.PrintErrln(err)
			return
		}
		cmd.Println("Services started!")

		WatchCmd.Run(cmd, append(args, "http://localhost:8080"))
	},
}

func init() {
	rootCmd.AddCommand(devCmd)

	devCmd.AddCommand(stopDevCmd)
	devCmd.AddCommand(removeDevCmd)
	devCmd.AddCommand(startDevCmd)
}
