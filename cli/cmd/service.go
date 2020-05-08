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
	"errors"

	"github.com/datastax/stargate/cli/pkg/docker"

	"github.com/spf13/cobra"
)

// ServiceCmd is the parent command for controlling a local stargate instance
var ServiceCmd = &cobra.Command{
	Short:   "Start a local, dockerized service",
	Long:    `Start a local, dockerized service`,
	Use:     "service (start|stop|remove)",
	Example: "stargate service start",
}

var withCassandra bool

// StopServiceCmd stops a local instance
var StopServiceCmd = &cobra.Command{
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

// RemoveServiceCmd force removes a local instance
var RemoveServiceCmd = &cobra.Command{
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

// StartServiceCmd starts a local instance
var StartServiceCmd = &cobra.Command{
	Short:   "Start a local, dockerized service server",
	Long:    `Start a local, dockerized service server`,
	Use:     "start [CASSANDRA_HOST]",
	Example: "stargate service start",
	Args:    cobra.MaximumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		client, err := docker.NewClient()
		if err != nil {
			cmd.PrintErrln(err)
			return
		}
		if withCassandra && len(args) == 1 {
			cmd.PrintErrln(errors.New("If you are starting with --with-cassandra, you should not specify a cassandra host"))
			return
		}
		cassandraURL := "stargate-cassandra"
		if len(args) == 1 {
			cassandraURL = args[0]
		}

		if withCassandra {
			err = client.StartCassandra(&docker.StartCassandraOptions{
				DockerImageHost: "docker.io/library/",
				ImageName:       "cassandra",
			})
			if err != nil {
				cmd.PrintErrln(err)
				return
			}
		}

		err = client.StartService(&docker.StartServiceOptions{
			CassandraURL:    cassandraURL,
			ExposedPorts:    []string{"8080"},
			DockerImageHost: "docker.io/",
			ImageName:       "datastax/stargate",
		})
		if err != nil {
			cmd.PrintErrln(err)
		} else {
			cmd.Println("Success!")
		}
	},
}

func init() {
	rootCmd.AddCommand(ServiceCmd)

	ServiceCmd.AddCommand(StopServiceCmd)
	ServiceCmd.AddCommand(RemoveServiceCmd)
	ServiceCmd.AddCommand(StartServiceCmd)

	ServiceCmd.PersistentFlags().BoolVarP(&withCassandra, "with-cassandra", "c", false, "withCassandra output")
}
