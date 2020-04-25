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
}

func init() {
	rootCmd.AddCommand(cassandraCmd)

	cassandraCmd.AddCommand(&cobra.Command{
		Short:   "Stop a local cassandra instance",
		Long:    `Stop a local cassandra instance`,
		Use:     "stop",
		Example: "stargate cassandra stop",
		Args:    cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			client, err := docker.NewClient()
			if err != nil {
				cmd.PrintErrln(err)
			}
			err = client.Stop("cassandra")
			if err != nil {
				cmd.PrintErrln(err)
			} else {
				cmd.Println("Success!")
			}
		},
	})

	cassandraCmd.AddCommand(&cobra.Command{
		Short:   "Force remove a local cassandra instance",
		Long:    `Force remove a local cassandra instance`,
		Use:     "remove",
		Example: "stargate service remove",
		Args:    cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			client, err := docker.NewClient()
			if err != nil {
				cmd.PrintErrln(err)
			}
			err = client.Remove("cassandra")
			if err != nil {
				cmd.PrintErrln(err)
			} else {
				cmd.Println("Success!")
			}
		},
	})

	cassandraCmd.AddCommand(&cobra.Command{
		Short:   "Start a local, dockerized cassandra instance",
		Long:    `Start a local, dockerized cassandra instance`,
		Use:     "start",
		Example: "stargate cassandra start",
		Args:    cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			client, err := docker.NewClient()
			if err != nil {
				cmd.PrintErrln(err)
			}
			err = client.StartCassandra(&docker.StartCassandraOptions{
				DockerImageHost: "docker.io/library/",
				ImageName:       "cassandra",
			})
			if err != nil {
				cmd.PrintErrln(err)
			} else {
				cmd.Println("Success!")
			}
		},
	})
}
