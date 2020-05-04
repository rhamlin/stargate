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

var stopCassandraCmd = &cobra.Command{
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
}

var removeCassandraCmd = &cobra.Command{
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
}

var startCassandraCmd = &cobra.Command{
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
}

func init() {
	rootCmd.AddCommand(cassandraCmd)

	cassandraCmd.AddCommand(stopCassandraCmd)
	cassandraCmd.AddCommand(removeCassandraCmd)
	cassandraCmd.AddCommand(startCassandraCmd)
}
