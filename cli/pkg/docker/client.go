package docker

import (
	"context"

	"github.com/docker/docker/client"
)

// Client is a container for a docker client
type Client struct {
	ctx context.Context
	cli *client.Client
}

// NewClient returns a new Cli struct
func NewClient() (Client, error) {
	ctx := context.Background()
	cli, err := client.NewEnvClient()
	if err != nil {
		return Client{}, err
	}

	return Client{ctx, cli}, nil
}
