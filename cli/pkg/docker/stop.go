package docker

import (
	"time"
)

// Stop running docker image
func (client *Client) Stop(container string) error {
	t, err := time.ParseDuration("3s")
	if err != nil {
		return err
	}

	cli := client.cli
	ctx := client.ctx

	return cli.ContainerStop(ctx, "stargate-"+container, &t)
}
