package docker

import (
	"github.com/docker/docker/api/types"
)

// Remove stopped docker container
func (client *Client) Remove(container string) error {
	cli := client.cli
	ctx := client.ctx
	return cli.ContainerRemove(ctx, "stargate-"+container, types.ContainerRemoveOptions{
		RemoveVolumes: false,
		RemoveLinks:   false,
		Force:         true,
	})
}
