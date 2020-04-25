package docker

import (
	"github.com/docker/docker/api/types"
)

// EnsureNetwork checks to see if the stargate bridge network exists and creates it if it doesnt
func (client *Client) EnsureNetwork() error {
	cli := client.cli
	ctx := client.ctx

	networks, err := cli.NetworkList(ctx, types.NetworkListOptions{})
	if err != nil {
		return err
	}

	networkFound := false

	for _, n := range networks {
		if n.Name == "stargate" {
			networkFound = true
			break
		}
	}
	if !networkFound {
		_, err := cli.NetworkCreate(ctx, "stargate", types.NetworkCreate{})
		if err != nil {
			return err
		}
	}

	return nil
}
