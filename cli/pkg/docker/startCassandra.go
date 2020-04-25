package docker

import (
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
)

// StartCassandraOptions defines the input of Client.Start
type StartCassandraOptions struct {
	DockerImageHost string
	ImageName       string
}

// StartCassandra running docker image
func (client *Client) StartCassandra(opts *StartCassandraOptions) error {
	cli := client.cli
	ctx := client.ctx

	name := "stargate-" + opts.ImageName

	err := client.EnsureNetwork()
	if err != nil {
		return err
	}

	client.Remove(opts.ImageName)

	image, err := client.EnsureImage(opts.DockerImageHost, opts.ImageName)
	if err != nil {
		return err
	}

	config := container.Config{
		Image: image,
	}

	hostConfig := container.HostConfig{
		// PublishAllPorts: true,
	}

	networkConfig := network.NetworkingConfig{
		EndpointsConfig: map[string]*network.EndpointSettings{
			"stargate": {NetworkID: "stargate"},
		},
	}

	resp, err := cli.ContainerCreate(ctx, &config, &hostConfig, &networkConfig, name)
	if err != nil {
		return err
	}

	return cli.ContainerStart(ctx, resp.ID, types.ContainerStartOptions{})
}
