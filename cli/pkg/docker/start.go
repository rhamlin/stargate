package docker

import (
	"context"
	"strings"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
)

// Start running docker image
func Start(url, image, portValue string) error {
	ctx := context.Background()
	cli, err := client.NewEnvClient()
	if err != nil {
		return err
	}

	name := "stargate-" + image

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

	err = cli.ContainerStart(ctx, name, types.ContainerStartOptions{})
	if err == nil {
		return nil
	}

	reader, err := cli.ImagePull(ctx, url+image, types.ImagePullOptions{})
	// This block exists because we're currently building the stargate-service image locally and will be removed when the image is available
	if err != nil {
		summary, err2 := cli.ImageList(ctx, types.ImageListOptions{})
		if err2 != nil {
			return err
		}
		for _, r := range summary {
			if len(r.RepoTags) > 0 && strings.Index(r.RepoTags[0], "stargate:") == 0 {
				image = r.RepoTags[0]
			}
		}
	} else {
		defer reader.Close()
	}

	config := container.Config{
		Image: image,
	}

	hostConfig := container.HostConfig{}

	if portValue != "" {
		port, err := nat.NewPort("tcp", portValue)
		if err != nil {
			return err
		}
		config.ExposedPorts = nat.PortSet{
			port: {},
		}
		hostConfig.PortBindings = nat.PortMap{
			port: []nat.PortBinding{
				{HostPort: port.Port()},
			},
		}
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
