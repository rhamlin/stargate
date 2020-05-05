package docker

import (
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/go-connections/nat"
)

// StartServiceOptions defines the input of Client.Start
type StartServiceOptions struct {
	CassandraURL    string
	ExposedPorts    []string
	DockerImageHost string
	ImageName       string
}

// StartService running docker image
func (client *Client) StartService(opts *StartServiceOptions) error {
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
		Image:        image,
		ExposedPorts: nat.PortSet{},
		Env: []string{
			`SG_CASS_CONTACT_POINTS=` + opts.CassandraURL + `:9042`,
		},
	}

	hostConfig := container.HostConfig{
		PortBindings: nat.PortMap{},
	}

	var empty struct{}
	for _, portValue := range opts.ExposedPorts {
		port, err := nat.NewPort("tcp", portValue)
		if err != nil {
			return err
		}
		config.ExposedPorts[port] = empty
		hostConfig.PortBindings[port] = []nat.PortBinding{
			{HostPort: port.Port()},
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
