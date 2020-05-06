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

	hostConfig := container.HostConfig{}

	networkConfig := network.NetworkingConfig{
		EndpointsConfig: map[string]*network.EndpointSettings{
			"stargate": {NetworkID: "stargate"},
		},
	}

	name := "stargate-" + opts.ImageName

	resp, err := cli.ContainerCreate(ctx, &config, &hostConfig, &networkConfig, name)
	if err != nil {
		return err
	}

	return cli.ContainerStart(ctx, resp.ID, types.ContainerStartOptions{})
}
