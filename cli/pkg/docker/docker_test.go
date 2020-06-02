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
	"fmt"
	"testing"

	"github.com/datastax/stargate/cli/pkg/config"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/filters"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type DockerSuite struct {
	suite.Suite
	client Client
}

func (suite *DockerSuite) SetupSuite() {
	client, err := NewClient()
	if err != nil {
		panic(1)
	}
	suite.client = client

	suite.TearDownTest()
}

func (suite *DockerSuite) TearDownTest() {
	suite.client.Remove(config.CassandraContainerName())
	suite.client.Remove(config.StargateContainerName())
	suite.client.cli.NetworkRemove(suite.client.ctx, "stargate")
}

const validHost = "http://localhost:8080"
const validPath = "../../../src/main/resources/schema.conf"

//host of "" means docker.io
var host = ""

func (suite *DockerSuite) TestGetNameWithVersionBadInput() {
	name, err := suite.client.GetNameWithVersion("not-an-image")
	assert.NotNil(suite.T(), err)
	assert.Empty(suite.T(), name)
}

func (suite *DockerSuite) TestGetNameWithVersion() {
	name, err := suite.client.GetNameWithVersion(config.CassandraImage())
	assert.Nil(suite.T(), err)
	assert.Contains(suite.T(), name, "cassandra")
}

func (suite *DockerSuite) TestEnsureNetworkWithNoNetwork() {
	suite.client.cli.NetworkRemove(suite.client.ctx, config.StargateImage())
	err := suite.client.EnsureNetwork()
	assert.Nil(suite.T(), err)
}

func (suite *DockerSuite) TestEnsureNetworkWithNetwork() {
	err := suite.client.EnsureNetwork()
	assert.Nil(suite.T(), err)
	err = suite.client.EnsureNetwork()
	assert.Nil(suite.T(), err)
}

func (suite *DockerSuite) TestEnsureImageBadHost() {
	image, err := suite.client.EnsureImage("example.com", config.CassandraImage())
	assert.Empty(suite.T(), image)
	assert.NotNil(suite.T(), err)
}

func (suite *DockerSuite) TestEnsureImageBadImage() {
	_, err := suite.client.EnsureImage(host, "not_a-real_image-name_stargate-Test")
	assert.NotNil(suite.T(), err)
}
func (suite *DockerSuite) TestEnsureImageNoImage() {
	suite.client.cli.ImageRemove(suite.client.ctx, config.CassandraImage(), types.ImageRemoveOptions{
		Force: true,
	})
	image, err := suite.client.EnsureImage(host, config.CassandraImage())
	assert.Contains(suite.T(), image, "cassandra")
	assert.Nil(suite.T(), err)
}
func (suite *DockerSuite) TestEnsureImageWithImage() {
	image, err := suite.client.EnsureImage(host, config.CassandraImage())
	assert.Contains(suite.T(), image, "cassandra")
	assert.Nil(suite.T(), err)
	image, err = suite.client.EnsureImage(host, config.CassandraImage())
	assert.Contains(suite.T(), image, "cassandra")
	assert.Nil(suite.T(), err)
}

func (suite *DockerSuite) TestEnsureImageWithImageService() {
	image, err := suite.client.EnsureImage(host, config.StargateImage())
	assert.Contains(suite.T(), image, "stargate")
	assert.Nil(suite.T(), err)
	image, err = suite.client.EnsureImage(host, config.StargateImage())
	assert.Contains(suite.T(), image, "stargate")
	assert.Nil(suite.T(), err)
}

func (suite *DockerSuite) TestEnsureImageForLocalStargate() {
	image, err := suite.client.EnsureImage(host, config.StargateImage())
	assert.Nil(suite.T(), err)
	assert.Equal(suite.T(), "datastax/stargate:v0.1.1", image)
}

func (suite *DockerSuite) TestStartCassandraNoInput() {
	err := suite.client.StartCassandra(&StartCassandraOptions{})
	assert.NotNil(suite.T(), err)
}

func (suite *DockerSuite) TestStartCassandra() {
	err := suite.client.StartCassandra(&StartCassandraOptions{
		DockerImageHost: host,
		ImageName:       config.CassandraImage(),
	})
	assert.Nil(suite.T(), err)
}
func (suite *DockerSuite) TestStartServiceNoInput() {
	err := suite.client.StartService(&StartServiceOptions{})
	assert.NotNil(suite.T(), err)
}

func (suite *DockerSuite) TestStartService() {
	err := suite.client.StartService(&StartServiceOptions{
		ExposedPorts:    []string{"8080"},
		DockerImageHost: host,
		ImageName:       config.StargateImage(),
	})
	assert.Nil(suite.T(), err)
}
func (suite *DockerSuite) TestStop() {
	err := suite.client.StartService(&StartServiceOptions{
		ExposedPorts:    []string{"8080"},
		DockerImageHost: host,
		ImageName:       config.StargateImage(),
	})
	assert.Nil(suite.T(), err)

	err = suite.client.Stop(config.StargateContainerName())
	assert.Nil(suite.T(), err)

	args := filters.NewArgs()
	args.Add("name", config.StargateContainerName())
	containers, err := suite.client.cli.ContainerList(suite.client.ctx, types.ContainerListOptions{
		All:     true,
		Filters: args,
	})

	assert.Nil(suite.T(), err)

	stargateContainerState := ""
	//api quirk
	expectedContainerName := fmt.Sprintf("/%s", config.StargateContainerName())
	for _, c := range containers {
		for _, containerName := range c.Names {
			fmt.Println(containerName)
			if containerName == expectedContainerName {
				stargateContainerState = c.State
				break
			}
		}
	}
	assert.Equal(suite.T(), "exited", stargateContainerState)
}

func TestDocker(t *testing.T) {
	suite.Run(t, new(DockerSuite))
}
