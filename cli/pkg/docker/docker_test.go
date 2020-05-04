package docker

import (
	"testing"

	"github.com/docker/docker/api/types"
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
}

const validHost = "http://localhost:8080"
const validPath = "../../../src/main/resources/schema.conf"

func (suite *DockerSuite) TestEnsureNetworkWithNoNetwork() {
	suite.client.cli.NetworkRemove(suite.client.ctx, "stargate")
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
	image, err := suite.client.EnsureImage("example.com", "cassandra")
	assert.Empty(suite.T(), image)
	assert.NotNil(suite.T(), err)
}

func (suite *DockerSuite) TestEnsureImageBadImage() {
	_, err := suite.client.EnsureImage("docker.io/library/", "steve-donnelly-is-a-dingus")
	assert.NotNil(suite.T(), err)
}
func (suite *DockerSuite) TestEnsureImageNoImage() {
	suite.client.cli.ImageRemove(suite.client.ctx, "cassandra", types.ImageRemoveOptions{
		Force: true,
	})
	image, err := suite.client.EnsureImage("docker.io/library/", "cassandra")
	assert.Equal(suite.T(), "cassandra", image)
	assert.Nil(suite.T(), err)
}
func (suite *DockerSuite) TestEnsureImageWithImage() {
	image, err := suite.client.EnsureImage("docker.io/library/", "cassandra")
	assert.Equal(suite.T(), "cassandra", image)
	assert.Nil(suite.T(), err)
	image, err = suite.client.EnsureImage("docker.io/library/", "cassandra")
	assert.Equal(suite.T(), "cassandra", image)
	assert.Nil(suite.T(), err)
}

func (suite *DockerSuite) TestEnsureImageForLocalStargate() {
	image, err := suite.client.EnsureImage("docker.io/library/", "service")
	assert.Equal(suite.T(), "stargate:1.0", image)
	assert.Nil(suite.T(), err)
}

func TestDocker(t *testing.T) {
	suite.Run(t, new(DockerSuite))
}
