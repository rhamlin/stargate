package upload

import (
	"testing"
	"time"

	"github.com/datastax/stargate/cli/pkg/docker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type UploadSuite struct {
	suite.Suite
	client docker.Client
}

func (suite *UploadSuite) SetupSuite() {
	client, err := docker.NewClient()
	if err != nil {
		panic(1)
	}
	err = client.StartCassandra(&docker.StartCassandraOptions{
		DockerImageHost: "docker.io/library/",
		ImageName:       "cassandra",
	})
	if err != nil {
		panic(1)
	}
	err = client.StartService(&docker.StartServiceOptions{
		CassandraURL:    "stargate-cassandra",
		ExposedPorts:    []string{"8080"},
		DockerImageHost: "docker.io/",
		ImageName:       "service",
	})
	if err != nil {
		panic(1)
	}
	suite.client = client
	time.Sleep(15 * time.Second)
}

func (suite *UploadSuite) TearDownSuite() {
	suite.client.Remove("service")
	suite.client.Remove("cassandra")
}

const validHost = "http://localhost:8080/test"
const validPath = "../../../src/main/resources/schema.conf"

func (suite *UploadSuite) TestInvalidPathInput() {
	err := Upload("", validHost)
	assert.NotNil(suite.T(), err)
}

func (suite *UploadSuite) TestMalformedHostInput() {
	err := Upload(validPath, "\n")
	assert.NotNil(suite.T(), err)
}

func (suite *UploadSuite) TestBadHostInput() {
	err := Upload(validPath, "")
	assert.NotNil(suite.T(), err)
}

func (suite *UploadSuite) TestBadPathInput() {
	err := Upload("./upload_test.go", validHost)
	assert.NotNil(suite.T(), err)
}

func (suite *UploadSuite) TestValidInput() {
	err := Upload(validPath, validHost)
	assert.Nil(suite.T(), err)
}

func TestUpload(t *testing.T) {
	suite.Run(t, new(UploadSuite))
}
