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
	"os/exec"
	"strings"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/filters"
)

// GetNameWithVersion finds a full versioned name for an image
func (client *Client) GetNameWithVersion(image string) (string, error) {
	ctx := client.ctx
	cli := client.cli
	args := filters.NewArgs()
	args.Add("reference", image)
	summary, err := cli.ImageList(ctx, types.ImageListOptions{
		Filters: args,
	})
	if err != nil {
		return "", err
	}
	for _, r := range summary {
		if len(r.RepoTags) > 0 && strings.Index(r.RepoTags[0], image) == 0 {
			return r.RepoTags[0], nil
		}
	}
	return "", fmt.Errorf("Could not find a matching image named %s out of %#v images", image, summary)
}

// EnsureImage makes sure that the image we need is present and returns the correct name
func (client *Client) EnsureImage(dockerHost, imageName string) (string, error) {
	var fullImage = ""
	//dockerHost = "" signifies docker.io
	if dockerHost == "" {
		fullImage = imageName
	} else {
		fullImage = fmt.Sprintf("%s/%s", dockerHost, imageName)
	}
	fmt.Println("Pulling image:", fullImage)
	pullCmd := exec.Command("docker", "pull", fullImage)
	_, err := pullCmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("unable to pull '%s' due to '%s' from the docker command", fullImage, err.Error())
	}

	return client.GetNameWithVersion(imageName)
}
