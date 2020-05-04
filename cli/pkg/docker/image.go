package docker

import (
	"errors"
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
		if len(r.RepoTags) > 0 && strings.Index(r.RepoTags[0], image+":") == 0 {
			return r.RepoTags[0], nil
		}
	}
	return "", errors.New("Could not find a matching image")
}

// EnsureImage makes sure that the image we need is present and returns the correct name
func (client *Client) EnsureImage(dockerHost, image string) (string, error) {
	ctx := client.ctx
	cli := client.cli
	reader, err := cli.ImagePull(ctx, dockerHost+image, types.ImagePullOptions{})
	if err != nil {
		// This block exists because we're currently building the stargate-service image locally and will be removed when the image is available
		if image != "service" {
			return "", err
		}
		name, err2 := client.GetNameWithVersion("stargate")
		if err2 != nil {
			return "", err
		}
		return name, nil
	}
	defer reader.Close()
	name, err := client.GetNameWithVersion(image)
	if err != nil {
		return "", err
	}
	return name, nil
}
