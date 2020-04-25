package docker

import (
	"strings"

	"github.com/docker/docker/api/types"
)

// EnsureImage makes sure that the image we need is present and returns the correct name
func (client *Client) EnsureImage(dockerHost, image string) (string, error) {
	ctx := client.ctx
	cli := client.cli
	reader, err := cli.ImagePull(ctx, dockerHost+image, types.ImagePullOptions{})
	// This block exists because we're currently building the stargate-service image locally and will be removed when the image is available
	if err != nil {
		summary, err2 := cli.ImageList(ctx, types.ImageListOptions{})
		if err2 != nil {
			return "", err
		}
		for _, r := range summary {
			if len(r.RepoTags) > 0 && strings.Index(r.RepoTags[0], "stargate:") == 0 {
				image = r.RepoTags[0]
			}
		}
	} else {
		defer reader.Close()
	}
	return image, nil
}