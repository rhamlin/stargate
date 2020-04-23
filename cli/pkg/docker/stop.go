package docker

import (
	"context"
	"time"

	"github.com/docker/docker/client"
)

// Stop running docker image
func Stop(image string) error {
	ctx := context.Background()
	cli, err := client.NewEnvClient()
	if err != nil {
		return err
	}

	t, err := time.ParseDuration("3s")
	if err != nil {
		return err
	}

	return cli.ContainerStop(ctx, "stargate-"+image, &t)
}
