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
)

// EnsureNetwork checks to see if the stargate bridge network exists and creates it if it doesnt
func (client *Client) EnsureNetwork() error {
	cli := client.cli
	ctx := client.ctx

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

	return nil
}
