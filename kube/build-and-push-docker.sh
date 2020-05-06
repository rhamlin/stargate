#!/bin/bash

#   Copyright DataStax, Inc.
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#   http://www.apache.org/licenses/LICENSE-2.0
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

script_path="./kube/build-and-push-docker.sh" 
if [ ! "$BASH_SOURCE" == "$script_path" ]; then
    echo "script must be ran as: $script_path"
    exit 1 
fi

./kube/build-docker.sh

image="stargate:1.0"
aws_ecr="592537962371.dkr.ecr.us-east-2.amazonaws.com"

docker tag "${image}" "${aws_ecr}/${image}"
docker push "${aws_ecr}/${image}"

