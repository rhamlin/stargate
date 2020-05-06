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

script_path="./kube/build-docker.sh" 
if [ ! "$BASH_SOURCE" == "$script_path" ]; then
    echo "script must be ran as: $script_path"
    exit 1 
fi


date_suffix=$(date "+%y%m%d-%H%M%S")
clone_path="./stargate-clone-${date_suffix}"

git clone --depth 1 file:///`pwd` "$clone_path"
ln -vs $clone_path stargate-clone
pushd "$clone_path"
mvn package
if [ $? -ne 0 ]; then
    exit 1
fi
popd

docker build "." -f "./kube/Dockerfile" -t "stargate:1.0"

rm -f "./stargate-clone"
rm -fr $clone_path
