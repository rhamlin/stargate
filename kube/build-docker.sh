#!/bin/bash

script_path="./kube/build-docker.sh" 
if [ ! "$BASH_SOURCE" == "$script_path" ]; then
    echo "script must be ran as: $script_path"
    exit 1 
fi


date_suffix=$(date "+%y%m%d-%H%M%S")
clone_path="./stargate-clone-${date_suffix}"

git clone "." "$clone_path"
pushd "$clone_path"
mvn package
if [ $? -ne 0 ]; then
    exit 1
fi
popd
ln -s "$clone_path" "./stargate-clone"

docker build "." -f "./kube/Dockerfile" -t "stargate:1.0"

rm -f "./stargate-clone"



