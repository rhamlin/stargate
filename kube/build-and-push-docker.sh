#!/bin/bash

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

