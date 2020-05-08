#!/bin/bash

cassandraContainer="stargate-cassandra"
stargateContainer="stargate"

function containerStatus {
    status=$(docker inspect -f '{{.State.Status}}' $1 2>/dev/null)
    success=$?
    if [ $success -ne 0 ]; then echo 0; elif [ "$status" == "running" ]; then echo 1; else echo 2; fi
}	
function containerIp {
    docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $1
}
function waitForCassandra {
  probe=1; while [ $probe -ne 0 ]; do docker exec "$cassandraContainer" cqlsh; probe=$?; sleep 3; done
}


# start cassandra
status=$(containerStatus "$cassandraContainer")
if [ $status -eq 0 ]; then
    echo "starting cassandra in docker"
    docker run -d -P --name "$cassandraContainer" "cassandra:3.11.6"
    waitForCassandra
elif [ $status -eq 2 ]; then
    echo "restarting cassandra"
    docker restart "$cassandraContainer"
    waitForCassandra
else
    echo "container $cassandraContainer already running"
fi
cassandraIp=$(containerIp "$cassandraContainer")

status=$(containerStatus "$stargateContainer")
if [ $status -eq 0 ] || [ $status -eq 2 ]; then
    docker rm "$stargateContainer" 2>/dev/null
    echo "starting stargate in docker"
    docker run -d -P --name "$stargateContainer" -e "SG_CASS_CONTACT_POINTS=${cassandraIp}:9042" "datastax/stargate:latest"
    if [ $? -ne 0 ]; then
       echo "failed to start stargate, please ensure that youre logged into docker with: docker login --username \$DOCKER_USERNAME"
       exit 1
    fi
else
    echo "container $stargateContainer already running"
fi
stargateIp=$(containerIp "$stargateContainer")
echo "stargate listening at: $stargateIp:8080"


sleep 3
echo
echo "# posting example schema to namespace 'test' with:"
echo 'curl "'${stargateIp}':8080/test" -H "content-type: multipart/form-data" --data-binary "@src/main/resources/schema.conf"'
curl "${stargateIp}:8080/test" -H "content-type: multipart/form-data" --data-binary "@src/main/resources/schema.conf"
