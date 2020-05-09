Getting started in three easy steps.

Getting a microservice and cassandra off the ground can be a big task. Stargate makes this as easy as three steps.

Prerequisites: You must have **docker** to use stargate locally.

1. Create your schema
    ```
    echo 'entities {
      Todo {
          fields {
              todo: string
              isComplete: boolean
          }
      }
    }
    queryConditions: {
      Todo: [
      ]
    }' > stargate.conf
    ```
2. Install and Start Stargate
    - <a name="tempworkflow">temporary workflow</a>, we know this is suboptimal   
        * make sure you are part of the datastax organization on dockerhub and login with: `docker login --username $DOCKER_USERNAME`
        * `./localstack.sh` from the repo root to start cassandra and stargate docker containers
        * `stargateIp=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' stargate)` to get the ip of the container
        * `curl "${stargateIp}:8080/myNamespace" -H "content-type: application/hocon" --data-binary "@./stargate.conf"` to create a db named 'myNamespace'
    
3. Query the database

Create a Todo:
```sh
curl -X POST http://${stargateIp}:8080/myNamespace/Todo -H "content-type: application/json" -d'
{ 
 "todo": "Get stargate running",
 "isComplete": false
}
'
```

Get todos:
```sh
curl -X GET http://${stargateIp}:8080/myNamespace/Todo -H "content-type: application/json" -d'
{ 
 "-match": "all"
}
'
```

Update todo:
```sh
# replace "ce955224-1e85-41d8-946f-12ad201b83b9" with whichever entity you want to modify
curl -X PUT http://${stargateIp}:8080/myNamespace/Todo -H "content-type: application/json" -d'
{ 
 "-match": ["entityId", "=", "ce955224-1e85-41d8-946f-12ad201b83b9"],
 "isComplete": true
}
'
```
