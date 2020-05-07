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
          ["isComplete", "="]
      ]
    }' > stargate.conf
    ```
2. Install and Start Stargate
    - **future workflow**, cli binaries not available yet
        * `brew install stargate`
        * `stargate dev start myNamespace stargate.conf`
    
    - <a name="tempworkflow">temporary workflow</a>   
        * `./localstack.sh` from the repo root to start cassandra and stargate docker containers
        * `stargateIp=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' stargate)` to get the ip of the container
        * `curl "${stargateIp}:8080/test" -H "content-type: multipart/form-data" --data-binary "@./stargate.conf"` to create a db named 'test'
    
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