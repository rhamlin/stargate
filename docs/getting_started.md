# Getting started in three easy steps.

Getting a microservice and cassandra off the ground can be a big task. Stargate makes this as easy as three steps.

Prerequisites: You must have **docker** to use stargate locally.

1. Create an example schema configuration file
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
        ["todo", "="]
      ]
    }' > stargate.conf
    ```

2. Download the stargate CLI from https://github.com/datastax/stargate/releases to your current working directory and run
```sh
tar -xzf ./stargate*.tar.gz
./stargate service start --with-cassandra 
./stargate apply myNamespace stargate.conf
```
to start up local cassandra and stargate instances, creating a database named `myNamespace` from the configuration in step 1.
   
    
3. Query the database

Create a Todo:
```sh
curl -X POST "http://localhost:8080/v1/api/myNamespace/query/entity/Todo" \
     -H "content-type: application/json" -d'
{ 
 "todo": "Get stargate running",
 "isComplete": false
}
' > ./createResponse.out
cat ./createResponse.out
```

Get todos:
```sh
curl -X GET "http://localhost:8080/v1/api/myNamespace/query/entity/Todo" \
     -H "content-type: application/json" -d'
{ 
 "-match": "all"
}
'
```

Update todo:
```sh
todoId=$(cat ./createResponse.out | jq -r .[0].entityId)
curl -X PUT "http://localhost:8080/v1/api/myNamespace/query/entity/Todo" \
     -H "content-type: application/json" -d'
{ 
 "-match": ["entityId", "=", "'"${todoId}"'"],
 "isComplete": true
}
'
```
