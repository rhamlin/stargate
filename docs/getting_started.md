Getting started in three easy steps.

Getting a microservice and cassandra off the ground can be a big task. Stargate makes this as easy as three steps.

Prerequisites: You must have **docker** to use stargate locally.

1. Create your schema
```
echo "entities {
  Todo {
      fields {
          todo: string
          isComplete: boolean
      }
      relations {
      
      }
  }
}
queries: {
  Todo: [
      [\"isComplete\", \"=\"]
  ]
}" > stargate.conf
```
2. Install and Start Stargate
    * `brew install stargate`
    * `stargate dev start myNamespace stargate.conf`
    
3. Query the database

Create a Todo:
```sh
curl -X POST http://localhost:8080/myNamespace/Todo \
-H "content-type: application/json" -d'
{ 
 "todo": "Get stargate running",
 "isComplete": false
}
'
```

Get todos:
```sh
curl -X GET http://localhost:8080/myNamespace/Todo \
-H "content-type: application/json" -d'
{ 
 "-match": "all"
}
'
```

Update todo:
```sh
curl -X PUT http://localhost:8080/myNamespace/Todo \
-H "content-type: application/json" -d'
{ 
 "-match": ["entityId", "=", "?"],
 "isComplete": true
}
'
```