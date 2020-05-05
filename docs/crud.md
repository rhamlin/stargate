# CRUD
Stargate offers a request-optimized database microservices layer. After you create your [schema definition](schema_definition.md), you can call the database with standard REST conventions.

## URL Convention
URLs have the convention of `/{namespace}/{entity}/{operation}`. 

For example: These are all valid urls in the [Todo database](getting_started_todo_app.md):
- GET http://localhost:8080/todo/Todo
- POST http://localhost:8080/todo/Todo
- PUT http://localhost:8080/todo/Todo
- DELETE http://localhost:8080/todo/Todo
- POST http://localhost:8080/todo/continue

## Queries
Queries are any `GET` request on an entity's `get` path. Queries can be specified and called at runtime, called flexible queries, or defined upfront to give you a convenient API. Queries can accept the following flags:
* **Match** A query on an entity can have any match statement defined in the Query Condition block in the schema. Match flags can only be at the root level of the query. Match statements are **required** for queries.
* **Include** Which fields to include in the result. If nothing is specified, all scalar fields are returned. 
* **Limit** How many results should be returned.
* **Continue** Returns a paging token in the result
* **TTL** How long the paging token is valid (in seconds)

The following query returns a paginated list of the first 100 Todos with Title and Status fields that are matched with John Doe.
```sh
curl -X GET http://localhost:8080/todo/Todo \
-H "content-type: application/json" -d'
{
 "-match": ["user.username", "=", "John Doe"], 
 "-limit": 100,
 "-include": [ "title", "isComplete"] ,
 "-continue": true,
 "-ttl": 120,
 "user": {}
 }
}
'
```

### Predefined Queries
Queries can also be pre-H "content-type: application/json" \ -defined. This will give the rest URL with a special path called `q`. These are the queries that are defined in the [Queries Block](schema_definition.md).
The following flags are supported:
 - limit
 - continue
 - ttl

```sh
curl -X POST http://localhost:8080/todo/q/todoByUsername \
-H "content-type: application/json" -d'
{
 "-match": {"username": "John Doe"}, 
 "-limit": 100,
 "-continue": true,
 "-ttl": 120
 }
}
'
```

### Getting all records
You can retrieve all entities by `"-match": "all"`

## Pagination
When calling an query with a `-continue` flag, the last result in the list may be a paging token. There is a special URL path called `continue` which will accept this paging token.
-  `/{entity}/continue/{token}`

For example
```sh
curl -X GET http://localhost:8080/todo/continue/8ffac247-77a5-4f38-94c0-ad2b0e95c9ae -H "content-type: application/json"
```

Result:
```
[{"title":"Create a Relation","isComplete":false,"user":[]},{"-continue":"8ffac247-77a5-4f38-94c0-ad2b0e95c9ae"}]
```

You can continue to call the paging api with the same token. The page is complete if an empty array is returned `[]`.
## Mutations
Stargate allows a flexible API to manipulate data in Cassandra.
### Create
Create operations are any `POST` request on an entity's path. Create operations have a root entity that will be created as well as relationships that can be linked, unlinked, or replaced.
```sh
curl -X POST http://localhost:8080/todo/Todo \
-H "content-type: application/json" -d'
{ 
 "title": "Create a Relation",
 "isComplete": false
}
'
```
### Update 
Update operations are any `PUT` request on an entity's path. Update operations require a `match` clause. Both scalars and relationships can be updated with an update statement.
```sh
curl -X PUT http://localhost:8080/todo/Todo \
-H "content-type: application/json" -d'
{ 
 "-match": ["user.username", "=", "John Doe"],
 "isComplete": true
}
'
```
### Delete
Delete operations are any `DELETE` request on an entity's path. Delete operations require a `match` clause. All entities and relationships that are specified in the delete statement will be removed. 

Removing Todo and all of its relationships:
```sh
curl -X DELETE http://localhost:8080/todo/Todo \
-H "content-type: application/json" -d'
{ 
  "-match": ["user.username", "=", "John Doe"]
}
'
```

Removing Todo and cascading to User:
```sh
curl -X DELETE http://localhost:8080/todo/Todo \
-H "content-type: application/json" -d'
{ 
  "-match": ["user.username", "=", "John Doe"],
  "user": {}
}
'
```

### Relationship management
Relationships can be added, removed, or replaced on any entity.
* **Link**- Allows adding relationships to an entity. Accepts a `match` or `create` flag.
* **Unlink**- Allows removing relationships from an entity. Accepts a `match` flag.
* **Replace**- Takes a list of relationships that will be the new set of relationships. Accepts a `create` or `match` flag.

Adding a new todo and connecting it to a user:
```sh
curl -X POST http://localhost:8080/todo/Todo \
-H "content-type: application/json" -d'
{ 
 "title": "Create a Relation",
 "isComplete": false,
 "user": {
   "-update": {
     "-match": ["username", "=", "John Doe"]
   }
 }
}
'
```
Update todos to give them to a User:
```sh
curl -X PUT http://localhost:8080/todo/Todo \
-H "content-type: application/json" -d'
{ 
 "-match": ["isComplete", "=", false],
 "user": {
   "-replace": {
      "-match": ["username", "=", "John Doe"]
   }
 }
}
'