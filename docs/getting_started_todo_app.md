# What is Stargate?

Stargate is a tool that quickly creates a microservice driven database with a logical data model. It makes querying and managing relationships easy.

This tutorial will walk you through specifying a data model and defining access patterns. By the end, you’ll have fully functioning API endpoints to test!
What you need
To work along with this tutorial, you’ll need to download the stargate tool and have access to the mac or linux terminal.
```sh
$ brew install stargate
```
# 1. Create a project
A project holds your logical data model, its entities, queries and API endpoints. Simply put, it’s your database.

### Create the stargate configuration file
$ touch todo.conf 
Start the stargate service
$ stargate dev start todo todo.conf

Great! You’re ready to start working on your database.
# 2. Design
Let’s take a quick look at a portion of the todo configuration file. Here you can define an entity-relationship model. The stargate service will create the entity of Todo and give it two fields: title, and completed status.

1. Add the following to your todo.conf file
    ```hocon
    entities {
        Todo {
            fields {
                title: string
                isComplete: boolean
            }
            relations {
            }
        }
    }
    ```
2. Create a todo entry
Stargate has refreshed the database in the background and has created our first endpoint. Out of the box, stargate gives you create, get, update, and delete functionality on each entity. Below is an example of a create statement.
    ```sh
    curl -X POST http://localhost:8080/todo/Todo \
    -H "content-type: application/json" -d'
    { 
     "title": "Create a Stargate Database",
     "isComplete": false
    }
    '
    ```
    Now let’s check that the database has our change
    ```sh
    curl -X GET http://localhost:8080/todo/Todo \
    -H "content-type: application/json" -d'
    { 
     "-match": "all"
    }
    '
    ```
# 3. Add a Relationship
Relationships are what connect the data in one entity to another. They're similar to joins in relational databases like MySQL. Let’s create a relationship to keep track of which user creates the todo.

A relationship is defined on both entities and they are linked together using an inverse reference. This allows us to link together entities and query them from either side.
```hocon
entities {
    Todo {
        fields {
            title: string
            isComplete: boolean
        }
        relations {
            user { type: User, inverse: todos }
        }
    }
    User {
        fields {
            username: string
        }
        relations {
            todos { type: Todo, inverse: user }
        }
    }
}
```
# 4. Add a custom query
A custom query defines the access patterns for how entities are retrieved. For this example, we want to retrieve Todos by the user’s username. We use a dot notation to access an entity through a relationship.
```hocon
queryConditions: {
    Todo: [
        ["user.username", "="]
    ]
}
```
Now we’re ready to create our first todos by user. 

1. First we need to create a new user:
    ```sh
    curl -X POST http://localhost:8080/todo/User \
    -H "content-type: application/json" -d'
    { 
     "username": "John Doe"
    }
    '
    ```
2. Create our todo
Here we must now connect our previously created User to our new Todo entry. To accomplish this, we must link the User entity.
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
3. Now we can retrieve user by username. 
We use a special match field to indicate how we should search the database, as defined in our queries section. 
    ```sh
    curl -X GET http://localhost:8080/todo/Todo \
    -H "content-type: application/json" -d'
    {
     "-match":["user.username", "=", "John Doe"], 
     "user": {}
     }
    }
    '
    ```
    
Example response:
    ```sh
    [{
       "entityId": "b3298af9-2e36-4e7f-8415-688aa4924183",
       "isComplete": false,
       "title": "Create a Relation",
       "user": [{
           "entityId": "268e0cf2-b896-42ec-98b1-b91c085c7ffd",
           "username": "John Doe"
       }]
    }]
    ```
4. Update the todo to complete.
Now we can update the todo with a new status. We do that with a PUT request
    ```sh
    curl -X PUT http://localhost:8080/todo/Todo \
    -H "content-type: application/json" -d'
    {
     "-match":["entityId", "=", "b3298af9-2e36-4e7f-8415-688aa4924183"], 
     "isComplete": true
     }
    }
    '
    ```

# Congrats! You’re ready to build an application with your new database! 
## Next Steps
Once you’re ready to go to production, you can check out our [Deployment Guide](docs/deploy.md)
For more information about the features of Stargate, check out our [Docs](docs/)
## Recap
This is the final todo config that we used for our app.

todo.conf
```hocon
entities {
    Todo {
        fields {
            title: string
            isComplete: boolean
        }
        relations {
            user { type: User, inverse: todos }
        }
    }
    User {
        fields {
            username: string
        }
        relations {
            todos { type: Todo, inverse: user }
        }
    }
}
queryConditions: {
    Todo: [
        ["user.username", "="]
    ]
}
```