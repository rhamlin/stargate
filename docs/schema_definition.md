# Stargate Schema File
The stargate schema file is a declarative format that describes the behavior of the stargate microservice. Stargate uses this file to generate the schema for cassandra. 
## Model Block
The model block is the part of the schema that describes entities, their relationships, and their data type.

For example:
```
entities {
    Todo {
        fields {
            todo: string
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

The entity name will be used to identify the entity in the REST urls. 

## Field Block
The field block describes the scalar fields in the database.  This has the format of `name: datatype`. The allowable values are any alphanumeric value and underscore. The dash `-` is reserved for Stargate functions.

Field names are not opinionated on casing and will match the input and output json. All fields are optional for input.

### Data types
Stargate has several primitive types.

| Type | Description |
|------|-------------|
| ASCII_STRING | US-ASCII character string |
LONG | 64-bit signed long |
| BLOB | Arbitrary bytes (no validation), expressed as hexadecimal |
| BOOLEAN | true or false | 
| DATE | Value is a date with no corresponding time value; Cassandra encodes date as a 32-bit integer representing days since epoch (January 1, 1970). Dates can be represented in queries and inserts as a string, such as 2015-05-03 (yyyy-mm-dd)
| DECIMAL | Variable-precision decimal |
| DOUBLE | 64-bit IEEE-754 floating point |
| FLOAT | 32-bit IEEE-754 floating point |
| INT | 32-bit signed integer |
| SHORT | 2 byte integer | 
| STRING | UTF-8 encoded string | 
| TIME | Value is encoded as a 64-bit signed integer representing the number of nanoseconds since midnight. Values can be represented as strings, such as 13:30:54.234. | 
| TIMESTAMP | Date and time with millisecond precision, encoded as 8 bytes since epoch. Can be represented as a string, such as 2015-05-03 13:30:54.234. |
| UUID | A UUID in standard UUID format | 
| BIG_INT | Arbitrary-precision integer |

## Relations Block
The relations block describes the relationship between entities. These will manifest as fields on the json input/output structures. Relations are an easy way to create graph-like traversals in your queries. The relations block is **required** on all entities but it can be empty.

```
 relations {
            user { type: User, inverse: todos }
        }
```
### Cardinality
All relations have the cardinality of many-to-many. Cardinality can be enforced by the user by judicious use of match statements.
### Type
The type statement refers to the entity that it will connect to. This is similar to a ‘data type’ for the field. This can be any valid entity.
### Inverse
Inverses are a way to bidirectionally traverse between two entities. Simply, the inverse statement is the field that it maps to. There must be an accompanying relation field in the inverse’s relations section that indicates which entity this ‘links’ to. 

## Query Conditions Block
Query conditions are a list of all possible partial predicates that are allowable in an entity. It has the format of a literal followed by the comparison. 

For example, the following block has the following SQL analogs: `where user.username = ?` and `where isComplete = ? AND user.username = ?`.
```
queryConditions: {
       Todo: [
        ["user.username", "="]
        ["isComplete", "=", user.username", "="]
    ]
}
```
### Field Name Convention
Fields must end in a scalar. Relationships have the ability to be traversed using the `.`. For example `user.username` indicates that we want to match any Todo that has a user relationship with the scalar value of `username`.
Comparisons Supported
The comparisons that are supported are `=` `>` `>=` `<` `<=` and `IN`. 

## Queries Block
The queries block is an optional block that allows a REST endpoint with that query to be created.

It describes the entities and the queries that are under the entity. 

For example, the following will create a rest endpoint of `{namespace}/q/todoByUsername` which allows retrieval of todos by username.
```
queries: {
   Todo: {
       todoByUsername {
           "-match": ["user.username", "=", "username"]
           "-include": ["title", "isComplete"],
           "user": {
               "-include": ["username"]
           }
       }
   }
}
```
### Query Name
The query name is the first block under the entity. In the example above, it is the `customerByFirstName`.  

### Match Statement
The match statement is how the entity should be retrieved. It is conceptually similar to a ‘filter’ in data manipulation languages or ‘where’ in a sql database.

The match statement must match in its entirety to a statement in the Query Conditions block. Stargate does not support new match statements at runtime. 
### Literals
The match statement may contain a literal. The literal must match the data type of the field it is  referring to.
### Variables
Every third argument in the match statement is a variable. In the example above, it is the 'customerName' These are specified at query time with a `-matchArguments` parameter.
### Include Statement
The include statement states which fields need to be included in the resulting json. Only fields on that entity are supported.
### Relationships
Any relationship that is defined on the entity can be included in the query. The relationship should have an `-include` statement to define which fields should be returned.
Relationships cannot include a `-match` statement.