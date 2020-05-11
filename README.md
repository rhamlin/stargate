<!--
    Copyright DataStax, Inc.
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
-->
# Stargate

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0)
![build](https://github.com/datastax/stargate/workflows/Docker%20Image%20CI/badge.svg)

Welcome to Stargate, the easy query layer for your NoSQL database.

## Getting Started
* [Getting started in 3 quick steps](docs/getting_started.md)
* [Build a Todo application database in 5 minutes](docs/getting_started_todo_app.md)

## Understanding Stargate
* [Schema Definition](docs/schema_definition.md)
* [CRUD Operations](docs/crud.md)
* [Deploy](docs/deploy.md)

## Features
* **Cloud native.** Scale-out and in with workload demand. Be highly available and resilient to failure. Have quick response times across all channels.
* **It just works.** You give us a logical data model and Stargate gives you a deployable microservice. Stargate handles the database and schema.
* **CRUD made easy.** Stargate exposes a fully featured CRUD API. Create, update, and delete records in cassandra with ease.
* **Advanced Cassandra support.** Trust that your database can scale out horizontally to meet increasing workload demand. 

### Command line utility

Ok now you're sold, go download the binaries [here](releases) for the command line utility. 

### Production

We provide an easy helm chart [here](kube/charts/stargate)

## Contributing

Requirements:

* [OpenJDK 11](https://adoptopenjdk.net/releases.html)
* [Apache Maven](https://maven.apache.org/)
* [Docker](https://www.docker.com) if you want to build the image.
* [Apache Cassandra 3.11](https://cassandra.apache.org/) or [DSE 6.8](https://downloads.datastax.com/#enterprise)

Run the following:

    git clone git@github.com:datastax/stargate
    mvn compile test
    mvn exec:exec #with DSE or Apache Cassandra running in the background

If all this is working satisfactory you are ready to develop new features for Stargate.

### Debugging

Changing the `appstax` logger from `INFO` to `TRACE` will give a per request headers and output, not appropriate to run very long in production but should be fine in a dev context.
See the following example:

    <logger name="appstax" level="info" additivity="false">
to

    <logger name="appstax" level="trace" additivity="false">
