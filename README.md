# Stargate

![Docker Image CI](https://github.com/datastax/stargate/workflows/Docker%20Image%20CI/badge.svg)

Welcome to Stargate the easy query layer for your NoSQL database. Getting started is [here](docs/getting_started.md)

## Why Stargate

Placeholder

## Deployment 

### Command line utility

Ok now you're sold, go download the binaries [here](releases) for the command line utility. 

### Production

We provide an easy helm chart (here)[]

## Contributing

Requirements:

* [OpenJDK 11](https://adoptopenjdk.net/releases.html)
* [Apache Maven](https://maven.apache.org/)
* [Docker](https://www.docker.com) if you want to build the image.
* [Apache Cassandra 3.11](https://cassandra.apache.org/) or [DSE 6.8](https://downloads.datastax.com/#enterprise)

Run the following:

    git clone git@github.com:datastax/stargate
    mvn compile test
    mvn run:exec #with DSE or Apache Cassandra running in the background

If all this is working satisfactory you are ready to develop new features for Stargate.

### Debugging

Changing the `appstax` logger from `INFO` to `TRACE` will give a per request headers and output, not appropriate to run very long in production but should be fine in a dev context.
See the following example:

    <logger name="appstax" level="info" additivity="false">
to

    <logger name="appstax" level="trace" additivity="false">
