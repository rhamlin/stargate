# Deploying locally
## The easy way
1. `stargate dev start [namespace] [path]`
- This will start cassandra & stargate locally and watch the path.
2. `stargate dev stop`
- Stops the cassandra instance

## These are the steps the dev command runs:
1. Deployment

    `stargate service -c start`

    The -c service will also deploy a local cassandra instance and allow stargate to talk to it.
2. Validate the schema file

    `stargate validate [path]`
    - path is schema file
3. Apply the schema file

    `stargate apply [namespace] [path]`
4. Watch the schema file

    `stargate watch [path]`
5. When you're done:
    
    `stargate service -c stop`