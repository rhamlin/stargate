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
