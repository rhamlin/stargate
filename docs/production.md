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
# Production

## Authentication

As of 0.2.0 authentication is very minimal and only allows setting a username and a password hashed with [bcrypt](https://en.wikipedia.org/wiki/Bcrypt).

### Enabling Auth

First generate a password that is hashed by bcrypt. There are several ways to do this
but we provide a simple utility command in stargate with `stargate encrypt`

    stargate encrypt
    $enter your password
    ********
    $enter your password again
    ********

Then set the following environment variables on startup (please generate your own hash)
    
    export SG_SERVICE_AUTH_ENABLED=true
    export SG_SERVICE_AUTH_USER=admin
    export SG_SERVICE_AUTH_PASS_HASH='$2a$12$BW57tXiKKTl.nf853gZWju2QpkfhYXRDYEw5ucEF6yCKgSw3QuaTG'

