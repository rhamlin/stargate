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

# Stargate Helm Chart

## Requirements:

* Apache Cassandra or DataStax Enterprise installed somewhere in Kubernetes or outside Kubernetes.
* [helm 3.1+](https://helm.sh/docs/intro/install/)

## Starting from scratch on your local workstation using dockerhub

* [Install minikube](https://kubernetes.io/docs/tasks/tools/install-minikube/)
* run `minikube start`
* run `kubectl create secret docker-registry regcred  --docker-username=YOURDOCKERUSER --docker-password=YOURDOCKERPASS`
* [Install Helm 3.1+](https://helm.sh/docs/intro/install/)
* run `helm repo add incubator https://kubernetes-charts-incubator.storage.googleapis.com`
* run `helm install cassandra incubator/cassandra --set config.cluster_size=1 --set config.seed_size=1`. The Chart is from [here](https://hub.helm.sh/charts/incubator/cassandra)
* run `helm install mysg kube/charts/stargate --set image.tag=latest --set imagePullSecrets={regcred} --set stargate.cassandraDataCenter=datacenter1`
* run `kubectl port-forward $(kubectl get pods -l app.kubernetes.io/instance=mysg -o jsonpath='{.items[0].metadata.name}') 8080:80`

You now have access to Stargate on your localhost on port 8080.

## Common Configuration Options

#### Passing different contact points with a non default data center

    helm install mysg kube/charts/stargate --set stargate.cassandraContactPoints="node1:9042,node2:9042,node3:9042" --set stargate.cassandraDataCenter="dc"

#### Changing RF for Stargate Apps

    helm install mysg kube/charts/stargate --set stargate.cassandraReplicationFactor=3

#### Raising request limits 

    helm install mysg kube/charts/stargate --set stargate.maxSchemaSizeKB=256 --set stargate.maxRequestSize=8192 --set stargate.maxMutationSizeKB=2048

## All configuration 

```
replicaCount: 1

image:
  repository: docker.pkg.github.com/datastax/stargate/stargate
  pullPolicy: IfNotPresent
  # Overrides the image tag whose default is the chart version.
  tag: ""

imagePullSecrets: []
nameOverride: ""
fullnameOverride: ""

stargate:
  port: 80
  #How long the paging token is valid (in seconds)
  defaultTTL: 60
  #default page size
  defaultLimit: 100
  #max size in KB of the schema file
  maxSchemaSizeKB: 128
  #max size in kb for writes
  maxMutationSizeKB: 1024
  #max size in kb for http requests
  maxRequestSizeKB: 4096
  #contact points for the Apache Cassandra or DataStax Enterprise cluster
  cassandraContactPoints: "cassandra-0.cassandra.default.svc.cluster.local:9042"
  #datacetenter to connect to
  cassandraDataCenter: datacenter1
  #number of replicas to use in cassandra for all newly created apps
  cassandraReplicationFactor: 1

serviceAccount:
  create: false
  annotations:
  # The name of the service account to use.
  # If not set and create is true, a name is generated using the fullname template
  name: ""

role:
  create: false
  # The name of the role to use.
  # If not set and create is true, a name is generated using the fullname template
  name: ""
  rules: []

roleBinding:
  create: false
  # The name of the roleBinding to use.
  # If not set and create is true, a name is generated using the fullname template
  name: ""
  annotations: {}

clusterRole:
  create: false
  # The name of the role to use.
  # If not set and create is true, a name is generated using the fullname template
  name: ""
  rules: []


clusterRoleBinding:
  create: false
  # The name of the clusterRoleBinding to use.
  # If not set and create is true, a name is generated using the fullname template
  name: ""
  annotations: {}

podAnnotations: {}

podSecurityContext: {}
  # fsGroup: 2000

securityContext: {}
  # capabilities:
  #   drop:
  #   - ALL
  # readOnlyRootFilesystem: true
  # runAsNonRoot: true
  # runAsUser: 1000

service:
  type: ClusterIP
  port: 80

ingress:
  enabled: false
  annotations: {}
    # kubernetes.io/ingress.class: nginx
    # kubernetes.io/tls-acme: "true"
  hosts:
    - host: chart-example.local
      paths: []
  tls: []
  #  - secretName: chart-example-tls
  #    hosts:
  #      - chart-example.local

resources: {}
  # We usually recommend not to specify default resources and to leave this as a conscious
  # choice for the user. This also increases chances charts run on environments with little
  # resources, such as Minikube. If you do want to specify resources, uncomment the following
  # lines, adjust them as necessary, and remove the curly braces after 'resources:'.
  # limits:
  #   cpu: 100m
  #   memory: 128Mi
  # requests:
  #   cpu: 100m
  #   memory: 128Mi

autoscaling:
  enabled: false
  minReplicas: 1
  maxReplicas: 100
  targetCPUUtilizationPercentage: 80
  # targetMemoryUtilizationPercentage: 80

nodeSelector: {}

tolerations: []

affinity: {}
```
