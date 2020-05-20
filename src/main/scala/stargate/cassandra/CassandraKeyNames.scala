/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package stargate.cassandra

/**
  * 
  *
  * @param partitionKeys list of CassandraColumn that match to the partition key in an actual Apache Cassandra table.
  * @param clusteringKeys list of clustering keys that match to the clustering columns in an actual Apache Cassandra table.
  */
final  case class CassandraKeyNames(partitionKeys: List[String], clusteringKeys: List[String]) {
    def combined: List[String] = (partitionKeys ++ clusteringKeys)
}
