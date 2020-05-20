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
  * @param key 
  * @param data
  */
final case class CassandraColumns(key: CassandraKey, data: List[CassandraColumn]) {
    def names: CassandraColumnNames = CassandraColumnNames(key.names, data.map(_.name))
    def combined: List[CassandraColumn] = key.combined ++ data
  }
