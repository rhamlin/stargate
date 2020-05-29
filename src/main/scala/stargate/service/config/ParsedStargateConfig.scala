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
package stargate.service.config

import scala.beans.BeanProperty
import com.datastax.oss.driver.api.core.auth.AuthProvider
import com.datastax.oss.driver.internal.core.auth.PlainTextAuthProvider
import com.datastax.oss.driver.api.core.context.DriverContext

/**
  * shared singleton to use for the ParsedStargateConfig there is no thread safety for this so
  * one must make sure that writes to it are locked in a thread safe manner.
  */
object ParsedStargateConfig {
  var globalConfig: ParsedStargateConfig = _
}

/**
  * the parsed combination of stargate-docker.conf and defaults.conf
  *
  * @param httpPort port to run the stargate http service on
  * @param defaultTTL default ttl for continuing paging requests. Raising this means pages can be attempted again much later, but there maybe more ram and cpu used to manage paging sessions
  * @param defaultLimit default page size limit for all requests
  * @param maxSchemaSizeKB max allowed size in KB for schema json posted to the ${namespace}/create and validate links
  * @param maxRequestSizeKB max allowed request size in KB for all http requests
  * @param maxMutationSizeKB max allowed mutation size in KB for all create entity requests ${namespace}/query/entitity/${entityName}/ POST
  * @param cassandraContactPoints contact points used for stargate to connect to cassandra, usually 2 or 3 nodes will suffice
  * @param cassandraDataCenter datacenter to connect to. Note if a listed contact point is not in this datacenter the connection will not work
  * @param cassandraReplication number of copies of data stored (ie RF 1 means there is only a single copy of all data and losing it means losing that data permanently)
  * @param stargateKeyspace
  * @param cassandraAuthProvider custom auth provider to use for Apache Cassandra. See https://docs.datastax.com/en/developer/java-driver/4.6/manual/core/authentication/ for more details
  * @param cassandraUserName when using the PlainTextProvider the user name to use to connect to Apache Cassandra
  * @param cassandraPassword when using the PlainTextProvider the password to use to connect to Apache Cassandra
  */
final case class ParsedStargateConfig(
  @BeanProperty val httpPort: Int,
  @BeanProperty val defaultTTL: Int,
  @BeanProperty val defaultLimit: Int,
  @BeanProperty val maxSchemaSizeKB: Long,
  @BeanProperty val maxRequestSizeKB: Long,
  @BeanProperty val maxMutationSizeKB: Long,
  @BeanProperty val cassandraContactPoints: List[(String, Int)],
  @BeanProperty val cassandraDataCenter: String,
  @BeanProperty val cassandraReplication: Int,
  @BeanProperty val stargateKeyspace: String,
  @BeanProperty val cassandraAuthProvider: String,
  @BeanProperty val cassandraUserName: String,
  @BeanProperty val cassandraPassword: String
) {

  def copyWithNewKeyspace(newKeyspace: String): ParsedStargateConfig =
    ParsedStargateConfig(
      this.httpPort,
      this.defaultTTL,
      this.defaultLimit,
      this.maxSchemaSizeKB,
      this.maxRequestSizeKB,
      this.maxMutationSizeKB,
      this.cassandraContactPoints,
      this.cassandraDataCenter,
      this.cassandraReplication,
      newKeyspace,
      this.cassandraAuthProvider,
      this.cassandraUserName,
      this.cassandraPassword
    )

  /**
    * returns the maximum allowed schema size in bytes
    */
  val maxSchemaSize: Long = maxSchemaSizeKB * 1024

  /**
    * returns the maximum allowed mutation size in bytes
    */
  val maxMutationSize: Long = maxMutationSizeKB * 1024

  /**
    * returns the maximum allowed http request size in bytes
    */
  val maxRequestSize: Long = maxRequestSizeKB * 1024
}
