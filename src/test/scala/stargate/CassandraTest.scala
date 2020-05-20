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

package stargate
import java.util.UUID
import java.util.concurrent.{ConcurrentSkipListSet, TimeUnit}

import com.datastax.oss.driver.api.core.CqlSession
import com.typesafe.scalalogging.Logger

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.{Random, Try}
import stargate.service.config.ParsedStargateConfig
import scala.io.Source

trait CassandraTestSession {
  def session: CqlSession
  def newKeyspace: String
}

trait CassandraTest {
  val logger = Logger(classOf[CassandraTest])

  // hard-coding this for now
  val dataCenter: String = "datacenter1"
  val dockerId: String = "test-dse-" + UUID.randomUUID()

  var contacts: List[(String,Int)] = List(("localhost", 9042))
  var session: CqlSession = null
  val keyspaces: ConcurrentSkipListSet[String] = new java.util.concurrent.ConcurrentSkipListSet[String]()
  var alreadyRunning: Boolean = true
  
  // make this @BeforeClass for any tests that depend on casssandra
  def ensureCassandraRunning(): Unit = {
    val username = "cassandra"
    val password = "cassandra"
    val authProvider = "PlainTextAuthProvider"
    var parsedStargateConfig = new ParsedStargateConfig(
      8080,
      100,
      100,
      32000L,
      32000L,
      32000L,
      contacts,
      dataCenter,
      1,
      "test-cassandra",
      authProvider,
      username,
      password,
    )
    val persistentDseContainer = "stargate-tests-cassandra"
    lazy val localDocker = Try(Integer.parseInt(util.process.exec(List("docker", "port", persistentDseContainer, "9042")).get.split(":")(1).trim))
    if(localDocker.isSuccess) {
      logger.info("using already running docker-cassandra instance for test: {}", localDocker.get)
      contacts = List(("localhost", localDocker.get))
      alreadyRunning = true
    } else {
      val image = "cassandra:3.11.6"
      val yamlFile = getClass().getResource("/cassandra.yaml").getFile().replace("/C:", "") //hack on windows
      logger.info(s"yaml file located is $yamlFile")
      val create = util.process.exec(List("docker", "create", "--name", dockerId, "-P", image)).get
      val copy = util.process.exec(List("docker", "cp", yamlFile, s"$dockerId:/etc/cassandra/")).get
      val start = util.process.exec(List("docker", "start", dockerId)).get
      logger.warn(s"""starting cassandra docker container for test: ${dockerId})
        |    this can take nearly a minute to start, so it's generally faster to: 1) run cassandra in the background on port 9042, or
        |    2) run docker with name ${persistentDseContainer} with the following command:
        |    docker run -d -P --name ${persistentDseContainer} ${image}""".stripMargin)
      val port =  Integer.parseInt(util.process.exec(List("docker", "port", dockerId, "9042")).get.split(":")(1).trim)
      logger.warn(s"port to connect to is $port")
      contacts = List(("localhost", port))
      parsedStargateConfig = new ParsedStargateConfig(
          parsedStargateConfig.httpPort,
          parsedStargateConfig.defaultTTL,
          parsedStargateConfig.defaultLimit,
          parsedStargateConfig.maxSchemaSizeKB,
          parsedStargateConfig. maxRequestSizeKB,
          parsedStargateConfig.maxMutationSizeKB,
          List(("localhost", port)),
          parsedStargateConfig.cassandraDataCenter,
          parsedStargateConfig.cassandraReplication,
          parsedStargateConfig.stargateKeyspace,
          parsedStargateConfig.cassandraAuthProvider,
          parsedStargateConfig.cassandraUserName,
          parsedStargateConfig.cassandraPassword
      )
      util.retry(cassandra.session(parsedStargateConfig), Duration.apply(60, TimeUnit.SECONDS), Duration.apply(5, TimeUnit.SECONDS)).get
      alreadyRunning = false
    }
    session = cassandra.session(parsedStargateConfig)
  }

  // make this @AfterClass for any tests that depend on cassandra
  def cleanup(): Unit = {
    keyspaces.asScala.foreach(cleanupKeyspace)
    session.close()
    if(!alreadyRunning) {
      val stop = util.process.exec(List("docker", "kill", dockerId)).get
      val rm = util.process.exec(List("docker", "rm", dockerId)).get
    }
  }


  def newKeyspace: String = {
    val keyspace = ("keyspace" + Random.alphanumeric.take(10).mkString).toLowerCase
    logger.info("creating keyspace: {}", keyspace)
    cassandra.createKeyspace(session, keyspace, 1)
    keyspaces.add(keyspace)
    keyspace
  }

  def cleanupKeyspace(keyspace: String) = {
    val wiped = Try(cassandra.wipeKeyspace(session, keyspace))
    logger.info(s"cleaning up keyspace ${keyspace}: ${wiped.toString}")
    ()
  }
}

