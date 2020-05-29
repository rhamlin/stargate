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
import java.util.concurrent.{ConcurrentSkipListSet, TimeUnit}

import com.datastax.oss.driver.api.core.CqlSession
import com.typesafe.scalalogging.Logger
import stargate.service.config.ParsedStargateConfig

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.{Random, Try}
import scala.util.Failure
import scala.util.Success

trait CassandraTestSession {
  def session: CqlSession
  def newKeyspace: String
}

trait CassandraTest {
  val logger = Logger(classOf[CassandraTest])
  private val persistentDseContainer = "stargate-tests-cassandra"
  private var contacts: List[(String, Int)] = _

  // hard-coding this for now
  val dataCenter: String = "datacenter1"

  var cqlSession: CqlSession = null
  val keyspaces: ConcurrentSkipListSet[String] = new java.util.concurrent.ConcurrentSkipListSet[String]()
  var alreadyRunning: Boolean = false
  var parsedStargateConfig: ParsedStargateConfig = _

  /**
    * start cassandra
    */
  def startCassandra(): Unit = {
    logger.info("checking for existing cassandra test instance")
    lazy val localDocker: Try[Int] = {
      val dockerOutput = Try(util.process.exec(List("docker", "port", persistentDseContainer, "9042")))
      dockerOutput match {
        case Failure(e) =>
          logger.error(e.getMessage())
          localDocker
        case Success(value) =>
          logger.info("checking for cql port number on cassandra test instance")
          Try(Integer.parseInt(value.get.split(":")(1).trim))
      }
    }
    var statusText: Try[String] = {
      if (localDocker.isFailure) {
        Failure[String](new RuntimeException("unknown container skipping"))
      } else {
        logger.info("checking for existing cassandra test instance run status")
        util.process.exec(List("docker", "inspect", "-f", "'{{.State.Running}}'", persistentDseContainer))
      }
    }
    if (statusText.isSuccess) {
      logger.warn(s"container status output is running == ${statusText.get}")
    }
    if (localDocker.isSuccess && statusText.isSuccess && statusText.get.contains("true")) {
      logger.info("using already running docker-cassandra instance for test: {}", localDocker.get)
      contacts = List(("localhost", localDocker.get))
      alreadyRunning = true
    } else {
      val image = "cassandra:3.11.6"
      val yamlFile = getClass().getResource("/cassandra.yaml").getFile().replace("/C:", "") //hack on windows
      logger.warn("creating cassandra test instance")
      if (statusText.isFailure) {
        util.process.exec(List("docker", "create", "--name", persistentDseContainer, "-P", image)).get
        util.process.exec(List("docker", "cp", yamlFile, s"$persistentDseContainer:/etc/cassandra/")).get
      }
      util.process.exec(List("docker", "start", persistentDseContainer)).get
      logger.warn(s"""starting cassandra docker container for test: ${persistentDseContainer})
                     |    this can take nearly a minute to start, so it's generally faster to: 1) run cassandra in the background on port 9042, or
                     |    2) run docker with name ${persistentDseContainer} with the following command:
                     |    docker run -d -P --name ${persistentDseContainer} ${image}""".stripMargin)
      val port = Integer.parseInt(
        util.process.exec(List("docker", "port", persistentDseContainer, "9042")).get.split(":")(1).trim
      )
      logger.warn(s"port to connect to is $port")
      contacts = List(("localhost", port))
    }
  }

  // make this @BeforeClass for any tests that depend on cassandra
  def ensureCassandraRunning(): Unit = {
    startCassandra()
    val username = "cassandra"
    val password = "cassandra"
    val authProvider = "PlainTextAuthProvider"
    parsedStargateConfig = ParsedStargateConfig(
      9090,
      100,
      100,
      32000L,
      32000L,
      32000L,
      contacts,
      dataCenter,
      1,
      "cassandra-test",
      authProvider,
      username,
      password
    )
    if (!alreadyRunning) {
      util
        .retry(
          cassandra.session(parsedStargateConfig),
          Duration.apply(60, TimeUnit.SECONDS),
          Duration.apply(5, TimeUnit.SECONDS)
        )
        .get
      alreadyRunning = true
    }
    if (cqlSession == null) {
      cqlSession = cassandra.session(parsedStargateConfig)
    }
  }

  // make this @AfterClass for any tests that depend on cassandra
  def cleanup(): Unit = {
    keyspaces.asScala.foreach(cleanupKeyspace)
  }

  def newKeyspace: String = {
    val keyspace = ("keyspace" + Random.alphanumeric.take(10).mkString).toLowerCase
    logger.info("creating keyspace: {}", keyspace)
    cassandra.createKeyspace(cqlSession, keyspace, 1)
    keyspaces.add(keyspace)
    keyspace
  }

  def cleanupKeyspace(keyspace: String) = {
    val wiped = Try(cassandra.wipeKeyspace(cqlSession, keyspace))
    logger.info(s"cleaning up keyspace ${keyspace}: ${wiped.toString}")
    ()
  }
}
