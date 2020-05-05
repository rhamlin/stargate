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

import java.io.File
import java.util.UUID
import java.util.concurrent._

import com.datastax.oss.driver.api.core.CqlSession
import com.fasterxml.jackson.databind.ObjectMapper
import com.swrve.ratelimitedlogger.RateLimitedLog
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.{LazyLogging, Logger}
import io.prometheus.client.exporter.MetricsServlet
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}
import org.eclipse.jetty.servlet.{ServletHandler, ServletHolder}
import stargate.cassandra.CassandraTable
import stargate.metrics.RequestCollector
import stargate.model.{OutputModel, queries}
import stargate.query.pagination.{StreamEntry, Streams}
import stargate.service.{datamodelRepository, http}

import scala.beans.BeanProperty
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try


class StargateServlet(val config: ParsedStarGateConfig)
    extends HttpServlet
    with LazyLogging
    with RequestCollector {

  val apps = new ConcurrentHashMap[String, OutputModel]()
  val continuationCache =
    new ConcurrentHashMap[UUID, (StreamEntry, ScheduledFuture[Unit])]()
  val continuationCleaner: ScheduledExecutorService =
    Executors.newScheduledThreadPool(1)

  val maxSchemaSize: Long = config.maxSchemaSizeKB * 1024
  val maxMutationSize: Long = config.maxMutationSizeKB * 1024
  val maxRequestSize: Long = config.maxRequestSizeKB * 1024
  val rateLimitedLog: RateLimitedLog = RateLimitedLog
    .withRateLimit(logger.underlying)
    .maxRate(5)
    .every(java.time.Duration.ofSeconds(10))
    .build()

  import StargateServlet._
  val cqlSession: CqlSession = cassandra.session(config.cassandraContactPoints, config.cassandraDataCenter)
  val datamodelRepoTable: CassandraTable = util.await(datamodelRepository.ensureRepoTableExists(config.stargateKeyspace, config.cassandraReplication, cqlSession, executor)).get
  // reload previous datamodels
  Await.result(datamodelRepository.fetchAllLatestDatamodels(datamodelRepoTable, cqlSession, executor), Duration.Inf).foreach(name_config => {
    logger.info(s"reloading datamodel from cassandra: ${name_config._1}")
    val model = stargate.schema.outputModel(stargate.model.parser.parseModel(name_config._2), name_config._1)
    apps.put(name_config._1, model)
  })

  def lookupModel(appName: String): OutputModel = {
    val model = this.apps.get(appName)
    require(model != null, s"invalid database name: ${appName}")
    model
  }

  def schemaChange(appName: String, req: HttpServletRequest, resp: HttpServletResponse) = {
    req.getMethod match {
      case "POST" => timeSchemaCreate(() => {
        http.validateHoconHeader(req)
        http.validateSchemaSize(req.getContentLengthLong, maxSchemaSize)
        val input = new String(req.getInputStream.readAllBytes)
        postSchema(appName, input, resp)
      })
      case "DELETE" => deleteSchema(appName, resp)
      case op => Future.failed(new RuntimeException(s"unsupported op: ${op}"))
    }
  }

  def postSchema(
      appName: String,
      input: String,
      resp: HttpServletResponse
  ): Unit = {
    val model = stargate.schema.outputModel(stargate.model.parser.parseModel(input), appName)
    val previousDatamodel = util.await(datamodelRepository.fetchLatestDatamodel(appName, datamodelRepoTable, cqlSession, executor)).get
    if(!previousDatamodel.contains(input)) {
      logger.info(s"""creating keyspace "${appName}" for new datamodel""")
      datamodelRepository.updateDatamodel(appName, input, datamodelRepoTable, cqlSession, executor)
      cassandra.recreateKeyspace(cqlSession, appName, config.cassandraReplication)
      Await.result(model.createTables(cqlSession, executor), Duration.Inf)
    } else {
      logger.info(s"""reusing existing keyspace "${appName}" with latest datamodel""")
    }
    apps.put(appName, model)
  }
  def deleteSchema(appName: String, resp: HttpServletResponse): Unit = {
    logger.info(s"""deleting datamodels and keyspace for app "${appName}" """)
    val removed = apps.remove(appName)
    util.await(datamodelRepository.deleteDatamodel(appName, datamodelRepoTable, cqlSession, executor)).get
    cassandra.wipeKeyspace(cqlSession, appName)
    if(removed == null) {
      resp.setStatus(404)
    }
  }


  def runPredefinedQuery(
      appName: String,
      queryName: String,
      input: String,
      resp: HttpServletResponse
  ): Unit = {
    val model = lookupModel(appName)
    val payloadMap = util.fromJson(input).asInstanceOf[Map[String, Object]]
    val query = Try(model.input.queries(queryName))
    require(query.isSuccess, s"""no such query "${queryName}" for database "${appName}" """)
    val runtimePayload = queries.predefined.transform(query.get, payloadMap)
    val result = stargate.query.getAndTruncate(model, query.get.entityName, runtimePayload, config.defaultLimit, config.defaultTTL, cqlSession, executor)
    val entities = cacheStreams(result)
    resp.getWriter.write(util.toJson(Await.result(entities, Duration.Inf)))
  }

  def cacheStreams(
      truncatedFuture: Future[(List[Map[String, Object]], Streams)]
  ): Future[List[Map[String, Object]]] = {
    truncatedFuture.map(truncated_streams => {
      val (truncated, streams) = truncated_streams
      streams.foreach(stream => {
        // do not allow cleanup to run until stream is actually added to cache
        val lock = new Semaphore(0)
        val cleanup: ScheduledFuture[Unit] =
          continuationCleaner.schedule(() => {
            logger.trace("cleanup", continuationCache.keys, "-", stream._1)
            lock.acquire()
            continuationCache.remove(stream._1)
            ()
          }, stream._2.ttl, TimeUnit.SECONDS)
        continuationCache.put(stream._1, (stream._2, cleanup))
        lock.release()
      })
      truncated
    })(executor)
  }

  def continueQuery(
      appName: String,
      continueId: UUID,
      resp: HttpServletResponse
  ): Unit = {
    val model = lookupModel(appName)
    val continue_cleanup = continuationCache.remove(continueId)
    require(continue_cleanup != null, s"""no continuable query found for id ${continueId} in database "${appName}" """)
    val (entry, cleanup) = continue_cleanup
    cleanup.cancel(false)

    val truncateFuture = stargate.query.pagination.truncate(
      model.input,
      entry.entityName,
      entry.getRequest,
      entry.entities,
      continueId,
      config.defaultLimit,
      config.defaultTTL,
      executor
    )
    val entities = Await.result(cacheStreams(truncateFuture), Duration.Inf)
    resp.getWriter.write(util.toJson(entities))
  }

  def runQuery(
      appName: String,
      entity: String,
      op: String,
      input: String,
      resp: HttpServletResponse
  ): Unit = {
    val payload = util.fromJson(input)
    runQuery(appName, entity, op, payload, resp)
  }

  def runQuery(
      appName: String,
      entity: String,
      op: String,
      payload: Object,
      resp: HttpServletResponse
  ): Unit = {
    val model = lookupModel(appName)
    require(model.input.entities.contains(entity), s"""database "${appName}" does not have an entity named "${entity}" """)
    val payloadMap = Try(payload.asInstanceOf[Map[String, Object]])
    logger.trace(s"query payload: $payload")

    val result: Future[Object] = op match {
      case "GET" => {
        val result = query.untyped.getAndTruncate(model, entity, payloadMap.get, config.defaultLimit, config.defaultTTL, cqlSession, executor)
        cacheStreams(result)
      }
      case "POST" => model.mutation.create(entity, payload, cqlSession, executor)
      case "PUT" =>  model.mutation.update(entity, payloadMap.get, cqlSession, executor)
      case "DELETE" => model.mutation.delete(entity, payloadMap.get, cqlSession, executor)
      case _ => Future.failed(new RuntimeException(s"unsupported op: ${op}"))
    }
    logger.trace(op, Await.result(result, Duration.Inf))
    resp.getWriter.write(util.toJson(Await.result(result, Duration.Inf)))
  }

  override def doPut(
      req: HttpServletRequest,
      resp: HttpServletResponse
  ): Unit = {
    route(req, resp)
  }

  override def doDelete(
      req: HttpServletRequest,
      resp: HttpServletResponse
  ): Unit = {
    route(req, resp)
  }

  override def doGet(
      req: HttpServletRequest,
      resp: HttpServletResponse
  ): Unit = {
    route(req, resp)
  }

  override def doPost(
      req: HttpServletRequest,
      resp: HttpServletResponse
  ): Unit = {
    super.getServletContext
    route(req, resp)
  }

  def route(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
    try {
      val contentLength = req.getContentLengthLong
      http.validateRequestSize(contentLength, maxRequestSize)
      val op = req.getMethod
      val path = http.sanitizePath(req.getServletPath)
      logger.trace(
        s"http request: { path: '$path', method: '$op', content-length: $contentLength, content-type: '${req.getContentType}' }"
      )
      path match {
        case s"/$appName/continue/${id}" =>
          timeRead(() => {
            continueQuery(appName, UUID.fromString(id), resp)
          })
        case s"/${appName}/q/${query}" =>
          timeRead(() => {
            http.validateJsonContentHeader(req)
            val input = new String(req.getInputStream.readAllBytes)
            runPredefinedQuery(appName, query, input, resp)
          })
        case s"/${appName}/${entity}" =>
          http.validateJsonContentHeader(req)
          http.validateMutation(op, contentLength, maxMutationSize)
          val input = new String(req.getInputStream.readAllBytes)
          val payload = util.fromJson(input)
          runQuery(appName, entity, op, payload, resp)
        case s"/${appName}" =>
          schemaChange(appName, req, resp)
        case _ =>
          countError()
          resp.setStatus(HttpServletResponse.SC_NOT_FOUND)
          val msg = s"path: $path does not match /:appName/:entity/:id pattern"
          rateLimitedLog.warn(msg)
          resp.getWriter.write(util.toJson(msg))
      }
    } catch {
      case e: Exception =>
        countError()
        rateLimitedLog.error(s"exception: $e")
        resp.setStatus(HttpServletResponse.SC_BAD_GATEWAY)
        resp.getWriter.write(util.toJson(e.getMessage))
    }
  }
}

object StargateServlet {
  val executor: ExecutionContext = ExecutionContext.global
}
case class ParsedStarGateConfig(
    @BeanProperty val httpPort: Int,
    @BeanProperty val defaultTTL: Int,
    @BeanProperty val defaultLimit: Int,
    @BeanProperty val maxSchemaSizeKB: Long,
    @BeanProperty val maxRequestSizeKB: Long,
    @BeanProperty val maxMutationSizeKB: Long,
    @BeanProperty val cassandraContactPoints: List[(String, Int)],
    @BeanProperty val cassandraDataCenter: String,
    @BeanProperty val cassandraReplication: Int,
    @BeanProperty val stargateKeyspace: String
)
case class Params(conf: String = "")

object Main {
  private val logger = Logger("main")
  private val sgVersion = "0.1.0"

  private def mapConfig(config: Config): ParsedStarGateConfig = {
    val om = new ObjectMapper
    ParsedStarGateConfig(
      config.getInt("http.port"),
      config.getInt("defaultTTL"),
      config.getInt("defaultLimit"),
      config.getLong("validation.maxSchemaSizeKB"),
      config.getLong("validation.maxMutationSizeKB"),
      config.getLong("validation.maxRequestSizeKB"),
      config.getString("cassandra.contactPoints")
          .split(",").map(_.split(":")).map(hp => (hp(0), Integer.parseInt(hp(1)))).toList,
      config.getString("cassandra.dataCenter"),
      config.getInt("cassandra.replication"),
      config.getString("stargateKeyspace")
    )
  }

  def logStartup() = {
    logger.info("Launch Mission To StarGate")
    logger.info(" -----------")
    logger.info("|         * |")
    logger.info("| *         |")
    logger.info("|    *      |")
    logger.info("|         * |")
    logger.info("|    *      |")
    logger.info(" -----------")
    logger.info("            ")
    logger.info("            ")
    logger.info("            ")
    logger.info("     ^^")
    logger.info("    ^^^^")
    logger.info("   ^^^^^^")
    logger.info("  ^^^^^^^^")
    logger.info(" ^^^^^^^^^^")
    logger.info("   ^^^^^^")
    logger.info("     ||| ")
    logger.info("     ||| ")
    logger.info("     ||| ")
    logger.info("     ||| ")
    logger.info("      | ")
    logger.info("      | ")
    logger.info("      | ")
    logger.info("        ")
    logger.info("      |  ")
    logger.info("0000     0000")
    logger.info("00000   00000")
    logger.info("============  ")
    logger.info(s"StarGate Version: $sgVersion")
  }

  def main(args: Array[String]) = {
    metrics.registerJVMMetrics()
    val parser = new scopt.OptionParser[Params]("stargate-server") {
      head("stargate-server", sgVersion)
      opt[String]('c', "conf")
        .optional()
        .action((x, c) => c.copy(conf = x))
        .text("optional custom stargate.conf to use")
      help('h', "help").text("shows the help text")

    }
    var appConf = ""
    parser.parse(args, Params()) match {
      case Some(p) =>appConf = p.conf
      case None =>
        sys.exit(0)
    }
    val config = (if (appConf.isBlank)
      ConfigFactory.parseResources("stargate-docker.conf").resolve()
      else ConfigFactory.parseFile(new File(appConf)).resolve())
    val parsedConfig = mapConfig(config)
    logger.info(s"parsedConfig: ${util.toPrettyJson(parsedConfig)}")
    logger.info(s"contact points ${parsedConfig.getCassandraContactPoints.mkString(",")}")
    val server = new org.eclipse.jetty.server.Server(parsedConfig.httpPort)
    val handler = new ServletHandler()
    val servlet = new StargateServlet(parsedConfig)
    handler.addServletWithMapping(new ServletHolder(servlet), "/")
    //expose the MetricsServlet to the /metrics endpoint. To scrape metrics for Prometheus now you just need to point to the appropriate host and port combination
    //ie http://localhost:8080/metrics
    handler.addServletWithMapping(
      new ServletHolder(new MetricsServlet()),
      "/metrics"
    );
    server.setHandler(handler)
    //has to be the last set handler, will wrap existing handler
    //note this will mean that metrics calls will be counted as well in the total request count
    metrics.registerServletHandlerStatistics(server)
    logStartup()
    server.start
    server.join
  }
}

package object service extends scala.App {

  Main.main(args)

}
