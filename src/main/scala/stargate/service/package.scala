package stargate

import java.io.File
import java.util.UUID
import java.util.concurrent._

import stargate.model.OutputModel
import stargate.query.pagination.{StreamEntry, Streams}
import stargate.service.http
import stargate.metrics
import stargate.metrics.RequestCollector
import com.datastax.oss.driver.api.core.CqlSession
import com.swrve.ratelimitedlogger.RateLimitedLog
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.{LazyLogging, Logger}
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}
import org.eclipse.jetty.servlet.{ServletHandler, ServletHolder}

import scala.beans.BeanProperty
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.util.Try
import io.prometheus.client.exporter.MetricsServlet


class AppstaxServlet(val config: ParsedStarGateConfig) 
  extends HttpServlet 
  with LazyLogging 
  with RequestCollector{

  val apps = new ConcurrentHashMap[String, (CqlSession, OutputModel)]()
  val continuationCache = new ConcurrentHashMap[UUID, (StreamEntry,ScheduledFuture[Unit])]()
  val continuationCleaner: ScheduledExecutorService = Executors.newScheduledThreadPool(1)

  val defaultLimit: Int = config.defaultLimit
  val defaultTTL: Int = config.defaultTTL
  val maxSchemaSize: Long = config.maxSchemaSizeKB * 1024
  val maxMutationSize: Long = config.maxMutationSizeKB * 1024
  val maxRequestSize: Long = config.maxRequestSizeKB * 1024
  val rateLimitedLog: RateLimitedLog  = RateLimitedLog
    .withRateLimit(logger.underlying)
    .maxRate(5).every(java.time.Duration.ofSeconds(10))
    .build()

  import AppstaxServlet._

  def newSession(keyspace: String): CqlSession = {
    val contacts = config.cassandraContactPoints.asScala.toList.map(c => (c.getString("host"), c.getInt("port")))
    val dataCenter = config.cassandraDataCenter
    val replication = config.cassandraReplication
    cassandra.sessionWithNewKeyspace(contacts, dataCenter, keyspace, replication)
  }


  def postSchema(appName: String, input: String, resp: HttpServletResponse): Unit = {
    val model = stargate.schema.outputModel(stargate.model.parser.parseModel(input))
    val session = newSession(appName)
    implicit val ec: ExecutionContext = executor
    Await.ready(Future.sequence(model.tables.map(cassandra.create(session, _))), Duration.Inf)
    apps.put(appName, (session, model))
    resp.getWriter.write(model.toString)
  }

  def runPredefinedQuery(appName: String, queryName: String, input: String, resp: HttpServletResponse): Unit = {
    val (session, model) = apps.get(appName)
    val payloadMap = util.fromJson(input).asInstanceOf[Map[String,Object]]
    val query = model.input.queries(queryName)
    val runtimePayload = stargate.model.queries.transform(query, payloadMap)
    runQuery(appName, query.entityName, "GET", runtimePayload, resp)
  }

  def cacheStreams(truncatedFuture: Future[(List[Map[String,Object]], Streams)]): Future[List[Map[String,Object]]] = {
    truncatedFuture.map(truncated_streams => {
      val (truncated, streams) = truncated_streams
      streams.foreach(stream => {
        // dont allow cleanup to run until stream is actually added to cache
        val lock = new Semaphore(0)
        val cleanup: ScheduledFuture[Unit] = continuationCleaner.schedule(() => {
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

  def continueQuery(appName: String, continueId: UUID, resp: HttpServletResponse): Unit = {
    val (session, model) = apps.get(appName)
    val (entry, cleanup) = continuationCache.remove(continueId)
    cleanup.cancel(false)

    val truncateFuture = stargate.query.pagination.truncate(model.input, entry.entityName, entry.getRequest, entry.entities, continueId, defaultLimit, defaultTTL, executor)
    val entities = Await.result(cacheStreams(truncateFuture), Duration.Inf)
    resp.getWriter.write(util.toJson(entities))
  }

  def runQuery(appName: String, entity: String, op: String, input: String, resp: HttpServletResponse): Unit = {
    val payload = util.fromJson(input)
    runQuery(appName, entity, op, payload, resp)
  }

  def runQuery(appName: String, entity: String, op: String, payload: Object, resp: HttpServletResponse): Unit = {
    val (session, model) = apps.get(appName)
    val payloadMap = Try(payload.asInstanceOf[Map[String,Object]])
    logger.trace(s"query payload: $payload")

    val result: Future[Object] = op match {
      case "GET" => {
        val result = query.getAndTruncate(model, entity, payloadMap.get, defaultLimit, defaultTTL, session, executor)
        cacheStreams(result)
      }
      case "POST" => model.mutation.create(entity, payload, session, executor)
      case "PUT" => model.mutation.update(entity, payloadMap.get, session, executor)
      case "DELETE" => model.mutation.delete(entity, payloadMap.get, session, executor)
      case _ => Future.failed(new RuntimeException(s"unsupported op: ${op}"))
    }
    logger.trace(op, Await.result(result, Duration.Inf))
    resp.getWriter.write(util.toJson(Await.result(result, Duration.Inf)))
  }

  override def doPut(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
    route(req, resp)
  }

  override def doDelete(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
    route(req, resp)
  }

  override def doGet(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
    route(req, resp)
  }

  override def doPost(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
    super.getServletContext
    route(req, resp)
  }

  def route(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
    try {
      val contentLength = req.getContentLengthLong
      http.validateRequestSize(contentLength, maxRequestSize)
      val op = req.getMethod
      val path = http.sanitizePath(req.getServletPath)
      logger.trace(s"http request: { path: '$path', method: '$op', content-length: $contentLength, content-type: '${req.getContentType}' }")
      path match {
        case s"/$appName/continue/${id}" =>
          timeRead(()=>{
            http.validateJsonContentHeader(req)
            continueQuery(appName, UUID.fromString(id), resp)
          })
        case s"/${appName}/q/${query}" =>
          timeRead(()=>{
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
          timeSchemaCreate(()=>{
            http.validateFileHeader(req)
            http.validateSchemaSize(contentLength, maxSchemaSize)
            val input = new String(req.getInputStream.readAllBytes)
            postSchema(appName, input, resp)
          })
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

object AppstaxServlet {
  val executor: ExecutionContext = ExecutionContext.global
}
case class ParsedStarGateConfig(
                           @BeanProperty val httpPort: Int,
                           @BeanProperty val defaultTTL: Int,
                           @BeanProperty val defaultLimit: Int,
                           @BeanProperty val maxSchemaSizeKB: Long,
                           @BeanProperty val maxRequestSizeKB: Long,
                           @BeanProperty val maxMutationSizeKB: Long,
                           @BeanProperty val cassandraContactPoints: java.util.List[_ <: Config],
                           @BeanProperty val cassandraDataCenter: String,
                           @BeanProperty val cassandraReplication: Int)
object Main {
  private val logger = Logger("main")
  private def mapConfig(config: Config):ParsedStarGateConfig = {
    ParsedStarGateConfig(
      config.getInt("http.port"),
      config.getInt("defaultTTL"),
      config.getInt("defaultLimit"),
      config.getLong("validation.maxSchemaSizeKB"),
      config.getLong("validation.maxMutationSizeKB"),
      config.getLong("validation.maxRequestSizeKB"),
      config.getConfigList("cassandra.contactPoints"),
      config.getString("cassandra.dataCenter"),
      config.getInt("cassandra.replication")
    )
  }

  def logStartup() = {
    logger.info("Launch Mission To StarGate" )
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
    logger.info(s"StarGate Version: 1.0.0")
  }

  def main(args: Array[String]) = {
    metrics.registerJVMMetrics()
    val config = (if (args.length > 0) ConfigFactory.parseFile(new File(args(0))).resolve() else ConfigFactory.defaultApplication()).resolve()
    val parsedConfig = mapConfig(config)
    logger.info(s"parsedConfig: ${util.toPrettyJson(parsedConfig)}")
    val server = new org.eclipse.jetty.server.Server(parsedConfig.httpPort)
    val handler = new ServletHandler()
    val servlet = new AppstaxServlet(parsedConfig)
    handler.addServletWithMapping(new ServletHolder(servlet), "/")
    //expose the MetricsServlet to the /metrics endpoint. To scrape metrics for Prometheus now you just need to point to the appropriate host and port combination
    //ie http://localhost:8080/metrics
    handler.addServletWithMapping(new ServletHolder(new MetricsServlet()), "/metrics");
    server.setHandler(handler)
    //has to be the last set handler, will wrap exisiting handler
    //note this will mean that metrics calls will be counted as well in the total request count
    metrics.registerServletHandlerStastics(server) 
    logStartup() 
    server.start
    server.join
  }
}

package object service extends scala.App {

  Main.main(args)

}
