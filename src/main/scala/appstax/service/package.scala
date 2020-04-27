package appstax

import java.io.File
import java.util.UUID
import java.util.concurrent._

import appstax.model.OutputModel
import appstax.queries.pagination.{StreamEntry, Streams}
import com.datastax.oss.driver.api.core.CqlSession
import com.typesafe.config.{Config, ConfigFactory}
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}
import org.eclipse.jetty.servlet.{ServletHandler, ServletHolder}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.util.Try


class AppstaxServlet(val config: Config) extends HttpServlet {

  val apps = new ConcurrentHashMap[String, (CqlSession, OutputModel)]()
  val continuationCache = new ConcurrentHashMap[UUID, (StreamEntry,ScheduledFuture[Unit])]()
  val continuationCleaner: ScheduledExecutorService = Executors.newScheduledThreadPool(1)

  val defaultLimit: Int = config.getInt("defaultLimit")
  val defaultTTL: Int = config.getInt("defaultTTL")
  import AppstaxServlet._

  def newSession(keyspace: String): CqlSession = {
    val contacts = config.getConfig("cassandra").getConfigList("contactPoints").asScala.toList.map(c => (c.getString("host"), c.getInt("port")))
    val dataCenter = config.getString("cassandra.dataCenter")
    val replication = config.getInt("cassandra.replication")
    cassandra.sessionWithNewKeyspace(contacts, dataCenter, keyspace, replication)
  }


  def postSchema(appName: String, input: String, resp: HttpServletResponse): Unit = {
    println(s"POSTING SCHEMA $appName, with $input")
    val model = appstax.schema.outputModel(appstax.model.parser.parseModel(input))
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
    val runtimePayload = appstax.model.queries.transform(query, payloadMap)
    runQuery(appName, query.entityName, "GET", runtimePayload, resp)
  }


  def cacheStreams(truncatedFuture: Future[(List[Map[String,Object]], Streams)]): Future[List[Map[String,Object]]] = {
    truncatedFuture.map(truncated_streams => {
      val (truncated, streams) = truncated_streams
      streams.foreach(stream => {
        // dont allow cleanup to run until stream is actually added to cache
        val lock = new Semaphore(0)
        val cleanup: ScheduledFuture[Unit] = continuationCleaner.schedule(() => {
          println("cleanup", continuationCache.keys, "-", stream._1)
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

    val truncateFuture = appstax.queries.pagination.truncate(model.input, entry.entityName, entry.getRequest, entry.entities, continueId, defaultLimit, defaultTTL, executor)
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
    println(payload)

    val result: Future[Object] = op match {
      case "GET" => {
        val result = queries.getAndTruncate(model, entity, payloadMap.get, defaultLimit, defaultTTL, session, executor)
        cacheStreams(result)
      }
      case "POST" => model.createWrapper(entity)(session, payload, executor)
      case "PUT" => model.updateWrapper(entity)(session, payloadMap.get, executor)
      case "DELETE" => model.deleteWrapper(entity)(session, payloadMap.get, executor)
      case _ => Future.failed(new RuntimeException(s"unsupported op: ${op}"))
    }
    println(op, Await.result(result, Duration.Inf))
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
    val op = req.getMethod
    val input = new String(req.getInputStream.readAllBytes())
    val path = req.getServletPath
    path match {
      case s"/$appName/continue/${id}" =>
        continueQuery(appName, UUID.fromString(id), resp)
      case s"/${appName}/q/${query}" =>
        runPredefinedQuery(appName, query, input, resp)
      case s"/${appName}/${entity}" =>
        val payload = util.fromJson(input)
        runQuery(appName, entity, op, payload, resp)
      case s"/${appName}" =>
        postSchema(appName, input, resp)
      case _ => throw new RuntimeException(s"path: $path does not match /:appName/:entity/:id pattern")
    }
  }
}

object AppstaxServlet {
  val executor: ExecutionContext = ExecutionContext.global
}


object Main {

  def main(args: Array[String]) = {
    val config = (if (args.length > 0) ConfigFactory.parseFile(new File(args(0))).resolve() else ConfigFactory.defaultApplication()).resolve()

    val server = new org.eclipse.jetty.server.Server(config.getInt("http.port"))
    val handler = new ServletHandler()
    val servlet = new AppstaxServlet(config)

    handler.addServletWithMapping(new ServletHolder(servlet), "/")
    server.setHandler(handler)
    server.start
    server.join

  }
}

package object service extends scala.App {

  Main.main(args)

}
