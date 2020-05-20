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
package stargate.service

import java.util.UUID
import java.util.concurrent._

import javax.servlet.http.HttpServletResponse
import org.json4s.{DefaultFormats, Formats}
import org.scalatra._
import org.scalatra.json._
import stargate.model.{OutputModel, generator, queries}
import stargate.query.pagination.{StreamEntry, Streams}
import stargate.{cassandra, query, util}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.Try
import com.datastax.oss.driver.api.core.CqlSession
import stargate.cassandra.CassandraTable
import stargate.service.config.ParsedStargateConfig
import com.typesafe.scalalogging.LazyLogging

class StargateServlet(val sgConfig: ParsedStargateConfig,
    val cqlSession: CqlSession,
    val apps: Namespaces,
    val datamodelRepoTable: CassandraTable)
  extends ScalatraServlet
    with NativeJsonSupport
    with CqlSupport
    with LazyLogging
    {
  val executor: ExecutionContextExecutor = ExecutionContext.global
  val continuationCache = new ConcurrentHashMap[UUID, (StreamEntry, ScheduledFuture[Unit])]()
  val continuationCleaner: ScheduledExecutorService = Executors.newScheduledThreadPool(1)
  val maxSchemaSize: Long = sgConfig.maxSchemaSizeKB * 1024
  val maxMutationSize: Long = sgConfig.maxMutationSizeKB * 1024
  val maxRequestSize: Long = sgConfig.maxRequestSizeKB * 1024
  //Sets default json output
  protected implicit lazy val jsonFormats: Formats = DefaultFormats

  post(s"/$StargateApiVersion/api/:namespace/schema"){
      http.validateHoconHeader(request)
      http.validateSchemaSize(request.getContentLengthLong, maxSchemaSize)
      val input = new String(request.getInputStream.readAllBytes)
      postSchema(params("namespace"), input)
  }

  delete(s"/$StargateApiVersion/api/:namespace/schema"){
    deleteSchema(params("namespace"), response)
    //TODO return json success/failure
  }

  post(s"/$StargateApiVersion/validate"){
    http.validateSchemaSize(request.contentLength.getOrElse(-1), maxSchemaSize)
    val input = new String(request.getInputStream.readAllBytes)
    stargate.model.parser.parseModel(input)
  }

  get(s"/$StargateApiVersion/api/:namespace/apigen/:entityName/:op"){
    generateQuery(params("namespace"), params("entityName"), params("op"), response)
  }

  get(s"/$StargateApiVersion/api/:namespace/query/stored/:queryName",
    request.getContentLengthLong < 1L){
    namedQueryRunner(params("payload"))
  }

  private def namedQueryRunner(input: String): Unit ={
    val appName = params("namespace")
    val query = params("queryName")
    runPredefinedQuery(appName, query, input, response)
  }

  /**
   * proper api using GET with body. Some clients may need the above POST
   */
  get(s"/$StargateApiVersion/api/:namespace/query/stored/:queryName",
    request.getContentLengthLong > 0L){
    logger.warn("using proper API on GET with body")
    val input = new String(request.getInputStream.readAllBytes)
    namedQueryRunner(input)
  }

  /**
   * The is for clients that cannot use GET with a Body and instead will pass it in with a queryParameter
   */
  get(s"/$StargateApiVersion/api/:namespace/query/entity/:entityName",
    request.getContentLengthLong < 1L){
    val payload = util.fromJson(params("payload"))
    runEntityQueryRoute(payload)
  }

  private def runEntityQueryRoute(payload: Object): Unit ={
    //TODO get rid of runquery bit
    runQuery(params("namespace"), params("entityName"), "GET", payload, response)
  }

  /**
   * this is the preferred route to use. The above with a Get with a giant query string is for clients that cannot use GET with a Body
   */
  get(s"/$StargateApiVersion/api/:namespace/query/entity/:entityName",
    request.getContentLengthLong > 0L
  ){
    //TODO move validation to filter
    http.validateMutation("GET", request.contentLength.getOrElse(-1), maxMutationSize)
    val input = new String(request.getInputStream.readAllBytes)
    val payload = util.fromJson(input)
    runEntityQueryRoute(payload)
  }

  get(s"/$StargateApiVersion/api/:namespace/query/continue/:id"){
    continueQuery(params("namespace"), UUID.fromString(params("id")), response)
  }

  post(s"/$StargateApiVersion/api/:namespace/query/entity/:entityName"){
    //TODO move validation to filter
    http.validateMutation("POST", request.contentLength.getOrElse(-1), maxMutationSize)
    val input = new String(request.getInputStream.readAllBytes)
    val payload = util.fromJson(input)
    //TODO get rid of runquery bit
    runQuery(params("namespace"), params("entityName"), "POST", payload, response)
  }

  put(s"/$StargateApiVersion/api/:namespace/query/entity/:entityName"){
    //TODO move validation to filter
    http.validateMutation("PUT", request.contentLength.getOrElse(-1), maxMutationSize)
    val input = new String(request.getInputStream.readAllBytes)
    val payload = util.fromJson(input)
    //TODO get rid of runquery bit
    runQuery(params("namespace"), params("entityName"), "PUT", payload, response)
  }

  delete(s"/$StargateApiVersion/api/:namespace/query/entity/:entityName"){
    //TODO move validation to filter
    http.validateMutation("DELETE", request.contentLength.getOrElse(-1), maxMutationSize)
    val input = new String(request.getInputStream.readAllBytes)
    val payload = util.fromJson(input)
    //TODO get rid of runquery bit
    runQuery(params("namespace"), params("entityName"), "DELETE", payload, response)
  }

  def lookupModel(appName: String): OutputModel = {
    val model = this.apps.get(appName)
    require(model != null, s"invalid database name: $appName")
    model
  }

  def postSchema(
                  appName: String,
                  input: String,
                ): Unit = {
    val model = stargate.schema.outputModel(stargate.model.parser.parseModel(input), appName)
    val previousDatamodel = util.await(datamodelRepository.fetchLatestDatamodel(appName, datamodelRepoTable, cqlSession, executor)).get
    if(!previousDatamodel.contains(input)) {
      logger.info(s"""creating keyspace "$appName" for new datamodel""")
      datamodelRepository.updateDatamodel(appName, input, datamodelRepoTable, cqlSession, executor)
      cassandra.recreateKeyspace(cqlSession, appName, sgConfig.cassandraReplication)
      Await.result(model.createTables(cqlSession, executor), Duration.Inf)
    } else {
      logger.info(s"""reusing existing keyspace "$appName" with latest datamodel""")
    }
    apps.put(appName, model)
  }
  def deleteSchema(appName: String, resp: HttpServletResponse): Unit = {
    logger.info(s"""deleting datamodels and keyspace for app "$appName" """)
    val removed = this.apps.remove(appName)
    util.await(datamodelRepository.deleteDatamodel(appName, datamodelRepoTable, cqlSession, executor)).get
    cassandra.wipeKeyspace(cqlSession, appName)
    if(removed == null) {
      resp.setStatus(404)
    }
  }

  def generateQuery(appName: String, entity: String, op: String, resp: HttpServletResponse): Unit = {
    val model = lookupModel(appName)
    require(model.input.entities.contains(entity), s"""database "$appName" does not have an entity named "$entity" """)
    val validOps = Set("create", "get", "update", "delete")
    require(requirement = validOps.contains(op), message = s"operation $op must be one of the following: $validOps")
    val requestF = op match {
      case "create" => generator.specificCreateRequest(model, entity, cqlSession, executor)
      case "get" => generator.specificGetRequest(model, entity, 3, cqlSession, executor)
      case "update" => generator.specificUpdateRequest(model, entity, cqlSession, executor)
      case "delete" => generator.specificDeleteRequest(model, entity, cqlSession, executor)
    }
    val request = util.await(requestF).get
    resp.getWriter.write(util.toJson(request))
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
    require(query.isSuccess, s"""no such query "$queryName" for database "$appName" """)
    val runtimePayload = queries.predefined.transform(query.get, payloadMap)
    val result = stargate.query.getAndTruncate(model, query.get.entityName, runtimePayload, sgConfig.defaultLimit, sgConfig.defaultTTL, cqlSession, executor)
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
    require(continue_cleanup != null, s"""no continuable query found for id $continueId in database "$appName" """)
    val (entry, cleanup) = continue_cleanup
    cleanup.cancel(false)

    val truncateFuture = stargate.query.pagination.truncate(
      model.input,
      entry.entityName,
      entry.getRequest,
      entry.entities,
      continueId,
      sgConfig.defaultLimit,
      sgConfig.defaultTTL,
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
    require(model.input.entities.contains(entity), s"""database "$appName" does not have an entity named "$entity" """)
    val payloadMap = Try(payload.asInstanceOf[Map[String, Object]])
    logger.trace(s"query payload: $payload")

    val result: Future[Object] = op match {
      case "GET" =>
        val result = query.untyped.getAndTruncate(model, entity, payloadMap.get, sgConfig.defaultLimit, sgConfig.defaultTTL, cqlSession, executor)
        cacheStreams(result)
      case "POST" => model.mutation.create(entity, payload, cqlSession, executor)
      case "PUT" =>  model.mutation.update(entity, payloadMap.get, cqlSession, executor)
      case "DELETE" => model.mutation.delete(entity, payloadMap.get, cqlSession, executor)
      case _ => Future.failed(new RuntimeException(s"unsupported op: $op"))
    }
    logger.trace(op, Await.result(result, Duration.Inf))
    resp.getWriter.write(util.toJson(Await.result(result, Duration.Inf)))
  }
}

