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

import com.datastax.oss.driver.api.core.CqlSession
import javax.servlet.http.HttpServletResponse
import org.json4s.{DefaultFormats, Formats}
import org.scalatra._
import org.scalatra.json._
import org.scalatra.swagger._
import stargate.cassandra.CassandraTable
import stargate.metrics.RequestCollector
import stargate.model.{InputModel, OutputModel, generator, queries}
import stargate.query.pagination.{StreamEntry, Streams}
import stargate.{cassandra, query, util}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.Try

class StargateServlet(val sgConfig: ParsedStargateConfig)
                     (implicit val swagger: Swagger)
  extends ScalatraServlet
    with RequestCollector
    with NativeJsonSupport
    with SwaggerSupport
{
  val executor: ExecutionContextExecutor = ExecutionContext.global
  val apps = new ConcurrentHashMap[String, OutputModel]()
  val continuationCache =
    new ConcurrentHashMap[UUID, (StreamEntry, ScheduledFuture[Unit])]()
  val continuationCleaner: ScheduledExecutorService =
    Executors.newScheduledThreadPool(1)

  val maxSchemaSize: Long = sgConfig.maxSchemaSizeKB * 1024
  val maxMutationSize: Long = sgConfig.maxMutationSizeKB * 1024
  val maxRequestSize: Long = sgConfig.maxRequestSizeKB * 1024

  val cqlSession: CqlSession = cassandra.session(sgConfig.cassandraContactPoints, sgConfig.cassandraDataCenter)
  val datamodelRepoTable: CassandraTable = util.await(datamodelRepository.ensureRepoTableExists(sgConfig.stargateKeyspace, sgConfig.cassandraReplication, cqlSession, executor)).get
  // reload previous datamodels
  Await.result(datamodelRepository.fetchAllLatestDatamodels(datamodelRepoTable, cqlSession, executor), Duration.Inf).foreach(name_config => {
    logger.info(s"reloading datamodel from cassandra: ${name_config._1}")
    val model = stargate.schema.outputModel(stargate.model.parser.parseModel(name_config._2), name_config._1)
    apps.put(name_config._1, model)
  })

  //Sets default json output
  protected implicit lazy val jsonFormats: Formats = DefaultFormats
  //necessary magic for the swagger API
  protected val applicationDescription = "The Stargate API. It exposes http operations for querying Apache Cassandra and DataStax Enterprise."

  val createNamespaceSwagger: SwaggerSupportSyntax.OperationBuilder
  = (apiOperation[Map[String,Object]]("createNamespace")
    summary "create a new namespace"
    consumes "application/hocon"
    description "create a new namespace from a posted data model document"
    parameter bodyParam[String]("payload")
      .description( """"
                      |creates an app to query against based on a given
                      |hocon document. This does do validation and requires application/hocon
                      |```
                      |entities {
                      |    Customer {
                      |        fields {
                      |            id: uuid
                      |            email: string
                      |            firstName: string
                      |            lastName: string
                      |        }
                      |        relations {
                      |            addresses { type: Address, inverse: customers }
                      |            orders { type: Order, inverse: customer }
                      |        }
                      |    }
                      |    Order {
                      |        fields {
                      |            id: uuid
                      |            time: int
                      |            subtotal: int
                      |            tax: int
                      |            total: int
                      |        }
                      |        relations {
                      |            customer { type: Customer, inverse: orders }
                      |            deliveryAddress { type: Address, inverse: orders }
                      |            products { type: Product, inverse: orders }
                      |        }
                      |    }
                      |    Product {
                      |        fields {
                      |            id: uuid
                      |            name: string
                      |            price: int
                      |        }
                      |        relations {
                      |            orders { type: Order, inverse: products }
                      |        }
                      |    }
                      |    Address {
                      |        fields {
                      |            street: string
                      |            zipCode: string
                      |        }
                      |        relations {
                      |            customers { type: Customer, inverse: addresses }
                      |            orders { type: Order, inverse: deliveryAddress }
                      |        }
                      |    }
                      |}
                      |queries: {
                      |    Customer: {
                      |        customerByFirstName {
                      |            "-match": [firstName, "=", customerName]
                      |            "-include": [firstName, lastName, email],
                      |            "addresses": {
                      |                "-include": [street, zipCode]
                      |            }
                      |            "orders": {
                      |                "-include": [id, time, total]
                      |                "products": {
                      |                    "-include": [id, name, price]
                      |                }
                      |            }
                      |        }
                      |    }
                      |}
                      |queryConditions: {
                      |    Customer: [
                      |        ["firstName", "="]
                      |        ["email", "=", "orders.deliveryAddress.street", "="]
                      |    ]
                      |}
                      |```
                      |""".stripMargin)
    parameter pathParam[String]("namespace")
    .example("testNS")
    .description("namespace to create")
    )
  post(s"/$StargateApiVersion/api/:namespace/schema",
    operation(createNamespaceSwagger)){
    timeSchemaCreate(() => {
        http.validateHoconHeader(request)
        http.validateSchemaSize(request.getContentLengthLong, maxSchemaSize)
        val input = new String(request.getInputStream.readAllBytes)
        postSchema(params("namespace"), input, response)
      })
  }

  val deleteNamespaceSwagger: SwaggerSupportSyntax.OperationBuilder
  = (apiOperation[Map[String, Object]]("deleteNamespace")
    summary "delete namespace"
    description "delete the specified namespace"
    parameter pathParam[String]("namespace")
      .example("testNS")
      .description("namespace to delete")
    )
  delete(s"/$StargateApiVersion/api/:namespace/schema", operation(deleteNamespaceSwagger)){
    deleteSchema(params("namespace"), response)
    //TODO return json success/failure
  }

  val validateNamespaceDocSwagger: SwaggerSupportSyntax.OperationBuilder
  = (apiOperation[Map[String, Object]]("validateNamespace")
    summary "validation of app document"
    description "validate the data model document is valid"
    consumes "application/hocon"
    parameter bodyParam[Map[String, Object]]("payload")
    .description(
      """"
        |Payload will validate the following hocon document for correctness
        |```
        |entities {
        |    Customer {
        |        fields {
        |            id: uuid
        |            email: string
        |            firstName: string
        |            lastName: string
        |        }
        |        relations {
        |            addresses { type: Address, inverse: customers }
        |            orders { type: Order, inverse: customer }
        |        }
        |    }
        |    Order {
        |        fields {
        |            id: uuid
        |            time: int
        |            subtotal: int
        |            tax: int
        |            total: int
        |        }
        |        relations {
        |            customer { type: Customer, inverse: orders }
        |            deliveryAddress { type: Address, inverse: orders }
        |            products { type: Product, inverse: orders }
        |        }
        |    }
        |    Product {
        |        fields {
        |            id: uuid
        |            name: string
        |            price: int
        |        }
        |        relations {
        |            orders { type: Order, inverse: products }
        |        }
        |    }
        |    Address {
        |        fields {
        |            street: string
        |            zipCode: string
        |        }
        |        relations {
        |            customers { type: Customer, inverse: addresses }
        |            orders { type: Order, inverse: deliveryAddress }
        |        }
        |    }
        |}
        |queries: {
        |    Customer: {
        |        customerByFirstName {
        |            "-match": [firstName, "=", customerName]
        |            "-include": [firstName, lastName, email],
        |            "addresses": {
        |                "-include": [street, zipCode]
        |            }
        |            "orders": {
        |                "-include": [id, time, total]
        |                "products": {
        |                    "-include": [id, name, price]
        |                }
        |            }
        |        }
        |    }
        |}
        |queryConditions: {
        |    Customer: [
        |        ["firstName", "="]
        |        ["email", "=", "orders.deliveryAddress.street", "="]
        |    ]
        |}
        |```
        |""".stripMargin)
    )
  post(s"/$StargateApiVersion/validate", operation(validateNamespaceDocSwagger)) {
    http.validateSchemaSize(request.contentLength.getOrElse(-1), maxSchemaSize)
    val input = new String(request.getInputStream.readAllBytes)
    stargate.model.parser.parseModel(input)
  }

  val generateQuerySwagger: SwaggerSupportSyntax.OperationBuilder
  = (apiOperation[Map[String,Object]]("generateQuery")
    summary "generate a query from a given endpoint"
    description "can be used to intelligently and dynamically generate endpoints documentation"
    parameter pathParam[String]("namespace")
      .example("namespace")
      .description("app to generate requests for")
    parameter pathParam[String]("entityName")
      .example("Customer")
      .description("entity to generate requests for")
    parameter pathParam[String]("op")
      .example("create")
      .description("operation that you want the doc for")
      .allowableValues("create", "delete", "get", "update")
    )
  get(s"/$StargateApiVersion/api/:namespace/apigen/:entityName/:op"){
    generateQuery(params("namespace"), params("entityName"), params("op"), response)
  }

  val runQuerySwagger: SwaggerSupportSyntax.OperationBuilder
  = (apiOperation[List[Map[String, Object]]]("runQuery")
      summary "run predefined query"
      description "submit predefined queries"
      parameter queryParam[Map[String, Object]]("payload")
      .example(
        """
          |```
          |{
          |        "-match":{
          |            "customerName": "Steve"
          |        }
          |}```
          |""".stripMargin)
      .description(
        """the match parameters for your query. The following query
          |is searching all customers by the `customerName` of `Steve`
          |```
          |{
          |        "-match":{
          |            "customerName": "Steve"
          |        }
          |}
          |```
          |""".stripMargin)
    parameter headerParam[String]("X-HTTP-Method-Override")
        .defaultValue("GET")
    parameter pathParam[String]("namespace")
        .example("testNS")
        .description("app to submit query against")
      parameter pathParam[String]("queryName")
        .example("customerByFirstName")
        .description("query key to submit a request against"))
  /**
   * Some client may need to use query strings the request because there is a Body
   */
  get(s"/$StargateApiVersion/api/:namespace/query/stored/:queryName",
    request.getContentLengthLong < 1L,
    operation(runQuerySwagger)) {
    namedQueryRunner(params("payload"))
  }

  private def namedQueryRunner(input: String) ={
    timeRead(() => {
      val appName = params("namespace")
      val query = params("queryName")
      runPredefinedQuery(appName, query, input, response)
    })
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

  val entityQuerySwagger: SwaggerSupportSyntax.OperationBuilder
    = (apiOperation[List[Map[String, Object]]]("entityQuerySwagger")
    summary "run entity query"
    description "to query an existing app and entity"
    parameter queryParam[String]("payload")
      .description(
        """Query payload to submit to do searches.
          |The following example finds all users with the `firstName` of "Steve" and
          |retrieves all of their orders and addresses:
          |```
          |{
          |    "-match":["firstName","=", "Steve"],
          |        "addresses":{},
          |        "orders":{}
          |}
          |```
          |""".stripMargin)
    parameter pathParam[String]("namespace")
      .example("testNS")
      .description("namespace to submit query against")
    parameter pathParam[String]("entityName")
      .example("Customer")
      .description("entity to query against"))
  /**
   * The is for clients that cannot use GET with a Body and instead will pass it in with a queryParameter
   */
  get(s"/$StargateApiVersion/api/:namespace/query/entity/:entityName",
    request.getContentLengthLong < 1L,
    operation(entityQuerySwagger)) {
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

  val continueQuerySwagger: SwaggerSupportSyntax.OperationBuilder
    = (apiOperation[List[Map[String, Object]]]("continueQuery")
    summary "continue paging through a result set"
    description "Can continue existing queries by passing an id from a previous query"
    parameter pathParam[String]("namespace")
     .example("testNS")
     .description("app to submit query against")
    parameter pathParam[String]("id")
     .example("5ed0b80b-2934-4c58-acba-830d8bce7d13"))
     .description("id of the original query")
  get(s"$StargateApiVersion/api/:namespace/query/continue/:id", operation(continueQuerySwagger)){
    timeRead(() => {
      continueQuery(params("namespace"), UUID.fromString(params("id")), response)
    })
  }

  val createQuerySwagger: SwaggerSupportSyntax.OperationBuilder
  = (apiOperation[Map[String, Object]]("createEntity")
    summary "add a new record"
    description "Adds a new entity to the database"
    parameter bodyParam[String]("payload")
      .description(
        """Payload to add a new entity record.
          |What follows is a new `Customer` named `Steve`:
          |```
          |{
          |        "firstName": "Steve",
          |        "addresses": {
          |            "street": "kent st",
          |            "zipCode":"22046"
          |        },
          |        "orders":[
          |            {
          |                "time": 12345,
          |                "products": {
          |                        "-update": {
          |                            "-match": ["name", "=", "widget"]
          |                        }
          |                }
          |            },
          |            {
          |                "total": 0,
          |                "products": {
          |                        "-create": []
          |                }
          |            }
          |        ]
          |    }
          |```
          |""".stripMargin)
    parameter pathParam[String]("namespace")
      .example("testNS")
      .description("namespace to submit query against")
    parameter pathParam[String]("entityName")
      .example("Customer")
      .description("entity to query against"))
  post(s"/$StargateApiVersion/api/:namespace/query/entity/:entityName", operation(createQuerySwagger)) {
    //TODO move validation to filter
    http.validateMutation("POST", request.contentLength.getOrElse(-1), maxMutationSize)
    val input = new String(request.getInputStream.readAllBytes)
    val payload = util.fromJson(input)
    //TODO get rid of runquery bit
    runQuery(params("namespace"), params("entityName"), "POST", payload, response)
  }

  val updateQuerySwagger: SwaggerSupportSyntax.OperationBuilder
  = (apiOperation[Map[String, Object]]("updateEntity")
    summary "update records"
    description "Update records specified in the match criteria by the -update values specified"
    parameter bodyParam[String]("payload")
    .description(
      """match arguments to find records to update and the update value to apply.
        |What follows is an example with where all users with the firstName of steve
        |are given the street address of "other st":
        |
        |```
        |{
        |        "-match":["firstName","=","Steve"],
        |        "lastName": "Danger",
        |        "addresses":{
        |            "-update":{
        |                "-match":["customers.firstName","=","Steve"],
        |                "street":"other st"}}
        |```
        |""".stripMargin)
    parameter pathParam[String]("namespace")
      .example("namespace")
      .description("namespace to submit query against")
    parameter pathParam[String]("entityName")
      .example("Customer")
      .description("entity to query against"))
  put(s"/$StargateApiVersion/api/:namespace/query/entity/:entityName", operation(updateQuerySwagger)){
    //TODO move validation to filter
    http.validateMutation("PUT", request.contentLength.getOrElse(-1), maxMutationSize)
    val input = new String(request.getInputStream.readAllBytes)
    val payload = util.fromJson(input)
    //TODO get rid of runquery bit
    runQuery(params("namespace"), params("entityName"), "PUT", payload, response)
  }

  val deleteQuerySwagger: SwaggerSupportSyntax.OperationBuilder
  = (apiOperation("deleteEntity")
    summary "deletes a new record"
    description "Removes documents by id"
    parameter bodyParam[String]("payload")
    .description(
      """Payload to delete documents that match the criteria.
        |This for example will delete several documents by id
        |```
        |{
        |    "-match": [
        |        "customer.id",
        |        "=",
        |        "65e4908f-dc12-4997-bbc5-4e9500a0136c",
        |        "time",
        |        "=",
        |        705299721,
        |        "id",
        |        "=",
        |        "dec0870f-7dfe-4b4e-af95-552c4484e504"
        |    ]
        |}
        |```
        |The follow document recuses and deletes related products as well
        |```
        |{
        |    "-match": [
        |        "customer.id",
        |        "=",
        |        "65e4908f-dc12-4997-bbc5-4e9500a0136c",
        |        "time",
        |        "=",
        |        705299721,
        |        "id",
        |        "=",
        |        "dec0870f-7dfe-4b4e-af95-552c4484e504"
        |    ],
        |    "products": {}
        |}
        |```
        |""".stripMargin)
    parameter pathParam[String]("namespace")
    .example("testNS")
    .description("app to submit query against")
    parameter pathParam[String]("entityName")
     .example("Customer")
     .description("entity to query against"))
  delete(s"/$StargateApiVersion/api/:namespace/query/entity/:entityName", operation(deleteQuerySwagger)){
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
                  resp: HttpServletResponse
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
    val removed = apps.remove(appName)
    util.await(datamodelRepository.deleteDatamodel(appName, datamodelRepoTable, cqlSession, executor)).get
    cassandra.wipeKeyspace(cqlSession, appName)
    if(removed == null) {
      resp.setStatus(404)
    }
  }
  def generateQuery(appName: String, entity: String, op: String, resp: HttpServletResponse): Unit = {
    val model = lookupModel(appName)
    require(model.input.entities.contains(entity), s"""database "${appName}" does not have an entity named "${entity}" """)
    val validOps = Set("create", "get", "update", "delete")
    require(validOps.contains(op), s"operation ${op} must be one of the following: ${validOps}")
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
/**
  def route(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
    try {
      val contentLength = req.getContentLengthLong
      http.validateRequestSize(contentLength, maxRequestSize)
      val op = req.getMethod
      path match {

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
 TODO add filters to replace the validation and error counters
 */
}

