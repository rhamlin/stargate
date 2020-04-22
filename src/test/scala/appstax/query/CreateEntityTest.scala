package appstax.query

import java.util.UUID

import appstax.CassandraTest
import appstax.model.{InputModel, OutputModel, parser}
import appstax.schema.ENTITY_ID_COLUMN_NAME
import com.datastax.oss.driver.api.core.CqlSession
import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import com.typesafe.config.ConfigFactory
import org.junit.{AfterClass, BeforeClass, Test}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}


class CreateEntityTest {

  val om = new ObjectMapper
  om.configure(SerializationFeature.INDENT_OUTPUT, true)
  import ExecutionContext.Implicits.global

  def create(model: OutputModel, entityName: String, session: CqlSession, executor: ExecutionContext): (Map[String,Object], Future[Map[String,Object]]) = {
    val request = appstax.model.generator.createEntity(model.input, entityName)
    val response = model.createWrapper(entityName)(session, request, executor).map(_(0))(executor)
    (request, response)
  }

  def zipEntityIds(model: InputModel, entityName: String, request: Map[String,Object], response: Map[String,Object]): Map[String,Object] = {
    val entity = model.entities(entityName)
    val id = Map((ENTITY_ID_COLUMN_NAME, response(ENTITY_ID_COLUMN_NAME)))
    val withFields = request.keys.filter(entity.fields.contains).foldLeft(id)((merge, field) => merge.updated(field, request.get(field).orNull))
    val withRelations = entity.relations.foldLeft(withFields)((merge, relation) => {
      assert(request.contains(relation._1) == response.contains(relation._1))
      if(!request.contains(relation._1)) {
        merge
      } else {
        val requestEntites = request(relation._1).asInstanceOf[List[Map[String,Object]]]
        val linkResponse = response(relation._1).asInstanceOf[Map[String,Object]]
        val idResponses = linkResponse(appstax.keywords.relation.LINK).asInstanceOf[List[Map[String,Object]]]
        assert(requestEntites.length == idResponses.length)
        val zipped = requestEntites.zip(idResponses).map((req_resp) => zipEntityIds(model, relation._2.targetEntityName, req_resp._1, req_resp._2))
        merge ++ Map((relation._1, zipped))
      }
    })
    withRelations
  }

  def getRequestRelations(model: InputModel, entityName: String, entityPayload: Map[String,Object]): Map[String, Object] = {
    val entity = model.entities(entityName)
    entity.relations.filter(rel => entityPayload.contains(rel._1)).foldLeft(Map.empty[String,Object])((merge, relation) => {
      val relatedList = entityPayload(relation._1).asInstanceOf[List[Map[String,ObjectMapper]]]
      if(relatedList.nonEmpty) {
        val recurse = getRequestRelations(model, relation._2.targetEntityName, relatedList(0))
        merge.updated(relation._1, recurse)
      } else {
        merge
      }
    })
  }
  def getRequestByEntityId(model: InputModel, entityName: String, entityPayload: Map[String,Object]): Map[String, Object] = {
    val conditions = Map((appstax.keywords.mutation.MATCH, List(ENTITY_ID_COLUMN_NAME, "=", entityPayload(ENTITY_ID_COLUMN_NAME))))
    conditions ++ getRequestRelations(model, entityName, entityPayload)
  }

  def diffCreateAndGet(create: Map[String,Object], get: Map[String,Object]): Unit = {
    get.keys.foreach(field => {
      val getVal = get(field)
      if(getVal.isInstanceOf[List[Object]]) {
        val createVal = create.get(field).map(_.asInstanceOf[List[Map[String,Object]]]).getOrElse(List.empty).sortBy(_(ENTITY_ID_COLUMN_NAME).asInstanceOf[UUID])
        val getValList = getVal.asInstanceOf[List[Map[String,Object]]].sortBy(_(ENTITY_ID_COLUMN_NAME).asInstanceOf[UUID])
        assert(createVal.length == getValList.length)
        createVal.zip(getValList).map(cg => diffCreateAndGet(cg._1, cg._2))
      } else {
        assert(getVal == create(field), List(getVal, "=", create(field)).toString)
      }
    })
  }


  @Test
  def createAndGetTest: Unit = {
    val inputModel = parser.parseModel(ConfigFactory.parseResources("schema.conf"))
    val model = appstax.schema.outputModel(inputModel)
    val session = CreateEntityTest.newSession
    Await.result(Future.sequence(model.tables.map(t => appstax.cassandra.create(session, t))), Duration.Inf)
    model.input.entities.keys.foreach(entityName => {
      List.range(0, 20).foreach(_ => {
        val (req, respF) = create(model, entityName, session, global)
        val resp = Await.result(respF, Duration.Inf)
        val zipped = zipEntityIds(model.input, entityName, req, resp)
        println(om.writeValueAsString(appstax.util.scalaToJava(zipped)))
        val getReq = getRequestByEntityId(model.input, entityName, zipped)
        val getResultAsync = model.getWrapper(entityName)(session, getReq, global)
        val getResult = Await.result(appstax.AppstaxServlet.truncateAsyncLists(getResultAsync, 1000), Duration.Inf)
        assert(getResult.length == 1)
        diffCreateAndGet(zipped, getResult(0))
      })
    })
    CreateEntityTest.cleanupSession(session)
  }
}

object CreateEntityTest extends CassandraTest {
  @BeforeClass def before = this.ensureCassandraRunning
  @AfterClass def after = this.cleanup
}
