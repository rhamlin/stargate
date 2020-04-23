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
import scala.util.Random


class EntityCRUDTest {

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

  def createEntityWithIds(model: OutputModel, entityName: String, session: CqlSession, executor: ExecutionContext): Future[Map[String,Object]] = {
    val (request, response) = create(model, entityName, session, executor)
    response.map(zipEntityIds(model.input, entityName, request, _))
  }

  // get whole entity tree without looping back to parent entities
  def getRequestRelations(model: InputModel, entityName: String, visited: Set[String]): Map[String, Object] = {
    val relations = model.entities(entityName).relations.filter(r => !visited.contains(r._2.targetEntityName))
    relations.map(r => (r._1, getRequestRelations(model, r._2.targetEntityName, visited ++ Set(r._2.targetEntityName))))
  }
  def getRequestByEntityId(model: InputModel, entityName: String, entityId: UUID): Map[String, Object] = {
    val conditions = Map((appstax.keywords.mutation.MATCH, List(ENTITY_ID_COLUMN_NAME, "=", entityId)))
    conditions ++ getRequestRelations(model, entityName, Set(entityName))
  }
  def getEntity(model: OutputModel, entityName: String, entityId: UUID, session: CqlSession, executor: ExecutionContext): Future[Map[String,Object]] = {
    val request = getRequestByEntityId(model.input, entityName, entityId)
    val result = model.getWrapper(entityName)(session, request, executor)
    appstax.AppstaxServlet.truncateAsyncLists(result, 1000).map(list => {assert(list.length == 1); list(0)})
  }

  // checks that two entity trees are the same, ignoring missing or empty-list relations
  def diff(expected: Map[String,Object], queried: Map[String,Object]): Unit = {
    queried.keys.foreach(field => {
      val getVal = queried(field)
      if(getVal.isInstanceOf[List[Object]]) {
        val createVal = expected.get(field).map(_.asInstanceOf[List[Map[String,Object]]]).getOrElse(List.empty).sortBy(_(ENTITY_ID_COLUMN_NAME).asInstanceOf[UUID])
        val getValList = getVal.asInstanceOf[List[Map[String,Object]]].sortBy(_(ENTITY_ID_COLUMN_NAME).asInstanceOf[UUID])
        assert(createVal.length == getValList.length)
        createVal.zip(getValList).map(cg => diff(cg._1, cg._2))
      } else {
        assert(getVal == expected(field), List(getVal, "=", expected(field)).toString)
      }
    })
  }

  def testUpdateScalars(model: OutputModel, entityName: String, entity: Map[String,Object], session: CqlSession, executor: ExecutionContext): Future[Map[String,Object]] = {
    val relations = model.input.entities(entityName).relations.values.filter(r => entity.contains(r.name)).toList
    val randomRelation = relations(Random.nextInt(relations.length))
    val newValues = appstax.model.generator.entityFields(model.input.entities(randomRelation.targetEntityName).fields.values.toList)
    val updaateReq = newValues ++ Map((appstax.keywords.mutation.MATCH, List(randomRelation.inverseName + "." + ENTITY_ID_COLUMN_NAME, "=", entity(ENTITY_ID_COLUMN_NAME))))
    val updateRes = model.updateWrapper(randomRelation.targetEntityName)(session, updaateReq, executor)
    updateRes.map(_ => {
      val children = entity(randomRelation.name).asInstanceOf[List[Map[String,Object]]]
      val updatedChildren = children.map(_ ++ newValues)
      entity.updated(randomRelation.name, updatedChildren)
    })
  }


  @Test
  def crudTest: Unit = {
    val inputModel = parser.parseModel(ConfigFactory.parseResources("schema.conf"))
    val model = appstax.schema.outputModel(inputModel)
    val session = EntityCRUDTest.newSession
    Await.result(Future.sequence(model.tables.map(t => appstax.cassandra.create(session, t))), Duration.Inf)
    model.input.entities.keys.foreach(entityName => {
      List.range(0, 20).foreach(_ => {
        val created = Await.result(createEntityWithIds(model, entityName, session, global), Duration.Inf)
        println(om.writeValueAsString(appstax.util.scalaToJava(created)))
        val get = Await.result(getEntity(model, entityName, created(ENTITY_ID_COLUMN_NAME).asInstanceOf[UUID], session, global), Duration.Inf)
        diff(created, get)


      })
    })
    EntityCRUDTest.cleanupSession(session)
  }
}

object EntityCRUDTest extends CassandraTest {
  @BeforeClass def before = this.ensureCassandraRunning
  @AfterClass def after = this.cleanup
}
