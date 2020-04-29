package appstax.query

import java.util.UUID

import appstax.{CassandraTest, queries}
import appstax.model.{InputModel, MutationOps, OutputModel, RelationField, parser}
import appstax.schema.ENTITY_ID_COLUMN_NAME
import com.datastax.oss.driver.api.core.CqlSession
import com.typesafe.config.ConfigFactory
import org.junit.{AfterClass, BeforeClass, Test}
import org.junit.Assert._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Random, Try}


class EntityCRUDTest {

  import EntityCRUDTest._
  implicit val executor: ExecutionContext = EntityCRUDTest.executor

  // checks that two entity trees are the same, ignoring missing or empty-list relations
  def diff(expected: Map[String,Object], queried: Map[String,Object]): Unit = {
    queried.keys.foreach(field => {
      val getVal = queried(field)
      if(getVal.isInstanceOf[List[Object]]) {
        val createVal = expected.get(field).map(_.asInstanceOf[List[Map[String,Object]]]).getOrElse(List.empty).sortBy(_(ENTITY_ID_COLUMN_NAME).asInstanceOf[UUID])
        val getValList = getVal.asInstanceOf[List[Map[String,Object]]].sortBy(_(ENTITY_ID_COLUMN_NAME).asInstanceOf[UUID])
        assertEquals(createVal.length, getValList.length)
        createVal.zip(getValList).map(cg => diff(cg._1, cg._2))
      } else {
        assertEquals(getVal, expected(field))
      }
    })
  }

  def chooseRandomRelation(model: OutputModel, entityName: String, entity: Map[String,Object]): RelationField = {
    val relations = model.input.entities(entityName).relations.values.filter(r => entity.contains(r.name)).toList
    relations(Random.nextInt(relations.length))
  }

  def updateNestedScalars(model: OutputModel, mutations: MutationOps, entityName: String, entity: Map[String,Object], session: CqlSession, executor: ExecutionContext): Future[Map[String,Object]] = {
    val randomRelation = chooseRandomRelation(model, entityName, entity)
    val newValues = appstax.model.generator.entityFields(model.input.entities(randomRelation.targetEntityName).fields.values.toList)
    val updaateReq = newValues ++ Map((appstax.keywords.mutation.MATCH, List(randomRelation.inverseName + "." + ENTITY_ID_COLUMN_NAME, "=", entity(ENTITY_ID_COLUMN_NAME))))
    val updateRes = mutations.update(randomRelation.targetEntityName, updaateReq, session, executor)
    updateRes.map(_ => {
      val children = entity(randomRelation.name).asInstanceOf[List[Map[String,Object]]]
      val updatedChildren = children.map(_ ++ newValues)
      entity.updated(randomRelation.name, updatedChildren)
    })(executor)
  }

  def linkNestedRelation(model: OutputModel, mutations: MutationOps, entityName: String, entity: Map[String,Object], session: CqlSession, executor: ExecutionContext): Future[Map[String,Object]] = {
    val randomRelation = chooseRandomRelation(model, entityName, entity)
    val newChildren = List.range(1, Random.between(2, 5)).map(_ => appstax.model.generator.createEntity(model.input, randomRelation.targetEntityName, 1))
    val requestMatch = Map((appstax.keywords.mutation.MATCH, List(ENTITY_ID_COLUMN_NAME, "=", entity(ENTITY_ID_COLUMN_NAME))))
    val requestLink = Map((randomRelation.name, Map((appstax.keywords.relation.LINK, Map((appstax.keywords.mutation.CREATE, newChildren))))))
    val request = requestMatch ++ requestLink
    val updateRes = mutations.update(entityName,request, session, executor)
    updateRes.map(response => {
      val linkedIds = response(0)(randomRelation.name).asInstanceOf[Map[String,List[Map[String,Object]]]](appstax.keywords.relation.LINK).map(_(ENTITY_ID_COLUMN_NAME))
      assert(linkedIds.length == newChildren.length)
      val childrenWithIds = newChildren.zip(linkedIds).map(ci => ci._1.updated(ENTITY_ID_COLUMN_NAME, ci._2))
      entity.updatedWith(randomRelation.name)(_.map(_.asInstanceOf[List[Map[String,Object]]] ++ childrenWithIds))
    })(executor)
  }

  def unlinkNestedRelation(model: OutputModel, mutations: MutationOps, entityName: String, entity: Map[String,Object], session: CqlSession, executor: ExecutionContext): Future[Map[String,Object]] = {
    val randomRelation = chooseRandomRelation(model, entityName, entity)
    val children = entity(randomRelation.name).asInstanceOf[List[Map[String,Object]]]
    if(children.isEmpty) {
      return Future.successful(entity)
    }
    val randomChild = children(Random.nextInt(children.length))(ENTITY_ID_COLUMN_NAME).asInstanceOf[UUID]
    val requestMatch = Map((appstax.keywords.mutation.MATCH, List(ENTITY_ID_COLUMN_NAME, "=", entity(ENTITY_ID_COLUMN_NAME))))
    val requestUnlink = Map((randomRelation.name, Map((appstax.keywords.relation.UNLINK, List(ENTITY_ID_COLUMN_NAME, "=", randomChild)))))
    val request = requestMatch ++ requestUnlink
    val updateRes = mutations.update(entityName, request, session, executor)
    updateRes.map(response => {
      assert(response.length == 1)
      val unlinkedIds = response(0)(randomRelation.name).asInstanceOf[Map[String,List[Map[String,Object]]]](appstax.keywords.relation.UNLINK).map(_(ENTITY_ID_COLUMN_NAME))
      assert(unlinkedIds == List(randomChild))
      entity.updatedWith(randomRelation.name)(_.map(_.asInstanceOf[List[Map[String,Object]]].filter(_(ENTITY_ID_COLUMN_NAME) != randomChild)))
    })(executor)
  }

  def replaceNestedRelation(model: OutputModel, mutations: MutationOps, entityName: String, entity: Map[String,Object], session: CqlSession, executor: ExecutionContext): Future[Map[String,Object]] = {
    val randomRelation = chooseRandomRelation(model, entityName, entity)
    val children = entity(randomRelation.name).asInstanceOf[List[Map[String,Object]]]
    if(children.isEmpty) {
      return Future.successful(entity)
    }
    val randomChild = children(Random.nextInt(children.length))(ENTITY_ID_COLUMN_NAME).asInstanceOf[UUID]
    val requestMatch = Map((appstax.keywords.mutation.MATCH, List(ENTITY_ID_COLUMN_NAME, "=", entity(ENTITY_ID_COLUMN_NAME))))
    val requestUnlink = Map((randomRelation.name, Map((appstax.keywords.relation.UNLINK, List(ENTITY_ID_COLUMN_NAME, "=", randomChild)))))
    val request = requestMatch ++ requestUnlink
    val updateRes = mutations.update(entityName, request, session, executor)
    updateRes.map(response => {
      assert(response.length == 1)
      val relationResponse = response(0)(randomRelation.name).asInstanceOf[Map[String,List[Map[String,Object]]]]
      val linkedIds = relationResponse(appstax.keywords.relation.LINK).map(_(ENTITY_ID_COLUMN_NAME))
      val unlinkedIds = relationResponse(appstax.keywords.relation.UNLINK).map(_(ENTITY_ID_COLUMN_NAME))
      assert(linkedIds == List(randomChild))
      assert(unlinkedIds.length == children.length - 1)
      entity.updatedWith(randomRelation.name)(_.map(_.asInstanceOf[List[Map[String,Object]]].filter(_(ENTITY_ID_COLUMN_NAME) == randomChild)))
    })(executor)
  }

  def deleteNestedEntity(model: OutputModel, mutations: MutationOps, entityName: String, entity: Map[String,Object], session: CqlSession, executor: ExecutionContext): Future[Map[String,Object]] = {
    val randomRelation = chooseRandomRelation(model, entityName, entity)
    val children = entity(randomRelation.name).asInstanceOf[List[Map[String,Object]]]
    if(children.isEmpty) {
      return Future.successful(entity)
    }
    val randomChild = children(Random.nextInt(children.length))(ENTITY_ID_COLUMN_NAME).asInstanceOf[UUID]
    val requestMatch = Map((appstax.keywords.mutation.MATCH, List(ENTITY_ID_COLUMN_NAME, "=", randomChild)))
    val deleteRes = mutations.delete(randomRelation.targetEntityName, requestMatch, session, executor)
    deleteRes.flatMap(response => {
      val deleted = getEntities(model, randomRelation.targetEntityName, randomChild, session, executor)
      deleted.map(deleted => { assert(deleted.isEmpty); response})(executor)
    })(executor).map(_ => {
      entity.updatedWith(randomRelation.name)(_.map(_.asInstanceOf[List[Map[String,Object]]].filter(_(ENTITY_ID_COLUMN_NAME) != randomChild)))
    })(executor)
  }

  def crudTest(model: OutputModel, mutations: MutationOps): Unit = {
    val inputModel = parser.parseModel(ConfigFactory.parseResources("schema.conf"))
    val model = appstax.schema.outputModel(inputModel)
    val session = EntityCRUDTest.newSession
    implicit val ec: ExecutionContext = EntityCRUDTest.executor
    Await.result(Future.sequence(model.tables.map(t => appstax.cassandra.create(session, t))), Duration.Inf)
    model.input.entities.keys.foreach(entityName => {
      List.range(0, 20).foreach(_ => {
        val created = Await.result(createEntityWithIds(model, mutations, entityName, session, executor), Duration.Inf)
        val get1 = Await.result(getEntity(model, entityName, created(ENTITY_ID_COLUMN_NAME).asInstanceOf[UUID], session, executor), Duration.Inf)
        diff(created, get1)

        val updated = Await.result(updateNestedScalars(model, mutations, entityName, created, session, executor), Duration.Inf)
        val get2 = Await.result(getEntity(model, entityName, created(ENTITY_ID_COLUMN_NAME).asInstanceOf[UUID], session, executor), Duration.Inf)
        diff(updated, get2)

        val linked = Await.result(linkNestedRelation(model, mutations, entityName, updated, session, executor), Duration.Inf)
        val get3 = Await.result(getEntity(model, entityName, created(ENTITY_ID_COLUMN_NAME).asInstanceOf[UUID], session, executor), Duration.Inf)
        diff(linked, get3)

        val unlinked = Await.result(unlinkNestedRelation(model, mutations, entityName, linked, session, executor), Duration.Inf)
        val get4 = Await.result(getEntity(model, entityName, created(ENTITY_ID_COLUMN_NAME).asInstanceOf[UUID], session, executor), Duration.Inf)
        diff(unlinked, get4)

        val replaced = Await.result(unlinkNestedRelation(model, mutations, entityName, unlinked, session, executor), Duration.Inf)
        val get5 = Await.result(getEntity(model, entityName, created(ENTITY_ID_COLUMN_NAME).asInstanceOf[UUID], session, executor), Duration.Inf)
        diff(replaced, get5)

        val deleted = Await.result(deleteNestedEntity(model, mutations, entityName, replaced, session, executor), Duration.Inf)
        val get6 = Await.result(getEntity(model, entityName, created(ENTITY_ID_COLUMN_NAME).asInstanceOf[UUID], session, executor), Duration.Inf)
        diff(deleted, get6)
      })
    })
  }

  @Test
  def crudTest: Unit = {
    val inputModel = parser.parseModel(ConfigFactory.parseResources("schema.conf"))
    val model = appstax.schema.outputModel(inputModel)
    crudTest(model, model.mutation)
  }

  @Test
  def batchedCrudTest: Unit = {
    val inputModel = parser.parseModel(ConfigFactory.parseResources("schema.conf"))
    val model = appstax.schema.outputModel(inputModel)
    crudTest(model, model.batchMutation)
  }
}




object EntityCRUDTest extends CassandraTest {

  implicit val executor: ExecutionContext = ExecutionContext.global

  def create(model: OutputModel, mutations: MutationOps, entityName: String, session: CqlSession, executor: ExecutionContext): (Map[String,Object], Future[Map[String,Object]]) = {
    val request = appstax.model.generator.createEntity(model.input, entityName)
    val response = mutations.create(entityName, request, session, executor).map(_(0))(executor)
    (request, response)
  }

  def zipEntityIds(model: InputModel, entityName: String, request: Map[String,Object], response: Map[String,Object]): Map[String,Object] = {
    val entity = model.entities(entityName)
    val id = Map((ENTITY_ID_COLUMN_NAME, response(ENTITY_ID_COLUMN_NAME)))
    val withFields = request.keys.filter(entity.fields.contains).foldLeft(id)((merge, field) => merge.updated(field, request.get(field).orNull))
    val withRelations = entity.relations.foldLeft(withFields)((merge, relation) => {
      assertEquals(request.contains(relation._1), response.contains(relation._1))
      if(!request.contains(relation._1)) {
        merge
      } else {
        val requestEntites = request(relation._1).asInstanceOf[List[Map[String,Object]]]
        val linkResponse = response(relation._1).asInstanceOf[Map[String,Object]]
        val idResponses = linkResponse(appstax.keywords.relation.LINK).asInstanceOf[List[Map[String,Object]]]
        assertEquals(requestEntites.length, idResponses.length)
        val zipped = requestEntites.zip(idResponses).map((req_resp) => zipEntityIds(model, relation._2.targetEntityName, req_resp._1, req_resp._2))
        merge ++ Map((relation._1, zipped))
      }
    })
    withRelations
  }

  def createEntityWithIds(model: OutputModel, mutations: MutationOps, entityName: String, session: CqlSession, executor: ExecutionContext): Future[Map[String,Object]] = {
    val (request, response) = create(model, mutations, entityName, session, executor)
    response.map(zipEntityIds(model.input, entityName, request, _))(executor)
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
  def getEntities(model: OutputModel, entityName: String, entityId: UUID, session: CqlSession, executor: ExecutionContext): Future[List[Map[String,Object]]] = {
    val request = getRequestByEntityId(model.input, entityName, entityId)
    queries.getAndTruncate(model, entityName, request, 1000, session, executor)
  }
  def getEntity(model: OutputModel, entityName: String, entityId: UUID, session: CqlSession, executor: ExecutionContext): Future[Map[String,Object]] = {
    getEntities(model, entityName, entityId, session, executor).map(list => {assert(list.length == 1); list(0)})(executor)
  }
  def getAllEntities(model: OutputModel, entityName: String, session: CqlSession, executor: ExecutionContext): Future[List[Map[String,Object]]] = {
    val request = getRequestRelations(model.input, entityName, Set(entityName)).updated(appstax.keywords.mutation.MATCH, List.empty)
    queries.getAndTruncate(model, entityName, request, 1000, session, executor)
  }


  @BeforeClass def before = this.ensureCassandraRunning
  @AfterClass def after = this.cleanup
}
