package appstax.queries

import java.util.UUID

import appstax.model.{InputModel, RelationField}
import appstax.util.AsyncList

import scala.concurrent.{ExecutionContext, Future}

object pagination {

  // maps uuid to stream of entities, as well as the TTL on that stream
  type Streams = Map[UUID, (Int, AsyncList[Map[String,Object]])]

  // given a tree of entities in (lazy) AsyncLists, chop off the first N entities, and return a uuid pointing to the rest of the list
  def truncate(model: InputModel, entityName: String, getRequest: Map[String,Object],
               entities: AsyncList[Map[String,Object]], defaultLimit: Integer, defaultTTL: Integer, executor: ExecutionContext): Future[(List[Map[String,Object]], Streams)] = {

    val limit = getRequest.get(appstax.keywords.pagination.LIMIT).map(_.asInstanceOf[Integer]).getOrElse(defaultLimit)
    val continue = getRequest.get(appstax.keywords.pagination.CONTINUE).map(_.asInstanceOf[java.lang.Boolean]).getOrElse(java.lang.Boolean.FALSE)
    val ttl = getRequest.get(appstax.keywords.pagination.TTL).map(_.asInstanceOf[Integer]).getOrElse(defaultTTL)

    val (head, tail) = entities.splitAt(limit, executor)
    val uuid = UUID.randomUUID()
    val resultStream = if(continue) Map((uuid, (ttl, tail))) else Map.empty

    val entity = model.entities(entityName)
    val includedRelations: Map[String, (RelationField, Object)] = entity.relations.filter(r => getRequest.contains(r._1)).map(nr => (nr._1, (nr._2, getRequest(nr._1))))
    val updatedEntities = head.map(entities => {
      entities.map(entity => {
        truncateRelations(model, entityName, includedRelations, entity, defaultLimit, defaultTTL, executor)
      })
    })(executor)
    updatedEntities.flatMap(futureUpdatedEntities => {
      implicit val ec: ExecutionContext = executor
      Future.sequence(futureUpdatedEntities).map(entities_streams => {
        (entities_streams.map(_._1), entities_streams.map(_._2).reduce(_ ++ _))
      })
    })(executor)
  }

  // for one entity and a selection of relations (and their corresponding get requests), truncate all children and return their streams
  def truncateRelations(model: InputModel, entityName: String, relationRequests: Map[String, (RelationField, Object)],
               entity: Map[String,Object], defaultLimit: Integer, defaultTTL: Integer, executor: ExecutionContext): Future[(Map[String,Object], Streams)] = {
    val childResults = relationRequests.toList.map(name_relation_request => {
      val (relationName, (relation, request)) = name_relation_request
      val recurse = truncate(model, relation.targetEntityName, request.asInstanceOf[Map[String,Object]],
        entity(relationName).asInstanceOf[AsyncList[Map[String,Object]]], defaultLimit, defaultTTL, executor)
      recurse.map((relationName, _))
    })
    implicit val ec: ExecutionContext = executor
    Future.sequence(childResults).map(childResults => {
      childResults.foldLeft((entity, Map.empty : Streams))((entity_streams, childResult) => {
        val (entity, streams) = entity_streams
        (entity.updated(childResult._1, childResult._2._1), streams ++ childResult._2._2)
      })
    })
  }


}