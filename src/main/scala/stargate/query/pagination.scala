package stargate.query

import java.util.UUID

import stargate.model.queries.GetSelection
import stargate.model.{InputModel, RelationField}
import stargate.util.AsyncList

import scala.concurrent.{ExecutionContext, Future}

object pagination {

  // maps uuid to stream of entities, as well as the TTL and get request on that stream
  case class StreamEntry(entityName: String, getRequest: GetSelection, ttl: Int, entities: AsyncList[Map[String,Object]])
  type Streams = Map[UUID, StreamEntry]

  // given a tree of entities in (lazy) AsyncLists, chop off the first N entities, and return a uuid pointing to the rest of the list
  def truncate(model: InputModel, entityName: String, getRequest: GetSelection,
               entities: AsyncList[Map[String,Object]], defaultLimit: Integer, defaultTTL: Integer, executor: ExecutionContext): Future[(List[Map[String,Object]], Streams)] = {
    truncate(model, entityName, getRequest, entities, UUID.randomUUID, defaultLimit, defaultTTL, executor)
  }

  def truncate(model: InputModel, entityName: String, getRequest: GetSelection,
               entities: AsyncList[Map[String,Object]], streamId: UUID, defaultLimit: Int, defaultTTL: Int, executor: ExecutionContext): Future[(List[Map[String,Object]], Streams)] = {

    val limit = getRequest.limit.getOrElse(defaultLimit)
    val continue = getRequest.continue
    val ttl = getRequest.ttl.getOrElse(defaultTTL)

    val (head, tail) = entities.splitAt(limit, executor)
    val resultStream: Streams = if(continue) Map((streamId, StreamEntry(entityName, getRequest, ttl, tail))) else Map.empty

    val futureUpdatedEntities = head.map(entities => {
      entities.map(entity => {
        truncateRelations(model, entityName, getRequest.relations, entity, defaultLimit, defaultTTL, executor)
      })
    })(executor)
    val updatedEntities = futureUpdatedEntities.flatMap(futureUpdatedEntities => {
      implicit val ec: ExecutionContext = executor
      Future.sequence(futureUpdatedEntities).map(entities_streams => {
        (entities_streams.map(_._1), entities_streams.map(_._2).fold(Map.empty:Streams)(_ ++ _))
      })
    })(executor)
    updatedEntities.flatMap(entities_streams => {
      val (entities, stream) = entities_streams
      // if continuing is enabled, and more are left in the stream, append a { "-continue": uuid } to the end of the stream
      val withContinueId = if(continue) {
        tail.isEmpty(executor).map(noMore => {
          if(noMore) { entities } else { entities ++ List(Map((stargate.keywords.pagination.CONTINUE, streamId))) }
        })(executor)
      } else {
        Future.successful(entities)
      }
      withContinueId.map((_, stream ++ resultStream))(executor)
    })(executor)
  }

  // for one entity and a selection of relations (and their corresponding get requests), truncate all children and return their streams
  def truncateRelations(model: InputModel, entityName: String, relationRequests: Map[String, GetSelection],
               entity: Map[String,Object], defaultLimit: Integer, defaultTTL: Integer, executor: ExecutionContext): Future[(Map[String,Object], Streams)] = {
    implicit val ec: ExecutionContext = executor
    val modelEntity = model.entities(entityName)
    val childResults = relationRequests.toList.map(name_relation_request => {
      val (relationName, request) = name_relation_request
      val relation = modelEntity.relations(relationName)
      val recurse = truncate(model, relation.targetEntityName, request,
        entity(relationName).asInstanceOf[AsyncList[Map[String,Object]]], defaultLimit, defaultTTL, executor)
      recurse.map((relationName, _))
    })
    Future.sequence(childResults).map(childResults => {
      childResults.foldLeft((entity, Map.empty : Streams))((entity_streams, childResult) => {
        val (entity, streams) = entity_streams
        (entity.updated(childResult._1, childResult._2._1), streams ++ childResult._2._2)
      })
    })
  }


}