package appstax.queries

import java.util.UUID

import appstax.{cassandra, schema}
import appstax.cassandra.{CassandraFunction, CassandraTable, PagedResults}
import appstax.model.{OutputModel, ScalarComparison, ScalarCondition}
import appstax.util.AsyncList
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{Row, SimpleStatement, Statement}
import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.datastax.oss.driver.api.querybuilder.delete.Delete
import com.datastax.oss.driver.api.querybuilder.insert.{Insert, RegularInsert}
import com.datastax.oss.driver.api.querybuilder.term.Term
import com.datastax.oss.driver.internal.core.util.Strings

import scala.concurrent.{ExecutionContext, Future}


// functions used to implement appstax mutations
package object write {

  def insertStatement(tableName: String, columns: Map[String, Object]): Insert = {
    val base = QueryBuilder.insertInto(Strings.doubleQuote(tableName)).asInstanceOf[RegularInsert]
    columns.foldLeft(base)((builder, col) => builder.value(Strings.doubleQuote(col._1), QueryBuilder.literal(col._2)))
  }

  def insertStatement(table: CassandraTable, columns: Map[String, Object]): Option[Insert] = {
    val keySet = table.columns.key.names.combined.toSet
    if(keySet.forall(col => (columns.get(col).orNull) != null)) {
      val columnSet = table.columns.data.map(_.name) ++ keySet
      Some(insertStatement(table.name, columns.filter(kv => columnSet.contains(kv._1))))
    } else {
      None
    }
  }

  def deleteStatement(tableName: String, conditions: List[ScalarCondition[Term]]): Delete = {
    val base = QueryBuilder.deleteFrom(Strings.doubleQuote(tableName)).where()
    conditions.foldLeft(base)(read.appendWhere)
  }

  def deleteStatement(tableName: String, conditions: Map[String,Object]): Delete = {
    deleteStatement(tableName, conditions.toList.map(key_val => ScalarCondition[Term](key_val._1, ScalarComparison.EQ, QueryBuilder.literal(key_val._2))))
  }

  def deleteStatement(table: CassandraTable, columns: Map[String, Object]): Option[Delete] = {
    val keySet = table.columns.key.names.combined.toSet
    if(keySet.forall(col => (columns.get(col).orNull) != null)) {
      Some(deleteStatement(table.name, columns.view.filterKeys(keySet.contains).toMap))
    } else {
      None
    }
  }

  def entityIdPayload(entityId: UUID): Map[String,UUID] = Map((schema.ENTITY_ID_COLUMN_NAME, entityId))
  def entityIdPayload(entity: Map[String,Object]): Map[String,UUID] = entityIdPayload(entity(schema.ENTITY_ID_COLUMN_NAME).asInstanceOf[UUID])


  def createEntity(tables: List[CassandraTable], payload: Object, session: CqlSession, executor: ExecutionContext): (UUID, List[PagedResults[Row]]) = {
    // TODO - could use cassandra-generated timeuuid instead of random, but then the first write would block all the secondary writes
    val uuid = UUID.randomUUID()
    val payloadMap = payload.asInstanceOf[Map[String,Object]].updated(schema.ENTITY_ID_COLUMN_NAME, uuid)
    val inserts = tables.map(t => insertStatement(t, payloadMap))
    val results = inserts.filter(_.isDefined).map(statement => cassandra.executeAsync(session, statement.get.build, executor))
    (uuid, results)
  }

  def tableConditionsForEntity(table: CassandraTable, entity: Map[String,Object]): List[Option[ScalarCondition[Term]]] = {
    table.columns.key.combined.map(col => {
      entity.get(col.name).map(value => ScalarCondition[Term](col.name, ScalarComparison.EQ, QueryBuilder.literal(value)))
    })
  }

  def deleteEntity(table: CassandraTable, entity: Map[String,Object], session: CqlSession, executor: ExecutionContext): Option[Future[Any]] = {
    deleteStatement(table, entity).map(statement => cassandra.executeAsync(session, statement.build, executor).toList(executor))
  }

  def deleteEntity(tables: List[CassandraTable], entity: Map[String,Object], session: CqlSession, executor: ExecutionContext): Future[Map[String,Object]] = {
    val deleteResults = tables.map(table => deleteEntity(table, entity, session, executor))
    implicit val ec: ExecutionContext = executor
    Future.sequence(deleteResults.filter(_.isDefined).map(_.get)).map(_ => entityIdPayload(entity))
  }

  def updateEntity(tables: List[CassandraTable], currentEntity: Map[String,Object], changes: Map[String,Object], session: CqlSession, executor: ExecutionContext): Future[Map[String,Object]] = {
    implicit val ec: ExecutionContext = executor
    val updateResults = tables.map(table => {
      val keyChanged = table.columns.key.combined.exists(col => changes.get(col.name).orNull != null)
      if(keyChanged) {
        val deleteResult = deleteEntity(table, currentEntity, session, executor)
        val insertPayload = (currentEntity++changes)
        val maybeInsert = write.insertStatement(table, insertPayload)
        val insertResult = maybeInsert.map(statement => cassandra.executeAsync(session, statement.build, executor).toList(executor))
        implicit val ec: ExecutionContext = executor
        Future.sequence(List(deleteResult, insertResult).filter(_.isDefined).map(_.get))
      } else {
        val keyColumns = table.columns.key.names.combined.toSet
        val dataColumns = table.columns.data.map(_.name).toSet
        val insertPayload = currentEntity.filter(kv => keyColumns.contains(kv._1)) ++ changes.filter(kv => dataColumns.contains(kv._1))
        val maybeInsert = write.insertStatement(table, insertPayload)
        maybeInsert.map(maybeInsert => cassandra.executeAsync(session, maybeInsert.build, executor).toList(executor)).getOrElse(Future.successful(None))
      }
    })
    Future.sequence(updateResults).map(_ => entityIdPayload(currentEntity))
  }

  def updateDirectedRelation(statement: (String,UUID,UUID) => Statement[_], model: OutputModel, fromEntity: String, relationName: String): CassandraFunction[(UUID,UUID), Future[Any]] = {
    val relationTableName = model.relationTables((fromEntity, relationName)).name
    (session: CqlSession, from_to: (UUID, UUID), executor: ExecutionContext) => {
      val (from, to) = from_to
      cassandra.executeAsync(session, statement(relationTableName, from, to), executor).toList(executor)
    }
  }
  def createDirectedRelationStatement(tableName: String, from: UUID, to: UUID): SimpleStatement = {
    QueryBuilder.insertInto(Strings.doubleQuote(tableName)).value(Strings.doubleQuote(schema.RELATION_FROM_COLUMN_NAME), QueryBuilder.literal(from)).value(Strings.doubleQuote(schema.RELATION_TO_COLUMN_NAME), QueryBuilder.literal(to)).build
  }
  def deleteDirectedRelationStatement(tableName: String, from: UUID, to: UUID): SimpleStatement = {
    QueryBuilder.deleteFrom(Strings.doubleQuote(tableName))
      .whereColumn(Strings.doubleQuote(schema.RELATION_FROM_COLUMN_NAME)).isEqualTo(QueryBuilder.literal(from))
      .whereColumn(Strings.doubleQuote(schema.RELATION_TO_COLUMN_NAME)).isEqualTo(QueryBuilder.literal(to)).build
  }

  def updateBidirectionalRelation(statement: (String,UUID,UUID) => Statement[_], model: OutputModel, fromEntity: String, fromRelationName: String): CassandraFunction[(UUID,UUID), Future[Any]] = {
    val fromRelation =  model.input.entities(fromEntity).relations(fromRelationName)
    val toEntity = fromRelation.targetEntityName
    val toRelationName = fromRelation.inverseName
    val updateFrom = updateDirectedRelation(statement, model, fromEntity, fromRelationName)
    val updateTo = updateDirectedRelation(statement, model, toEntity, toRelationName)

    (session: CqlSession, from_to: (UUID, UUID), executor: ExecutionContext) => {
      val (from, to) = from_to
      implicit val ec: ExecutionContext = executor
      Future.sequence(List(
        updateFrom(session, (from, to), executor),
        updateTo(session, (to, from), executor)
      ))
    }
  }
  def createBidirectionalRelation(model: OutputModel, fromEntity: String, fromRelationName: String): CassandraFunction[(UUID,UUID), Future[Any]] = {
    updateBidirectionalRelation(createDirectedRelationStatement, model, fromEntity, fromRelationName)
  }
  def deleteBidirectionalRelation(model: OutputModel, fromEntity: String, fromRelationName: String): CassandraFunction[(UUID,UUID), Future[Any]] = {
    updateBidirectionalRelation(deleteDirectedRelationStatement, model, fromEntity, fromRelationName)
  }


}
