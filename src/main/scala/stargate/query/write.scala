package stargate.query

import java.util.UUID

import stargate.cassandra.CassandraTable
import stargate.model.{OutputModel, ScalarComparison, ScalarCondition}
import stargate.schema
import stargate.query.read
import com.datastax.oss.driver.api.core.cql.{SimpleStatement, Statement}
import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.datastax.oss.driver.api.querybuilder.delete.Delete
import com.datastax.oss.driver.api.querybuilder.insert.{Insert, RegularInsert}
import com.datastax.oss.driver.api.querybuilder.term.Term
import com.datastax.oss.driver.internal.core.util.Strings


// functions used to implement appstax mutations
object write {

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

  def createEntity(tables: List[CassandraTable], payload: Map[String,Object]): (UUID, List[SimpleStatement]) = {
    // TODO - could use cassandra-generated timeuuid instead of random, but then the first write would block all the secondary writes
    val uuid = UUID.randomUUID()
    val payloadMap = payload.updated(schema.ENTITY_ID_COLUMN_NAME, uuid)
    val inserts = tables.map(t => insertStatement(t, payloadMap)).filter(_.isDefined).map(_.get.build)
    (uuid, inserts)
  }

  def tableConditionsForEntity(table: CassandraTable, entity: Map[String,Object]): List[Option[ScalarCondition[Term]]] = {
    table.columns.key.combined.map(col => {
      entity.get(col.name).map(value => ScalarCondition[Term](col.name, ScalarComparison.EQ, QueryBuilder.literal(value)))
    })
  }

  def deleteEntity(tables: List[CassandraTable], entity: Map[String,Object]): List[SimpleStatement] = {
    val deleteResults = tables.map(table => deleteStatement(table, entity))
    deleteResults.filter(_.isDefined).map(_.get.build)
  }

  def updateEntity(tables: List[CassandraTable], currentEntity: Map[String,Object], changes: Map[String,Object]): List[SimpleStatement] = {
    val updateStatements = tables.map(table => {
      val keyChanged = table.columns.key.combined.exists(col => changes.get(col.name).orNull != null)
      if(keyChanged) {
        val maybeDelete = write.deleteStatement(table, currentEntity)
        val insertPayload = (currentEntity++changes)
        val maybeInsert = write.insertStatement(table, insertPayload)
        List(maybeDelete, maybeInsert).filter(_.isDefined).map(_.get.build)
      } else {
        val keyColumns = table.columns.key.names.combined.toSet
        val dataColumns = table.columns.data.map(_.name).toSet
        val insertPayload = currentEntity.filter(kv => keyColumns.contains(kv._1)) ++ changes.filter(kv => dataColumns.contains(kv._1))
        val maybeInsert = write.insertStatement(table, insertPayload)
        maybeInsert.toList.map(_.build)
      }
    })
    updateStatements.flatten
  }

  def createDirectedRelationStatement(tableName: String, from: UUID, to: UUID): SimpleStatement = {
    QueryBuilder.insertInto(Strings.doubleQuote(tableName)).value(Strings.doubleQuote(schema.RELATION_FROM_COLUMN_NAME), QueryBuilder.literal(from)).value(Strings.doubleQuote(schema.RELATION_TO_COLUMN_NAME), QueryBuilder.literal(to)).build
  }
  def deleteDirectedRelationStatement(tableName: String, from: UUID, to: UUID): SimpleStatement = {
    QueryBuilder.deleteFrom(Strings.doubleQuote(tableName))
      .whereColumn(Strings.doubleQuote(schema.RELATION_FROM_COLUMN_NAME)).isEqualTo(QueryBuilder.literal(from))
      .whereColumn(Strings.doubleQuote(schema.RELATION_TO_COLUMN_NAME)).isEqualTo(QueryBuilder.literal(to)).build
  }

  def updateBidirectionalRelation(statement: (String,UUID,UUID) => SimpleStatement, model: OutputModel, fromEntity: String, fromRelationName: String): (UUID,UUID)=>List[SimpleStatement] = {
    val fromRelation =  model.input.entities(fromEntity).relations(fromRelationName)
    val toEntity = fromRelation.targetEntityName
    val toRelationName = fromRelation.inverseName
    val fromTable = model.relationTables((fromEntity, fromRelationName))
    val toTable = model.relationTables((toEntity, toRelationName))
    (fromId: UUID, toId: UUID) => List(statement(fromTable.name, fromId, toId), statement(toTable.name, toId, fromId))
  }
  def createBidirectionalRelation(model: OutputModel, fromEntity: String, fromRelationName: String): (UUID,UUID)=>List[SimpleStatement] = {
    updateBidirectionalRelation(createDirectedRelationStatement, model, fromEntity, fromRelationName)
  }
  def deleteBidirectionalRelation(model: OutputModel, fromEntity: String, fromRelationName: String): (UUID,UUID)=>List[SimpleStatement] = {
    updateBidirectionalRelation(deleteDirectedRelationStatement, model, fromEntity, fromRelationName)
  }

}
