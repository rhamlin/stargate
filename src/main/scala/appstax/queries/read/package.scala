package appstax.queries

import java.util.UUID

import appstax.{cassandra, schema}
import appstax.model.{OutputModel, ScalarComparison, ScalarCondition}
import appstax.util.AsyncList
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.datastax.oss.driver.api.querybuilder.relation.OngoingWhereClause
import com.datastax.oss.driver.api.querybuilder.select.Select
import com.datastax.oss.driver.api.querybuilder.term.Term
import com.datastax.oss.driver.internal.core.util.Strings

import scala.concurrent.{ExecutionContext, Future}

// functions used to implement appstax queries
package object read {

  def appendWhere[T <: OngoingWhereClause[T]](select: OngoingWhereClause[T], condition: ScalarCondition[Term]) = {
    val where = select.whereColumn(Strings.doubleQuote(condition.field))
    condition.comparison match {
      case ScalarComparison.LT => where.isLessThan(condition.argument)
      case ScalarComparison.LTE => where.isLessThanOrEqualTo(condition.argument)
      case ScalarComparison.EQ => where.isEqualTo(condition.argument)
      case ScalarComparison.GTE => where.isGreaterThanOrEqualTo(condition.argument)
      case ScalarComparison.GT => where.isGreaterThan(condition.argument)
      case ScalarComparison.IN => where.in(condition.argument)
    }
  }
  // duplicated because where.in(BindMarker) handles List named-arguments correctly, but where.in(Term) does not
  def appendNamedWhere[T <: OngoingWhereClause[T]](select: OngoingWhereClause[T], condition: ScalarCondition[String]) = {
    val where = select.whereColumn(Strings.doubleQuote(condition.field))
    val marker = QueryBuilder.bindMarker(Strings.doubleQuote(condition.argument))
    condition.comparison match {
      case ScalarComparison.LT => where.isLessThan(marker)
      case ScalarComparison.LTE => where.isLessThanOrEqualTo(marker)
      case ScalarComparison.EQ => where.isEqualTo(marker)
      case ScalarComparison.GTE => where.isGreaterThanOrEqualTo(marker)
      case ScalarComparison.GT => where.isGreaterThan(marker)
      case ScalarComparison.IN => where.in(marker)
    }
  }

  def selectStatementWithNamedArgs(tableName: String, conditions: List[ScalarCondition[String]]): Select = {
    val selectAll = QueryBuilder.selectFrom(Strings.doubleQuote(tableName)).all().where()
    conditions.foldLeft(selectAll)(appendNamedWhere)
  }

  // again, duplicated because of the different handling between IN(Term) and IN(Binding)
  def selectStatement(tableName: String, conditions: List[ScalarCondition[Term]]): Select = {
    val selectAll = QueryBuilder.selectFrom(Strings.doubleQuote(tableName)).all().where()
    conditions.foldLeft(selectAll)(appendWhere)
  }

  def select(tableName: String, conditions: List[ScalarCondition[String]]): CassandraGetFunction[Row] = cassandra.executeAsync(selectStatementWithNamedArgs(tableName, conditions))


  def relatedSelect(relationTable: String): CassandraPagedFunction[List[UUID], UUID] = {
    val conditions = List(ScalarCondition(schema.RELATION_FROM_COLUMN_NAME, ScalarComparison.IN, schema.RELATION_FROM_COLUMN_NAME))
    (session: CqlSession, ids: List[UUID], executor: ExecutionContext) => {
      val rows = select(relationTable, conditions)(session, Map((schema.RELATION_FROM_COLUMN_NAME, ids)), executor)
      rows.map(_.getUuid(schema.RELATION_TO_COLUMN_NAME), executor)
    }
  }


  def entityIdToObject(model: OutputModel, entityName: String, id: UUID, session: CqlSession, executor: ExecutionContext): Future[Option[Map[String,Object]]] = {
    val select = selectStatement(schema.baseTableName(entityName), List(ScalarCondition(schema.ENTITY_ID_COLUMN_NAME, ScalarComparison.EQ, QueryBuilder.literal(id))))
    cassandra.executeAsync(session, select.build, executor).maybeHead(executor).map(_.map(cassandra.rowToMap))(executor)
  }

  def entityIdsToObject(model: OutputModel, entityName: String, ids: AsyncList[UUID], session: CqlSession, executor: ExecutionContext): AsyncList[Map[String,Object]] = {
    val entities = ids.map(id => entityIdToObject(model, entityName, id, session, executor), executor)
    AsyncList.filterSome(AsyncList.unfuture(entities, executor), executor)
  }



}
