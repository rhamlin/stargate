package stargate.query

import java.lang
import java.util.UUID

import stargate.model.{OutputModel, ScalarComparison, ScalarCondition}
import stargate.util.AsyncList
import stargate.{cassandra, schema}
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.datastax.oss.driver.api.querybuilder.relation.OngoingWhereClause
import com.datastax.oss.driver.api.querybuilder.select.{Select, SelectFrom}
import com.datastax.oss.driver.api.querybuilder.term.Term
import com.datastax.oss.driver.internal.core.util.Strings

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

// functions used to implement appstax queries
object read {

  case class ListTerm(terms: List[Term]) extends Term {
    override def isIdempotent: Boolean = throw new UnsupportedOperationException
    override def appendTo(builder: lang.StringBuilder): Unit = throw new UnsupportedOperationException
    def asIterable: java.lang.Iterable[Term] = terms.asJava
  }

  def appendWhere[T <: OngoingWhereClause[T]](select: OngoingWhereClause[T], condition: ScalarCondition[Term]) = {
    val where = select.whereColumn(Strings.doubleQuote(condition.field))
    condition.comparison match {
      case ScalarComparison.LT => where.isLessThan(condition.argument)
      case ScalarComparison.LTE => where.isLessThanOrEqualTo(condition.argument)
      case ScalarComparison.EQ => where.isEqualTo(condition.argument)
      case ScalarComparison.GTE => where.isGreaterThanOrEqualTo(condition.argument)
      case ScalarComparison.GT => where.isGreaterThan(condition.argument)
      case ScalarComparison.IN => where.in(condition.argument.asInstanceOf[ListTerm].asIterable)
    }
  }

  def selectStatement(tableName: String, columns: SelectFrom=>Select, conditions: List[ScalarCondition[Term]]): Select = {
    val select = QueryBuilder.selectFrom(Strings.doubleQuote(tableName))
    val selectColumns = columns(select)
    conditions.foldLeft(selectColumns.where())(appendWhere)
  }
  def selectStatement(tableName: String, conditions: List[ScalarCondition[Term]]): Select = selectStatement(tableName, _.all(), conditions)
  def selectStatement(tableName: String, columns: List[String], conditions: List[ScalarCondition[Term]]): Select = {
    selectStatement(tableName, selectFrom => columns.foldLeft(selectFrom.columns(List.empty[String].asJava))((select, col) => select.column(Strings.doubleQuote(col))), conditions)
  }
  def selectStatement(tableName: String, maybeColumns: Option[List[String]], conditions: List[ScalarCondition[Term]]): Select = {
    maybeColumns.map(selectStatement(tableName, _, conditions)).getOrElse(selectStatement(tableName, conditions))
  }

  def relatedSelect(relationTable: String, fromIds: List[UUID], session: CqlSession,  executor: ExecutionContext): AsyncList[UUID] = {
    val conditions = List(ScalarCondition[Term](schema.RELATION_FROM_COLUMN_NAME, ScalarComparison.IN, ListTerm(fromIds.map(QueryBuilder.literal))))
    val rows = cassandra.queryAsync(session, selectStatement(relationTable, conditions).build, executor)
    rows.map(_.getUuid(schema.RELATION_TO_COLUMN_NAME), executor)
  }

  def entityIdToObject(model: OutputModel, entityName: String, maybeColumns: Option[List[String]], id: UUID, session: CqlSession, executor: ExecutionContext): Future[Option[Map[String,Object]]] = {
    val select = selectStatement(schema.baseTableName(entityName), maybeColumns, List(ScalarCondition(schema.ENTITY_ID_COLUMN_NAME, ScalarComparison.EQ, QueryBuilder.literal(id))))
    cassandra.queryAsync(session, select.build, executor).maybeHead(executor).map(_.map(cassandra.rowToMap))(executor)
  }
  def entityIdToObject(model: OutputModel, entityName: String, id: UUID, session: CqlSession, executor: ExecutionContext): Future[Option[Map[String, Object]]] = {
    entityIdToObject(model, entityName, None, id, session, executor)
  }
}
