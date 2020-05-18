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
  object ListTerm {
    def fromObjects(args: List[Object]) = ListTerm(args.map(QueryBuilder.literal))
  }
  def termCondition(cond: ScalarCondition[Object]): ScalarCondition[Term] = {
    val arg = if(cond.comparison != ScalarComparison.IN) {
      QueryBuilder.literal(cond.argument)
    } else {
      ListTerm.fromObjects(cond.argument.asInstanceOf[List[Object]])
    }
    cond.replaceArgument(arg)
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

  def selectStatement(keyspace: String, tableName: String, columns: SelectFrom=>Select, conditions: List[ScalarCondition[Term]]): Select = {
    val select = QueryBuilder.selectFrom(Strings.doubleQuote(keyspace), Strings.doubleQuote(tableName))
    val selectColumns = columns(select)
    conditions.foldLeft(selectColumns.where())(appendWhere)
  }
  def selectStatement(keyspace: String, tableName: String, conditions: List[ScalarCondition[Term]]): Select = selectStatement(keyspace, tableName, _.all(), conditions)
  def selectStatement(keyspace: String, tableName: String, columns: List[String], conditions: List[ScalarCondition[Term]]): Select = {
    selectStatement(keyspace, tableName, selectFrom => columns.foldLeft(selectFrom.columns(List.empty[String].asJava))((select, col) => select.column(Strings.doubleQuote(col))), conditions)
  }
  def selectStatement(keyspace: String, tableName: String, maybeColumns: Option[List[String]], conditions: List[ScalarCondition[Term]]): Select = {
    maybeColumns.map(selectStatement(keyspace, tableName, _, conditions)).getOrElse(selectStatement(keyspace, tableName, conditions))
  }

  def relatedSelect(keyspace: String, relationTable: String, fromIds: List[UUID], session: CqlSession,  executor: ExecutionContext): AsyncList[UUID] = {
    val conditions = List(ScalarCondition[Term](schema.RELATION_FROM_COLUMN_NAME, ScalarComparison.IN, ListTerm.fromObjects(fromIds)))
    val rows = cassandra.queryAsync(session, selectStatement(keyspace, relationTable, conditions).build, executor)
    rows.map(_.getUuid(schema.RELATION_TO_COLUMN_NAME), executor)
  }

  def entityIdToObject(model: OutputModel, entityName: String, maybeColumns: Option[List[String]], id: UUID, session: CqlSession, executor: ExecutionContext): Future[Option[Map[String,Object]]] = {
    val baseTable = model.baseTables(entityName)
    val select = selectStatement(baseTable.keyspace, baseTable.name, maybeColumns, List(ScalarCondition(schema.ENTITY_ID_COLUMN_NAME, ScalarComparison.EQ, QueryBuilder.literal(id))))
    cassandra.queryAsync(session, select.build, executor).maybeHead(executor).map(_.map(cassandra.rowToMap))(executor)
  }
  def entityIdToObject(model: OutputModel, entityName: String, id: UUID, session: CqlSession, executor: ExecutionContext): Future[Option[Map[String, Object]]] = {
    entityIdToObject(model, entityName, None, id, session, executor)
  }
}
