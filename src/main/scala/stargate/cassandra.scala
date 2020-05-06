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

package stargate

import java.net.InetSocketAddress
import java.util.concurrent.CompletionStage

import com.datastax.oss.driver.api.core.`type`.DataType
import com.datastax.oss.driver.api.core.cql.{AsyncResultSet, ResultSet, Row, Statement}
import com.datastax.oss.driver.api.core.{CqlSession, CqlSessionBuilder}
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable
import com.datastax.oss.driver.internal.core.util.Strings
import stargate.util.AsyncList

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.jdk.FutureConverters._

object cassandra {

  val schemaOpTimeout: java.time.Duration = java.time.Duration.ofSeconds(10)

  case class CassandraKeyNames(partitionKeys: List[String], clusteringKeys: List[String]) {
    def combined: List[String] = (partitionKeys ++ clusteringKeys)
  }
  case class CassandraColumnNames(key: CassandraKeyNames, data: List[String]) {
    def combined: List[String] = key.combined ++ data
  }

  case class CassandraColumn(name: String, `type`: DataType)
  case class CassandraKey(partitionKeys: List[CassandraColumn], clusteringKeys: List[CassandraColumn]) {
    def names: CassandraKeyNames = CassandraKeyNames(partitionKeys.map(_.name), clusteringKeys.map(_.name))
    def combined: List[CassandraColumn] = (partitionKeys ++ clusteringKeys)
  }
  case class CassandraColumns(key: CassandraKey, data: List[CassandraColumn]) {
    def names: CassandraColumnNames = CassandraColumnNames(key.names, data.map(_.name))
    def combined: List[CassandraColumn] = key.combined ++ data
  }
  case class CassandraTable(keyspace: String, name: String, columns: CassandraColumns)

  case class KeyConditionScore(missingColumns: Int, partition: Int, clustering: Int, skipped: Int) extends Ordered[KeyConditionScore] {
    def perfect: Boolean = (missingColumns + partition + clustering + skipped) == 0
    def tuple: (Int, Int, Int, Int) = (missingColumns, partition, clustering, skipped)
    override def compare(that: KeyConditionScore): Int = Ordering[(Int,Int,Int,Int)].compare(this.tuple, that.tuple)
  }

  type PagedResults[T] = AsyncList[T]

  def convertAsyncResultSet(resultSet: Future[AsyncResultSet], executor: ExecutionContext): PagedResults[Row] = {
    AsyncList.unfuture(resultSet.map(ars => {
      val head = AsyncList.fromList(ars.currentPage().asScala.toList)
      val tail = if (ars.hasMorePages) {
        () => convertAsyncResultSet(ars.fetchNextPage().asScala, executor)
      } else {
        () => AsyncList.empty[Row]
      }
      AsyncList.append(head, tail, executor)
    })(executor), executor)
  }

  def convertAsyncResultSet(resultSet: CompletionStage[AsyncResultSet], executor: ExecutionContext): PagedResults[Row] = {
    convertAsyncResultSet(resultSet.asScala, executor)
  }

  def rowToMap(row: Row): Map[String, Object] = {
    row.getColumnDefinitions.iterator.asScala.map(col => (col.getName.toString, row.getObject(col.getName))).toMap
  }

  def queryAsync(cqlSession: CqlSession, statement: Statement[_], executor: ExecutionContext): PagedResults[Row] = {
    convertAsyncResultSet(cqlSession.executeAsync(statement), executor)
  }

  def executeAsync(cqlSession: CqlSession, statement: Statement[_], executor: ExecutionContext): Future[Unit] = {
    queryAsync(cqlSession, statement, executor).toList(executor).map(_ => ())(executor)
  }

  def create(session: CqlSession, table: CassandraTable): Future[AsyncResultSet] = {
    val base = SchemaBuilder.createTable(Strings.doubleQuote(table.keyspace), Strings.doubleQuote(table.name)).ifNotExists().asInstanceOf[CreateTable]
    val partitionKeys = table.columns.key.partitionKeys.foldLeft(base)((builder, next) => builder.withPartitionKey(Strings.doubleQuote(next.name), next.`type`))
    val clusteringKeys = table.columns.key.clusteringKeys.foldLeft(partitionKeys)((builder, next) => builder.withClusteringColumn(Strings.doubleQuote(next.name), next.`type`))
    val dataCols = table.columns.data.foldLeft(clusteringKeys)((builder, next) => builder.withColumn(Strings.doubleQuote(next.name), next.`type`))
    session.executeAsync(dataCols.build.setTimeout(schemaOpTimeout)).asScala
  }

  def sessionBuilder(contacts: List[(String, Int)], dataCenter: String): CqlSessionBuilder = {
    val builder = contacts.foldLeft(CqlSession.builder)((builder, contact) => builder.addContactPoint(InetSocketAddress.createUnresolved(contact._1, contact._2)))
    builder.withLocalDatacenter(dataCenter)
  }

  def session(contacts: List[(String, Int)], dataCenter: String): CqlSession = {
      sessionBuilder(contacts, dataCenter).build
  }

  def createKeyspace(session: CqlSession, name: String, replication: Int): Unit = {
    session.execute(SchemaBuilder.createKeyspace(Strings.doubleQuote(name)).ifNotExists().withSimpleStrategy(replication).build.setTimeout(schemaOpTimeout))
    session.checkSchemaAgreement()
    ()
  }
  def wipeKeyspace(session: CqlSession, keyspace: String): Unit = {
    session.execute(SchemaBuilder.dropKeyspace(Strings.doubleQuote(keyspace)).ifExists.build.setTimeout(schemaOpTimeout))
    session.checkSchemaAgreement()
    ()
  }
  def recreateKeyspace(session: CqlSession, keyspace: String, replication: Int): Unit = {
    wipeKeyspace(session, keyspace)
    createKeyspace(session, keyspace, replication)
  }

}
