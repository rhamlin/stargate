package stargate

import java.net.InetSocketAddress
import java.util.concurrent.CompletionStage

import com.datastax.oss.driver.api.core.`type`.DataType
import com.datastax.oss.driver.api.core.cql.{AsyncResultSet, Row, Statement}
import com.datastax.oss.driver.api.core.{CqlSession, CqlSessionBuilder}
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable
import com.datastax.oss.driver.internal.core.util.Strings
import stargate.util.AsyncList

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.jdk.FutureConverters._


object cassandra {

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
  case class CassandraTable(name: String, columns: CassandraColumns)

  case class KeyConditionScore(partition: Int, clustering: Int, skipped: Int) extends Ordered[KeyConditionScore] {
    def perfect: Boolean = (partition + clustering + skipped) == 0
    def tuple: (Int, Int, Int) = (partition, clustering, skipped)
    override def compare(that: KeyConditionScore): Int = Ordering[(Int,Int,Int)].compare(this.tuple, that.tuple)
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
    val base = SchemaBuilder.createTable(Strings.doubleQuote(table.name)).ifNotExists().asInstanceOf[CreateTable]
    val partitionKeys = table.columns.key.partitionKeys.foldLeft(base)((builder, next) => builder.withPartitionKey(Strings.doubleQuote(next.name), next.`type`))
    val clusteringKeys = table.columns.key.clusteringKeys.foldLeft(partitionKeys)((builder, next) => builder.withClusteringColumn(Strings.doubleQuote(next.name), next.`type`))
    val dataCols = table.columns.data.foldLeft(clusteringKeys)((builder, next) => builder.withColumn(Strings.doubleQuote(next.name), next.`type`))
    session.executeAsync(dataCols.build).asScala
  }

  def createKeyspace(session: CqlSession, name: String, replication: Int): Future[AsyncResultSet] = {
    session.executeAsync(SchemaBuilder.createKeyspace(name).ifNotExists().withSimpleStrategy(replication).build).asScala
  }

  def sessionBuilder(contacts: List[(String, Int)], dataCenter: String): CqlSessionBuilder = {
    val builder = contacts.foldLeft(CqlSession.builder)((builder, contact) => builder.addContactPoint(InetSocketAddress.createUnresolved(contact._1, contact._2)))
    builder.withLocalDatacenter(dataCenter)
  }
  def session(contacts: List[(String, Int)], dataCenter: String): CqlSession = sessionBuilder(contacts, dataCenter).build
  def session(contacts: List[(String, Int)], dataCenter: String, keyspace: String): CqlSession = sessionBuilder(contacts, dataCenter).withKeyspace(keyspace).build

  def wipeKeyspaceSession(contacts: List[(String, Int)], dataCenter: String, keyspace: String): CqlSession = {
    val sessionWithoutKeyspace = session(contacts, dataCenter)
    sessionWithoutKeyspace.execute(SchemaBuilder.dropKeyspace(keyspace).ifExists.build)
    sessionWithoutKeyspace
  }
  def sessionWithNewKeyspace(contacts: List[(String, Int)], dataCenter: String, keyspace: String, replication: Int): CqlSession = {
    val recreateSession = wipeKeyspaceSession(contacts, dataCenter, keyspace)
    recreateSession.execute(SchemaBuilder.createKeyspace(keyspace).withSimpleStrategy(replication).build)
    recreateSession.close
    session(contacts, dataCenter, keyspace)
  }

}
