package stargate.service

import java.time.Instant

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.`type`.DataTypes
import com.datastax.oss.driver.api.querybuilder.{QueryBuilder, SchemaBuilder}
import com.datastax.oss.driver.api.querybuilder.term.Term
import stargate.{cassandra, model}
import stargate.cassandra.{CassandraColumn, CassandraColumns, CassandraKey, CassandraTable}
import stargate.model.{ScalarComparison, ScalarCondition}
import stargate.query.{read, write}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try

object datamodelRepository {

  val TABLE_NAME = "datamodel_repo"
  val NAME_COLUMN = "name"
  val TIMESTAMP_COLUMN = "timestamp"
  val DATAMODEL_COLUMN = "datamodel"


  def repoTable(keyspace: String): CassandraTable = {
    CassandraTable(keyspace, TABLE_NAME, CassandraColumns(
      CassandraKey(List(CassandraColumn(NAME_COLUMN, DataTypes.TEXT)), List(CassandraColumn(TIMESTAMP_COLUMN, DataTypes.TIMESTAMP))),
      List(CassandraColumn(DATAMODEL_COLUMN, DataTypes.TEXT))
    ))
  }

  def ensureRepoTableExists(keyspace: String, replication: Int, session: CqlSession, executor: ExecutionContext): Future[CassandraTable] = {
    val table = repoTable(keyspace)
    val ensureKeyspace = cassandra.createKeyspaceAsync(session, keyspace, replication, executor)
    val ensureTable = ensureKeyspace.flatMap(_ => cassandra.createTableAsync(session, table))(executor)
    ensureTable.map(_ => table)(executor)
  }

  def updateDatamodel(appName: String, datamodel: String, repoTable: CassandraTable, session: CqlSession, executor: ExecutionContext): Future[Unit] = {
    require(Try(model.parser.parseModel(datamodel)).isSuccess)
    val row: Map[String,Object] = Map((NAME_COLUMN, appName), (TIMESTAMP_COLUMN, java.lang.Long.valueOf(System.currentTimeMillis())), (DATAMODEL_COLUMN, datamodel))
    cassandra.executeAsync(session, write.insertStatement(repoTable.keyspace, repoTable.name, row).build, executor)
  }
  def updateDatamodelBlocking(appName: String, datamodel: String, repoTable: CassandraTable, session: CqlSession, executor: ExecutionContext): Unit = {
    Await.result(updateDatamodel(appName, datamodel, repoTable, session, executor), Duration.Inf)
  }

  def fetchDatamodels(condition: List[ScalarCondition[Term]], repoTable: CassandraTable, session: CqlSession, executor: ExecutionContext): Future[Map[(String,Instant),String]] = {
    val rows = cassandra.queryAsync(session, read.selectStatement(repoTable.keyspace, repoTable.name, condition).build, executor)
    rows.toList(executor).map(_.map(cassandra.rowToMap).map(row =>
      ((row(NAME_COLUMN).asInstanceOf[String], row(TIMESTAMP_COLUMN).asInstanceOf[Instant]), row(DATAMODEL_COLUMN).asInstanceOf[String]))
      .toMap)(executor)
  }
  def fetchLatestDatamodels(condition: List[ScalarCondition[Term]], repoTable: CassandraTable, session: CqlSession, executor: ExecutionContext): Future[Map[String,String]] = {
    fetchDatamodels(condition, repoTable, session, executor).map(byNameAndTime => {
      val latestTimes = byNameAndTime.toList.groupMap(_._1._1)(_._1._2).view.mapValues(_.max).toMap
      latestTimes.map((nameTime:(String,Instant)) => (nameTime._1, byNameAndTime(nameTime._1, nameTime._2)))
    })(executor)
  }

  def fetchDatamodel(appName: String, repoTable: CassandraTable, session: CqlSession, executor: ExecutionContext): Future[List[(Instant,String)]] = {
    val condition = List(ScalarCondition[Term](NAME_COLUMN, ScalarComparison.EQ, QueryBuilder.literal(appName)))
    fetchDatamodels(condition, repoTable, session, executor).map(_.toList.map(ntc => (ntc._1._2, ntc._2)))(executor)
  }
  def fetchLatestDatamodel(appName: String, repoTable: CassandraTable, session: CqlSession, executor: ExecutionContext): Future[Option[String]] = {
    val condition = List(ScalarCondition[Term](NAME_COLUMN, ScalarComparison.EQ, QueryBuilder.literal(appName)))
    fetchLatestDatamodels(condition, repoTable, session, executor).map(_.headOption.map(_._2))(executor)
  }

  def fetchAllDatamodels(repoTable: CassandraTable, session: CqlSession, executor: ExecutionContext): Future[Map[(String,Instant),String]] = fetchDatamodels(List.empty, repoTable, session, executor)
  def fetchAllLatestDatamodels(repoTable: CassandraTable, session: CqlSession, executor: ExecutionContext): Future[Map[String,String]] = fetchLatestDatamodels(List.empty, repoTable, session, executor)

  def deleteDatamodel(appName:String, repoTable: CassandraTable, session: CqlSession, executor: ExecutionContext): Future[Unit] = {
    val statement = write.deleteStatement(repoTable.keyspace, repoTable.name, List(ScalarCondition[Term](NAME_COLUMN, ScalarComparison.EQ, QueryBuilder.literal(appName))))
    cassandra.executeAsync(session, statement.build, executor)
  }
}
