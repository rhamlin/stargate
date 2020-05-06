package stargate.query

import stargate.{CassandraTest, CassandraTestSession}
import stargate.model.{OutputModel, parser}
import com.datastax.oss.driver.api.core.CqlSession
import com.typesafe.config.ConfigFactory
import org.junit.{AfterClass, BeforeClass, Test}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try

trait PaginationTestTrait extends CassandraTestSession {

  def generate(model: OutputModel, branching: Int, session: CqlSession, executor: ExecutionContext): Future[Unit] = {
    val c = List.range(0, branching).map(_ => Map.empty[String,Object])
    val b = List.range(0, branching).map(_ => Map(("c", c)))
    val a = List.range(0, branching).map(_ => Map(("b", b), ("c", c)))
    model.mutation.create("A", a, session, executor).map(_ => ())(executor)
  }

  def query(model: OutputModel, limit: Int, branching: Int, session: CqlSession, executor: ExecutionContext): Unit = {
    val req = Map[String,Object](
      (stargate.keywords.mutation.MATCH, "all"),
      (stargate.keywords.pagination.LIMIT, Integer.valueOf(limit)),
      (stargate.keywords.pagination.CONTINUE, java.lang.Boolean.valueOf(true)),
      ("b", Map(
        (stargate.keywords.pagination.LIMIT, Integer.valueOf(limit)),
        (stargate.keywords.pagination.CONTINUE, java.lang.Boolean.valueOf(true)),
        ("c", Map(
          (stargate.keywords.pagination.LIMIT, Integer.valueOf(limit)),
          (stargate.keywords.pagination.CONTINUE, java.lang.Boolean.valueOf(true)),
        )),
      ))
    )
    val (entities, streams) = Await.result(stargate.query.untyped.getAndTruncate(model, "A", req, limit, 0, session, executor), Duration.Inf)
    assert(entities.length <= limit || (entities.length == limit + 1 && entities.last.contains(stargate.keywords.pagination.CONTINUE)))
    streams.values.foreach((ttl_stream) => Await.result(ttl_stream.entities.length(executor), Duration.Inf) == branching - limit)
  }

  def paginationTest(model: OutputModel, branching: Int, limit: Int, session: CqlSession, executor: ExecutionContext): Unit = {
    Await.result(generate(model, branching, session, executor), Duration.Inf)
    query(model, limit, branching, session, executor)
  }

  @Test
  def paginationTest: Unit = {
    val inputModel = parser.parseModel(ConfigFactory.parseResources("pagination-schema.conf"))
    val keyspace = newKeyspace
    val model = stargate.schema.outputModel(inputModel, keyspace)
    val executor = ExecutionContext.global
    Await.ready(model.createTables(session, executor), Duration.Inf)
    paginationTest(model, 5, 3, session, executor)
  }
}
