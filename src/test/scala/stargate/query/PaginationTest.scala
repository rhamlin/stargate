package stargate.query

import stargate.CassandraTest
import stargate.model.{OutputModel, parser}
import com.datastax.oss.driver.api.core.CqlSession
import com.typesafe.config.ConfigFactory
import org.junit.{AfterClass, BeforeClass, Test}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try

class PaginationTest {


  def generate(model: OutputModel, branching: Int, session: CqlSession, executor: ExecutionContext): Future[Unit] = {
    val c = List.range(0, branching).map(_ => Map.empty[String,Object])
    val b = List.range(0, branching).map(_ => Map(("c", c)))
    val a = List.range(0, branching).map(_ => Map(("b", b), ("c", c)))
    model.mutation.create("A", a, session, executor).map(_ => ())(executor)
  }

  def query(model: OutputModel, limit: Int, branching: Int, session: CqlSession, executor: ExecutionContext): Unit = {
    val req = Map(
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
    val model = stargate.schema.outputModel(inputModel)
    val executor = ExecutionContext.global
    val session = PaginationTest.newSession
    Await.ready(model.createTables(session, executor), Duration.Inf)
    paginationTest(model, 5, 3, session, executor)
  }
}

object PaginationTest extends CassandraTest {

  @BeforeClass def before = this.ensureCassandraRunning
  @AfterClass def after = this.cleanup
}