package appstax.query

import appstax.CassandraTest
import appstax.model.{OutputModel, parser}
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
    model.createWrapper("A")(session, a, executor).map(_ => ())(executor)
  }

  def query(model: OutputModel, limit: Int, branching: Int, session: CqlSession, executor: ExecutionContext): Unit = {
    val req = Map(
      (appstax.keywords.mutation.MATCH, List.empty),
      (appstax.keywords.pagination.LIMIT, Integer.valueOf(limit)),
      (appstax.keywords.pagination.CONTINUE, java.lang.Boolean.valueOf(true)),
      ("b", Map(
        (appstax.keywords.pagination.LIMIT, Integer.valueOf(limit)),
        (appstax.keywords.pagination.CONTINUE, java.lang.Boolean.valueOf(true)),
        ("c", Map(
          (appstax.keywords.pagination.LIMIT, Integer.valueOf(limit)),
          (appstax.keywords.pagination.CONTINUE, java.lang.Boolean.valueOf(true)),
        )),
      ))
    )
    val (entities, streams) = Await.result(appstax.queries.getAndTruncate(model, "A", req, limit, 0, session, executor), Duration.Inf)
    assert(entities.length <= limit || (entities.length == limit + 1 && entities.last.contains(appstax.keywords.pagination.CONTINUE)))
    streams.values.foreach((ttl_stream) => Await.result(ttl_stream.entities.length(executor), Duration.Inf) == branching - limit)
  }

  def paginationTest(model: OutputModel, branching: Int, limit: Int, session: CqlSession, executor: ExecutionContext): Boolean = {
    Await.result(generate(model, branching, session, executor), Duration.Inf)
    query(model, limit, branching, session, executor)
    true
  }

  @Test
  def paginationTest: Unit = {
    val inputModel = parser.parseModel(ConfigFactory.parseResources("pagination-schema.conf"))
    val model = appstax.schema.outputModel(inputModel)
    val executor = ExecutionContext.global
    val session = PaginationTest.newSession
    Await.ready(model.createTables(session, executor), Duration.Inf)
    val test = Try(paginationTest(model, 5, 3, session, executor))
    PaginationTest.cleanupSession(session)
    assert(test.get)
  }
}

object PaginationTest extends CassandraTest {

  @BeforeClass def before = this.ensureCassandraRunning
  @AfterClass def after = this.cleanup
}