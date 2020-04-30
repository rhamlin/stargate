package stargate.query

import stargate.CassandraTest
import stargate.model.parser
import com.typesafe.config.ConfigFactory
import org.junit.{AfterClass, BeforeClass, Test}
import org.junit.Assert._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}

class PredefinedQueryTest {

  import PredefinedQueryTest._


  @Test
  def test: Unit = {
    val inputModel = parser.parseModel(ConfigFactory.parseResources("predefined-query-schema.conf"))
    val model = stargate.schema.outputModel(inputModel)
    val executor = ExecutionContext.global
    val session = newSession
    Await.ready(model.createTables(session, executor), Duration.Inf)

    List.range(0, 10).foreach(_ => {
      val entity = EntityCRUDTest.createEntityWithIds(model, model.mutation, "A", session, executor)
      Await.result(entity, Duration.Inf)
    })
    val req = stargate.model.queries.predefined.transform(model.input.queries("getAandB"),  Map((stargate.keywords.mutation.MATCH, Map.empty)))
    val entities = Await.result(stargate.query.untyped.getAndTruncate(model, "A", req, 10000, session, executor), Duration.Inf)
    entities.foreach(a => {
      assertEquals(a.keySet, Set("x", "y", "b"))
      a("b").asInstanceOf[List[Map[String,Object]]].foreach(b => {
        assertEquals(b.keySet, Set("y", "z"))
      })
    })
  }
}


object PredefinedQueryTest extends CassandraTest {

  @BeforeClass def before = this.ensureCassandraRunning
  @AfterClass def after = this.cleanup
}