package appstax.query

import appstax.CassandraTest
import com.typesafe.config.ConfigFactory
import org.junit.{AfterClass, BeforeClass, Test}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import appstax.schema.ENTITY_ID_COLUMN_NAME

class ReadWriteTest {

  import ReadWriteTest._


  @Test
  def testCreateDelete = {
    val session = ReadWriteTest.newSession
    Await.ready(model.createTables(session, executor), Duration.Inf)

    List.range(0, 100).foreach(_ => {
      model.input.entities.values.foreach(entity => {
        val tables = model.entityTables(entity.name)
        val random = appstax.model.generator.createEntity(model.input, entity.name, 1)
        val (id, future) = appstax.queries.write.createEntity(tables, random, session, executor)
        val payload = random.updated(ENTITY_ID_COLUMN_NAME, id)
        Await.result(Future.sequence(future.map(_.toList(executor))), Duration.Inf)
        tables.foreach(table => {
          val conditions = appstax.queries.write.tableConditionsForEntity(table, payload).map(_.get)
          val select = appstax.queries.read.selectStatement(table.name, conditions).build
          val rs = session.execute(select)
          assert(rs.iterator.asScala.toList.length == 1)
        })
        val deleted = appstax.queries.write.deleteEntity(tables, payload, session, executor)
        assert(Await.result(deleted, Duration.Inf)(ENTITY_ID_COLUMN_NAME) == id)
        tables.foreach(table => {
          val conditions = appstax.queries.write.tableConditionsForEntity(table, payload).map(_.get)
          val select = appstax.queries.read.selectStatement(table.name, conditions).build
          val rs = session.execute(select)
          assert(rs.iterator.asScala.toList.isEmpty)
        })
      })
    })
  }


}


object ReadWriteTest extends CassandraTest {

  val modelConfig = ConfigFactory.parseResources("read-write-test-schema.conf")
  val inputModel = appstax.model.parser.parseModel(modelConfig)
  val model = appstax.schema.outputModel(inputModel)
  implicit val executor: ExecutionContext = ExecutionContext.global

  @BeforeClass def before = this.ensureCassandraRunning
  @AfterClass def after = this.cleanup
}