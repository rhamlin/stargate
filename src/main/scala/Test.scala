import java.net.InetSocketAddress
import java.util.UUID

import appstax.model._
import appstax.schema
import appstax.util.AsyncList

import scala.concurrent.duration._
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.querybuilder.{QueryBuilder, SchemaBuilder}
import com.typesafe.config.ConfigFactory

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.util.{Random, Try}

object Test extends App {

  implicit val ec: ExecutionContext = ExecutionContext.global

  val inputModel = parser.parseModel(ConfigFactory.parseResources("schema.conf"))
  val model = schema.outputModel(inputModel)
  println(model.tables.mkString("\n"))


  def main() = {

    val keyspace = "asdfsadf1"
    var sesh = CqlSession.builder().addContactPoint(InetSocketAddress.createUnresolved("localhost", 32780)).withLocalDatacenter("dc1").withKeyspace("test").build
    sesh.execute(SchemaBuilder.createKeyspace(keyspace).ifNotExists().withSimpleStrategy(1).build)
    sesh.close

    sesh = CqlSession.builder().addContactPoint(InetSocketAddress.createUnresolved("localhost", 32780)).withLocalDatacenter("dc1").withKeyspace(keyspace).build
    val x = Try(Await.result(Future.sequence(model.tables.map(appstax.cassandra.create(sesh, _))), Duration.Inf))

    def createCustomer = Map(
      ("firstName", Random.alphanumeric.take(5).mkString),
      ("lastName", Random.alphanumeric.take(5).mkString),
      ("email", Random.alphanumeric.take(5).mkString),
      ("id", UUID.randomUUID()),
      ("addresses",
        Map(("street", "XY" + Random.alphanumeric.take(5).mkString), ("zipCode", Random.alphanumeric.take(5).mkString))
      ),
      ("orders",
        Map(
        )
      )
    )
    def createCustomerQ = appstax.queries.mutation(model, "Customer", _, sesh, ec)
    val result = createCustomerQ(createCustomer)

    Await.ready(result, Duration.Inf)
    println(result.value.get.get)
    println()

    val id:UUID = result.value.get.get.asInstanceOf[List[Object]].head.asInstanceOf[Map[String,UUID]]("entityId")

    def getCustomer: Map[String,Object] = Map(
      ("-match", List("entityId", ScalarComparison.EQ, id)),
      ("addresses", Map()),
      ("orders", Map())
    )
    def getCustomerQ = appstax.queries.get(model, "Customer", _, sesh, ec)
    val listresult = getCustomerQ(getCustomer)
    println(listresult)


    def updateCustomer: Map[String,Object] = Map(
      ("-match", List("addresses.street", ScalarComparison.GTE, "X", "addresses.street", ScalarComparison.LT, "Z")),
      ("firstName", "WOMBO")
    )
    def updateCustomerQ: Map[String,Object] => Future[List[Map[String, Object]]] = appstax.queries.update(model, "Customer", _, sesh, ec)
    val updateResultF = updateCustomerQ(updateCustomer)
    val updateResult = Await.result(updateResultF, Duration.Inf)
    println("updated", updateResult.mkString("\n"))
    updateResult.foreach(entity => {
      val id = entity(schema.ENTITY_ID_COLUMN_NAME)
      val getent = appstax.queries.get(model, "Customer", Map(("-match", List("entityId", ScalarComparison.EQ, id)), ("addresses",Map())), sesh, ec)
      println(getent)
    })

    println("\n\ndeleted")
    val delRes = appstax.queries.delete(model, "Customer", Map(("-match", List("firstName", ScalarComparison.EQ, "WOMBO"))), sesh, ec)
    println(Await.result(delRes, Duration.Inf).mkString("\n"))


    sesh.close

  }

  main()

}
