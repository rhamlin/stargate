package stargate.service

import org.junit.AfterClass
import org.junit.BeforeClass
import scala.util.Random
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.ExecutionContext
import stargate.cassandra
import stargate.service.testsupport.ServletContext
import stargate.CassandraTest
import org.junit.Before
import org.junit.After
import sttp.client._
import sttp.model.StatusCode
import com.typesafe.scalalogging.LazyLogging
import scala.io.Source

object MergedServletTest extends CassandraTest {
  var sc: ServletContext = _
  implicit val backend = HttpURLConnectionBackend()

  @BeforeClass
  def startup() = {
    val rand = new Random()
    val namespace = s"sgTest${rand.nextInt(10000)}"
    val entity = "Customer"
    logger.info("ensuring cassandra is started")
    ensureCassandraRunning()
    parsedStargateConfig = parsedStargateConfig.copyWithNewKeyspace(newKeyspace = newKeyspace)

    //launch java servlet
    new Thread {
      override def run(): Unit = {
        stargate.service.serverStart(parsedStargateConfig)
      }
    }.start()

    //need to hack in a sentinel
    Thread.sleep(5000)
    logger.info((s"attempting to create namespace ${namespace}"))
    if (quickRequest == null) {
      throw new RuntimeException("badly broken mess, I can't even submit a request")
    }
    val hoconBody = Source.fromResource("schema.conf").getLines().mkString("\n")
    logger.info(s"posting body ${hoconBody}")
    val r = Option(
      quickRequest
        .post(uri"http://localhost:9090/${StargateApiVersion}/api/${namespace}/schema")
        .contentType("application/hocon")
        .body(hoconBody)
        .send()
    )
    if (r.isEmpty) {
      throw new RuntimeException(
        s"unable to setup servlet test env as there was no response from the attempt to create a schema"
      )
    }
    if (r.get.code != StatusCode(200)) {
      throw new RuntimeException(
        s"unable to setup servlet test env response code was ${r.get.code} and error was ${r.get.statusText}"
      )
    }
    logger.info("setting servlet context")
    //pull namespaces from the service
    sc = ServletContext(entity, namespace, rand, parsedStargateConfig)
  }

  @AfterClass
  def shutdown() = {
    logger.info("test complete executing cleanup")
    this.cleanup()
  }
}

class MergedServletTest extends QueryServletTest with StargateServletTest with SwaggerServletTest {}
