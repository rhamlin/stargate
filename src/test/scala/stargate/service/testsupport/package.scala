package stargate.service

import com.datastax.oss.driver.api.core.CqlSession
import com.typesafe.config.ConfigFactory
import stargate.cassandra.CassandraTable
import stargate.model._
import stargate.service.config.ParsedStargateConfig

import scala.concurrent.ExecutionContextExecutor
import scala.util.Random
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.webapp.WebAppContext
import org.eclipse.jetty.servlet.DefaultServlet

/**
  * provides test aide support
  */
package object testsupport {

  /**
    * for help with testing
    * @param entity entity to use
    * @param namespace namespace name
    * @param rand random number generator to share the same seed information
    * @param sgConfig parsed configuration
    */
  final case class ServletContext(
    entity: String,
    namespace: String,
    rand: Random,
    sgConfig: ParsedStargateConfig
  )
}
