package stargate

import scala.io.Source
import com.typesafe.scalalogging.Logger
import org.eclipse.jetty.webapp.WebAppContext
import org.eclipse.jetty.server.Server
import scala.concurrent.ExecutionContext
import com.datastax.oss.driver.api.core.CqlSession
import org.eclipse.jetty.servlet.DefaultServlet
import io.prometheus.client.jetty.JettyStatisticsCollector
import org.eclipse.jetty.server.handler.StatisticsHandler
import io.prometheus.client.hotspot.DefaultExports
import stargate.service.config.StargateConfig
import org.eclipse.jetty.servlet.ServletHolder
import scala.reflect.api.Names
import javax.servlet.DispatcherType
import java.{util => ju}
import org.eclipse.jetty.servlet.FilterHolder
import stargate.service.security.BasicAuthFilter

package object service {
  private val logger = Logger("stargage.service")
  val StargateApiVersion = "v1"
  val StargateVersion: String = getClass.getPackage().getImplementationVersion

  def logStartup() = {
    logger.info("Launch Mission To StarGate")
    logger.info(" -----------")
    logger.info("|         * |")
    logger.info("| *         |")
    logger.info("|    *      |")
    logger.info("|         * |")
    logger.info("|    *      |")
    logger.info(" -----------")
    logger.info("            ")
    logger.info("            ")
    logger.info("            ")
    logger.info("     ^^")
    logger.info("    ^^^^")
    logger.info("   ^^^^^^")
    logger.info("  ^^^^^^^^")
    logger.info(" ^^^^^^^^^^")
    logger.info("   ^^^^^^")
    logger.info("     ||| ")
    logger.info("     ||| ")
    logger.info("     ||| ")
    logger.info("     ||| ")
    logger.info("      | ")
    logger.info("      | ")
    logger.info("      | ")
    logger.info("        ")
    logger.info("      |  ")
    logger.info("0000     0000")
    logger.info("00000   00000")
    logger.info("============  ")
    logger.info(s"""StarGate Version: ${stargate.service.StargateVersion}""")
  }

  var registeredJettyMetrics = false
  var stats: StatisticsHandler = _

  /**
    * strictly in here for testing
    */
  def serverStart(sgConfig: StargateConfig): Unit = {
    //initial configuration
    val cqlSession: CqlSession = cassandra.session(sgConfig.cassandra)
    val executor = ExecutionContext.global
    val datamodelRepoTable: cassandra.CassandraTable = datamodelRepository.createDatamodelRepoTable(sgConfig, cqlSession, executor)
    val namespaces = new Namespaces(datamodelRepoTable, cqlSession)
    val server = new Server(sgConfig.httpPort)
    val context = new WebAppContext()
    context setContextPath "/"
    context.setResourceBase("src/main/webapp")
    if (sgConfig.auth.enabled){
      context.addFilter(new FilterHolder(new BasicAuthFilter(sgConfig.auth)), "/*" ,  ju.EnumSet.of(DispatcherType.INCLUDE,DispatcherType.REQUEST))
    }
    context.addServlet(new ServletHolder(new StargateServlet(sgConfig, cqlSession, namespaces, datamodelRepoTable, executor)), s"/${StargateApiVersion}/api/*")
    context.addServlet(new ServletHolder(new SwaggerServlet(namespaces, sgConfig)),"/api-docs/*")
    context.addServlet(classOf[DefaultServlet], "/")
     
    server.setHandler(context)
    //supposedly this can be run multiple times, but we are able to trigger this breaking in test
    DefaultExports.initialize()
    if (!registeredJettyMetrics){
      stats = new StatisticsHandler()
      //get current server handler and set it inside StatisticsHandler
      stats.setHandler(server.getHandler)
      //set StatisticsHandler as the handler for servlet
      server.setHandler(stats)
      new JettyStatisticsCollector(stats).register()
      // Register collector.
      registeredJettyMetrics = true
    }

    logStartup()
    server.start
    server.join
  }
}
