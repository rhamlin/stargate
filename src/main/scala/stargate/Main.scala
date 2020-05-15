package stargate

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.Logger
import io.prometheus.client.exporter.MetricsServlet
import org.eclipse.jetty.servlet.{DefaultServlet, ServletHolder}
import org.eclipse.jetty.webapp.WebAppContext
import org.scalatra.servlet.ScalatraListener
import stargate.service.ParsedStargateConfig

object Main {
  private val logger = Logger("main")
  private val sgVersion = "0.1.0"

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
    logger.info(s"StarGate Version: $sgVersion")
  }
  private def mapConfig(config: Config): ParsedStargateConfig = {
    ParsedStargateConfig(
      config.getInt("http.port"),
      config.getInt("defaultTTL"),
      config.getInt("defaultLimit"),
      config.getLong("validation.maxSchemaSizeKB"),
      config.getLong("validation.maxMutationSizeKB"),
      config.getLong("validation.maxRequestSizeKB"),
      config.getString("cassandra.contactPoints")
        .split(",").map(_.split(":")).map(hp => (hp(0), Integer.parseInt(hp(1)))).toList,
      config.getString("cassandra.dataCenter"),
      config.getInt("cassandra.replication"),
      config.getString("stargateKeyspace")
    )
  }

  def main(args: Array[String]) = {
    metrics.registerJVMMetrics()
    val parser = new scopt.OptionParser[Params]("stargate-server") {
      head("stargate-server", sgVersion)
      opt[String]('c', "conf")
        .optional()
        .action((x, c) => c.copy(conf = x))
        .text("optional custom stargate.conf to use")
      help('h', "help").text("shows the help text")

    }
    var appConf = ""
    parser.parse(args, Params()) match {
      case Some(p) =>appConf = p.conf
      case None =>
        sys.exit(0)
    }
    val config = (if (appConf.isBlank)
      ConfigFactory.parseResources("stargate-docker.conf").resolve()
    else ConfigFactory.parseFile(new File(appConf)).resolve())
    val parsedConfig = mapConfig(config)
    ParsedStargateConfig.globalConfig = parsedConfig

    val server = new org.eclipse.jetty.server.Server(parsedConfig.httpPort)
    val handler = new WebAppContext()
    handler.setContextPath("/")
    handler.setResourceBase("src/main/resources/webapp")
    //wires up scalatra
    handler.addEventListener(new ScalatraListener())
    handler.setInitParameter(ScalatraListener.LifeCycleKey, "stargate.service.ScalatraBootstrap")
    handler.addServlet(classOf[DefaultServlet], "/swagger/*")
    //expose the MetricsServlet to the /metrics endpoint. To scrape metrics for Prometheus now you just need to point to the appropriate host and port combination
    //ie http://localhost:8080/metrics
    handler.addServlet(
      new ServletHolder(new MetricsServlet()),
      s"/${stargate.service.StargateApiVersion}/metrics"
    );
    server.setHandler(handler)
    //has to be the last set handler, will wrap existing handler
    //note this will mean that metrics calls will be counted as well in the total request count
    metrics.registerServletHandlerStatistics(server)
    logStartup()
    server.start
    server.join
  }
}

case class Params(conf: String = "")
