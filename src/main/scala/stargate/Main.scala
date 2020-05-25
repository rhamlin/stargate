/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package stargate

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.Logger
import io.prometheus.client.exporter.MetricsServlet
import org.eclipse.jetty.servlet.{DefaultServlet, ServletHolder}
import org.eclipse.jetty.webapp.WebAppContext
import org.scalatra.servlet.ScalatraListener
import stargate.service.config.ParsedStargateConfig
import org.eclipse.jetty.server.Server

object Main {
  private val logger = Logger("main")

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
      config.getString("stargateKeyspace"),
      config.getString("cassandra.username"),
      config.getString("cassandra.password"),
      config.getString("cassandra.authProvider"),
    )
  }

  def main(args: Array[String]) = {
    val parser = new scopt.OptionParser[Params]("stargate-server") {
      head("stargate-server", stargate.service.StargateVersion)
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
    
    val server = new Server(parsedConfig.httpPort)
    val context = new WebAppContext()
    context setContextPath "/"
    context.setResourceBase("src/main/webapp")
    context.addEventListener(new ScalatraListener)
    context.setInitParameter(ScalatraListener.LifeCycleKey, "stargate.service.ScalatraBootstrap")
    //do not add servlets here..make ScalatraServlets and place in stargate.service.ScalaBootstrap
    context.addServlet(classOf[DefaultServlet], "/")

    server.setHandler(context)

    logStartup()
    server.start
    server.join
  }
}

case class Params(conf: String = "")
