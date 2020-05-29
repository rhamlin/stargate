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
import stargate.service.config.StargateConfig
import org.eclipse.jetty.server.Server

object Main {
  private val logger = Logger("stargate.Main")

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
    val parsedConfig = StargateConfig.parse(config)
    stargate.service.serverStart(parsedConfig)
  }
}

case class Params(conf: String = "")
