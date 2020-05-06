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

import org.eclipse.jetty.server.handler.StatisticsHandler
import io.prometheus.client.jetty.JettyStatisticsCollector
import io.prometheus.client.hotspot.DefaultExports
import org.eclipse.jetty.server.Server

package object metrics {

  /**
  * sets default Hotspot metrics for the JVM
  */
  def registerJVMMetrics(): Unit = {
    DefaultExports.initialize();
  }

  /**
    * Has to be called after all the other handlers you want to be called, 
    * it gets the previous handler and wraps it
    * @param server Jetty Server
    */
  def registerServletHandlerStatistics(server: Server): Unit = {
    // Configure StatisticsHandler.
    val stats = new StatisticsHandler()
    //get current server handler and set it inside StatisticsHandler
    stats.setHandler(server.getHandler)
    //set StatisticsHandler as the handler for servlet
    server.setHandler(stats)
    // Register collector.
    new JettyStatisticsCollector(stats).register()
  }
}
