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
  def registerServletHandlerStastics(server: Server): Unit = {
    // Configure StatisticsHandler.
    val stats = new StatisticsHandler()
    //get current server handler and set it inside StatisticsHandler
    stats.setHandler(server.getHandler())
    //set StatisticsHandler as the handler for servlet
    server.setHandler(stats)
    // Register collector.
    new JettyStatisticsCollector(stats).register()
  }
}
