package appstax

import org.eclipse.jetty.server.handler.StatisticsHandler
import io.prometheus.client.jetty.JettyStatisticsCollector
import io.prometheus.client.hotspot.DefaultExports
import org.eclipse.jetty.server.Server

package object metrics {

  def registerAll(server: Server): Unit = {
    registerServlet(server)
    registerJVM()
  }

  def registerJVM(): Unit = {
    DefaultExports.initialize();
  }

  def registerServlet(server: Server): Unit = {
    // Configure StatisticsHandler.
    val stats = new StatisticsHandler()
    stats.setHandler(server.getHandler())
    server.setHandler(stats)
    // Register collector.
    val collector = new JettyStatisticsCollector(stats)
    collector.register()
  }

}