package stargate.metrics

import org.eclipse.jetty.server.Server
import io.prometheus.client.Histogram
import io.prometheus.client.Counter
import io.prometheus.client.Gauge

trait RequestCollector {
  val totalRequests: Counter = Counter
    .build()
    .name("total_http_requests")
    .help("total http requests made")
    .register()
 
  val totalErrors: Counter = Counter
    .build()
    .name("total_errors")
    .help("total errors")
    .register()

  val writeRequestLatency: Histogram = Histogram
    .build()
    .name("write_http_requests_latency_seconds")
    .help("Write Http Request latency in seconds.")
    .register()

  val readRequestLatency: Histogram = Histogram
    .build()
    .name("read_http_requests_latency_seconds")
    .help("Request Http latency in seconds.")
    .register()

  val schemaCreateLatency: Histogram = Histogram
    .build()
    .name("schema_create_http_latency_seconds")
    .help("schema creation http latency in seconds.")
    .register()

  val schemaValidateOnlyLatency: Histogram = Histogram
    .build()
    .name("schema_validation_only_http_latency_seconds")
    .help("schema validation only http latency in seconds.")
    .register()

  val inProgressRequests = Gauge
    .build()
    .name("inprogress_http_requests")
    .help("In progress http requests.")
    .register()

  private def log[A](histo: Histogram, action: () => A): A = {
    totalRequests.inc()
    inProgressRequests.inc()
    val activeTimer = histo.startTimer()
    try {
      action()
    } finally {
      activeTimer.observeDuration()
      inProgressRequests.dec()
    }
  }

  def timeRead[A](read: () => A): A = {
    log(readRequestLatency, read)
  }

  def timeWrite[A](write: () => A): A = {
    log(writeRequestLatency, write)
  }

  def timeSchemaCreate[A](schemaCreate: () => A): A = {
    log(schemaCreateLatency, schemaCreate)
  }

  def timeSchemaValidateOnly(
      schemaValidate: () => {}
  ): Unit = {
    log(schemaValidateOnlyLatency, schemaValidate)
  }
  def countError() = {
    totalErrors.inc()
  }

}
