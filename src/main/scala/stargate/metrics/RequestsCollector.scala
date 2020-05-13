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

package stargate.metrics

import com.swrve.ratelimitedlogger.RateLimitedLog
import com.typesafe.scalalogging.LazyLogging
import io.prometheus.client.{Counter, Gauge, Histogram}

trait RequestCollector
  extends LazyLogging {
  private val totalRequests: Counter = Counter
    .build()
    .name("total_http_requests")
    .help("total http requests made")
    .register()
 
  private val totalErrors: Counter = Counter
    .build()
    .name("total_errors")
    .help("total errors")
    .register()

  private val writeRequestLatency: Histogram = Histogram
    .build()
    .name("write_http_requests_latency_seconds")
    .help("Write Http Request latency in seconds.")
    .register()

  private val readRequestLatency: Histogram = Histogram
    .build()
    .name("read_http_requests_latency_seconds")
    .help("Request Http latency in seconds.")
    .register()

  private val schemaCreateLatency: Histogram = Histogram
    .build()
    .name("schema_create_http_latency_seconds")
    .help("schema creation http latency in seconds.")
    .register()

  private val schemaValidateOnlyLatency: Histogram = Histogram
    .build()
    .name("schema_validation_only_http_latency_seconds")
    .help("schema validation only http latency in seconds.")
    .register()

  private val inProgressRequests: Gauge = Gauge
    .build()
    .name("inprogress_http_requests")
    .help("In progress http requests.")
    .register()

  val rateLimitedLog: RateLimitedLog = RateLimitedLog
    .withRateLimit(logger.underlying)
    .maxRate(5)
    .every(java.time.Duration.ofSeconds(10))
    .build()

  private def log[A](histo: Histogram, action: () => A): A = {
    totalRequests.inc()
    inProgressRequests.inc()
    val activeTimer = histo.startTimer()
    try {
      action()
    }catch{
      case e: Exception => {
        rateLimitedLog.error(s"unhandled error ${e.getMessage}")
        throw e
      }
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
  def countError(): Unit = {
    totalErrors.inc()
  }

}
