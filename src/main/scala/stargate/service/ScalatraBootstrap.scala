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

package stargate.service

import javax.servlet.ServletContext
import org.scalatra.LifeCycle
import org.scalatra.metrics.MetricsBootstrap
import stargate.service.config.ParsedStargateConfig
import stargate.cassandra
import com.datastax.oss.driver.api.core.CqlSession

class ScalatraBootstrap 
extends LifeCycle 
with MetricsBootstrap 
with CqlSupport{

  override def init(context: ServletContext) {
    val sgConfig: ParsedStargateConfig = ParsedStargateConfig.globalConfig
    val cqlSession: CqlSession = cassandra.session(sgConfig)
    val datamodelRepoTable: cassandra.CassandraTable = createDatamodelRepoTable(sgConfig, cqlSession)
    val namespaces = new Namespaces(datamodelRepoTable, cqlSession)
    context.mount (new StargateServlet(sgConfig, cqlSession, namespaces, datamodelRepoTable), "/*")
    context.mount (new SwaggerServlet(namespaces),"/api-docs")
  }
}
