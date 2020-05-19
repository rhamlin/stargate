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

import java.util.concurrent.ConcurrentHashMap

import com.datastax.oss.driver.api.core.CqlSession
import javax.servlet.http.HttpServletResponse
import stargate.model.{OutputModel, generator}
import stargate.util

import scala.concurrent.ExecutionContextExecutor

trait EntityGeneration {

  def lookupModel(apps: ConcurrentHashMap[String, OutputModel] , appName: String): OutputModel = {
    val model = apps.get(appName)
    require(model != null, s"invalid database name: $appName")
    model
  }

  def generateQuery(appName: String, 
                    entity: String, 
                    op: String, 
                    resp: HttpServletResponse,
                    cqlSession: CqlSession,
                    executor: ExecutionContextExecutor,
                    apps: ConcurrentHashMap[String, OutputModel]
                    ): String = {
    val model = lookupModel(apps, appName)
    require(model.input.entities.contains(entity), s"""database "$appName" does not have an entity named "$entity" """)
    val validOps = Set("create", "get", "update", "delete")
    require(requirement = validOps.contains(op), message = s"operation $op must be one of the following: $validOps")
    val requestF = op match {
      case "create" => generator.specificCreateRequest(model, entity, cqlSession, executor)
      case "get" => generator.specificGetRequest(model, entity, 3, cqlSession, executor)
      case "update" => generator.specificUpdateRequest(model, entity, cqlSession, executor)
      case "delete" => generator.specificDeleteRequest(model, entity, cqlSession, executor)
    }
    val request = util.await(requestF).get
    util.toJson(request)
  }
}
