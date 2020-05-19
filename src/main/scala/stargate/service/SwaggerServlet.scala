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

import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.typesafe.scalalogging.LazyLogging
import io.swagger.v3.core.util.Json
import io.swagger.v3.oas.models.OpenAPI
import org.json4s.{DefaultFormats, Formats}
import org.scalatra.{NotFound, Ok, ScalatraServlet}
import org.scalatra.json.NativeJsonSupport

class SwaggerServlet( val namespaces: Namespaces)
  extends ScalatraServlet
  with CqlSupport
  with LazyLogging
  with NativeJsonSupport
{
  protected implicit val jsonFormats: Formats = DefaultFormats.withBigDecimal

    get("/:namespace/swagger.json"){
    val nameSpace = params("namespace")
    val swagger = Option[OpenAPI](namespaces.getSwagger(nameSpace))
    swagger match {
      case Some(swagger) => Ok(Json.mapper.registerModule(new JavaTimeModule)
          .writerWithDefaultPrettyPrinter()
      .writeValueAsString(swagger))
      case None => NotFound
    }
  }

  get("/swagger/"){
    //default namespace creator swagger
    contentType="text/html"
    val host = request.getServerName
    val port = request.getLocalPort
    val scheme = request.getScheme
    template.renderSwagger(s"$scheme://$host:$port/swagger.json")
  }

  get("/:namespace/swagger/"){
    contentType="text/html"
    val host = request.getServerName
    val port = request.getLocalPort
    val scheme = request.getScheme
    val ns = params("namespace")
    template.renderSwagger(s"$scheme://$host:$port/api-docs/$ns/swagger.json")
  }
}
