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
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}
import stargate.util
import stargate.service.config.StargateConfig
import com.swrve.ratelimitedlogger.RateLimitedLog
import java.nio.charset.StandardCharsets
import java.net.URLDecoder

/**
  * SwaggerServlet serves the swagger api json and html
  * @param namespaces provides access to configured namespaces as well as the ability to modify namespaces
  */
class SwaggerServlet(val namespaces: Namespaces, sgConfig: StargateConfig) extends HttpServlet with LazyLogging {
  //TODO replace this with some wrapper
  protected implicit val jsonFormats: Formats = DefaultFormats.withBigDecimal
  val maxRequestSize: Long = sgConfig.maxRequestSizeKB * 1024
  val rateLimitedLog: RateLimitedLog = RateLimitedLog
    .withRateLimit(logger.underlying)
    .maxRate(5)
    .every(java.time.Duration.ofSeconds(10))
    .build()

  /**
    * GET /:namespace/swagger - renders the swagger-ui for a given namespace
    * GET /:namespace/swagger.json renders the json for a given namespace
    * GET /swagger - renders the default swagger-ui with the default json for namespace creation, deletion and validation.
    */
def route(request: HttpServletRequest, response: HttpServletResponse): Unit = {
    try {
      val namespace = request.getParameter("namespace")
      val contentLength = request.getContentLengthLong
      http.validateRequestSize(contentLength, maxRequestSize)
      val op = request.getMethod

      val path = http.sanitizePath(request.getPathInfo())
      logger.debug(s"Swagger PATH from servlet is $path")
      logger.trace(s"http request: { path: '$path', method: '$op', content-length: $contentLength, content-type: '${request.getContentType}' }")
      path match {
          case s"/${namespace}/swagger.json" =>
          val swagger = namespaces.getSwagger(namespace)
          swagger match {
            case Some(swagger) =>
              response.setContentType("application/json")
              response.getWriter.write(
              Json.mapper
                .registerModule(new JavaTimeModule)
                .writerWithDefaultPrettyPrinter()
                .writeValueAsString(swagger)
              )
            case  None => 
              response.setContentType("application/json")
              rateLimitedLog.warn(s"no swagger for namespace $namespace")
              response.setStatus(HttpServletResponse.SC_NOT_FOUND)
          }
        case s"/${namespace}/swagger" =>
          val contentType = "text/html;charset=utf-8"
          val host = request.getServerName
          val port = request.getLocalPort
          val scheme = request.getScheme
          val rendered: String = template.renderSwagger(s"$scheme://$host:$port/api-docs/$namespace/swagger.json")
          response.setContentType(contentType)
          response.getWriter.write(rendered)
        case s"/${namespace}" =>
          val contentType = "text/html;charset=utf-8"
          val host = request.getServerName
          val port = request.getLocalPort
          val scheme = request.getScheme
          val rendered = template.renderSwagger(s"$scheme://$host:$port/swagger.json")
          response.setContentType(contentType)
          response.getWriter.write(rendered)
                case _ =>
          response.setStatus(HttpServletResponse.SC_NOT_FOUND)
          val msg = s"path: $path is not a valid path, must be:\n" + 
           "* GET /api-docs/:namespace/swagger\n" +
           "* GET /api-docs/:namespace/swagger.json\n" +
           "* GET /api-docs/swagger"
          rateLimitedLog.warn(msg)
          response.getWriter.write(util.toJson(msg))
      }
    } catch {
      case e: Exception =>
        rateLimitedLog.error(s"exception: $e")
        response.setStatus(HttpServletResponse.SC_BAD_GATEWAY)
        response.getWriter.write(util.toJson(e.getMessage))
    }
  }
  override def doPut(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
    route(req, resp)
  }

  override def doDelete(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
    route(req, resp)
  }

  override def doGet(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
    route(req, resp)
  }

  override def doPost(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
    super.getServletContext
    route(req, resp)
  }


}
