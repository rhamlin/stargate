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

import java.util.regex.Pattern
import javax.servlet.http.HttpServletRequest

package object http {

  def validateRequestSize(contentLength: Long, maxRequestSize: Long): Unit = {
    if (contentLength > maxRequestSize){
      throw MaximumRequestSizeException(contentLength, maxRequestSize)
    }
  }

  def validateSchemaSize(contentLength: Long, maxSchemaSize: Long): Unit= {
    if (contentLength > maxSchemaSize){
      throw SchemaToLargeException(contentLength, maxSchemaSize)
    }
  }

  def validateMutation(op: String, contentLength: Long, maxMutationSize: Long): Unit ={
    if ((op == "PUT" || op == "POST") && contentLength > maxMutationSize){
      throw MaxMutationSizeException(contentLength, maxMutationSize)
    }
  }

  val hoconType = "application/hocon"
  val jsonType = "application/json"
  val pathRegex: Pattern = Pattern.compile("//")

  def validateHoconHeader(req: HttpServletRequest): Unit = {
    val contentType = req.getContentType
    if (contentType != hoconType) {
      throw new InvalidContentTypeException(hoconType, contentType)
    }
  }

  def validateJsonContentHeader(req: HttpServletRequest) : Unit = {
    val contentType = req.getContentType
    if (contentType != jsonType){
      throw InvalidContentTypeException(jsonType, contentType)
    }
  }
  def validateRestSize(req: HttpServletRequest): Unit = {

  }

  /**
   * based on idea from spring boot, to help prevent escaping out of url string and accessing another path
   * precompile the regex above because its compiled every time on String.replaceAll()
   * @param path serverPath to sanitize
   * @return string with all of the // replaced with /
   */
  def sanitizePath(path: String): String = {
    pathRegex.matcher(path).replaceAll("/")
  }
}

case class InvalidContentTypeException(expectedContentType: String, contentType: String)
  extends Exception(s"Expected $expectedContentType but was $contentType"){}
