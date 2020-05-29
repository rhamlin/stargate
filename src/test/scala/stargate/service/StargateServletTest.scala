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

import java.net.http.HttpRequest.BodyPublishers
import java.net.http.HttpResponse.BodyHandlers
import java.net.http.{HttpClient, HttpRequest}

import org.hamcrest.CoreMatchers._
import org.junit.Assert._
import org.junit.Test
import sttp.client._

import scala.io.Source

trait StargateServletTest extends HttpClientTestTrait {

  @Test
  def testNamespaceCreate(): Unit = {
    val newNamespace = s"testCreate${sc.rand.nextInt(2000)}"
    val r = quickRequest
      .post(wrap(s"${StargateApiVersion}/api/${newNamespace}/schema"))
      .contentType("application/hocon")
      .body(Source.fromResource("schema.conf").getLines().mkString("\n"))
      .send()
    assertEquals(Ok, r.code)
    assertTrue(r.contentType.isDefined)
    assertEquals("application/json", r.contentType.get)

    val getQuery = """{"-match":["firstName","=", "Steve"]}"""
    val url = wrap(s"${StargateApiVersion}/api/${newNamespace}/query/entity/${sc.entity}")
    val httpRequest = HttpRequest.newBuilder()
      .uri(url.toJavaUri)
      .method("GET", BodyPublishers.ofString(getQuery))
      .header("Content-Type", "application/json")
      .build()
    //validate I can hit the end point even if there are no results
    val statusCode = HttpClient.newHttpClient()
      .send(httpRequest, BodyHandlers.ofString()).statusCode()
    assertEquals(statusCode, 200)
  }

  @Test
  def testDeleteNamespace(): Unit = {
    val newNamespace = s"testCreate${sc.rand.nextInt(2000)}"
    var r = quickRequest
      .post(wrap(s"${StargateApiVersion}/api/${newNamespace}/schema"))
      .contentType("application/hocon")
      .body(Source.fromResource("schema.conf").getLines().mkString("\n"))
      .send()
    assertEquals(Ok, r.code)
    r = quickRequest
      .delete(wrap(s"${StargateApiVersion}/api/${newNamespace}/schema"))
      .contentType(ApplicationJson)
      .send()
    assertEquals(Ok, r.code)

    val getQuery = """{"-match":["firstName","=", "Steve"]}"""
    val url = wrap(s"${StargateApiVersion}/api/${newNamespace}/query/entity/${sc.entity}")
    val httpRequest = HttpRequest.newBuilder()
      .uri(url.toJavaUri)
      .method("GET", BodyPublishers.ofString(getQuery))
      .header("Content-Type", "application/json")
      .build()
    //validate there is no endpoint to hit
    val statusCode = HttpClient.newHttpClient()
      .send(httpRequest, BodyHandlers.ofString()).statusCode()
    //should get bad gateway with missing namespace
    assertEquals(502, statusCode )
  }

  @Test
  def testValidateSchema(): Unit = {
    val r = basicRequest
      .post(wrap(s"${StargateApiVersion}/api/validate"))
      .contentType("application/hocon")
      .body(Source.fromResource("schema.conf").getLines().mkString("\n"))
      .send()
    assertEquals(Ok, r.code)
  }
}
