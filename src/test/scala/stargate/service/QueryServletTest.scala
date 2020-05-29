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

import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.scalalogging.LazyLogging
import org.hamcrest.CoreMatchers._
import org.junit.Assert._
import org.junit.{After, Before, Test}
import sttp.client._

trait QueryServletTest extends HttpClientTestTrait with LazyLogging {

  val url = wrap(s"${StargateApiVersion}/api/${sc.namespace}/query/entity/${sc.entity}")

  @After
  def tearDown(): Unit = {
    val r = quickRequest
      .delete(wrap(s"${StargateApiVersion}/api/${sc.namespace}/query/entity/${sc.entity}"))
      .contentType("application/json")
      .body("""
              |{
              |  "-match": "all"
              |}
              |""".stripMargin)
      .send()
    if(r.code != Ok){
      logger.error(s"unable to delete records during teardown: ${r.code}:${r.statusText}")
    }
  }
  @Before
  def setupRecord(): Unit = {
    logger.info(s"url for record setup ${url}")
    val r = quickRequest
      .post(url)
      .contentType("application/json")
      .body("""
          |{
          | "firstName": "Steve",
          | "lastName": "Mikey",
          | "addresses": {
          |    "street": "my street"
          |  }
          |}
          |""".stripMargin)
      .send()
    if (r.code != Ok){
      throw new RuntimeException(s"can't setup new record to query ${r.code}:${r.statusText}")
    }
  }

  @Test
  def testEntityCreate(): Unit = {
    val r = quickRequest
      .post(wrap(s"${StargateApiVersion}/api/${sc.namespace}/query/entity/${sc.entity}"))
      .contentType("application/json")
      .body("""
          |{
          |        "firstName": "Steve",
          |        "lastName": "Jacobs"
          |    }
          |""".stripMargin)
      .send()
    assertEquals(r.code, Ok)
    assertTrue(r.contentType.isDefined)
    assertEquals(r.contentType.get, "application/json")
  }

  @Test
  def testEntityDelete(): Unit = {
     var r = quickRequest
      .post(url)
      .contentType("application/json")
      .body("""
          |{
          | "firstName": "Steve"
          |}
          |""".stripMargin)
      .send()
    if (r.code != Ok){
      throw new RuntimeException(s"can't setup new record to query ${r.code}:${r.statusText}")
    }

    r = quickRequest
      .delete(wrap(s"${StargateApiVersion}/api/${sc.namespace}/query/entity/${sc.entity}"))
      .contentType("application/json")
      .body("""
          |{ 
          |  "-match": ["firstName", "=", "Steve"]
          |}
          |""".stripMargin)
      .send()
    assertEquals(r.code, Ok)
    assertTrue(r.contentType.isDefined)
    assertEquals(r.contentType.get, "application/json")
    logger.info(s"body from server after delete request is $r.body")

    val newUrl = wrap(s"${StargateApiVersion}/api/${sc.namespace}/query/entity/${sc.entity}")
    val getQuery = ("""
                      |{
                      | "-match":["firstName","=", "Steve"]
                      |}
                      |""".stripMargin)
    val httpRequest = HttpRequest.newBuilder()
      .uri(newUrl.toJavaUri)
      .method("GET", BodyPublishers.ofString(getQuery))
      .header("Content-Type", "application/json")
      .build()
    val response = HttpClient.newHttpClient()
      .send(httpRequest, BodyHandlers.ofString())
    assertEquals(200, response.statusCode())
    assertTrue(response.headers().firstValue("Content-Type").isPresent)
    assertEquals(response.headers().firstValue("Content-Type").get, "application/json")
    logger.info(s"query response after delete  is ${response.body}")
    val objectMapper = new ObjectMapper()
    val resultBody = objectMapper.readValue( response.body ,
      classOf[java.util.ArrayList[java.util.HashMap[String,java.util.ArrayList[java.util.HashMap[String, String]]]]])
    assertEquals("there were unexpected matches after delete", 0, resultBody.size())
  }

  @Test
  def testEntityUpdate(): Unit = {
    val r = quickRequest
      .put(wrap(s"${StargateApiVersion}/api/${sc.namespace}/query/entity/${sc.entity}"))
      .contentType("application/json")
      .body("""
          |{
          |"-match":["firstName","=","Steve"],
          | "lastName": "Danger",
          | "addresses":{
          |   "-update":{
          |      "-match":["customers.firstName","=","Steve"],
          |      "street":"other st"}
          |      }
          |}
          |""".stripMargin)
      .send()
    assertEquals(r.code, Ok)
    assertTrue(r.contentType.isDefined)
    assertEquals(r.contentType.get, "application/json")
    logger.info(s"update query response is ${r.body}")
    val getQuery = ("""
          |{
          | "-match":["firstName","=", "Steve"],
          |   "addresses":{},
          |     "orders":{}
          |}
          |""".stripMargin)
    val url = wrap(s"${StargateApiVersion}/api/${sc.namespace}/query/entity/${sc.entity}")
    val httpRequest = HttpRequest.newBuilder()
      .uri(url.toJavaUri)
      .method("GET", BodyPublishers.ofString(getQuery))
      .header("Content-Type", "application/json")
      .build()
    val response = HttpClient.newHttpClient()
      .send(httpRequest, BodyHandlers.ofString())
    assertEquals(response.statusCode(), 200)
    assertTrue(response.headers().firstValue("Content-Type").isPresent)
    assertEquals(response.headers().firstValue("Content-Type").get, "application/json")
    logger.info(s"query response after update is ${response.body}")
    val objectMapper = new ObjectMapper()
    val resultBody = objectMapper.readValue( response.body ,
      classOf[java.util.ArrayList[java.util.HashMap[String,java.util.ArrayList[java.util.HashMap[String, String]]]]])
    assertEquals("total affected records is unexpected", 1, resultBody.size())
    assertEquals("expected name", 1, resultBody.size())
    assertEquals("cannot find name of updated record", "Steve", resultBody.get(0).get("firstName"))
    assertEquals("updated street name does not match", "other st",
      resultBody.get(0).get("addresses").get(0).get("street"))
  }

  @Test
  def testEntityQuery(): Unit = {
    val getQuery = """{"-match":["firstName","=", "Steve"]}"""
    val url = wrap(s"${StargateApiVersion}/api/${sc.namespace}/query/entity/${sc.entity}")
    val httpRequest = HttpRequest.newBuilder()
      .uri(url.toJavaUri)
      .method("GET", BodyPublishers.ofString(getQuery))
      .header("Content-Type", "application/json")
      .build()

    val r = HttpClient.newHttpClient()
        .send(httpRequest, BodyHandlers.ofString())
    assertEquals(r.statusCode(), 200)
    assertTrue(r.headers().firstValue("Content-Type").isPresent)
    assertEquals(r.headers().firstValue("Content-Type").get, "application/json")

    //validate body contents
    val objectMapper = new ObjectMapper()
    val resultBody = objectMapper.readValue( r.body ,
      classOf[java.util.ArrayList[java.util.HashMap[String, String]]])
    assertEquals("total affected records is unexpected", 1, resultBody.size())
    logger.info(s"entity query output is ${r.body}")
    assertEquals("cannot find record", "Steve", resultBody.get(0).get("firstName"))
    assertEquals("cannot find record", "Mikey", resultBody.get(0).get("lastName"))
  }

  @Test
  def testContinueQuery(): Unit = {
    var r = quickRequest
      .post(url)
      .contentType("application/json")
      .body("""
              |{
              | "firstName": "Steve",
              | "lastName": "James"
              |}
              |""".stripMargin)
      .send()
    if (r.code != Ok){
      throw new RuntimeException(s"can't setup new record to query ${r.code}:${r.statusText}")
    }
    val getQuery = """{"-match":["firstName","=", "Steve"],"-limit":1,"-continue":true}"""
    val queryUrl = wrap(s"${StargateApiVersion}/api/${sc.namespace}/query/entity/${sc.entity}")
    val request = HttpRequest.newBuilder()
      .uri(queryUrl.toJavaUri)
      .method("GET", BodyPublishers.ofString(getQuery))
      .header("Content-Type", "application/json")
      .build()
    val response = HttpClient.newHttpClient()
      .send(request, BodyHandlers.ofString())
    logger.info(s"continue requested query response is ${response.body}")
    val objectMapper = new ObjectMapper()
    val resultBody = objectMapper.readValue( response.body , classOf[java.util.ArrayList[java.util.HashMap[String,Object]]])
    val queryId: String = resultBody.get(1).get("-continue").toString
    r = quickRequest
      .get(wrap(s"${StargateApiVersion}/api/${sc.namespace}/query/continue/${queryId}"))
      .send()
    logger.info(s"2nd page requested response is ${r.body}")
    assertEquals(r.code, Ok)
    assertTrue("content type is not defined", r.contentType.isDefined)
    assertEquals(r.contentType.get, "application/json")
    assertThat("cant find user account on 2nd page", r.body, containsString("Steve"))
  }

  @Test
  def testStoredQuery(): Unit = {
    val getQuery = """
                     |{
                     |        "-match":{
                     |            "customerName": "Steve"
                     |        }
                     |}
                     |""".stripMargin
    val myQuery = "customerByFirstName"
    val queryUrl = wrap(s"${StargateApiVersion}/api/${sc.namespace}/query/stored/${myQuery}")
    val request = HttpRequest.newBuilder()
      .uri(queryUrl.toJavaUri)
      .method("GET", BodyPublishers.ofString(getQuery))
      .header("Content-Type", "application/json")
      .build()
    val response = HttpClient.newHttpClient()
      .send(request, BodyHandlers.ofString())
    logger.info(s"continue requested query response is ${response.body}")
    val objectMapper = new ObjectMapper()
    val responseBody = objectMapper.readValue( response.body , classOf[java.util.ArrayList[java.util.HashMap[String,Object]]])
    assertEquals(response.statusCode(), 200)
    assertTrue(response.headers().firstValue("Content-Type").isPresent)
    assertEquals(response.headers().firstValue("Content-Type").get, "application/json")
    logger.info(s"show body for named query response $responseBody")
    assertEquals(responseBody.get(0).get("firstName"), "Steve")
  }
}
