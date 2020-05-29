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

import org.hamcrest.CoreMatchers._
import org.junit.Assert._
import org.junit.Test
import sttp.client._

trait SwaggerServletTest extends HttpClientTestTrait {
  
  @Test
  def testSwaggerEntityHtmlRenders(): Unit = {
    val r = quickRequest.get(wrap(s"api-docs/${sc.namespace}/swagger"))
    .send()
    assertEquals(Ok, r.code)
    assertTrue(r.contentType.isDefined)
    assertEquals(s"${TextHtml};charset=utf-8", r.contentType.get)
    assertThat("cannot find swagger url", r.body, containsString(s"${sc.namespace}/swagger.json"))
  }

  @Test
  def testSwaggerEntityJsonRenders(): Unit = {
    val r = quickRequest.get(wrap(s"api-docs/${sc.namespace}/swagger.json"))
    .send()
    assertEquals(Ok, r.code)
    assertTrue(r.contentType.isDefined)
    assertEquals(ApplicationJson.toString(), r.contentType.get)
    assertThat("namespace missing", r.body, containsString(sc.namespace))
    assertThat("entity missing", r.body, containsString(sc.entity))
  }

  @Test
  def testSwaggerNamespacesRenders(): Unit = {
    val r = quickRequest.get(wrap("api-docs/swagger"))
    .send()
    assertEquals(Ok, r.code)
    assertTrue(r.contentType.isDefined)
    assertEquals(s"${TextHtml};charset=utf-8", r.contentType.get)
    assertThat("cannot find swagger url", r.body, containsString("swagger.json"))
  }
}
