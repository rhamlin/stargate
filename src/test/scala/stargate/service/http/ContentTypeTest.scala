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

package stargate.service.http

import org.junit.Test
import stargate.service.http._

class ContentTypeTest {

  @Test
  def testJson() = {
    validateJsonContentType("application/json")
    validateJsonContentType("APPLICATION/json")
    validateJsonContentType("APPLICATION/JSON")
  }

  @Test(expected = classOf[InvalidContentTypeException])
  def testInvalidateJson() = {
    validateJsonContentType("application/wrong")
  }

  @Test
  def testJsonWithCharSet() = {
    validateJsonContentType("application/json;charset=utf-8")
    validateJsonContentType("application/JSON;charset=utf-8")
    validateJsonContentType("APPLICATION/json;charset=utf-8")
    validateJsonContentType("APPLICATION/json; charset=utf-8")
    validateJsonContentType("APPLICATION/json;   charset=utf-8")
    validateJsonContentType(" APPLICATION/json ;   CHARSET=UTF-8")
  }

  @Test
  def testInvalidCharSet() = {
    validateJsonContentType("APPLICATION/json;charset=utf-16")
  }

  @Test
  def testHocon() = {
    validateHoconContentType("application/hocon")
    validateHoconContentType("APPLICATION/hocon")
    validateHoconContentType("APPLICATION/HOCON")
  }

  @Test(expected = classOf[InvalidContentTypeException])
  def testInvalidHocon = {
    validateHoconContentType("application/wrong")
    validateHoconContentType("APPLICATION/JSON")
    validateHoconContentType("APPLICATION/wrong")
  }

}
