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

import java.util.Locale

import org.junit.{Assert, Test}

class HumanizeTest {

  @Test
  def testHumanizeBytes() = {
    Locale.setDefault(Locale.US)
    Assert.assertEquals("0 b", humanize.bytes(0))
    Assert.assertEquals("1023 b", humanize.bytes(1023))
    Assert.assertEquals("1.00 kb", humanize.bytes(1024))
    Assert.assertEquals("10.00 kb", humanize.bytes(10240))
    Assert.assertEquals("1.00 mb", humanize.bytes(1048576))
    Assert.assertEquals("10.00 gb", humanize.bytes(10737418240L))
  }

}
