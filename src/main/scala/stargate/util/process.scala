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

package stargate.util

import scala.util.{Failure, Success, Try}
import scala.jdk.CollectionConverters._

object process {

  def exec(command: List[String]): Try[String] = {
    exec(asyncExec(command))
  }

  def exec(process: Process): Try[String] = {
    process.waitFor
    if(process.exitValue == 0) {
      Success(new String(process.getInputStream.readAllBytes))
    } else {
      Failure(new RuntimeException(new String(process.getErrorStream.readAllBytes)))
    }
  }

  def asyncExec(command: String*): Process = {
    asyncExec(command.toList)
  }

  def asyncExec(command: List[String]): Process = {
    new ProcessBuilder().command(command.asJava).start()
  }
}
