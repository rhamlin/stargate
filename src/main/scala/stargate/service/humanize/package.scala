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

package object humanize {
  val KB: Long = 1024
  val MB: Long = 1024 * 1024
  val GB: Long = 1024 * 1024 * 1024

  def bytes(bytes: Long): String = {
    if (bytes < 1024) {
      s"$bytes b"
    } else if (bytes < MB) {
      f"${bytes.toFloat / KB.toFloat}%.2f kb"
    } else if (bytes < GB) {
      f"${bytes.toFloat / MB.toFloat}%.2f mb"
    } else {
      f"${bytes.toFloat / GB.toFloat}%.2f gb"
    }
  }
}
