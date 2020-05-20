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
package stargate.cassandra

/**
  * 
  *
  * @param missingColumns
  * @param missingKeys
  * @param missingPartitionKeys
  * @param skipped
  */
final case class KeyConditionScore(missingColumns: Int, missingKeys: Int, missingPartitionKeys: Boolean, skipped: Int) extends Ordered[KeyConditionScore] {
    def perfect: Boolean = (missingColumns + missingKeys + skipped) == 0 && !missingPartitionKeys
    def tuple: (Int, Int, Boolean, Int) = (missingColumns, missingKeys, missingPartitionKeys, skipped)
    override def compare(that: KeyConditionScore): Int = Ordering[(Int,Int,Boolean,Int)].compare(this.tuple, that.tuple)
  }

