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

case class InvalidConversionException(fieldValue: String, typeName: String, e: Exception)
  extends Exception (s"cannot convert $fieldValue to $typeName", e)

case class InvalidACIIScalarException(original: String)
  extends Exception(s"invalid ASCII original: $original")

case class InvalidKeywordException(keyword: String) extends Exception {}

case class InvalidScalarException(value: Any, expectedType: Class[_], actualType: Class[_])
  extends Exception(s"$value was expected to be of type $expectedType but was $actualType"){}

case class UnknownFieldException(keyName: String, entityName: String)
  extends Exception(s"do not know how to process $keyName on $entityName"){}

case class UnsupportedObjectException(keyName: String, original: Class[_])
  extends Exception(s"do not how to validate $keyName which is type of $original")

case class MaximumRequestSizeException(contentLength: Long, maxRequestSizeKB: Long)
  extends Exception(s"request is above maximum size of $maxRequestSizeKB KB by ${humanize.bytes(contentLength-maxRequestSizeKB)}"){}

case class MaxMutationSizeException(contentLength: Long, maxMutationSizeKB: Long)
  extends Exception(s"Mutation is above maximum size of $maxMutationSizeKB KB by ${humanize.bytes(contentLength-maxMutationSizeKB)}"){}

case class SchemaToLargeException(contentLength: Long, maxSchemaSizeKB: Long)
  extends Exception(s"the schema is above maximum size of $maxSchemaSizeKB KB by ${humanize.bytes(contentLength-maxSchemaSizeKB)}"){}
