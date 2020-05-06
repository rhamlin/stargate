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

import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.time.{Instant, LocalDate, LocalTime}
import java.util.UUID

object validations {

  private def parseTime(str: String): LocalTime = {
    try LocalTime.parse(str) catch {
      case e: Exception => throw InvalidConversionException(str, "LocalTime", e)
    }
  }

  private def parseTimestamp(str: String): Instant = {
    try Instant.parse(str) catch {
      case e: Exception => throw InvalidConversionException(str, "Instant", e)
    }
  }

  private def parseUUID(str: String): UUID = {
    try java.util.UUID.fromString(str) catch {
      case e: IllegalArgumentException => throw InvalidConversionException(str, "UUID", e)
    }
  }

  private def parseDate(str: String) : LocalDate = {
    try LocalDate.parse(str) catch {
      case e: Exception => throw InvalidConversionException(str, "LocalDate", e)
    }
  }

  /**
   * validates it's a valid ascii string to use with Cassandra
   * @param obj to validate
   * @tparam A type to handle
   * @return  returns validated object
   */
  def ascii[A](obj: A): A = obj match {
    case _ : String => if (!Charset.forName("US-ASCII").newEncoder().canEncode(obj.toString)) {
      throw InvalidACIIScalarException(obj.toString)
    } else {
      obj
    }
    case _ => throw InvalidScalarException(obj, classOf[String], obj.getClass)
  }

  /**
   * validates the object is a valid long to use with Cassandra
   * @param obj
   * @tparam A
   * @return
   */
  def bigInt[A](obj: A): A = obj match {
    case _: java.lang.Long => obj
    case _: Long => obj
    case _ => throw InvalidScalarException(obj, classOf[Long], obj.getClass)
  }

  def blob[A](obj: A): A = obj match{
    case _: ByteBuffer => obj
    case _ => throw InvalidScalarException(obj, classOf[Byte], obj.getClass)
  }

  def boolean[A](obj: A): A = obj match{
    case _: Boolean => obj
    case _ => throw InvalidScalarException(obj, classOf[Boolean], obj.getClass)
  }

  def date(obj: Object): LocalDate = obj match{
      case _ :String => parseDate(obj.toString)
      case _ :LocalDate => obj.asInstanceOf[LocalDate]
      case _ => throw InvalidScalarException(obj, classOf[LocalDate], obj.getClass)
  }

  def decimal[A](obj: A): A = obj match{
    case _ : BigDecimal => obj
    case _ : Double => obj
    case _ : Float => obj
    case _ => throw InvalidScalarException(obj, classOf[BigDecimal], obj.getClass)
  }

  def double[A](obj: A): A = obj match{
    case _ : BigDecimal => obj
    case _ : Double => obj
    case _ : Float => obj
    case _ => throw InvalidScalarException(obj, classOf[Double], obj.getClass)
  }

  def float[A](obj: A): A = obj match{
    case _ : BigDecimal => obj
    case _ : Double => obj
    case _ : Float => obj
    case _ => throw InvalidScalarException(obj, classOf[Float], obj.getClass)
  }

  def int[A](obj: A): A = obj match{
    case _ :Integer => obj
    case _ => throw InvalidScalarException(obj, classOf[Int], obj.getClass)
  }

  def smallInt[A](obj: A): A = obj match{
    case _: Short => obj
    case _ => throw InvalidScalarException(obj, classOf[Short], obj.getClass)
  }

  def text[A](obj: A): A = obj match{
    case _: String => obj
    case _ => throw InvalidScalarException(obj, classOf[String], obj.getClass)
  }

  def time[A](obj : A): LocalTime = obj match {
    case _: String => parseTime(obj.toString)
    case _: java.time.LocalTime => obj.asInstanceOf[LocalTime]
    case _ => throw InvalidScalarException(obj, classOf[LocalTime], obj.getClass)
  }

  def timestamp[A](obj: A): Instant = obj match {
    case _: String => parseTimestamp(obj.toString)
    case _: Long => Instant.ofEpochSecond(obj.asInstanceOf[Long])
    case _: Instant => obj.asInstanceOf[Instant]
    case _ => throw InvalidScalarException(obj, classOf[Instant], obj.getClass)
  }

  def uuid [A](obj: A): UUID = obj match{
    case _: String => parseUUID(obj.toString)
    case _: UUID => obj.asInstanceOf[UUID]
    case _ => throw InvalidScalarException(obj, classOf[UUID], obj.getClass)
  }

  def varInt[A](obj: A): A = obj match{
    case _: BigInt => obj
    case _ => throw InvalidScalarException(obj, classOf[BigInt], obj.getClass)
  }
}
