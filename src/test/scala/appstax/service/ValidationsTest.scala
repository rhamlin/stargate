package appstax.service

import java.nio.ByteBuffer
import java.time.{Instant, LocalDate, LocalTime}
import java.util.UUID

import org.junit.Assert._
import org.junit.Test

class ValidationsTest {

  @Test
  def testValidateAscii(): Unit = {
    var obj = "jfa".asInstanceOf[Object]
    assertEquals(validations.ascii(obj), "jfa")
    assertEquals(validations.ascii("jfa"), "jfa")
    obj = "jfa".asInstanceOf[java.lang.String]
    assertEquals(validations.ascii(obj), "jfa")
  }

  @Test(expected = classOf[InvalidScalarException])
  def testValidateAsciiErrors(): Unit = {
    validations.ascii(1)
  }

  @Test
  def testValidateBigInt(): Unit = {
    val obj = 1L.asInstanceOf[Object]
    assertEquals(validations.bigInt(obj), 1L)
    assertEquals(validations.bigInt(1L), 1L)
    val javaLong: java.lang.Long = 1L
    assertEquals(validations.bigInt(javaLong), javaLong)
  }

  @Test(expected = classOf[InvalidScalarException])
  def testValidateBigIntErrors(): Unit = {
    validations.bigInt("1")
  }

  @Test
  def testValidateBoolean(): Unit ={
    assertEquals(validations.boolean(true.asInstanceOf[Object]), true)
    assertEquals(validations.boolean(true), true)
    assertEquals(validations.boolean( true.asInstanceOf[java.lang.Boolean]), true)
  }

  @Test(expected = classOf[InvalidScalarException])
  def testValidateBooleanErrors(): Unit = {
    validations.boolean("true")
  }

  @Test
  def testValidateDate(): Unit = {
    assertEquals(validations.date(LocalDate.of(2011, 1, 2)), LocalDate.of(2011, 1, 2))
    assertEquals(validations.date(LocalDate.of(2011, 1, 2).asInstanceOf[Object]), LocalDate.of(2011, 1,2))
    assertEquals(validations.date(LocalDate.of(2011, 1, 2)), LocalDate.of(2011, 1,2))
    //ISO format so stable day and month
    assertEquals(validations.date("2011-01-02"), LocalDate.of(2011, 1, 2))
  }

  @Test(expected = classOf[InvalidConversionException])
  def testValidateDateErrorsWithParse(): Unit = {
    validations.date("1")
  }

  @Test(expected = classOf[InvalidScalarException])
  def testValidateDateError(): Unit = {
    validations.date(UUID.randomUUID())
  }

  @Test
  def testValidateDecimal(): Unit = {
    val value = "1.41"
    val actual = BigDecimal(value)
    val expected = BigDecimal(value)
    assertEquals(validations.decimal(actual), expected)
    assertEquals(validations.decimal(actual.asInstanceOf[Object]), expected)
    assertEquals(validations.decimal(1.41), 1.41, 0.02)
    assertEquals(validations.decimal(1.41f), 1.41f, 0.02)
  }

  @Test(expected = classOf[InvalidScalarException])
  def testValidateDecimalErrors(): Unit = {
    validations.decimal("u1")
  }

  @Test
  def testValidateFloat(): Unit = {
    val value = "1.41"
    val actual = BigDecimal(value).floatValue
    val expected = BigDecimal(value).floatValue
    assertEquals(validations.float(actual), expected, 0.01)
    assertEquals(validations.float(actual.asInstanceOf[Object]), expected)
    assertEquals(validations.float(actual.asInstanceOf[Double]), expected.asInstanceOf[Double], 0.02)
  }

  @Test(expected = classOf[InvalidScalarException])
  def testValidateFloatErrors(): Unit = {
    validations.float("u1")
  }

  @Test
  def testValidateInt(): Unit = {
    val actual = 10
    val expected = 10
    assertEquals(validations.int(actual), expected)
    assertEquals(validations.int(actual.asInstanceOf[Object]), expected)
  }

  @Test(expected = classOf[InvalidScalarException])
  def testValidateIntErrors(): Unit = {
    validations.int("u1")
  }

  @Test
  def testValidateVarInt(): Unit = {
    val value = "10"
    val actual = BigInt(value)
    val expected = BigInt(value)
    assertEquals(validations.varInt(actual), expected)
    assertEquals(validations.varInt(actual.asInstanceOf[Object]), expected)
  }

  @Test(expected = classOf[InvalidScalarException])
  def testValidateVarIntErrors(): Unit = {
    validations.varInt("u1")
  }

  @Test
  def testValidateSmallInt(): Unit = {
    val actual: Short = 10
    val expected: Short = 10
    assertEquals(validations.smallInt(actual), expected)
    assertEquals(validations.smallInt(actual.asInstanceOf[Object]), expected)
  }

  @Test(expected = classOf[InvalidScalarException])
  def testValidateSmallIntErrors(): Unit = {
    validations.smallInt("u1")
  }

  @Test
  def testValidateDouble(): Unit = {
    val value = "1.41"
    val actual = BigDecimal(value).toDouble
    val expected = BigDecimal(value).toDouble
    assertEquals(validations.double(actual), expected, 0.01)
    assertEquals(validations.double(actual.asInstanceOf[Object]), expected)
    assertEquals(validations.double(actual.asInstanceOf[Float]), expected.asInstanceOf[Float], 0.02)
  }

  @Test(expected = classOf[InvalidScalarException])
  def testValidateDoubleErrors(): Unit = {
    validations.double("u1")
  }

  @Test
  def testValidateTime(): Unit = {
    assertEquals(validations.time(LocalTime.of(13, 1, 2)), LocalTime.of(13, 1, 2))
    assertEquals(validations.time(LocalTime.of(13, 1, 2).asInstanceOf[Object]), LocalTime.of(13, 1,2))
    assertEquals(validations.time(LocalTime.of(13, 1, 2)), LocalTime.of(13, 1,2))
    assertEquals(validations.time("13:01:02"), LocalTime.of(13, 1, 2))
  }

  @Test(expected = classOf[InvalidConversionException])
  def testValidateTimeErrorsWithParse(): Unit = {
    validations.time("1")
  }

  @Test(expected = classOf[InvalidScalarException])
  def testValidateTimeError(): Unit = {
    validations.time(UUID.randomUUID())
  }

  @Test
  def testValidateTimestamp(): Unit = {
    val epoch = 1587753602L
    val actual = Instant.ofEpochSecond(epoch)
    val expected = Instant.ofEpochSecond(epoch)
    assertEquals(validations.timestamp(epoch), expected)
    assertEquals(validations.timestamp(actual), expected)
    assertEquals(validations.timestamp(actual.asInstanceOf[Object]), expected)
    assertEquals(validations.timestamp("2020-04-24T18:40:02Z"), expected)
  }

  @Test(expected = classOf[InvalidConversionException])
  def testValidateTimestampErrorsWithParse(): Unit = {
    validations.timestamp("1")
  }

  @Test(expected = classOf[InvalidScalarException])
  def testValidateTimestampError(): Unit = {
    validations.timestamp(UUID.randomUUID())
  }

  @Test
  def testValidateUUID(): Unit = {
    //type4 uuid
    val actual = UUID.fromString("d21a6e2f-f328-4994-bc41-990cd9037c97")
    val expected = UUID.fromString("d21a6e2f-f328-4994-bc41-990cd9037c97")
    assertEquals(validations.uuid(actual), expected)
    assertEquals(validations.uuid(actual.asInstanceOf[Object]), expected)
    assertEquals(validations.uuid("d21a6e2f-f328-4994-bc41-990cd9037c97"), expected)
    //type1 uuid
    assertEquals(validations.uuid("83566baa-8661-11ea-bc55-0242ac130003"), UUID.fromString("83566baa-8661-11ea-bc55-0242ac130003"))
  }

  @Test(expected = classOf[InvalidConversionException])
  def testValidateUUIDErrorsWithParse(): Unit = {
    validations.uuid("309809“")
  }

  @Test(expected = classOf[InvalidScalarException])
  def testValidateUUIDError(): Unit = {
    validations.uuid(BigInt(10))
  }

  @Test
  def testValidateASCII(): Unit = {
    val t = "abc"
    assertEquals(validations.ascii(t), t)
    assertEquals(validations.ascii(t.asInstanceOf[Object]), t)
  }

  @Test(expected = classOf[InvalidACIIScalarException])
  def testValidateASCIIErrors(): Unit = {
    validations.ascii("ßŒ")
  }

  @Test
  def testValidateText(): Unit = {
    val t = "abc"
    assertEquals(validations.text(t), t)
    assertEquals(validations.text(t.asInstanceOf[Object]), t)
  }

  @Test
  def testValidateBlob(): Unit = {
    val actual = ByteBuffer.wrap("hello".getBytes)
    val expected = ByteBuffer.wrap("hello".getBytes)
    assertEquals(validations.blob(actual), expected)
  }

 @Test(expected = classOf[InvalidScalarException])
  def testValidateBlobErrors() : Unit = {
   validations.blob("fjk")
 }

}

