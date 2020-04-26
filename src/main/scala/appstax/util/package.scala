package appstax

import java.util
import java.util.concurrent.TimeUnit

import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}

import scala.collection.JavaConverters.mapAsJavaMap
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._
import scala.util.Try

package object util {

  val objectMapper = new ObjectMapper
  val indentedObjectMapper = {
    val om = new ObjectMapper
    om.configure(SerializationFeature.INDENT_OUTPUT, true)
    om
  }
  def fromJson(s: String): Object = javaToScala(objectMapper.readValue(s, classOf[Object]))
  def toJson(o: Object): String = objectMapper.writeValueAsString(scalaToJava(o))
  def toPrettyJson(o: Object): String = indentedObjectMapper.writeValueAsString(scalaToJava(o))

  def enumerationNames(enum: Enumeration): Map[String, enum.Value] = enum.values.iterator.map(v => (v.toString,v)).toMap

  def javaToScala(x: Object): Object = {
    x match {
      case x: java.util.List[Object] =>  x.asScala.map(javaToScala).toList
      case x: java.util.Map[Object, Object] => x.asScala.map((kv:(Object,Object)) => (javaToScala(kv._1), javaToScala(kv._2))).toMap
      case x => x
    }
  }
  def scalaToJava(x: Object): Object = {
    x match {
      case x: List[Object] =>  x.map(scalaToJava).asJava
      case x: Map[Object, Object] => mapAsJavaMap(x.map((kv:(Object,Object)) => (scalaToJava(kv._1), scalaToJava(kv._2))))
      case x => x
    }
  }

  def retry[T](value: => T, remaining: Duration, backoff: Duration): Try[T] = {
    val t0 = System.nanoTime()
    val result = Try(value)
    val t1 = System.nanoTime()
    val nextRemaining = remaining - Duration(t1 - t0, TimeUnit.NANOSECONDS) - backoff
    if(result.isSuccess || nextRemaining._1 < 0) {
      result
    } else {
      Thread.sleep(backoff.toMillis)
      retry(value, nextRemaining, backoff)
    }
  }

}
