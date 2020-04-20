package appstax

import java.util

import scala.collection.JavaConverters.mapAsJavaMap
import scala.jdk.CollectionConverters._

package object util {

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

}
