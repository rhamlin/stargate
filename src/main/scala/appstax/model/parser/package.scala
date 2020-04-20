package appstax.model

import com.datastax.oss.driver.api.core.`type`.DataTypes
import com.typesafe.config.{Config, ConfigFactory, ConfigList, ConfigObject, ConfigValue, ConfigValueFactory}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

package object parser {

  def parseModel(text: String): InputModel = {
    parseModel(ConfigFactory.parseString(text))
  }

  def parseModel(config: Config): InputModel = {
    val entities = parseEntities(config.getConfig("entities"))
    val conditions = parseQueries(config.getConfig("queries"))
    InputModel(entities, conditions)
  }

  def parseEntities(config: Config): Map[String, Entity] = {
    config.root.keySet.asScala.map(entityName => parseEntity(entityName, config.getConfig(entityName))).map(x => (x.name, x)).toMap
  }

  def parseEntity(name: String, config: Config): (Entity) = {
    val fields = parseEntityFields(config.getConfig("fields"))
    val relations = parseEntityRelations(config.getConfig("relations"))
    val fieldsWithEntityIds = fields.updated(appstax.schema.ENTITY_ID_COLUMN_NAME, ScalarField(appstax.schema.ENTITY_ID_COLUMN_NAME, ScalarType.UUID))
    Entity(name, fieldsWithEntityIds, relations)
  }

  def parseEntityFields(configObject: Config): Map[String, ScalarField] = {
    val fields = configObject.root.unwrapped.asScala.asInstanceOf[mutable.Map[String,String]]
    fields.toList.map(name_type => ScalarField(name_type._1, ScalarType.fromString(name_type._2))).map(x => (x.name, x)).toMap
  }

  def parseEntityRelations(config: Config): Map[String, RelationField] = {
    val relations = config.root.unwrapped.asInstanceOf[java.util.Map[String,java.util.Map[String,String]]].asScala
    relations.iterator.map(x => RelationField(x._1, x._2.get("type"), x._2.get("inverse"))).map(x => (x.name, x)).toMap
  }

  def parseQueries(config: Config): Map[String, List[NamedConditions]] = {
    config.root.keySet.asScala.map(entityName => {
      (entityName, parseEntityConditions(config.getList(entityName)))
    }).toMap
  }
  def parseEntityConditions(conditionsList: ConfigList): List[NamedConditions] = {
    conditionsList.iterator.asScala.iterator.map(value => {
      val conditions = value.asInstanceOf[ConfigList].unwrapped.asScala.toList.asInstanceOf[List[String]]
      val namedPairs = conditions.grouped(2).toList
      namedPairs.map(pair => NamedCondition(pair(0), ScalarComparison.fromString(pair(1))))
    }).toList
  }
}
