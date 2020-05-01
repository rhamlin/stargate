package stargate.model

import com.typesafe.config.{Config, ConfigFactory, ConfigList}
import stargate.keywords
import stargate.model.queries.predefined.GetQuery

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.Try

object parser {

  def parseModel(text: String): InputModel = {
    parseModel(ConfigFactory.parseString(text))
  }

  def parseModel(config: Config): InputModel = {
    val entities = parseEntities(config.getConfig(keywords.config.ENTITIES))
    val queries = parseQueries(Try(config.getConfig(keywords.config.QUERIES)).getOrElse(ConfigFactory.empty), entities)
    val conditions = parseQueryConditions(Try(config.getConfig(keywords.config.QUERY_CONDITIONS)).getOrElse(ConfigFactory.empty))
    InputModel(entities, queries, conditions)
  }

  def parseEntities(config: Config): Map[String, Entity] = {
    config.root.keySet.asScala.map(entityName => parseEntity(entityName, config.getConfig(entityName))).map(x => (x.name, x)).toMap
  }

  def parseEntity(name: String, config: Config): (Entity) = {
    val fields = parseEntityFields(config.getConfig(keywords.config.entity.FIELDS))
    val relations = parseEntityRelations(config.getConfig(keywords.config.entity.RELATIONS))
    val fieldsWithEntityIds = fields.updated(stargate.schema.ENTITY_ID_COLUMN_NAME, ScalarField(stargate.schema.ENTITY_ID_COLUMN_NAME, ScalarType.UUID))
    Entity(name, fieldsWithEntityIds, relations)
  }

  def parseEntityFields(configObject: Config): Map[String, ScalarField] = {
    val fields = configObject.root.unwrapped.asScala.asInstanceOf[mutable.Map[String,String]]
    fields.toList.map(name_type => ScalarField(name_type._1, ScalarType.fromString(name_type._2))).map(x => (x.name, x)).toMap
  }

  def parseEntityRelations(config: Config): Map[String, RelationField] = {
    val relations = config.root.unwrapped.asInstanceOf[java.util.Map[String,java.util.Map[String,String]]].asScala
    relations.iterator.map(x => RelationField(x._1, x._2.get(keywords.config.entity.RELATION_TYPE), x._2.get(keywords.config.entity.RELATION_INVERSE))).map(x => (x.name, x)).toMap
  }

  def parseQueryConditions(config: Config): Map[String, List[NamedConditions]] = {
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



  def parseQueries(config: Config, entities: Entities): Map[String, GetQuery] = {
    config.root.keySet.asScala.flatMap(entityName => {
      val entityConfig = config.getConfig(entityName)
      entityConfig.root.keySet.asScala.map(queryName => {
        val queryConfig = entityConfig.getConfig(queryName)
        (queryName, parseGetQuery(queryName, entityName, queryConfig, entities))
      })
    }).toMap
  }

  def parseGetQuery(name: String, entityName: String, config: Config, entities: Entities): GetQuery = {
    GetQuery(
      queryName = name,
      entityName = entityName,
      `match` = queries.parser.validateNamedConditions(entities, entityName, stargate.util.javaToScala(config.getValue(keywords.mutation.MATCH).unwrapped)),
      selection = queries.parser.validateGetSelection(entities, entityName, stargate.util.javaToScala(config.root().unwrapped()).asInstanceOf[Map[String,Object]].removed(keywords.mutation.MATCH))
    )
  }
}
