package appstax.model

import appstax.keywords
import appstax.model.queries.{CreateMutation, CreateQuery, DeleteQuery, DeleteSelection, GetQuery, GetSelection, LinkMutation, Mutation, Query, RelationMutation, ReplaceMutation, UnlinkMutation, UpdateMutation, UpdateQuery}
import com.typesafe.config.{Config, ConfigFactory, ConfigList}

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.Try

package object parser {



  def parseModel(text: String): InputModel = {
    parseModel(ConfigFactory.parseString(text))
  }

  def parseModel(config: Config): InputModel = {
    val entities = parseEntities(config.getConfig(keywords.config.ENTITIES))
    val queries = parseQueries(config.getConfig(keywords.config.QUERIES))
    val conditions = parseQueryConditions(config.getConfig(keywords.config.QUERY_CONDITIONS))
    InputModel(entities, queries, conditions)
  }

  def parseEntities(config: Config): Map[String, Entity] = {
    config.root.keySet.asScala.map(entityName => parseEntity(entityName, config.getConfig(entityName))).map(x => (x.name, x)).toMap
  }

  def parseEntity(name: String, config: Config): (Entity) = {
    val fields = parseEntityFields(config.getConfig(keywords.config.entity.FIELDS))
    val relations = parseEntityRelations(config.getConfig(keywords.config.entity.RELATIONS))
    val fieldsWithEntityIds = fields.updated(appstax.schema.ENTITY_ID_COLUMN_NAME, ScalarField(appstax.schema.ENTITY_ID_COLUMN_NAME, ScalarType.UUID))
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



  def parseQueries(config: Config): Map[String, appstax.model.queries.GetQuery] = {
    config.root.keySet.asScala.flatMap(entityName => {
      val entityConfig = config.getConfig(entityName)
      entityConfig.root.keySet.asScala.map(queryName => {
        val queryConfig = entityConfig.getConfig(queryName)
        (queryName, parseGetQuery(queryName, entityName, queryConfig))
      })
    }).toMap
  }

  def parseGetQuery(name: String, entityName: String, config: Config): GetQuery = {
    GetQuery(
      queryName = name,
      entityName = entityName,
      `match` = parseWhereClause(config.getStringList(keywords.mutation.MATCH).asScala.toList),
      selection = parseGetSelection(config)
    )
  }

  def parseWhereClause(config: List[String]): Where = {
    def parseCondition(cond: List[String]): ScalarCondition[String] = {
      val field :: op :: argumentName :: _ = cond
      ScalarCondition(field, ScalarComparison.fromString(op), argumentName)
    }
    config.grouped(3).map(parseCondition).toList
  }

  def getRelationsConf(config: Config): Config = {
    val removeKeywords = config.root.keySet.asScala.filter(_.startsWith(keywords.KEYWORD_PREFIX))
    removeKeywords.foldLeft(config)((config, keyword) => config.withoutPath(keyword))
  }

  def parseGetSelection(config: Config): GetSelection = {
    val include = config.getStringList(keywords.config.query.INCLUDE).asScala.toList
    val relationsConf = getRelationsConf(config)
    val relations = relationsConf.root.keySet.asScala.map(name => (name, parseGetSelection(relationsConf.getConfig(name)))).toMap
    GetSelection(include, relations)
  }
}
