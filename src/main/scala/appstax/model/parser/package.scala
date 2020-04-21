package appstax.model

import appstax.model.queries.{CreateMutation, CreateQuery, DeleteQuery, DeleteSelection, GetQuery, GetSelection, LinkMutation, Mutation, Query, RelationMutation, ReplaceMutation, UnlinkMutation, UpdateMutation, UpdateQuery}
import com.typesafe.config.{Config, ConfigFactory, ConfigList}

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.Try

package object parser {

  val ENTITIES_CONFIG = "entities"
  val QUERIES_CONFIG = "queries"
  val QUERY_CONDITIONS_CONFIG = "queryConditions"

  val ENTITY_FIELDS_CONFIG = "fields"
  val ENTITY_RELATIONS_CONFIG = "relations"
  val RELATION_TYPE_CONFIG = "type"
  val RELATION_INVERSE_CONFIG = "inverse"

  val QUERY_KEYWORD_PREFIX = "-"
  val QUERY_ENTITY_CONFIG = QUERY_KEYWORD_PREFIX + "entity"
  val QUERY_INCLUDE_CONFIG = QUERY_KEYWORD_PREFIX + "include"
  val QUERY_MATCH_CONFIG = QUERY_KEYWORD_PREFIX + "match"
  val QUERY_OP_CONFIG = QUERY_KEYWORD_PREFIX + "op"
  val OP_GET_CONFIG = "get"
  val OP_CREATE_CONFIG = "create"
  val OP_UPDATE_CONFIG = "update"
  val OP_DELETE_CONFIG = "delete"
  val MUTATION_OP_CONFIGS: Set[String] = Set(OP_CREATE_CONFIG, OP_UPDATE_CONFIG)
  val QUERY_RELATIONOP_CONFIG = QUERY_KEYWORD_PREFIX + "relation"
  val OP_LINK_CONFIG = "link"
  val OP_UNLINK_CONFIG = "unlink"
  val OP_REPLACE_CONFIG = "replace"
  val LINK_OP_CONFIGS: Set[String] = Set(OP_LINK_CONFIG, OP_UPDATE_CONFIG, OP_REPLACE_CONFIG)

  def parseModel(text: String): InputModel = {
    parseModel(ConfigFactory.parseString(text))
  }

  def parseModel(config: Config): InputModel = {
    val entities = parseEntities(config.getConfig(ENTITIES_CONFIG))
    val queries = parseQueries(config.getConfig(QUERIES_CONFIG))
    val conditions = parseQueryConditions(config.getConfig(QUERY_CONDITIONS_CONFIG))
    InputModel(entities, queries, conditions)
  }

  def parseEntities(config: Config): Map[String, Entity] = {
    config.root.keySet.asScala.map(entityName => parseEntity(entityName, config.getConfig(entityName))).map(x => (x.name, x)).toMap
  }

  def parseEntity(name: String, config: Config): (Entity) = {
    val fields = parseEntityFields(config.getConfig(ENTITY_FIELDS_CONFIG))
    val relations = parseEntityRelations(config.getConfig(ENTITY_RELATIONS_CONFIG))
    val fieldsWithEntityIds = fields.updated(appstax.schema.ENTITY_ID_COLUMN_NAME, ScalarField(appstax.schema.ENTITY_ID_COLUMN_NAME, ScalarType.UUID))
    Entity(name, fieldsWithEntityIds, relations)
  }

  def parseEntityFields(configObject: Config): Map[String, ScalarField] = {
    val fields = configObject.root.unwrapped.asScala.asInstanceOf[mutable.Map[String,String]]
    fields.toList.map(name_type => ScalarField(name_type._1, ScalarType.fromString(name_type._2))).map(x => (x.name, x)).toMap
  }

  def parseEntityRelations(config: Config): Map[String, RelationField] = {
    val relations = config.root.unwrapped.asInstanceOf[java.util.Map[String,java.util.Map[String,String]]].asScala
    relations.iterator.map(x => RelationField(x._1, x._2.get(RELATION_TYPE_CONFIG), x._2.get(RELATION_INVERSE_CONFIG))).map(x => (x.name, x)).toMap
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
    config.root.keySet.asScala.map(name => (name, parseGetQuery(name, config.getConfig(name)))).toMap
  }

  def parseGetQuery(name: String, config: Config): GetQuery = {
    GetQuery(
      queryName = name,
      entityName = config.getString(QUERY_ENTITY_CONFIG),
      `match` = parseWhereClause(config.getStringList(QUERY_MATCH_CONFIG).asScala.toList),
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
    val keywords = config.root.keySet.asScala.filter(_.startsWith(QUERY_KEYWORD_PREFIX))
    keywords.foldLeft(config)((config, keyword) => config.withoutPath(keyword))
  }

  def parseGetSelection(config: Config): GetSelection = {
    val include = config.getStringList(QUERY_INCLUDE_CONFIG).asScala.toList
    val relationsConf = getRelationsConf(config)
    val relations = relationsConf.root.keySet.asScala.map(name => (name, parseGetSelection(relationsConf.getConfig(name)))).toMap
    GetSelection(include, relations)
  }

  // currently not using pre-defined mutation parsing, but will probably update and use in near future
  def parseCreateQuery(name: String, config: Config): CreateQuery = {
    CreateQuery(
      queryName = name,
      entityName = config.getString(QUERY_ENTITY_CONFIG),
      create = parseCreateMutation(config)
    )
  }
  def parseUpdateQuery(name: String, config: Config): UpdateQuery = {
    UpdateQuery(
      queryName = name,
      entityName = config.getString(QUERY_ENTITY_CONFIG),
      update = parseUpdateMutation(config)
    )
  }

  def parseMutation(config: Config): Mutation = {
    val op = Try(config.getString(QUERY_OP_CONFIG)).getOrElse(OP_CREATE_CONFIG)
    op match {
      case OP_CREATE_CONFIG => parseCreateMutation(config)
      case OP_UPDATE_CONFIG => parseUpdateMutation(config)
    }
  }

  def parseLinkMutation(config: Config): RelationMutation = {
    val linkOp = Try(config.getString(QUERY_RELATIONOP_CONFIG)).getOrElse(OP_REPLACE_CONFIG)
    linkOp match {
      case OP_LINK_CONFIG => LinkMutation(parseMutation(config))
      case OP_UNLINK_CONFIG => UnlinkMutation(parseWhereClause(config.getStringList(QUERY_MATCH_CONFIG).asScala.toList))
      case OP_REPLACE_CONFIG => ReplaceMutation(parseMutation(config))
    }
  }

  def parseCreateMutation(config: Config): CreateMutation = {
    val include = config.getStringList(QUERY_INCLUDE_CONFIG).asScala.toList
    val relationsConf = getRelationsConf(config)
    val relations = relationsConf.root.keySet.asScala.map(name => (name, parseMutation(relationsConf.getConfig(name)))).toMap
    CreateMutation(include, relations)
  }

  def parseUpdateMutation(config: Config): UpdateMutation = {
    val where = parseWhereClause(config.getStringList(QUERY_MATCH_CONFIG).asScala.toList)
    val include = config.getStringList(QUERY_INCLUDE_CONFIG).asScala.toList
    val relationsConf = getRelationsConf(config)
    val relations = relationsConf.root.keySet.asScala.map(name => (name, parseLinkMutation(relationsConf.getConfig(name)))).toMap
    UpdateMutation(where, include, relations)
  }

  // TODO: a lot of duplication with parseGetQuery - refactor if possible
  def parseDeleteQuery(name: String, config: Config): DeleteQuery = {
    DeleteQuery(
      queryName = name,
      entityName = config.getString(QUERY_ENTITY_CONFIG),
      `match` = parseWhereClause(config.getStringList(QUERY_MATCH_CONFIG).asScala.toList),
      selection = parseDeleteSelection(config)
    )
  }

  def parseDeleteSelection(config: Config): DeleteSelection = {
    val relationsConf = getRelationsConf(config)
    val relations = relationsConf.root.keySet.asScala.map(name => (name, parseDeleteSelection(relationsConf.getConfig(name)))).toMap
    DeleteSelection(relations)
  }
}
