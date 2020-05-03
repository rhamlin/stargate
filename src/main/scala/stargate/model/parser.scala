package stargate.model

import com.typesafe.config.{Config, ConfigFactory, ConfigList, ConfigObject}
import stargate.keywords
import stargate.model.queries.predefined.GetQuery
import stargate.schema

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.{Success, Try}

object parser {

  def validIdentifier(id: String): Boolean = id.matches("""[\w]+""")

  def parseModel(text: String): InputModel = {
    parseModel(ConfigFactory.parseString(text))
  }

  def parseModel(config: Config): InputModel = {
    val entitiesConf = Try(config.getConfig(keywords.config.ENTITIES))
    require(entitiesConf.isSuccess, s"""data model config must contain "${keywords.config.ENTITIES}" object at root level of config""")
    val entities = parseEntities(entitiesConf.get)
    checkEntityRelations(entities)
    val queries = parseQueries(Try(config.getConfig(keywords.config.QUERIES)).getOrElse(ConfigFactory.empty), entities)
    val conditions = parseQueryConditions(entities, Try(config.getConfig(keywords.config.QUERY_CONDITIONS)).getOrElse(ConfigFactory.empty))
    InputModel(entities, queries, conditions)
  }

  def parseEntities(config: Config): Map[String, Entity] = {
    config.root.keySet.asScala.map(entityName => {
      require(validIdentifier(entityName), s"""Entity name "${entityName}" is in valid, only alphanumeric and underscores allowed""")
      parseEntity(entityName, config.getConfig(entityName))
    }).map(x => (x.name, x)).toMap
  }

  def parseEntity(name: String, config: Config): Entity = {
    val allowed = Set(keywords.config.entity.FIELDS, keywords.config.entity.RELATIONS)
    val unused = config.root.keySet.asScala.diff(allowed)
    require(unused.isEmpty, s"""Found unused configurations ${unused} in entity "${name}", only allowed keys are ${allowed}""")

    val fieldsConf = if(config.hasPath(keywords.config.entity.FIELDS)) Try(config.getConfig(keywords.config.entity.FIELDS)) else Success(ConfigFactory.empty)
    require(fieldsConf.isSuccess, s"""|"${keywords.config.entity.FIELDS}" config in entity "${name}": ${fieldsConf.failed.get}""".stripMargin)
    val fields = parseEntityFields(name, config.getConfig(keywords.config.entity.FIELDS))

    val relationsConf = if(config.hasPath(keywords.config.entity.RELATIONS)) Try(config.getConfig(keywords.config.entity.RELATIONS)) else Success(ConfigFactory.empty)
    require(relationsConf.isSuccess, s"""|"${keywords.config.entity.RELATIONS}" config in entity "${name}": ${relationsConf.failed.get}""".stripMargin)
    val relations = parseEntityRelations(name, config.getConfig(keywords.config.entity.RELATIONS))

    fields.get(schema.ENTITY_ID_COLUMN_NAME).foreach(f => require(f.scalarType == ScalarType.UUID, s"""field ${schema.ENTITY_ID_COLUMN_NAME} in entity "${name}" must be type ${ScalarType.UUID.name}"""))
    val fieldsWithEntityIds = fields.updated(schema.ENTITY_ID_COLUMN_NAME, ScalarField(schema.ENTITY_ID_COLUMN_NAME, ScalarType.UUID))
    Entity(name, fieldsWithEntityIds, relations)
  }

  def parseEntityFields(entityName: String, config: Config): Map[String, ScalarField] = {
    val fields = config.root.unwrapped.asScala.toMap
    fields.toList.map(name_type => {
      val (name, typeString) = name_type
      require(validIdentifier(name), s"""Invalid field name "${name}" in entity "${entityName}", only alphanumeric and underscores allowed""")

      val scalarType = Try(ScalarType.fromString(typeString.asInstanceOf[String]))
      require(scalarType.isSuccess, s"""Invalid type "${typeString}" for field "${name}" in entity "${entityName}", must be one of ${ScalarType.names.keySet}""")

      ScalarField(name, scalarType.get)
    }).map(x => (x.name, x)).toMap
  }

  def parseEntityRelations(entityName: String, config: Config): Map[String, RelationField] = {
    config.root.keySet.asScala.map(relationName => {
      require(validIdentifier(relationName), s"""Invalid relation name "${relationName}" in entity "${entityName}", only alphanumeric and underscores allowed""")
      val relationConf = Try(config.getConfig(relationName))
      def relationString(key: String) = {
        val relationVal = relationConf.map(_.getString(key))
        require(relationVal.isSuccess, s"""Relation "${relationName}" in entity "${entityName}" must have config "${key}" """)
        relationVal.get
      }
      val relationType = relationString(keywords.config.entity.RELATION_TYPE)
      val relationInverse = relationString(keywords.config.entity.RELATION_INVERSE)
      (relationName, RelationField(relationName, relationType, relationInverse))
    }).toMap
  }

  def checkEntityRelations(entities: Entities): Unit = {
    entities.values.foreach(entity => {
      entity.relations.values.foreach(relation => {
        require(entities.contains(relation.targetEntityName), s"""Relation "${relation.name}" on entity "${entity.name}" refers to non-existent entity "${relation.targetEntityName}" """)
        val inverse = Try(entities(relation.targetEntityName).relations(relation.inverseName))
        val matches = inverse.map(inv => relation.inverseName == inv.name && inv.targetEntityName == entity.name && inv.inverseName == relation.name)
        require(matches.isSuccess && matches.get,  s"""Relation "${relation.name}" on entity "${entity.name}" does not have valid inverse""")
      })
    })
  }

  def parseQueryConditions(entities: Entities, config: Config): Map[String, List[NamedConditions]] = {
    config.root.keySet.asScala.map(entityName => {
      require(entities.contains(entityName), s"""query conditions for entity "${entityName}" does not refer to a valid entity""")
      val conditionsList = Try(config.getList(entityName))
      require(conditionsList.isSuccess, s"""query conditions for entity "${entityName}" must be a list""")
      (entityName, parseEntityConditions(entities, entityName, conditionsList.get))
    }).toMap
  }
  def parseEntityConditions(entities: Entities, entityName: String, conditionsList: ConfigList): List[NamedConditions] = {
    conditionsList.iterator.asScala.map(value => {
      val unwrappedConditions = Try(value.asInstanceOf[ConfigList].unwrapped.asScala.toList)
      val conditionStrings = unwrappedConditions.map(_.map(_.asInstanceOf[String]))
      require(conditionStrings.isSuccess, s"""conditions ${value} on entity "${entityName}" must be a list of strings""")
      val namedPairs = conditionStrings.get.grouped(2).toList
      val parsedPairs = Try(namedPairs.map(pair => NamedCondition(pair(0), ScalarComparison.fromString(pair(1)))))
      require(parsedPairs.isSuccess, s"""conditions ${unwrappedConditions.get} on entity "${entityName}" must be a list of (field, comparison) pairs, where comparison is one of ${ScalarComparison.names.keySet}""")

      // ensure that every condition is on a valid scalar or related scalar
      val grouped = schema.groupNamedConditionsByPath(parsedPairs.get)
      grouped.foreach(path_conds => {
        val (path, conditions) = path_conds
        val targetEntityName = Try(schema.traverseEntityPath(entities, entityName, path))
        require(targetEntityName.isSuccess, s"""In query condition ${unwrappedConditions.get} on entity "${entityName}", "${path.mkString(".")}" does not represent a valid relation path""")
        val targetEntity = entities(targetEntityName.get)
        conditions.foreach(cond => {
          require(targetEntity.fields.contains(cond.field), s"""In query condition ${unwrappedConditions.get} on entity "${entityName}", entity "${targetEntityName.get}" does not have field "${cond.field}" """)
        })
      })
      parsedPairs.get
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
      `match` = queries.parser.parseNamedConditions(entities, List.empty, entityName, stargate.util.javaToScala(config.getValue(keywords.mutation.MATCH).unwrapped)),
      selection = queries.parser.parseGetSelection(entities, List.empty, entityName, stargate.util.javaToScala(config.root().unwrapped()).asInstanceOf[Map[String,Object]].removed(keywords.mutation.MATCH))
    )
  }
}
