package stargate.model.queries

import stargate.model._
import stargate.schema.{ENTITY_ID_COLUMN_NAME, GroupedConditions}
import stargate.{keywords, schema}

object parser {

  def checkUnusedFields(unusedKeys: Set[String], allowedKeywords: Set[String], allowedFields: Set[String]) = {
    val (unusedKeywords, unusedFields) = unusedKeys.partition(_.startsWith(keywords.KEYWORD_PREFIX))
    assert(unusedKeywords.isEmpty, s"Found keywords: ${unusedKeywords}, only the following are allowed in current context: ${allowedKeywords}")
    assert(unusedFields.isEmpty, s"Found unused fields: ${unusedFields}, only the following are allowed in current context: ${allowedFields}")
  }

  // no return, since no transformation is performed
  def parseGetSelection(entities: Entities, entityName: String, payload: Map[String,Object]): GetSelection = {
    val entity = entities(entityName)
    val allowedKeywords = Set(keywords.query.INCLUDE, keywords.pagination.LIMIT, keywords.pagination.CONTINUE, keywords.pagination.TTL)
    val unusedKeys = payload.keySet.diff(entity.relations.keySet ++ allowedKeywords)
    checkUnusedFields(unusedKeys, allowedKeywords, entity.relations.keySet)
    val include = payload.get(keywords.query.INCLUDE).map(_.asInstanceOf[List[String]])
    include.foreach(_.foreach(field => assert(entity.fields.contains(field))))
    val relations = entity.relations.values.filter(r => payload.contains(r.name)).map(r => (r.name, parseGetSelection(entities, r.targetEntityName, payload(r.name).asInstanceOf[Map[String,Object]])))
    val limit: Option[Int] = payload.get(keywords.pagination.LIMIT).map(_.asInstanceOf[Integer])
    val continue: java.lang.Boolean = payload.get(keywords.pagination.CONTINUE).map(_.asInstanceOf[java.lang.Boolean]).getOrElse(java.lang.Boolean.FALSE)
    val ttl: Option[Int] = payload.get(keywords.pagination.TTL).map(_.asInstanceOf[Integer])
    GetSelection(relations.toMap, include, limit, continue, ttl)
  }
  def parseGet(entities: Entities, entityName: String, payload: Map[String,Object]): GetQuery = {
    val conditions = parseConditions(entities, entityName, payload(keywords.mutation.MATCH))
    val selection = parseGetSelection(entities, entityName, payload.removed(keywords.mutation.MATCH))
    GetQuery(conditions, selection)
  }

  def parseEntity(entities: Entities, entityName: String, payload: Map[String, Object], allowedKeywords: Set[String]): (Map[String,Object], Map[String, RelationMutation]) = {
    assert(!payload.contains(ENTITY_ID_COLUMN_NAME), s"mutation may not specify field: ${ENTITY_ID_COLUMN_NAME}")
    val entity = entities(entityName)
    val allowedFields = entity.fields.keySet ++ entity.relations.keySet
    val unusedKeys = payload.keySet.diff(allowedFields ++ allowedKeywords)
    checkUnusedFields(unusedKeys, allowedKeywords, allowedFields)
    val updatedScalars = entity.fields.values.filter(f => payload.contains(f.name)).map(field => {
      (field.name, field.scalarType.convert(payload(field.name)))
    })
    val updatedRelations = entity.relations.values.filter(r => payload.contains(r.name)).map(relation => {
      (relation.name, parseLinkMutation(entities, relation.targetEntityName, payload(relation.name).asInstanceOf[Map[String,Object]]))
    })
    (updatedScalars.toMap, updatedRelations.toMap)
  }

  def parseCreateOne(entities: Entities, entityName: String, payload: Map[String, Object]): CreateOneMutation = {
    val entity = entities(entityName)
    val wrappedRelationsPayload: Map[String,Object] = payload.map(kv => {
      val wrapped = if(entity.relations.contains(kv._1)) { Map((keywords.relation.LINK, kv._2)) } else { kv._2 }
      (kv._1, wrapped)
    })
    val (fields, relations) = parseEntity(entities, entityName, wrappedRelationsPayload, Set.empty)
    CreateOneMutation(fields, relations.view.mapValues(v => v.asInstanceOf[LinkMutation].mutation).toMap)
  }

  def parseCreate(entities: Entities, entityName: String, payload: Object): CreateMutation = {
    val createOnes = if (payload.isInstanceOf[List[Map[String, Object]]]) {
      payload.asInstanceOf[List[Map[String, Object]]].map(parseCreateOne(entities, entityName, _))
    } else if (payload.isInstanceOf[Map[String, Object]]) {
      List(parseCreateOne(entities, entityName, payload.asInstanceOf[Map[String, Object]]))
    } else {
      throw new RuntimeException(s"create request must be either a Map or List[Map], instead got ${payload.getClass}")
    }
    CreateMutation(createOnes)
  }

  def parseConditions[T](validateArgument: (ScalarField, Object) => T, entities: Entities, entityName: String, payload: Object): GroupedConditions[T] = {
    def parseCondition(field_op_val: List[Object]): ScalarCondition[Object] = {
      val field :: comparison :: value :: _ = field_op_val
      ScalarCondition(field.asInstanceOf[String], ScalarComparison.fromString(comparison.toString), value)
    }
    if(payload == keywords.query.MATCH_ALL) {
      Map((List.empty, List.empty))
    } else if(payload.isInstanceOf[List[Object]]) {
      val payloadList = payload.asInstanceOf[List[Object]]
      assert(payloadList.nonEmpty)
      val conditions = payloadList.grouped(3).map(parseCondition).toList
      val groupedConditions = schema.groupConditionsByPath(conditions)
      groupedConditions.map(path_conds => {
        val (path, conditions) = path_conds
        val targetEntityName = schema.traverseEntityPath(entities, entityName, path)
        val targetEntity = entities(targetEntityName)
        val validatedConditions = conditions.map(condition => condition.replaceArgument(validateArgument(targetEntity.fields(condition.field), condition.argument)))
        (path, validatedConditions)
      })
    } else {
      throw new RuntimeException(
        s"""conditions must be either a non-empty List[Object] of (field, comparison, argument) triples,
        | or the string "${keywords.query.MATCH_ALL}", instead got ${payload.getClass}""".stripMargin)
    }
  }
  def parseConditions(entities: Entities, entityName: String, payload: Object): GroupedConditions[Object] = {
    parseConditions((field, arg) => field.scalarType.convert(arg), entities, entityName, payload)
  }
  def parseNamedConditions(entities: Entities, entityName: String, payload: Object): GroupedConditions[(ScalarType.Value, String)] = {
    parseConditions((field, arg) => (field.scalarType, arg.asInstanceOf[String]), entities, entityName, payload)
  }

  def parseUpdate(entities: Entities, entityName: String, payload: Map[String, Object]): UpdateMutation = {
    val conditions = parseConditions(entities, entityName, payload(keywords.mutation.MATCH))
    val (fields, relations) = parseEntity(entities, entityName, payload.removed(keywords.mutation.MATCH), Set.empty)
    UpdateMutation(conditions, fields, relations)
  }

  def parseMutation(entities: Entities, entityName: String, payload: Object): Mutation = {
    if (payload.isInstanceOf[List[Object]]) {
      // assume create if payload is list
      parseCreate(entities, entityName, payload)
    } else if (payload.isInstanceOf[Map[String, Object]]) {
      val payloadMap = payload.asInstanceOf[Map[String, Object]]
      if (payloadMap.keySet == Set(keywords.mutation.CREATE)) {
        parseCreate(entities, entityName, payloadMap(keywords.mutation.CREATE))
      } else if (payloadMap.keySet == Set(keywords.mutation.MATCH)) {
        MatchMutation(parseConditions(entities, entityName, payloadMap(keywords.mutation.MATCH)))
      } else if (payloadMap.keySet == Set(keywords.mutation.UPDATE)) {
        parseUpdate(entities, entityName, payloadMap(keywords.mutation.UPDATE).asInstanceOf[Map[String,Object]])
      } else {
        // default to create
        parseCreate(entities, entityName, payload)
      }
    } else {
      throw new RuntimeException(
        s"mutation request must be either a Map or List[Map], instead got ${payload.getClass}")
    }
  }

  def parseLinkMutation(entities: Entities, entityName: String, payload: Map[String,Object]): RelationMutation = {
    if (payload.isInstanceOf[List[Object]]) {
      // if list, assume replace-create
      ReplaceMutation(parseMutation(entities, entityName, payload))
    } else if (payload.isInstanceOf[Map[String, Object]]) {
      val payloadMap = payload.asInstanceOf[Map[String, Object]]
      if(payload.keySet == Set(keywords.relation.LINK)) {
        LinkMutation(parseMutation(entities, entityName, payloadMap(keywords.relation.LINK)))
      } else if(payload.keySet == Set(keywords.relation.UNLINK)) {
        UnlinkMutation(parseConditions(entities, entityName, payloadMap(keywords.relation.UNLINK)))
      } else if(payload.keySet == Set(keywords.relation.REPLACE)) {
        ReplaceMutation(parseMutation(entities, entityName, payloadMap(keywords.relation.REPLACE)))
      } else {
        // assume replace if not specified
        ReplaceMutation(parseMutation(entities, entityName, payloadMap))
      }
    } else {
      throw new RuntimeException(
        s"nested mutation request must be either a Map or List[Map], instead got ${payload.getClass}")
    }
  }

  def parseDeleteSelection(entities: Entities, entityName: String, payload: Map[String,Object]): DeleteSelection = {
    val entity = entities(entityName)
    val unusedKeys = payload.keySet.diff(entity.relations.keySet)
    checkUnusedFields(unusedKeys, Set.empty, entity.relations.keySet)
    val relations = entity.relations.values.filter(r => payload.contains(r.name)).map(r => {
      (r.name, parseDeleteSelection(entities, r.targetEntityName, payload(r.name).asInstanceOf[Map[String,Object]]))
    })
    DeleteSelection(relations.toMap)
  }
  def parseDelete(entities: Entities, entityName: String, payload: Map[String,Object]): DeleteQuery = {
    val conditions = parseConditions(entities, entityName, payload(keywords.mutation.MATCH))
    val selection = parseDeleteSelection(entities, entityName, payload.removed(keywords.mutation.MATCH))
    DeleteQuery(conditions, selection)
  }

}
