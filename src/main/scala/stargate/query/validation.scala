package stargate.query

import stargate.model.{InputModel, ScalarComparison, ScalarCondition}
import stargate.keywords
import stargate.schema
import stargate.schema.{ENTITY_ID_COLUMN_NAME, GroupedConditions}

object validation {

  def mapWrap[T](key: String, value: T) = Map((key, value))

  def validateEntity(allowedKeywords: Set[String], validateRelation: (String, Object) => Map[String,Object],
                         model: InputModel, entityName: String, payload: Map[String, Object]): Map[String, Object] = {
    assert(!payload.contains(ENTITY_ID_COLUMN_NAME), s"mutation may not specify field: ${ENTITY_ID_COLUMN_NAME}")
    val entity = model.entities(entityName)
    val unusedKeys = payload.keySet.diff(entity.fields.keySet ++ entity.relations.keySet ++ allowedKeywords)
    val (unusedKeywords, unusedFields) = unusedKeys.partition(_.startsWith(keywords.KEYWORD_PREFIX))
    assert(unusedKeywords.isEmpty, s"Found keywords: ${unusedKeywords}, but only the following are allowed in current context: ${allowedKeywords}")
    assert(unusedFields.isEmpty, s"Entity ${entityName} does not contain fields: ${unusedFields}")
    val updatedScalars = entity.fields.values.filter(f => payload.contains(f.name)).map(field => {
      (field.name, field.scalarType.convert(payload(field.name)))
    })
    val updatedRelations = entity.relations.values.filter(r => payload.contains(r.name)).map(relation => {
      (relation.name, validateRelation(relation.targetEntityName, payload(relation.name)))
    })
    (updatedScalars ++ updatedRelations).toMap
  }

  def validateCreateOne(model: InputModel, entityName: String, payload: Map[String, Object]): Map[String, Object] = {
    def validateRelation(targetEntityName: String, targetPayload: Object): Map[String,Object] = {
      mapWrap(keywords.relation.LINK, validateMutation(model, targetEntityName, targetPayload))
    }
    validateEntity(Set.empty, validateRelation, model, entityName, payload)
  }

  def validateCreate(model: InputModel, entityName: String, payload: Object): List[Map[String,Object]] = {
    if (payload.isInstanceOf[List[Map[String, Object]]]) {
      payload.asInstanceOf[List[Map[String, Object]]].map(validateCreateOne(model, entityName, _))
    } else if (payload.isInstanceOf[Map[String, Object]]) {
      List(validateCreateOne(model, entityName, payload.asInstanceOf[Map[String, Object]]))
    } else {
      throw new RuntimeException(s"create request must be either a Map or List[Map], instead got ${payload.getClass}")
    }
  }

  def validateConditions(model: InputModel, entityName: String, payload: Object): GroupedConditions[Object] = {
    def parseCondition(field_op_val: List[Object]): ScalarCondition[Object] = {
      val field :: comparison :: value :: _ = field_op_val
      ScalarCondition(field.asInstanceOf[String], ScalarComparison.fromString(comparison.toString), value)
    }
    if(payload == keywords.query.MATCH_ALL) {
      Map.empty
    } else if(payload.isInstanceOf[List[Object]]) {
      val payloadList = payload.asInstanceOf[List[Object]]
      val conditions = payloadList.grouped(3).map(parseCondition).toList
      val groupedConditions = schema.groupConditionsByPath(conditions)
      groupedConditions.map(path_conds => {
        val (path, conditions) = path_conds
        val targetEntityName = schema.traverseEntityPath(model, entityName, path)
        val targetEntity = model.entities(targetEntityName)
        val validatedConditions = conditions.map(condition => condition.replaceArgument(targetEntity.fields(condition.field).scalarType.convert(condition.argument)))
        (path, validatedConditions)
      })
    } else {
      throw new RuntimeException(
        s"""conditions must be either a non-empty List[Object] of (field, comparison, argument) triples,
        | or the string "${keywords.query.MATCH_ALL}", instead got ${payload.getClass}""".stripMargin)
    }
  }

  def validateUpdate(model: InputModel, entityName: String, payload: Map[String, Object]): Map[String, Object] = {
    val conditions = mapWrap(keywords.mutation.MATCH, validateConditions(model, entityName, payload(keywords.mutation.MATCH)))
    def validateRelation(targetEntityName: String, targetPayload: Object): Map[String,Object] = {
      validateLinkMutation(model, targetEntityName, targetPayload.asInstanceOf[Map[String,Object]])
    }
    conditions ++ validateEntity(Set(keywords.mutation.MATCH), validateRelation, model, entityName, payload)
  }

  def validateMutation(model: InputModel, entityName: String, payload: Object): Map[String,Object] = {
    if (payload.isInstanceOf[List[Object]]) {
      // assume create if payload is list
      mapWrap(keywords.mutation.CREATE, validateCreate(model, entityName, payload))
    } else if (payload.isInstanceOf[Map[String, Object]]) {
      val payloadMap = payload.asInstanceOf[Map[String, Object]]
      if (payloadMap.keySet == Set(keywords.mutation.CREATE)) {
        mapWrap(keywords.mutation.CREATE, validateCreate(model, entityName, payloadMap(keywords.mutation.CREATE)))
      } else if (payloadMap.keySet == Set(keywords.mutation.MATCH)) {
        mapWrap(keywords.mutation.MATCH, validateConditions(model, entityName, payloadMap(keywords.mutation.MATCH)))
      } else if (payloadMap.keySet == Set(keywords.mutation.UPDATE)) {
        mapWrap(keywords.mutation.UPDATE, validateUpdate(model, entityName, payloadMap(keywords.mutation.UPDATE).asInstanceOf[Map[String,Object]]))
      } else {
        // default to create
        mapWrap(keywords.mutation.CREATE, validateCreate(model, entityName, payload))
      }
    } else {
      throw new RuntimeException(
        s"mutation request must be either a Map or List[Map], instead got ${payload.getClass}")
    }
  }

  def validateLinkMutation(model: InputModel, entityName: String, payload: Map[String,Object]): Map[String,Object] = {
    if (payload.isInstanceOf[List[Object]]) {
      // if list, assume replace-create
      mapWrap(keywords.relation.REPLACE, validateMutation(model, entityName, payload))
    } else if (payload.isInstanceOf[Map[String, Object]]) {
      val payloadMap = payload.asInstanceOf[Map[String, Object]]
      if(payload.keySet == Set(keywords.relation.LINK)) {
        mapWrap(keywords.relation.LINK, validateMutation(model, entityName, payloadMap(keywords.relation.LINK)))
      } else if(payload.keySet == Set(keywords.relation.UNLINK)) {
        mapWrap(keywords.relation.UNLINK, validateConditions(model, entityName, payloadMap(keywords.relation.UNLINK)))
      } else if(payload.keySet == Set(keywords.relation.REPLACE)) {
        mapWrap(keywords.relation.REPLACE, validateMutation(model, entityName, payloadMap(keywords.relation.REPLACE)))
      } else {
        // assume replace if not specified
        mapWrap(keywords.relation.REPLACE, validateMutation(model, entityName, payloadMap))
      }
    } else {
      throw new RuntimeException(
        s"nested mutation request must be either a Map or List[Map], instead got ${payload.getClass}")
    }

  }

}
