package stargate.model

import java.util.UUID

import scala.util.Random

object generator {


  def randomScalar(`type`: ScalarType.Value): Object = {
    `type` match {
      case ScalarType.INT => Random.nextInt.asInstanceOf[Object]
      case ScalarType.FLOAT => Random.nextFloat.asInstanceOf[Object]
      case ScalarType.STRING => Random.alphanumeric.take(10).mkString.asInstanceOf[Object]
      case ScalarType.UUID => UUID.randomUUID.asInstanceOf[Object]
    }
  }

  def randomScalar(field: ScalarField): Object = randomScalar(field.scalarType)


  def entityFields(fields: List[ScalarField]): Map[String, Object] = {
    fields.map(f => (f.name, randomScalar(f))).filter(_._1 != stargate.schema.ENTITY_ID_COLUMN_NAME).toMap
  }

  def createEntities(model: InputModel, visitedEntities: Set[String], entityName: String, remaining: Int, maxBranching: Int, allowInverse: Boolean = false): (Int, Map[String, Object]) = {
    val entity = model.entities(entityName)
    val scalars = entityFields(entity.fields.values.toList)
    val (nextRemaining, result) = entity.relations.values.foldLeft((remaining - 1, scalars))((remaining_results, relation) => {
      val (remaining, results) = remaining_results
      if (remaining <= 0 || (visitedEntities(relation.targetEntityName) && !allowInverse)) {
        (remaining, results)
      } else {
        val numChildren = Random.nextInt(Math.min(remaining, maxBranching))
        val (nextRemaining, children) = List.range(0, numChildren).foldLeft((remaining, List.empty[Map[String, Object]]))((remaining_children, _) => {
          val (remaining, children) = remaining_children
          if (remaining <= 0) {
            (remaining, children)
          } else {
            val (size, recurse) = createEntities(model, Set(relation.targetEntityName) ++ visitedEntities, relation.targetEntityName, remaining, maxBranching, allowInverse)
            (remaining - size, List(recurse) ++ children)
          }
        })
        (nextRemaining, results ++ Map((relation.name, children)))
      }
    })
    (remaining - nextRemaining, result)
  }

  def createEntity(model: InputModel, entityName: String, remaining: Int = 50, maxBranching: Int = 5, allowInverse: Boolean = false): Map[String, Object] = {
    createEntities(model, Set(entityName), entityName, remaining, maxBranching, allowInverse)._2
  }
}