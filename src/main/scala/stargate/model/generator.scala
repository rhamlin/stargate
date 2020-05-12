/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package stargate.model

import java.util.UUID

import com.datastax.oss.driver.api.core.CqlSession
import stargate.model.queries.{GetQuery, GetSelection}

import scala.util.Random
import stargate.schema.{RELATION_JOIN_STRING, RELATION_SPLIT_REGEX}
import stargate.util.AsyncList
import stargate.{keywords, util}

import scala.concurrent.{ExecutionContext, Future}

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

  def createEntities(model: Entities, visitedEntities: Set[String], entityName: String, remaining: Int, maxBranching: Int, allowInverse: Boolean = false): (Int, Map[String, Object]) = {
    val entity = model(entityName)
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

  def createEntity(model: Entities, entityName: String, remaining: Int = 50, maxBranching: Int = 5, allowInverse: Boolean = false): Map[String, Object] = {
    createEntities(model, Set(entityName), entityName, remaining, maxBranching, allowInverse)._2
  }


  def resolveEntityValue(entity: Map[String, Object], path: List[String], executor: ExecutionContext): Future[Option[Object]] = {
    val (head :: _, tail) = path.splitAt(1)
    if(tail.isEmpty) {
      Future.successful(entity.get(head))
    } else {
      val children = entity.get(head).map(_.asInstanceOf[AsyncList[Map[String,Object]]]).getOrElse(AsyncList.empty)
      children.take(1, executor).flatMap(child => {
        if(child.isEmpty) {
          Future.successful(None)
        } else {
          resolveEntityValue(child.head, tail, executor)
        }
      })(executor)
    }
  }

  def randomMatchCondition(model: Entities, entityName: String, maxTerms: Int): List[ScalarCondition[Object]] = {
    val entity = model(entityName)
    val relatedEntities = entity.relations.view.mapValues(r => model(r.targetEntityName))
    val scalars = entity.fields.values ++ relatedEntities.toList.flatMap(re => re._2.fields.values.map(f => f.rename(re._1 + RELATION_JOIN_STRING + f.name)))
    val shuffled: List[ScalarField] = Random.shuffle(scalars.toList)
    shuffled.take(Random.between(1, maxTerms + 1)).map(f => ScalarCondition(f.name, ScalarComparison.EQ, randomScalar(f)))
  }
  def specificMatchCondition(model: OutputModel, entityName: String, maxTerms: Int, session: CqlSession, executor: ExecutionContext): Future[List[ScalarCondition[Object]]] = {
    val entity = model.input.entities(entityName)
    val query = GetQuery(Map.empty, GetSelection(entity.relations.view.mapValues(_ => GetSelection.empty).toMap, None, None, continue = false, None))
    val entities = stargate.query.get(model, entityName, query, session, executor)
    val random = randomMatchCondition(model.input.entities, entityName, maxTerms)
    entities.take(1, executor).flatMap(entities => {
      if(entities.isEmpty) {
        Future.successful(List.empty)
      } else {
        val entity = entities.head
        val specific = random.map(cond => {
          val path = cond.field.split(RELATION_SPLIT_REGEX).toList
          resolveEntityValue(entity, path, executor).map(_.map(cond.replaceArgument))(executor)
        })
        util.sequence(specific, executor).map(conds => {
          val definedConds = conds.filter(_.isDefined)
          val maybeConds = Option.when(definedConds.nonEmpty)(definedConds.map(_.get))
          maybeConds.getOrElse(List.empty)
        })(executor)
      }
    })(executor)
  }
  def untypedCondition(conds: List[ScalarCondition[Object]]): List[Object] = {
    conds.flatMap(cond => List(cond.field, cond.comparison.toString, cond.argument))
  }


  def getSelection(model: Entities, visitedEntities: Set[String], entityName: String, limit: Int): Map[String,Object] = {
    val entity = model(entityName)
    val nextVisited = visitedEntities + entityName
    val children = entity.relations.view.filterKeys(!nextVisited(_)).mapValues(r => getSelection(model, nextVisited, r.targetEntityName, limit)).toMap[String,Object]
    val flags = Map[String,Object]((keywords.pagination.LIMIT, limit), (keywords.pagination.CONTINUE, false))
    flags ++ children
  }
  def randomGetRequest(model: Entities, visitedEntities: Set[String], entityName: String, limit: Int): Map[String, Object] = {
    val condition = randomMatchCondition(model, entityName, 4)
    val selection = getSelection(model, Set.empty, entityName, limit)
    selection.updated(keywords.mutation.MATCH, untypedCondition(condition))
  }
}