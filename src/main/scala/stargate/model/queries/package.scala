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

import stargate.schema.GroupedConditions
import stargate.{keywords, schema}

package object queries {

  case class GetQuery(`match`: schema.GroupedConditions[Object], selection: GetSelection)
  case class GetSelection(relations: Map[String, GetSelection], include: Option[List[String]], limit: Option[Int], continue: Boolean, ttl: Option[Int])
  object GetSelection {
    val empty = GetSelection(Map.empty, None, None, continue = false, None)
  }

  case class DeleteQuery(`match`: schema.GroupedConditions[Object], selection: DeleteSelection)
  case class DeleteSelection(relations: Map[String, DeleteSelection])

  sealed trait Mutation
  case class CreateOneMutation(fields: Map[String,Object], relations: Map[String, Mutation])
  case class CreateMutation(creates: List[CreateOneMutation]) extends Mutation
  case class MatchMutation(`match`: GroupedConditions[Object]) extends Mutation
  case class UpdateMutation(`match`: GroupedConditions[Object], fields: Map[String,Object], relations: Map[String, RelationMutation]) extends Mutation
  sealed trait RelationMutation
  case class LinkMutation(mutation: Mutation) extends RelationMutation
  case class UnlinkMutation(`match`: GroupedConditions[Object]) extends RelationMutation
  case class ReplaceMutation(mutation: Mutation) extends RelationMutation



  object predefined {
    case class GetQuery(queryName: String, entityName: String, `match`: GroupedConditions[(ScalarType.Value, String)], selection: GetSelection)

    def transform(query: GetQuery, payload: Map[String,Object]): queries.GetQuery = {
      val matchArguments = payload(keywords.mutation.MATCH).asInstanceOf[Map[String,Object]]
      val substitutions = query.`match`.view.mapValues(_.map(cond => cond.replaceArgument(cond.argument._1.convert(matchArguments(cond.argument._2))))).toMap
      val selectionPayload = transform(query.selection, payload)
      queries.GetQuery(substitutions, selectionPayload)
    }
    def transform(selection: GetSelection, payload: Map[String,Object]): GetSelection = {
      val relations = selection.relations.map((name_selection:(String, GetSelection)) =>
        (name_selection._1, transform(name_selection._2, payload.get(name_selection._1).map(_.asInstanceOf[Map[String,Object]]).getOrElse(Map.empty))))
      val limit: Option[Int] = payload.get(keywords.pagination.LIMIT).map(_.asInstanceOf[Int]).orElse(selection.limit)
      val continue: Boolean = payload.get(keywords.pagination.CONTINUE).map(_.asInstanceOf[Boolean]).getOrElse(selection.continue)
      val ttl: Option[Int] = payload.get(keywords.pagination.TTL).map(_.asInstanceOf[Int]).orElse(selection.ttl)
      GetSelection(relations, selection.include, limit, continue, ttl)
    }

  }
}
