package stargate.model

import stargate.schema.GroupedConditions
import stargate.{keywords, schema}

object queries {

  case class GetQuery(`match`: schema.GroupedConditions[Object], selection: GetSelection)
  case class GetSelection(relations: Map[String, GetSelection], include: Option[List[String]], limit: Option[Int], continue: Boolean, ttl: Option[Int])

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
    case class GetQuery(queryName: String, entityName: String, `match`: Where, selection: GetSelection)


    def transform(query: GetQuery, payload: Map[String,Object]): Map[String,Object] = {
      val matchArguments = payload(keywords.mutation.MATCH).asInstanceOf[Map[String,Object]]
      val substitutions = query.`match`.map(cond => cond.replaceArgument(matchArguments(cond.argument)))
      val matchPayload = Map((keywords.mutation.MATCH, substitutions.flatMap(cond => List(cond.field, cond.comparison, cond.argument))))
      val selectionPayload = transform(query.selection)
      matchPayload ++ selectionPayload
    }
    def transform(selection: GetSelection): Map[String,Object] = {
      val fields = Map((keywords.query.INCLUDE, selection.include))
      val relations = selection.relations.map((name_selection:(String, GetSelection)) => (name_selection._1, transform(name_selection._2)))
      fields ++ relations
    }

  }
}
