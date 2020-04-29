package stargate.model

import stargate.keywords

object queries {

  sealed trait Query
  case class GetQuery(queryName: String, entityName: String, `match`: Where, selection: GetSelection) extends Query
  case class GetSelection(include: List[String], relations: Map[String, GetSelection])

  // pre-defined mutations currently not being used, but will probably update and use in near future
  case class CreateQuery(queryName: String, entityName: String, create: CreateMutation) extends Query
  case class UpdateQuery(queryName: String, entityName: String, update: UpdateMutation) extends Query
  case class DeleteQuery(queryName: String, entityName: String, `match`: Where, selection: DeleteSelection) extends Query
  case class DeleteSelection(relations: Map[String, DeleteSelection])

  sealed trait Mutation
  case class CreateMutation(include: List[String], relations: Map[String, Mutation]) extends Mutation
  case class UpdateMutation(`match`: Where, include: List[String], relations: Map[String, RelationMutation]) extends Mutation
  sealed trait RelationMutation
  case class LinkMutation(mutation: Mutation) extends RelationMutation
  case class UnlinkMutation(`match`: Where) extends RelationMutation
  case class ReplaceMutation(mutation: Mutation) extends RelationMutation


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
