package appstax

object keywords {

  val KEYWORD_PREFIX = "-"

  object mutation {
    val GET: String = KEYWORD_PREFIX + config.query.op.GET
    val CREATE: String = KEYWORD_PREFIX + config.query.op.CREATE
    val UPDATE: String = KEYWORD_PREFIX + config.query.op.UPDATE
    val MATCH: String = KEYWORD_PREFIX + config.query.op.MATCH
    val DELETE: String = KEYWORD_PREFIX + config.query.op.DELETE
  }

  object relation {
    val LINK: String = KEYWORD_PREFIX + "link"
    val UNLINK: String = KEYWORD_PREFIX + "unlink"
    val REPLACE: String = KEYWORD_PREFIX + "replace"
  }

  object pagination {
    val LIMIT: String = KEYWORD_PREFIX + "limit"
    val CONTINUE: String = KEYWORD_PREFIX + "continue"
    val TTL: String = KEYWORD_PREFIX + "ttl"
  }

  // should probably move keywords to top-level module or somewhere else if going to have both config/schema and runtime keywords
  object config {
    val ENTITIES: String = "entities"
    val QUERIES: String = "queries"
    val QUERY_CONDITIONS: String = "queryConditions"

    object entity {
      val FIELDS: String = "fields"
      val RELATIONS: String = "relations"
      val RELATION_TYPE: String = "type"
      val RELATION_INVERSE: String = "inverse"
    }

    object query {

      val INCLUDE: String = KEYWORD_PREFIX + "include"

      object op {
        val GET: String = "get"
        val CREATE: String = "create"
        val MATCH: String = "match"
        val UPDATE: String = "update"
        val DELETE: String = "delete"
      }
      object relation {
        val OP_LINK_CONFIG: String = "link"
        val OP_UNLINK_CONFIG: String = "unlink"
        val OP_REPLACE_CONFIG: String = "replace"
      }
    }
  }
}
