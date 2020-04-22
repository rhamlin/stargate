package appstax.queries

object keywords {

  val KEYWORD_PREFIX = "-"

  object mutation {
    val GET: String = KEYWORD_PREFIX + "get"
    val CREATE: String = KEYWORD_PREFIX + "create"
    val UPDATE: String = KEYWORD_PREFIX + "update"
    val MATCH: String = KEYWORD_PREFIX + "match"
    val DELETE: String = KEYWORD_PREFIX + "delete"
  }

  object relation {
    val LINK: String = KEYWORD_PREFIX + "link"
    val UNLINK: String = KEYWORD_PREFIX + "unlink"
    val REPLACE: String = KEYWORD_PREFIX + "replace"
  }
}
