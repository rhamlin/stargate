package appstax.service

case class InvalidConversionException(fieldValue: String, typeName: String, e: Exception)
  extends Exception (s"cannot convert $fieldValue to $typeName", e)

case class InvalidACIIScalarException(original: String)
  extends Exception(s"invalid ASCII original: $original")

case class InvalidKeywordException(keyword: String) extends Exception {}

case class InvalidScalarException(value: Any, expectedType: Class[_], actualType: Class[_])
  extends Exception(s"$value was expected to be of type $expectedType but was $actualType"){}

case class UnknownFieldException(keyName: String, entityName: String)
  extends Exception(s"do not know how to process $keyName on $entityName"){}

case class UnsupportedObjectException(keyName: String, original: Class[_])
  extends Exception(s"do not how to validate $keyName which is type of $original")
