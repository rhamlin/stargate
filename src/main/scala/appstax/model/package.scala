package appstax

import java.util

import appstax.cassandra.{CassandraColumn, CassandraTable}
import appstax.util.AsyncList
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.`type`.{DataType, DataTypes}

import scala.concurrent.{ExecutionContext, Future}

package object model {

  object ScalarType extends Enumeration {
    class Value(val name: String, val cassandra: DataType, val convert: Object => Object) extends super.Val(name)

    val INT = new Value("int", DataTypes.INT, identity)
    val FLOAT = new Value("float", DataTypes.FLOAT, identity)
    val STRING = new Value("string", DataTypes.TEXT, _.toString)
    val UUID = new Value("uuid", DataTypes.UUID, {
      case x: String => java.util.UUID.fromString(x)
      case x => x
    })
    val names = appstax.util.enumerationNames(this).asInstanceOf[Map[String, Value]]

    def fromString(name: String) = names(name)
  }
  case class ScalarField(name: String, scalarType: ScalarType.Value) {
    def column: CassandraColumn = CassandraColumn(name, scalarType.cassandra)
  }
  case class RelationField(name: String, targetEntityName: String, inverseName: String)
  case class Entity(name: String, fields: Map[String, ScalarField], relations: Map[String, RelationField])

  object ScalarComparison extends Enumeration {
    val LT = Value("<")
    val LTE = Value("<=")
    val EQ = Value("=")
    val GTE = Value(">=")
    val GT = Value(">")
    val IN = Value("IN")
    val names = appstax.util.enumerationNames(this)

    def isEquality(comp: Value) = comp.eq(EQ) || comp.eq(IN)
    def fromString(name: String) = names(name)
  }
  case class NamedCondition(field: String, comparison: ScalarComparison.Value)
  type NamedConditions = List[NamedCondition]
  case class ScalarCondition[A](field: String, comparison: ScalarComparison.Value, argument: A) {
    def named: NamedCondition = NamedCondition(field, comparison)
    def trimRelationPath: ScalarCondition[A] = ScalarCondition(field.split(schema.RELATION_SPLIT_REGEX).last, comparison, argument)
  }

  type Where = List[ScalarCondition[String]]
  def conditionNames(where: Where): NamedConditions = where.map(_.named)


  case class InputModel(
    entities: Map[String, Entity],
    conditions: Map[String, List[NamedConditions]]
  ) {
    // TODO - either remove this, or add support for specifying in config+parser, create default unknown value
    def cardinality(entity: String, field: String): Long = 1000
    def fieldColumnType(entity: String, field: String): DataType = entities(entity).fields(field).scalarType.cassandra
  }

  case class OutputModel(
    input: InputModel,
    entityTables: Map[String,List[CassandraTable]],
    relationTables: Map[(String,String), CassandraTable]) {

    def tables: List[CassandraTable] = (entityTables.values.flatten ++ relationTables.values).toList

    def getWrapper(entityName: String)(session: CqlSession, payload: Map[String,Object], executor: ExecutionContext): AsyncList[Map[String, Object]] =
      queries.get(this, entityName, payload, session, executor)
    def mutationWrapper(entityName: String)(session: CqlSession, payload: Map[String,Object], executor: ExecutionContext): Future[List[Map[String, Object]]] =
      queries.mutation(this, entityName, payload, session, executor)
    def createWrapper(entityName: String)(session: CqlSession, payload: Object, executor: ExecutionContext): Future[List[Map[String, Object]]] =
      queries.create(this, entityName, payload, session, executor)
    def updateWrapper(entityName: String)(session: CqlSession, payload: Map[String,Object], executor: ExecutionContext): Future[List[Map[String, Object]]] =
      queries.update(this, entityName, payload, session, executor)
    def deleteWrapper(entityName: String)(session: CqlSession, payload: Map[String,Object], executor: ExecutionContext): Future[List[Map[String, Object]]] =
      queries.delete(this, entityName, payload, session, executor)
  }

}
