package appstax

import appstax.cassandra.{CassandraColumn, CassandraTable}
import appstax.service.validations
import appstax.util.AsyncList
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.`type`.{DataType, DataTypes}

import scala.concurrent.{ExecutionContext, Future}

package object model {

  object ScalarType extends Enumeration {
    class Value(val name: String, val cassandra: DataType, val convert: Object => Object) extends super.Val(name)

    val ASCII_STRING = new Value("ascii_string", DataTypes.ASCII, validations.ascii)
    val LONG = new Value("long", DataTypes.BIGINT, validations.bigInt)
    val BLOB = new Value("blob", DataTypes.BLOB, validations.blob)
    val BOOLEAN = new Value("boolean", DataTypes.BOOLEAN, validations.boolean)
    val DATE = new Value("date", DataTypes.DATE, validations.date)
    val DECIMAL = new Value("decimal", DataTypes.DECIMAL, validations.decimal)
    val DOUBLE = new Value("double", DataTypes.DOUBLE, validations.double)
    val FLOAT = new Value("float", DataTypes.FLOAT, validations.float)
    val INT = new Value("int", DataTypes.INT, validations.int)
    val SHORT = new Value("short", DataTypes.SMALLINT, validations.smallInt)
    val STRING = new Value("string", DataTypes.TEXT, validations.text)
    val TIME = new Value("time", DataTypes.TIME, validations.time)
    val TIMESTAMP = new Value("timestamp", DataTypes.TIME, validations.timestamp)
    val UUID = new Value("uuid", DataTypes.UUID, validations.uuid)
    var BIG_INT = new Value("big_int", DataTypes.VARINT, validations.varInt)

    val names: Map[String, Value] = appstax.util.enumerationNames(this).asInstanceOf[Map[String, Value]]

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
    def replaceArgument[A2](arg: A2) = ScalarCondition(field, comparison, arg)
  }

  type Where = List[ScalarCondition[String]]
  def conditionNames(where: Where): NamedConditions = where.map(_.named)


  case class InputModel(
    entities: Map[String, Entity],
    queries: Map[String, model.queries.GetQuery],
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
    def createTables(session: CqlSession, executor: ExecutionContext): Future[Unit] = {
      implicit val ec: ExecutionContext = executor
      Future.sequence(this.tables.map(cassandra.create(session, _))).map(_ => ())
    }

    def get(entityName: String, payload: Map[String,Object], session: CqlSession, executor: ExecutionContext): AsyncList[Map[String, Object]] =
      appstax.queries.get(this, entityName, payload, session, executor)

    trait MutationOps {
      def create(entityName: String, payload: Object, session: CqlSession, executor: ExecutionContext): Future[List[Map[String, Object]]]
      def update(entityName: String, payload: Map[String,Object], session: CqlSession, executor: ExecutionContext): Future[List[Map[String, Object]]]
      def delete(entityName: String, payload: Map[String,Object], session: CqlSession, executor: ExecutionContext): Future[List[Map[String, Object]]]
    }

    val model: OutputModel = this
    val mutation: MutationOps = new MutationOps {
      override def create(entityName: String, payload: Object, session: CqlSession, executor: ExecutionContext): Future[List[Map[String, Object]]] =
        appstax.queries.createUnbatched(model, entityName, payload, session, executor)
      override def update(entityName: String, payload: Map[String, Object], session: CqlSession, executor: ExecutionContext): Future[List[Map[String, Object]]] =
        appstax.queries.updateUnbatched(model, entityName, payload, session, executor)
      override def delete(entityName: String, payload: Map[String, Object], session: CqlSession, executor: ExecutionContext): Future[List[Map[String, Object]]] =
        appstax.queries.deleteUnbatched(model, entityName, payload, session, executor)
    }
    val batchMutation: MutationOps = new MutationOps {
      override def create(entityName: String, payload: Object, session: CqlSession, executor: ExecutionContext): Future[List[Map[String, Object]]] =
        appstax.queries.createBatched(model, entityName, payload, session, executor)
      override def update(entityName: String, payload: Map[String, Object], session: CqlSession, executor: ExecutionContext): Future[List[Map[String, Object]]] =
        appstax.queries.updateBatched(model, entityName, payload, session, executor)
      override def delete(entityName: String, payload: Map[String, Object], session: CqlSession, executor: ExecutionContext): Future[List[Map[String, Object]]] =
        appstax.queries.deleteBatched(model, entityName, payload, session, executor)
    }
}
