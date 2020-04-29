package stargate

import stargate.cassandra._
import stargate.model._
import com.datastax.oss.driver.api.core.`type`.DataTypes

object schema {

  val ENTITY_ID_COLUMN_NAME = "entityId"
  val RELATION_FROM_COLUMN_NAME = "from"
  val RELATION_TO_COLUMN_NAME = "to"
  val RELATION_SPLIT_REGEX = "\\."

  def baseTableName(entityName: String) = entityName
  val baseTableKey = CassandraKey(List(CassandraColumn(ENTITY_ID_COLUMN_NAME, DataTypes.UUID)), List.empty)
  def viewTableName(entityName: String, key: CassandraKeyNames) = entityName + "_" + key.partitionKeys.mkString("_") + "_" + key.clusteringKeys.mkString("_")
  def relationTableName(entityName: String, relationName: String) = entityName + "_" + relationName
  val relationTableKey = CassandraKey(List(CassandraColumn(RELATION_FROM_COLUMN_NAME, DataTypes.UUID)), List(CassandraColumn(RELATION_TO_COLUMN_NAME, DataTypes.UUID)))
  val relationTableColumns = CassandraColumns(relationTableKey, List.empty)
  val relationTableTypes = Map((RELATION_FROM_COLUMN_NAME, DataTypes.UUID), (RELATION_TO_COLUMN_NAME, DataTypes.UUID))

  type GroupedConditions[T] = Map[List[String], List[ScalarCondition[T]]]

  def appendEntityIdKey(key: CassandraKeyNames): CassandraKeyNames = {
    if(key.combined.contains(ENTITY_ID_COLUMN_NAME)){
      key
    } else {
      CassandraKeyNames(key.partitionKeys, key.clusteringKeys ++ List(ENTITY_ID_COLUMN_NAME))
    }
  }

  def conditionsKey(conditions: NamedConditions, cardinality: String=>Long, minPartitionKeys: Long): CassandraKeyNames = {
    val sorted = conditions.sortBy(cond => cardinality(cond.field)).reverse
    val (eq, noteq) = sorted.partition(cond => ScalarComparison.isEquality(cond.comparison))
    val noteqSet = noteq.map(_.field).toSet
    var equalities = eq.map(_.field).filter(!noteqSet.contains(_))
    var partitionKeys: List[String] = List()
    var product = 1L
    while(equalities.nonEmpty && product < minPartitionKeys) {
      val next = equalities.head
      partitionKeys = next :: partitionKeys
      product = product * cardinality(next)
      equalities = equalities.tail
    }
    // TODO: handle case with no equals conditions, i.e. no columns to use as partition keys
    partitionKeys = partitionKeys.reverse
    val partitionKeySet = partitionKeys.toSet
    val clusteringKeys = sorted.map(_.field).filter(!partitionKeySet.contains(_)).distinct
    CassandraKeyNames(partitionKeys, clusteringKeys)
  }

  def keySupportsConditions(key: CassandraKeyNames, conditions: NamedConditions): KeyConditionScore = {
    if(conditions.isEmpty) {
      return KeyConditionScore(0,0,0)
    }
    val partitionSet = key.partitionKeys.toSet
    val clusteringSet = key.clusteringKeys.toSet
    val allKeys = key.combined
    val allConditions = conditions.map(_.field).toSet
    val eq_noteq = conditions.partition(x => ScalarComparison.isEquality(x.comparison))
    val eq :: noteq :: _ = List(eq_noteq._1, eq_noteq._2).map(_.map(_.field).toSet)
    val usedKeys = allKeys.takeWhile(allConditions.contains)
    val partitionScore = partitionSet.diff(eq).size
    val clusteringScore = noteq.diff(clusteringSet).size
    val skipScore = allConditions.size - usedKeys.length
    KeyConditionScore(partitionScore, clusteringScore, skipScore)
  }

  def tableScores(conditions: NamedConditions, tables: List[CassandraTable]): Map[KeyConditionScore, List[CassandraTable]] = {
    val scores = tables.map(t => (keySupportsConditions(t.columns.key.names, conditions), t))
    scores.groupMap(_._1)(_._2)
  }

  def baseTable(entity: Entity): CassandraTable = {
    val tableName = baseTableName(entity.name)
    val baseKeyNames = baseTableKey.names.combined.toSet
    val columns = CassandraColumns(baseTableKey, entity.fields.values.filter(f => !baseKeyNames(f.name)).map(_.column).toList)
    CassandraTable(tableName, columns)
  }

  def relationTables(entity: Entity): Map[(String,String), CassandraTable] = {
    def toTable(relation: String) = CassandraTable(relationTableName(entity.name, relation), relationTableColumns)
    entity.relations.toList.map((name_rel) => ((entity.name, name_rel._1), toTable(name_rel._1))).toMap
  }

  def groupConditionsByPath[T](conditions: List[T], conditionColumn: T=>String): Map[List[String], List[(String,T)]] = {
    val parsedByEntity = conditions.map(cond => {
      val cols = conditionColumn(cond).split(RELATION_SPLIT_REGEX)
      val (path, scalar) = cols.splitAt(cols.length - 1)
      (path.toList, (scalar(0), cond))
    })
    parsedByEntity.groupMap(_._1)(_._2)
  }

  def groupNamedConditionsByPath(conditions: NamedConditions): Map[List[String], NamedConditions] = {
    val grouped = groupConditionsByPath(conditions, (_:NamedCondition).field)
    grouped.view.mapValues(_.map(x => NamedCondition(x._1, x._2.comparison))).toMap
  }

  def groupConditionsByPath[T](conditions: List[ScalarCondition[T]]): GroupedConditions[T] = {
    val grouped = groupConditionsByPath(conditions, (_:ScalarCondition[T]).field)
    grouped.view.mapValues(_.map(x => ScalarCondition(x._1, x._2.comparison, x._2.argument))).toMap
  }

  def traverseRelationPath(model: InputModel, entityName: String, path: List[String]): List[RelationField] = {
    if(path.isEmpty) {
      List.empty
    } else {
      val relation = model.entities(entityName).relations(path.head)
      val nextEntity = relation.targetEntityName
      relation +: traverseRelationPath(model, nextEntity, path.tail)
    }
  }
  // TODO - clean up duplication between these two (to get entity name would have to get it from last O(N) of relation path)
  def traverseEntityPath(model: InputModel, entityName: String, path: List[String]): String = {
    if(path.isEmpty) {
      entityName
    } else {
      val nextEntity = model.entities(entityName).relations(path.head).targetEntityName
      traverseEntityPath(model, nextEntity, path.tail)
    }
  }

  def queryTables(model: InputModel, rootEntity: String, conditions: NamedConditions, minPartitions: Long = 1000): Map[String, List[CassandraTable]] = {
    if(conditions.isEmpty) {
      return Map.empty
    }
    val groupedConditions = groupNamedConditionsByPath(conditions)
    val tables = groupedConditions.toList.map(path_conds => {
      val entityName = traverseEntityPath(model, rootEntity, path_conds._1)
      val keyNames =  appendEntityIdKey(conditionsKey(path_conds._2, model.cardinality(entityName, _), minPartitions))
      val tableName = viewTableName(entityName, keyNames)
      def nameToCol(name: String) = CassandraColumn(name, model.fieldColumnType(entityName, name))
      val key = CassandraKey(keyNames.partitionKeys.map(nameToCol), keyNames.clusteringKeys.map(nameToCol))
      val columns = CassandraColumns(key, List.empty)
      (entityName, CassandraTable(tableName, columns))
    })
    tables.groupMap(_._1)(_._2)
  }

  def mergeMultimaps[K,V](maps: List[Map[K, List[V]]]): Map[K, List[V]] = maps.flatMap(_.toList).groupBy(_._1).view.mapValues(_.flatMap(_._2)).toMap
  def mergeMultimaps[K,V](a: Map[K, List[V]], b: Map[K, List[V]]): Map[K, List[V]] = mergeMultimaps(List(a,b))



  def conditionsTables(model: InputModel, minPartitions: Long = 1000): Map[String, List[CassandraTable]] = {
    mergeMultimaps(model.conditions.toList.map(name_conditionsList => {
      val (entityName, conditionsList) = name_conditionsList
      mergeMultimaps(conditionsList.map(conditions => queryTables(model, entityName, conditions, minPartitions)))
    }))
  }

  def modelTables(model: InputModel, minPartitions: Long = 1000): (Map[String, List[CassandraTable]], Map[(String,String), CassandraTable]) = {
    val baseTables = model.entities.view.mapValues(baseTable).mapValues(List(_)).toMap
    val _relationTables = model.entities.values.flatMap(relationTables).toMap
    val _queryTables = conditionsTables(model, minPartitions)
    // TODO: just doing a minimal dedupe of tables right now; should later determine mininmal set of tables needed for all conditions
    val entityTables = mergeMultimaps(baseTables, _queryTables).view.mapValues(_.toSet.toList).toMap
    (entityTables, _relationTables)
  }

  def outputModel(model: InputModel, minPartitions: Long = 1000): OutputModel = {
    val (entityTables, relationTables) = modelTables(model, minPartitions)
    OutputModel(model, entityTables, relationTables)
  }

}
