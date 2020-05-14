package stargate

import com.typesafe.config.ConfigFactory
import stargate.model.{Entities, InputModel, generator}

import scala.util.Try

package object notebook {

  case class DemoData(entityA: String, as: List[Map[String,Object]], relationName: String, entityB: String, bs: List[Map[String,Object]])

  def generateDemo(model: Entities, count: Int): Try[DemoData] = {
    Try({
      val possibleRelations = model.values.toList.map(e => {
        val hasScalars = e.fields.size > 1
        val relationsWithScalars = e.relations.values.filter(r => model(r.targetEntityName).fields.size > 1)
        (e.name, Option.when(hasScalars)(()).flatMap(_ => relationsWithScalars.headOption))
      })
      val chosenRelation = possibleRelations.filter(_._2.isDefined).head
      val a = chosenRelation._1
      val relation = chosenRelation._2.get.name
      val b = chosenRelation._2.get.targetEntityName
      def generate(e: String): List[Map[String, Object]] = List.range(0, count).map(_ => generator.entityFields(model(e).fields.values.toList))
      DemoData(a, generate(a), relation, b, generate(b))
    })
  }

  def pythonCrudFuncs(stargateHost: String, appName: String, entityName: String, op: String): String = {
    val method = op match {
      case "create" => "post"
      case "get" => "get"
      case "update" => "put"
      case "delete" => "delete"
    }
    val parseFunc = if(op == keywords.config.query.op.GET) "" else "Mutation"
    val requestFuncName = s"${op}Random${entityName}Request"
    val opFuncName = s"${op}${entityName}"
    s"""def ${requestFuncName}():
       |    return parseResponse(requests.get("http://${stargateHost}/${appName}/generator/${entityName}/${op}"))
       |
       |def ${opFuncName}(request):
       |    return parse${parseFunc}Response(requests.${method}("http://${stargateHost}/${appName}/${entityName}", json=request))
       |
       |def ${op}Random${entityName}():
       |    return ${opFuncName}(${requestFuncName}())
       |
       |""".stripMargin
  }

  def pythonDemo(stargateHost: String, appName: String, demo: DemoData): String = {
    val a_ids = s"${demo.entityA}_ids"
    val b_ids = s"${demo.entityB}_ids"
    s"""
       |${a_ids} = parseResponse(requests.post("http://${stargateHost}/${appName}/${demo.entityA}", json=${util.toPrettyJson(demo.as)}))
       |print(${a_ids})
       |
       |${b_ids} = parseResponse(requests.post("http://${stargateHost}/${appName}/${demo.entityB}", json=${util.toPrettyJson(demo.bs)}))
       |print(${b_ids})
       |""".stripMargin
  }



  def markdownCell(text: String): Map[String,Object] = {
    Map[String,Object](
      ("cell_type", "markdown"),
      ("metadata", Map.empty),
      ("source", List(text))
    )
  }

  def headerCell: Map[String,Object] = {
    util.javaToScala(ConfigFactory.parseResources("jupyter/header-cell.conf").resolve.root.unwrapped).asInstanceOf[Map[String,Object]]
  }
  def codeCell(code: String): Map[String,Object] = {
    val cell = util.javaToScala(ConfigFactory.parseResources("jupyter/empty-code-cell.conf").resolve.root.unwrapped).asInstanceOf[Map[String,Object]]
    cell.updated("source", List(code))
  }
  def initCell(code: String): Map[String,Object] = {
    val cell = util.javaToScala(ConfigFactory.parseResources("jupyter/empty-init-cell.conf").resolve.root.unwrapped).asInstanceOf[Map[String,Object]]
    cell.updated("source", List(code))
  }


  def pythonNotebook(stargateHost: String, appName: String, model: Entities): Object = {
    val notebook = util.javaToScala(ConfigFactory.parseResources("jupyter/python/empty-notebook.conf").resolve.root.unwrapped).asInstanceOf[Map[String,Object]]
    val initCells = util.javaToScala(ConfigFactory.parseResources("jupyter/python/init-cells.conf").resolve.getValue("init_cells").unwrapped).asInstanceOf[List[Object]]
    val libraryCell = initCell(model.values.flatMap(entity => {
      keywords.config.query.crudOps.toList.map(op => {
        pythonCrudFuncs(stargateHost, appName, entity.name, op)
      })
    }).mkString("\n"))
    val demoCells = generateDemo(model, 3).map(d => List(codeCell(pythonDemo(stargateHost, appName, d)))).getOrElse(List.empty)
    val cells = initCells ++ List(libraryCell, headerCell) ++ demoCells
    util.scalaToJava(notebook.updated("cells", cells))
  }


}
