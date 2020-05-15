package stargate

import java.util.stream.Collectors

import com.typesafe.config.ConfigFactory

import scala.jdk.CollectionConverters._
import stargate.model.{Entities, InputModel, ScalarComparison, generator}

import scala.util.{Random, Try}

package object notebook {

  case class DemoData(entityA: String, as: List[Map[String,Object]], relationName: String, entityB: String, bs: List[Map[String,Object]], duplicateField: String)
  case class DemoCode(create1: String, link1: String, get1: String)

  def generateDemo(model: Entities, count: Int): Try[DemoData] = {
    Try({
      val possibleRelations = Random.shuffle(model.values.toList).map(e => {
        val hasScalars = e.fields.size > 1
        val relationsWithScalars = Random.shuffle(e.relations.values.toList).filter(r => model(r.targetEntityName).fields.size > 1)
        (e.name, Option.when(hasScalars)(()).flatMap(_ => relationsWithScalars.headOption))
      })
      val chosenRelation = possibleRelations.filter(_._2.isDefined).head
      val a = chosenRelation._1
      val relation = chosenRelation._2.get.name
      val b = chosenRelation._2.get.targetEntityName
      def generate(e: String): List[Map[String, Object]] = List.range(0, count).map(_ => generator.entityFields(model(e).fields.values.toList))
      val duplicateField = model(b).fields.values.filter(_.name != schema.ENTITY_ID_COLUMN_NAME).head
      val duplicateValue = generator.randomScalar(duplicateField)
      val duplicateBs = generate(b).map(_.updated(duplicateField.name, duplicateValue))
      DemoData(a, generate(a), relation, b, duplicateBs, duplicateField.name)
    })
  }

  def indent(s: String): String = s.lines.collect(Collectors.toList()).asScala.map("  " + _).mkString("\n")
  def idCondition(id: String): String = s"""["${schema.ENTITY_ID_COLUMN_NAME}", "${ScalarComparison.EQ.toString}", ${id}]"""
  def linkByEntityIdRequest(fromId: String, fromRelation: String, toId: String): String = {
    s"""{
       |  "${keywords.mutation.MATCH}": ${idCondition(fromId)},
       |  "${fromRelation}": { "${keywords.relation.LINK}": { "${keywords.mutation.MATCH}": ${idCondition(toId)} } }
       |}""".stripMargin
  }
  def get(condition: String, relations: List[String], limit: Option[Int], continue: Boolean): String = {
    val limitConfig: String = limit.map(l => s""", "${keywords.pagination.LIMIT}": ${l}""").getOrElse("")
    val continueConfig: String = if(continue) s""", "${keywords.pagination.CONTINUE}": true""" else ""
    val relationConfig = relations.map(r => s""", "${r}": {}""").mkString
    s"""{ "${keywords.mutation.MATCH}": ${condition}${limitConfig}${continueConfig}${relationConfig} }""".stripMargin
  }
  def getAll(relations: List[String], limit: Option[Int], continue: Boolean): String = get(s""""${keywords.query.MATCH_ALL}"""", relations, limit, continue)
  def getById(id: String, relations: List[String], limit: Option[Int], continue: Boolean): String = get(idCondition(id), relations, limit, continue)


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

  def pythonDemoCode(stargateHost: String, appName: String, demo: DemoData): DemoCode = {
    val a_resp = s"${demo.entityA}_response"
    val b_resp = s"${demo.entityB}_response"
    val a_ids = s"${demo.entityA}_ids"
    val b_ids = s"${demo.entityB}_ids"
    val create1 = s"""${a_resp} = requests.post("http://${stargateHost}/${appName}/${demo.entityA}", json=${util.toPrettyJson(demo.as)})
       |${a_ids} = parseMutationResponse(${a_resp})
       |print("${demo.entityA} ids:" + str(${a_ids}))
       |
       |${b_resp} = requests.post("http://${stargateHost}/${appName}/${demo.entityB}", json=${util.toPrettyJson(demo.bs)})
       |${b_ids} = parseMutationResponse(${b_resp})
       |print("${demo.entityB} ids:" + str(${b_ids}))
       |""".stripMargin
    val linkRequest1: String = indent(linkByEntityIdRequest(s"${a_ids}[i]", demo.relationName, s"${b_ids}[i]")).strip
    val link1 =
      s"""for i in range(${demo.as.length}):
         |  requests.put("http://${stargateHost}/${appName}/${demo.entityA}", json=${linkRequest1})
         |""".stripMargin
    val getRequest1: String = getById(s"${a_ids}[i]", List(demo.relationName), Some(demo.as.length), false)
    val get1 =
      s"""for i in range(${demo.as.length}):
         |  response = requests.get("http://${stargateHost}/${appName}/${demo.entityA}", json=${getRequest1})
         |  print(json.dumps(parseResponse(response), indent=2))
         |""".stripMargin
    DemoCode(create1, link1, get1)
  }

  def demoCells(demo: DemoData, demoCode: DemoCode): List[Map[String,Object]] = {
    val createMarkdown = markdownCell(s"""### Create some instances of ${demo.entityA} and ${demo.entityB}""")
    val createCode = codeCell(demoCode.create1)
    val linkMarkdown = markdownCell(s"""### Link some ${demo.entityA} entities to some ${demo.entityB} entities""")
    val linkCode = codeCell(demoCode.link1)
    val getMarkdown1 = markdownCell(s"""### Get all ${demo.entityA} entities and their related ${demo.entityB} entities""")
    val getCode1 = codeCell(demoCode.get1)
    List(createMarkdown, createCode, linkMarkdown, linkCode, getMarkdown1, getCode1)
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
    val _demoCells = generateDemo(model, 3).map(d => demoCells(d, pythonDemoCode(stargateHost, appName, d))).getOrElse(List.empty)
    val cells = initCells ++ List(libraryCell, headerCell) ++ _demoCells
    util.scalaToJava(notebook.updated("cells", cells))
  }


}
