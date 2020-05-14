package stargate

import com.typesafe.config.ConfigFactory
import stargate.model.{Entities, InputModel, generator}

package object notebook {


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

  def markdownCell(text: String): Map[String,Object] = {
    Map[String,Object](
      ("cell_type", "markdown"),
      ("metadata", Map.empty),
      ("source", List(text))
    )
  }

  def codeCell(code: String): Map[String,Object] = {
    val cell = util.javaToScala(ConfigFactory.parseResources("jupyter/empty-code-cell.conf").resolve.root.unwrapped).asInstanceOf[Map[String,Object]]
    cell.updated("source", List(code))
  }


  def pythonNotebook(stargateHost: String, appName: String, model: Entities): Object = {
    val notebook = util.javaToScala(ConfigFactory.parseResources("jupyter/python/empty-notebook.conf").resolve.root.unwrapped).asInstanceOf[Map[String,Object]]
    val initCells = util.javaToScala(ConfigFactory.parseResources("jupyter/python/init-cells.conf").resolve.getValue("init_cells").unwrapped).asInstanceOf[List[Object]]
    val library = model.values.flatMap(entity => {
      keywords.config.query.crudOps.toList.map(op => {
        pythonCrudFuncs(stargateHost, appName, entity.name, op)
      })
    }).mkString("\n")
    util.scalaToJava(notebook.updated("cells", initCells ++ List(codeCell(library))))
  }


}
