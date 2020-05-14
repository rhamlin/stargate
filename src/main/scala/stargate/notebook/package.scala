package stargate

import com.typesafe.config.ConfigFactory
import stargate.model.{Entities, InputModel, generator}

package object notebook {


  def pythonCrudReq(stargateHost: String, appName: String, entityName: String, op: String): String = {
    s"""request = requests.get("http://${stargateHost}/${appName}/generator/${entityName}/${op}").json()
       |print("example ${op} request for entity ${entityName}:\\n" + json.dumps(request,indent=4))
       |""".stripMargin
  }
  def pythonCrudResp(stargateHost: String, appName: String, entityName: String, op: String): String = {
    val method = op match {
      case "create" => "post"
      case "get" => "get"
      case "update" => "put"
      case "delete" => "delete"
    }
    s"""response = requests.${method}("http://${stargateHost}/${appName}/${entityName}", json=request).json()
       |print("response for ${op} ${entityName}:\\n" + json.dumps(response,indent=4))
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
    val cells = model.values.flatMap(entity => {
      keywords.config.query.crudOps.toList.flatMap(op => {
        val md = markdownCell(s"""# ${op} ${entity.name}""")
        val req = codeCell(pythonCrudReq(stargateHost, appName, entity.name, op))
        val resp = codeCell(pythonCrudResp(stargateHost, appName, entity.name, op))
        List(md, req, resp)
      })
    })
    util.scalaToJava(notebook.updated("cells", initCells ++ cells))
  }


}
