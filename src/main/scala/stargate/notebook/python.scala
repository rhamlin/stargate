/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package stargate.notebook

import com.typesafe.config.ConfigFactory
import stargate.model.{Entities, ScalarComparison}
import stargate.{keywords, util}

object python {

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
       |    return parseResponse(requests.get("http://${stargateHost}/${apiVersion}/api/${appName}/apigen/${entityName}/${op}"))
       |
       |def ${opFuncName}(request):
       |    return parse${parseFunc}Response(requests.${method}("http://${stargateHost}/${apiVersion}/api/${appName}/query/entity/${entityName}", json=request))
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
    val create1 = s"""${a_resp} = requests.post("http://${stargateHost}/${apiVersion}/api/${appName}/query/entity/${demo.entityA}", json=${util.toPrettyJson(demo.as)})
                     |${a_ids} = parseMutationResponse(${a_resp})
                     |print("${demo.entityA} ids:" + str(${a_ids}))
                     |
                     |${b_resp} = requests.post("http://${stargateHost}/${apiVersion}/api/${appName}/query/entity/${demo.entityB}", json=${util.toPrettyJson(demo.bs)})
                     |${b_ids} = parseMutationResponse(${b_resp})
                     |print("${demo.entityB} ids:" + str(${b_ids}))
                     |""".stripMargin
    val linkRequest1: String = indent(linkByEntityIdRequest(s"${a_ids}[i]", demo.relationName, s"${b_ids}[i]")).strip
    val link1 =
      s"""for i in range(${demo.as.length}):
         |  requests.put("http://${stargateHost}/${apiVersion}/api/${appName}/query/entity/${demo.entityA}", json=${linkRequest1})
         |""".stripMargin
    val getRequest1: String = get(idInCondition(s"${a_ids}"), List(demo.relationName), Some(demo.as.length), false)
    val get1 =
      s"""response = requests.get("http://${stargateHost}/${apiVersion}/api/${appName}/query/entity/${demo.entityA}", json=${getRequest1})
         |print(json.dumps(parseResponse(response), indent=2))
         |""".stripMargin
    val duplicateValue = util.toJson(demo.bs(0)(demo.duplicateField))
    val link2Condition = conditionString(demo.duplicateField, ScalarComparison.EQ, duplicateValue)
    val linkRequest2 =
      s"""{
         |  "${keywords.mutation.MATCH}": ${idEqCondition(s"${a_ids}[0]")},
         |  "${demo.relationName}": { "${keywords.relation.REPLACE}": { "${keywords.mutation.MATCH}": ${link2Condition} } }
         |}""".stripMargin
    val link2 =
      s"""response = requests.put("http://${stargateHost}/${apiVersion}/api/${appName}/query/entity/${demo.entityA}", json=${linkRequest2})
         |print(json.dumps(parseResponse(response), indent=2))
         |""".stripMargin
    val getRequest2: String = get(idInCondition(s"${a_ids}"), List(demo.relationName), Some(1), true)
    val get2 =
      s"""page=0
         |response = parseResponse(requests.get("http://${stargateHost}/${apiVersion}/api/${appName}/query/entity/${demo.entityA}", json=${getRequest2}))
         |print("Page: " + str(page))
         |print(json.dumps(response, indent=2))
         |while len(response) == 2:
         |  page = page + 1
         |  response = parseResponse(requests.get("http://${stargateHost}/${apiVersion}/api/${appName}/query/continue/" + response[1]["${keywords.pagination.CONTINUE}"]))
         |  print("Page: " + str(page))
         |  print(json.dumps(response, indent=2))
         |""".stripMargin
    val cleanup =
      s"""requests.delete("http://${stargateHost}/${appName}/${demo.entityA}", json={ "${keywords.mutation.MATCH}": ${idInCondition(a_ids)} })
         |requests.delete("http://${stargateHost}/${appName}/${demo.entityB}", json={ "${keywords.mutation.MATCH}": ${idInCondition(b_ids)} })
         |""".stripMargin
    DemoCode(create1, link1, get1, link2, get2, cleanup)
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
