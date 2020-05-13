package stargate.service

import org.scalatra.swagger.{ApiInfo, ContactInfo, LicenseInfo, NativeSwaggerBase, Swagger}
import org.scalatra.ScalatraServlet

class ResourcesApp(implicit val swagger: Swagger) 
extends ScalatraServlet 
with NativeSwaggerBase{

}

object StargateApiInfo extends ApiInfo(
   "The Stargate API",
    "Docs for the Stargate API",
    "https://github.com/datastax/stargate",
    ContactInfo(
      "DataStax",
      "https://github.com/datastax/stargate",
      "sales@datastax.com"
    ),
    LicenseInfo(
      "Apache 2.0",
      "http://opensource.org/licenses/MIT"
    )
)

class StargateSwagger extends Swagger(Swagger.SpecVersion, "0.1.1", StargateApiInfo)