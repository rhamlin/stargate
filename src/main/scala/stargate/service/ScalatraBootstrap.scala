package stargate.service

import javax.servlet.ServletContext
import org.scalatra.LifeCycle

class ScalatraBootstrap extends LifeCycle {
  //implicit val swagger = new StargateSwagger
  override def init(context: ServletContext) {
    //context.mount (new ResourcesApp,"/api-docs")
    context.mount (new StargateServlet(ParsedStargateConfig.globalConfig), "/*")
  }
}
