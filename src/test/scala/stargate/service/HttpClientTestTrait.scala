package stargate.service

import org.json4s.native.Serialization
import stargate.service.testsupport.ServletContext
import stargate.service.testsupport._

trait HttpClientTestTrait {
  val sc: ServletContext = MergedServletTest.sc
  def wrap(url: String) : String = {
    s"http://localhost:9090/${url}"
  }
}
