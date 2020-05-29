package stargate.service

import org.json4s.native.Serialization
import sttp.client._
import stargate.service.testsupport.ServletContext
import sttp.model.{Uri, StatusCodes}
import sttp.model.MediaTypes
import sttp.model.MediaType

trait HttpClientTestTrait extends StatusCodes with MediaTypes {
  implicit val backend = HttpURLConnectionBackend()
  implicit val serialization = Serialization
  val sc: ServletContext = MergedServletTest.sc
  def wrap(url: String) : Uri = {
    uri"http://localhost:9090/${url}"
  }
}
