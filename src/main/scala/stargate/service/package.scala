package stargate

import scala.io.Source

package object service {
  val StargateApiVersion = "v1"
  val StargateVersion: String = getClass.getPackage().getImplementationVersion
}
