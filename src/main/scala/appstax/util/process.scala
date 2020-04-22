package appstax.util

import scala.util.{Failure, Success, Try}
import scala.jdk.CollectionConverters._

object process {

  def exec(command: List[String]): Try[String] = {
    exec(asyncExec(command))
  }

  def exec(process: Process): Try[String] = {
    process.waitFor
    if(process.exitValue == 0) {
      Success(new String(process.getInputStream.readAllBytes))
    } else {
      Failure(new RuntimeException(new String(process.getErrorStream.readAllBytes)))
    }
  }

  def asyncExec(command: String*): Process = {
    asyncExec(command.toList)
  }

  def asyncExec(command: List[String]): Process = {
    new ProcessBuilder().command(command.asJava).start()
  }
}
