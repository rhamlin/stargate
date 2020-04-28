package appstax.service

import java.util.regex.Pattern
import javax.servlet.http.HttpServletRequest

object http {

  def validateRequestSize(contentLength: Long, maxRequestSize: Long): Unit = {
    if (contentLength > maxRequestSize){
      throw MaximumRequestSizeException(contentLength, maxRequestSize)
    }
  }

  def validateSchemaSize(contentLength: Long, maxSchemaSize: Long): Unit= {
    if (contentLength > maxSchemaSize){
      throw SchemaToLargeException(contentLength, maxSchemaSize)
    }
  }

  def validateMutation(op: String, contentLength: Long, maxMutationSize: Long): Unit ={
    if ((op == "PUT" || op == "POST") && contentLength > maxMutationSize){
      throw MaxMutationSizeException(contentLength, maxMutationSize)
    }
  }


  val formType = "multipart/form-data"
  val jsonType = "application/json"
  val pathRegex: Pattern = Pattern.compile("//")

  def validateFileHeader(req: HttpServletRequest): Unit = {
    val contentType = req.getContentType
    if (contentType != formType) {
      throw new InvalidContentTypeException(formType, contentType)
    }
  }

  def validateJsonContentHeader(req: HttpServletRequest) : Unit = {
    val contentType = req.getContentType
    if (contentType != jsonType){
      throw InvalidContentTypeException(jsonType, contentType)
    }
  }
  def validateRestSize(req: HttpServletRequest): Unit = {

  }

  /**
   * based on idea from spring boot, to help prevent escaping out of url string and accessing another path
   * precompile the regex above because its compiled every time on String.replaceAll()
   * @param path serverPath to sanitize
   * @return string with all of the // replaced with /
   */
  def sanitizePath(path: String): String = {
    pathRegex.matcher(path).replaceAll("/")
  }
}

case class InvalidContentTypeException(expectedContentType: String, contentType: String)
        extends Exception(s"Expected $expectedContentType but was $contentType"){}
