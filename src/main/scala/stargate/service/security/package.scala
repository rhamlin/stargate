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
package stargate.service

import javax.servlet.Filter
import javax.servlet.FilterConfig
import javax.servlet.{FilterChain, ServletRequest, ServletResponse}
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import org.eclipse.jetty.security.LoginService
import org.eclipse.jetty.security.HashLoginService
import java.{util => ju}
import java.nio.charset.StandardCharsets
import at.favre.lib.crypto.bcrypt.BCrypt
import stargate.service.config.AuthConfig
import com.typesafe.scalalogging.LazyLogging

package object security {

  class BasicAuthFilter (val auth: AuthConfig) extends Filter {
    private val passwordHash = auth.passwordHash
    private val username = auth.user
    val realm = "StargateAuth"
    override def init(x: FilterConfig): Unit = {
      //using ctor with FilterHolder so do not need the filter config
    }
    /**
     * Uses bcrypt and basic auth to authenticate a single username and password. This is not particularly clever yet but it
     * has the advantage of being easy to read. This will filter all requests if added including the swagger json
     * 
     * @param servletRequest http request
     * @param servletResponse http response
     * @param filterChain if everything is successfull we pass on to the next filter
     */ 
    override def doFilter(servletRequest: ServletRequest, servletResponse: ServletResponse, filterChain: FilterChain): Unit = {
      val request = servletRequest.asInstanceOf[HttpServletRequest]
      val response = servletResponse.asInstanceOf[HttpServletResponse]
      val authHeader = Option(request.getHeader("Authorization"))
      if (authHeader.isEmpty){
        denied(response)
        return
      }
      if (!authHeader.get.toLowerCase().startsWith("basic")){
        denied(response)
        return
      }
      val base64Credentials = authHeader.get.substring("Basic".length()).trim()
      val credDecoded = ju.Base64.getDecoder().decode(base64Credentials)
      val credentials = new String(credDecoded, StandardCharsets.UTF_8)
      val credPair = credentials.split(":", 2);
      if(credPair(0) != username){
        denied(response)
        return
      }
      //verify with bcrypt
      val result = BCrypt.verifyer().verify(credPair(1).toCharArray(), passwordHash);
      if(!result.verified){
        denied(response)
        return
      }
      //pass on to next chain
      filterChain.doFilter(servletRequest, servletResponse);
    }

    private [this] def denied(response: HttpServletResponse): Unit = {
       response.setHeader("WWW-Authenticate", s"""Basic realm="${realm}"""");
       response.sendError(401, "denied");
    }
    
    override def destroy(): Unit = {
      //nothing to tear down atm
    }
    
  }
}
