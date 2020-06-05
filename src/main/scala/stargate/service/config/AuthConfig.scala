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
package stargate.service.config

import scala.beans.BeanProperty
import com.typesafe.config.Config

/**
  * 
  * @param enabled will enable http basic auth on all requests
  * @param user username for auth when enabled
  * @param passwordHash bcrypt hashed password for auth
  */
case class AuthConfig (
  @BeanProperty val enabled: Boolean,
  @BeanProperty val user: String,
  @BeanProperty val passwordHash: String
){
  
}

object AuthConfig {

  def parse(config: Config): AuthConfig = {
      AuthConfig(
        config.getBoolean("enabled"),
        config.getString("user"),
        config.getString("passwordHash")
        )
  }
}
