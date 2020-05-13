package stargate.service

import scala.beans.BeanProperty

object ParsedStargateConfig{
  var globalConfig: ParsedStargateConfig = _
}
case class ParsedStargateConfig(
                                 @BeanProperty val httpPort: Int,
                                 @BeanProperty val defaultTTL: Int,
                                 @BeanProperty val defaultLimit: Int,
                                 @BeanProperty val maxSchemaSizeKB: Long,
                                 @BeanProperty val maxRequestSizeKB: Long,
                                 @BeanProperty val maxMutationSizeKB: Long,
                                 @BeanProperty val cassandraContactPoints: List[(String, Int)],
                                 @BeanProperty val cassandraDataCenter: String,
                                 @BeanProperty val cassandraReplication: Int,
                                 @BeanProperty val stargateKeyspace: String
                               )
