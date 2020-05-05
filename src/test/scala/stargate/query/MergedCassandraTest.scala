package stargate.query

import com.datastax.oss.driver.api.core.CqlSession
import org.junit.{AfterClass, BeforeClass}
import stargate.{CassandraTest, CassandraTestSession}

class MergedCassandraTest
  extends CassandraTestSession
  with EntityCRUDTestTrait
  with PaginationTestTrait
  with PredefinedQueryTestTrait
  with ReadWriteTestTrait {

  override def session: CqlSession = MergedCassandraTest.session
  override def newKeyspace: String = MergedCassandraTest.newKeyspace
}


object MergedCassandraTest extends CassandraTest {

  @BeforeClass def before = this.ensureCassandraRunning
  @AfterClass def after = this.cleanup

}