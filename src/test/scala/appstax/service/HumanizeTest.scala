package appstax.service

import java.util.Locale

import org.junit.{Assert, Test}

class HumanizeTest {

  @Test
  def testHumanizeBytes() = {
    Locale.setDefault(Locale.US)
    Assert.assertEquals("0 b", humanize.bytes(0))
    Assert.assertEquals("1023 b", humanize.bytes(1023))
    Assert.assertEquals("1.00 kb", humanize.bytes(1024))
    Assert.assertEquals("10.00 kb", humanize.bytes(10240))
    Assert.assertEquals("1.00 mb", humanize.bytes(1048576) )
    Assert.assertEquals("10.00 gb", humanize.bytes(10737418240L))
  }


}
