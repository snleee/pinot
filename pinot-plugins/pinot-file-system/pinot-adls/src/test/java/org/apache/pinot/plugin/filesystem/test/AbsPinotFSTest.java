package org.apache.pinot.plugin.filesystem.test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import org.apache.pinot.plugin.filesystem.ADLSGen2PinotFS;
import org.apache.pinot.plugin.filesystem.AbsPinotFS;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.LocalPinotFS;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class AbsPinotFSTest {

  @BeforeMethod
  public void setup()
      throws URISyntaxException {
  }

  @Test
  public void testAbsPinotFS()
      throws URISyntaxException, IOException {
    PinotConfiguration pinotConfiguration = new PinotConfiguration();
    pinotConfiguration.setProperty("accessKey", "/Tp7zX9oASx/Te0PZ5c696BeZsWMp4njyaw9KG3BsXDE5lwgDX9Pj2f6580uJah85+ROqwvxRHYn+ASt4Rw7IQ==");
    pinotConfiguration.setProperty("accountName", "snleetest");

    AbsPinotFS pinotFS = new AbsPinotFS();
    pinotFS.init(pinotConfiguration);
    String[] files = pinotFS.listFiles(new URI("abs://snlee-test/"), false);
    System.out.println(Arrays.toString(files));
    files = pinotFS.listFiles(new URI("abs://snlee-test/"), true);
    System.out.println(Arrays.toString(files));

    PinotConfiguration adlsConfiguration = new PinotConfiguration();
    adlsConfiguration.setProperty("accessKey", "qT7OTIZafeDqwmq1+L/OdRBDHO1mxAUZVBJsfo2+IyvmcJZoHMyXRotqo/OMO+INJ7lAS/upDvTT+AStv+9k3A==");
    adlsConfiguration.setProperty("accountName", "snleetest2");
    adlsConfiguration.setProperty("fileSystemName", "snlee-test");

    ADLSGen2PinotFS adlsGen2PinotFS = new ADLSGen2PinotFS();
    adlsGen2PinotFS.init(adlsConfiguration);
    files = adlsGen2PinotFS.listFiles(new URI("abfs://snlee-test/"), false);
    System.out.println(Arrays.toString(files));
    files = adlsGen2PinotFS.listFiles(new URI("abfs://snlee-test/"), true);
    System.out.println(Arrays.toString(files));


    LocalPinotFS localPinotFS = new LocalPinotFS();
    String base = "file:/Users/seunghyun/snlee-test";
    files = localPinotFS.listFiles(new URI(base), true);
    System.out.println(Arrays.toString(files));
    files = localPinotFS.listFiles(new URI(base), false);
    System.out.println(Arrays.toString(files));
  }
}
