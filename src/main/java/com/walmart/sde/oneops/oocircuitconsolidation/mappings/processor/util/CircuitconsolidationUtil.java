package com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.config.IConstants;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.exception.UnSupportedOperation;

// TODO: this class should be moved to OO commons from main Project
public class CircuitconsolidationUtil {
  private final static Logger log = LoggerFactory.getLogger(CircuitconsolidationUtil.class);

  public static String getFileContent(String fileName) throws IOException {
    String fileAsString = new String();
    InputStream is = ClassLoader.getSystemResourceAsStream(fileName);
    BufferedReader buf = new BufferedReader(new InputStreamReader(is));
    String line = buf.readLine();
    StringBuilder sb = new StringBuilder();
    while (line != null) {
      sb.append(line).append("\n");
      line = buf.readLine();
    }
    fileAsString = sb.toString();
    log.info("Contents : " + fileAsString);
    buf.close();
    return fileAsString;
  }


  public static String getnsForPlatformCiComponents(String ns, String platformName, String ooPhase,
      String envName) {

    switch (ooPhase) {
      case IConstants.DESIGN_PHASE:

        return ns + "/_design/" + platformName;
      case IConstants.TRANSITION_PHASE:

        return ns + "/" + envName + "/manifest/" + platformName + "/1";
      case IConstants.OPERATE_PHASE:
        return ns + "/" + envName + "/bom/" + platformName + "/1";

      default:
        log.error("ooPhase {} not supported", ooPhase);

        throw new UnSupportedOperation(ooPhase + " not supported");

    }

  }


  public static String getNsForRelease(String ns, String platformName, String ooPhase,
      String envName) {
    switch (ooPhase) {
      case IConstants.DESIGN_PHASE:
        return ns;

      case IConstants.TRANSITION_PHASE:
        return ns + "/" + envName + "/manifest";

      case IConstants.OPERATE_PHASE:
        return ns + "/" + envName + "/bom";

      default:
        log.error("ooPhase {} not supported", ooPhase);

        throw new UnSupportedOperation(ooPhase + " not supported");

    }
  }

}
