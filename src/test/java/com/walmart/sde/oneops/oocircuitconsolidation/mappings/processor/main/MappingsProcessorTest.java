package com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.main;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import com.google.gson.Gson;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.config.IConstants;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.config.PostgressDbConnection;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.exception.UnSupportedOperation;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.util.MappingsCache;


public class MappingsProcessorTest {


  String host;
  String user;
  String pwd;

  private final Logger log = LoggerFactory.getLogger(getClass());


  private Gson gson = new Gson();

  @BeforeClass
  private void init() {

    host = System.getenv().get(IConstants.CMS_DB_HOST);
    user = System.getenv().get(IConstants.CMS_DB_USER);
    pwd = System.getenv().get(IConstants.CMS_DB_PASS);



  }


  @Test(enabled = true)
  private void testMappingsConfigFile() throws SQLException {

    /*
     * Begin: These settings were used for development purpose String ns="/TestOrg2/guineapigs1";
     * String platformName="guineapig-brown"; String ooPhase=IConstants.DESIGN_PHASE; String
     * envName=null; // null for design phase End: These settings were used for development purpose
     */
    /*
     * Begin: These settings were used for development purpose String ns="/TestOrg2/guineapigs1";
     * String platformName="guineapig-brown"; String ooPhase=IConstants.TRANSITION_PHASE; String
     * envName="dev"; // null for design phase
     */

    /*
     * String ns="/TestOrg2/TestTransformationAssembly6"; String platformName="TestCaseT6";
     * 
     * 
     */String ns = "/TestOrg2/TestTransformtionOperatePhase1";
    String platformName = "TestOpPhase1";

    String ooPhase = IConstants.OPERATE_PHASE;
    String envName = "dev"; // null for design phase


    MappingsProcessorMain mappingsProcessorMain = new MappingsProcessorMain();


    Properties props = mappingsProcessorMain.loadTransformationConfigurations(host, user, pwd);
    Connection conn = PostgressDbConnection.getConnection(props);

    MappingsCache mappingsCache = new MappingsCache();

    Map<String, List> transformationMappings =
        mappingsCache.createTransformationMappingsCache(conn, ooPhase);
    log.info("transformationMappings: " + gson.toJson(transformationMappings));

    mappingsProcessorMain.processMappings(transformationMappings, ns, platformName, ooPhase,
        envName, conn);

    log.info("Transformation implemented, commiting transaction..");
    log.info("**********************************************************************");
    // conn.commit();
    log.info("Transformation committed to database");
    log.info("**********************************************************************");
    log.info("closing connection: ");
    conn.close();
    log.info("closed connection: ");

  }


  @Test(enabled = false)
  private void rough() {

    String str1 = "compute-214116-1";
    String str2 = "compute-214116-3";
    String str3 = "compute-214116-2";
    String str4 = "compute-214117-1";
    String str5 = "compute-214117-3";
    String str6 = "compute-214117-2";

    List<String> bomComputeCisNamesList = new ArrayList<String>();
    bomComputeCisNamesList.add(str1);
    bomComputeCisNamesList.add(str2);
    bomComputeCisNamesList.add(str3);
    bomComputeCisNamesList.add(str4);
    bomComputeCisNamesList.add(str5);
    bomComputeCisNamesList.add(str6);

    Map<Integer, Integer> cloudAndInstancesMap = new HashMap<Integer, Integer>();
    for (String bomComputeCiName : bomComputeCisNamesList) {
      List<Integer> cloudCiIds = new ArrayList<Integer>();



      String[] strAtrr = bomComputeCiName.split("-");
      if (!strAtrr[0].equals("compute")) {

        throw new UnSupportedOperation(
            "input string do not contain <compute> keyword, its not a compute Ci");
      } else {

        int cloudCiId = new Integer(strAtrr[1]);
        int computeInstanceNumber;
        if (cloudAndInstancesMap.get(cloudCiId) != null) {
          computeInstanceNumber =
              Math.max(cloudAndInstancesMap.get(cloudCiId), new Integer(strAtrr[2]));

        } else {
          computeInstanceNumber = new Integer(strAtrr[2]);
        }

        cloudCiIds.add(cloudCiId);
        cloudAndInstancesMap.put(cloudCiId, computeInstanceNumber);

      }
      log.info("cloudCiIds: " + gson.toJson(cloudCiIds));


    }
    log.info("cloudAndInstancesMap: " + gson.toJson(cloudAndInstancesMap));
  }

  @Test(enabled = false)
  private void rough2() {

    String str1 = "compute-214116-1";
    String str2 = "compute-214116-3";
    String str3 = "co2mpute2-214116-2";
    String str4 = "compute-214117-1";
    String str5 = "compute-214117-3";
    String str6 = "compute-214117-2";

    List<String> bomComputeCisNamesList = new ArrayList<String>();
    bomComputeCisNamesList.add(str1);
    bomComputeCisNamesList.add(str2);
    bomComputeCisNamesList.add(str3);
    bomComputeCisNamesList.add(str4);
    bomComputeCisNamesList.add(str5);
    bomComputeCisNamesList.add(str6);

    Map<Integer, Integer> cloudAndInstancesMap = new HashMap<Integer, Integer>();
    for (String bomComputeCiName : bomComputeCisNamesList) {
      List<Integer> cloudCiIds = new ArrayList<Integer>();


      log.info(bomComputeCiName.substring("compute".length(), bomComputeCiName.length()));
      log.info(bomComputeCiName.replace("compute2", "os"));

  }

  }
}
