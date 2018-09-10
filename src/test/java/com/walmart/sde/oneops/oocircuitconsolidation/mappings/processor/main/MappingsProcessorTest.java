package com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.main;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import com.google.gson.Gson;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.config.IConstants;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.config.PostgressDbConnection;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.dal.KloopzCmDal;
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
     */
    String ns = "/TestOrg2/TestTransformtionOperatePhase17";
    String platformName = "guineapig-brown";

    // String ooPhase = IConstants.OPERATE_PHASE;
    String envName = "dev"; // null for design phase


    MappingsProcessorMain mappingsProcessorMain = new MappingsProcessorMain();


    Properties props = mappingsProcessorMain.loadTransformationConfigurations(host, user, pwd);
    Connection conn = PostgressDbConnection.getConnection(props);

    MappingsCache mappingsCache = new MappingsCache(conn);
    KloopzCmDal dal = new KloopzCmDal(conn);

   ReleaseManager releaseManager_DesignPhase =
        new ReleaseManager(ns, platformName, IConstants.DESIGN_PHASE, envName, dal);
    int releaseId_DesignPhase = releaseManager_DesignPhase.create_dj_release(0);

    Map<String, List> mappingsCacheForDeleteActions_designPhase =
        mappingsCache.createTransformtionMappingsCacheForDeleteActions(IConstants.DESIGN_PHASE);
    Map<String, List> mappingsCacheForNonDeleteActions_designPhase =
        mappingsCache.createTransformtionMappingsCacheForNonDeleteActions(IConstants.DESIGN_PHASE);

    log.info("mappingsCacheForDeleteActions_designPhase {}",
        mappingsCacheForDeleteActions_designPhase);

    log.info("mappingsCacheForNonDeleteActions_designPhase {}",
        mappingsCacheForNonDeleteActions_designPhase);

    mappingsProcessorMain.processMappings(mappingsCacheForDeleteActions_designPhase, ns,
        platformName, IConstants.DESIGN_PHASE, envName, dal, releaseId_DesignPhase);
    mappingsProcessorMain.processMappings(mappingsCacheForNonDeleteActions_designPhase, ns,
        platformName, IConstants.DESIGN_PHASE, envName, dal, releaseId_DesignPhase);


    Map<String, List> mappingsCacheForDeleteActions_transitionPhase =
        mappingsCache.createTransformtionMappingsCacheForDeleteActions(IConstants.TRANSITION_PHASE);
    Map<String, List> mappingsCacheForNonDeleteActions_transitionPhase = mappingsCache
        .createTransformtionMappingsCacheForNonDeleteActions(IConstants.TRANSITION_PHASE);

    log.info("mappingsCacheForDeleteActions_transitionPhase {}",
        mappingsCacheForDeleteActions_transitionPhase);

    log.info("mappingsCacheForNonDeleteActions_transitionPhase {}",
        mappingsCacheForNonDeleteActions_transitionPhase);

    ReleaseManager releaseManager_TransitionPhase =
        new ReleaseManager(ns, platformName, IConstants.TRANSITION_PHASE, envName, dal);
    int lastAppliedReleaseId_TransitionPhase =
        releaseManager_TransitionPhase.getLastApplied_dj_releaseForPhase(IConstants.DESIGN_PHASE);
    int releaseId_TransitionPhase =
        releaseManager_TransitionPhase.create_dj_release(lastAppliedReleaseId_TransitionPhase);

    mappingsProcessorMain.processMappings(mappingsCacheForDeleteActions_transitionPhase, ns,
        platformName, IConstants.TRANSITION_PHASE, envName, dal, releaseId_TransitionPhase);
    mappingsProcessorMain.processMappings(mappingsCacheForNonDeleteActions_transitionPhase, ns,
        platformName, IConstants.TRANSITION_PHASE, envName, dal, releaseId_TransitionPhase);

   
    Map<String, List> mappingsCacheForDeleteActions_operatePhase =
        mappingsCache.createTransformtionMappingsCacheForDeleteActions(IConstants.OPERATE_PHASE);
    Map<String, List> mappingsCacheForNonDeleteActions_operatePhase =
        mappingsCache.createTransformtionMappingsCacheForNonDeleteActions(IConstants.OPERATE_PHASE);

    log.info("mappingsCacheForDeleteActions_operatePhase {}",
        mappingsCacheForDeleteActions_operatePhase);

    log.info("mappingsCacheForNonDeleteActions_operatePhase {}",
        mappingsCacheForNonDeleteActions_operatePhase);

    ReleaseManager releaseManager_OperatePhase =
        new ReleaseManager(ns, platformName, IConstants.OPERATE_PHASE, envName, dal);

    int lastAppliedReleaseId_OperatePhase =
        releaseManager_OperatePhase.getLastApplied_dj_releaseForPhase(IConstants.TRANSITION_PHASE);

    int releaseId_OperatePhase =
        releaseManager_OperatePhase.create_dj_release(lastAppliedReleaseId_OperatePhase);

    mappingsProcessorMain.processMappings(mappingsCacheForDeleteActions_operatePhase, ns,
        platformName, IConstants.OPERATE_PHASE, envName, dal, releaseId_OperatePhase);
    mappingsProcessorMain.processMappings(mappingsCacheForNonDeleteActions_operatePhase, ns,
        platformName, IConstants.OPERATE_PHASE, envName, dal, releaseId_OperatePhase);



    log.info("Transformation implemented, commiting transaction..");
    log.info("**********************************************************************");
    conn.commit();
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

  @Test(enabled = false)
  private void organizeCisForRelations() {

    String compute1 = "compute-214116-1";
    String compute2 = "compute-214116-3";
    String compute3 = "compute-214116-2";
    String compute4 = "compute-214117-1";
    String compute5 = "compute-214117-3";
    String compute6 = "compute-214117-2";

    List<String> bomComputeCisNamesList = new ArrayList<String>();
    bomComputeCisNamesList.add(compute1);
    bomComputeCisNamesList.add(compute2);
    bomComputeCisNamesList.add(compute3);
    bomComputeCisNamesList.add(compute4);
    bomComputeCisNamesList.add(compute5);
    bomComputeCisNamesList.add(compute6);

    String os1 = "os-214116-1";
    String os2 = "os-214116-3";
    String os3 = "os-214116-2";
    String os4 = "os-214117-1";
    String os5 = "os-214117-3";
    String os6 = "os-214117-2";

    String osV2_1 = "osV2-214116-1";
    String osV2_2 = "osV2-214116-3";
    String osV2_3 = "osV2-214116-2";
    String osV2_4 = "osV2-214117-1";
    String osV2_5 = "osV2-214117-3";
    String osV2_6 = "osV2-214117-2";


    List<String> bomOsCisNamesList = new ArrayList<String>();
    bomOsCisNamesList.add(os1);
    bomOsCisNamesList.add(os2);
    bomOsCisNamesList.add(os3);
    bomOsCisNamesList.add(os4);
    bomOsCisNamesList.add(os5);
    bomOsCisNamesList.add(os6);

    bomOsCisNamesList.add(osV2_1);
    bomOsCisNamesList.add(osV2_2);
    bomOsCisNamesList.add(osV2_3);
    bomOsCisNamesList.add(osV2_4);
    bomOsCisNamesList.add(osV2_5);
    bomOsCisNamesList.add(osV2_6);



    Map<String, String> ciNamesOnetoOneMapForDeployments = new HashMap<String, String>();

    for (String bomComputeCiName : bomComputeCisNamesList) {

      log.info("bomComputeCiName: " + bomComputeCiName);

      String ciDeploymentCode =
          bomComputeCiName.substring(bomComputeCiName.indexOf("-"), bomComputeCiName.length());
      log.info("ciDeploymentCode: " + ciDeploymentCode);
      for (String bomOsCisName : bomOsCisNamesList) {

        if (bomOsCisName.contains(ciDeploymentCode)) {
          // bomComputeCisNamesList.remove(bomComputeCiName);
          // bomOsCisNamesList.remove(bomOsCisName);
          ciNamesOnetoOneMapForDeployments.put(bomComputeCiName, bomOsCisName);

          continue;
        }

      }

    }

    log.info("ciNamesOnetoOneMapForDeployments: {}", gson.toJson(ciNamesOnetoOneMapForDeployments));
  }

  @Test(enabled = false)
  private void organizeCisForRelations2() {

    String compute1 = "compute-214116-1";
    String compute2 = "compute-214116-3";
    String compute3 = "compute-214116-2";
    String compute4 = "compute-214117-1";
    String compute5 = "compute-214117-3";
    String compute6 = "compute-214117-2";

    List<String> bomComputeCisNamesList = new ArrayList<String>();
    bomComputeCisNamesList.add(compute1);
    bomComputeCisNamesList.add(compute2);
    bomComputeCisNamesList.add(compute3);
    bomComputeCisNamesList.add(compute4);
    bomComputeCisNamesList.add(compute5);
    bomComputeCisNamesList.add(compute6);

    String os1 = "os-214116-1";
    String os2 = "os-214116-3";
    String os3 = "os-214116-2";
    String os4 = "os-214117-1";
    String os5 = "os-214117-3";
    String os6 = "os-214117-2";

    String osV2_1 = "osV2-214116-1";
    String osV2_2 = "osV2-214116-3";
    String osV2_3 = "osV2-214116-2";
    String osV2_4 = "osV2-214117-1";
    String osV2_5 = "osV2-214117-3";
    String osV2_6 = "osV2-214117-2";


    List<String> bomOsCisNamesList = new ArrayList<String>();
    bomOsCisNamesList.add(os1);
    bomOsCisNamesList.add(os2);
    bomOsCisNamesList.add(os3);
    bomOsCisNamesList.add(os4);
    bomOsCisNamesList.add(os5);
    bomOsCisNamesList.add(os6);

    bomOsCisNamesList.add(osV2_1);
    bomOsCisNamesList.add(osV2_2);
    bomOsCisNamesList.add(osV2_3);
    bomOsCisNamesList.add(osV2_4);
    bomOsCisNamesList.add(osV2_5);
    bomOsCisNamesList.add(osV2_6);


    System.exit(0);

    Map<String, String> ciNamesOnetoOneMapForDeploymentsMap = new HashMap<String, String>();
    Map<String, Set<String>> ciNamesOnetoOneMapForDeploymentsMapSet =
        new HashMap<String, Set<String>>();
    Map<String, List<String>> ciNamesOnetoOneMapForDeploymentsMapList =
        new HashMap<String, List<String>>();


    Set<String> bomOsCiNamePrefixList = new HashSet<String>();

    for (String bomOsCisName : bomOsCisNamesList) {

      bomOsCiNamePrefixList.add(getBomCiPrefix(bomOsCisName));

    }
    log.info("bomOsCiNamePrefixList: " + gson.toJson(bomOsCiNamePrefixList));

    for (String bomComputeCiName : bomComputeCisNamesList) {

      log.info("bomComputeCiName: " + bomComputeCiName);
      String bomcCiSuffix = getBomCiSuffix(bomComputeCiName);

      Set<String> set = new HashSet<String>();
      List<String> list = new ArrayList<String>();
      for (String bomOsCisName : bomOsCisNamesList) {

        if (bomOsCisName.contains(bomcCiSuffix)) {
          log.info("bomOsCisName will be added: " + bomOsCisName);
          set.add(bomOsCisName);
          list.add(bomOsCisName);
          ciNamesOnetoOneMapForDeploymentsMap.put(bomComputeCiName, bomOsCisName);
          log.info(bomComputeCiName + ": curent vakue of set " + set);

          ciNamesOnetoOneMapForDeploymentsMapSet.put(bomComputeCiName, set);
          ciNamesOnetoOneMapForDeploymentsMapList.put(bomComputeCiName, list);
          continue;
        }


      }

    }

    log.info("ciNamesOnetoOneMapForDeploymentsMap: {}",
        gson.toJson(ciNamesOnetoOneMapForDeploymentsMap));
    log.info("ciNamesOnetoOneMapForDeploymentsMapSet: {}",
        gson.toJson(ciNamesOnetoOneMapForDeploymentsMapSet));
    log.info("ciNamesOnetoOneMapForDeploymentsMapList: {}",
        gson.toJson(ciNamesOnetoOneMapForDeploymentsMapList));

  }

  private String getBomCiPrefix(String bomCiName) {
    String bomcCiPrefix = bomCiName.substring(0, bomCiName.indexOf("-"));
    log.info("bomcCiPrefix: " + bomcCiPrefix);
    return bomcCiPrefix;

  }

  private String getBomCiSuffix(String bomCiName) {

    String bomcCiSuffix = bomCiName.substring(bomCiName.indexOf("-"), bomCiName.length());
    log.info("bomcCiSuffix: " + bomcCiSuffix);
    return bomcCiSuffix;
  }

  private Map<String, Set<String>> getFromBomCisAndToBomCisPairs(List<String> fromBomCiNamesList,
      List<String> toBomCiNamesList) {

    Map<String, Set<String>> fromBomCisAndToBomCisPairsMap = new HashMap<String, Set<String>>();


    for (String fromBomCiName : fromBomCiNamesList) {

      log.info("fromBomCiName: " + fromBomCiName);
      String fromBomCiNameSuffix = getBomCiSuffix(fromBomCiName);

      Set<String> set = new HashSet<String>();

      for (String toBomCiName : toBomCiNamesList) {

        if (toBomCiName.contains(fromBomCiNameSuffix)) {

          set.add(toBomCiName);
          fromBomCisAndToBomCisPairsMap.put(fromBomCiName, set);
          continue;
        }

      }
      fromBomCisAndToBomCisPairsMap.put(fromBomCiName, set);
    }

    log.info("fromBomCisAndToBomCisPairsMap: {}", gson.toJson(fromBomCisAndToBomCisPairsMap));
    return fromBomCisAndToBomCisPairsMap;

  }


  @Test(enabled = false)
  private void organizeCisForRelations3() {

    HashMap<String, Integer> fromCiIdAndCiNamesMap = new HashMap<String, Integer>();

    fromCiIdAndCiNamesMap.put("compute-214116-1", 101);
    fromCiIdAndCiNamesMap.put("compute-214116-3", 102);
    fromCiIdAndCiNamesMap.put("compute-214116-2", 103);
    fromCiIdAndCiNamesMap.put("compute-214117-1", 104);
    fromCiIdAndCiNamesMap.put("compute-214117-3", 105);
    fromCiIdAndCiNamesMap.put("compute-214117-2", 106);


    log.info("fromCiIdAndCiNamesMap: {}", gson.toJson(fromCiIdAndCiNamesMap));

    List<Integer> fromBomCiIdsList = new ArrayList<Integer>(fromCiIdAndCiNamesMap.values());
    List<String> fromBomCiNameList = new ArrayList<String>(fromCiIdAndCiNamesMap.keySet());

    HashMap<String, Integer> toCiIdAndCiNamesMap = new HashMap<String, Integer>();


    toCiIdAndCiNamesMap.put("os-214116-1", 1);
    toCiIdAndCiNamesMap.put("os-214116-3", 2);
    toCiIdAndCiNamesMap.put("os-214116-2", 3);
    toCiIdAndCiNamesMap.put("os-214117-1", 4);
    toCiIdAndCiNamesMap.put("os-214117-3", 5);
    toCiIdAndCiNamesMap.put("os-214117-2", 6);

    toCiIdAndCiNamesMap.put("osV2-214116-1", 11);
    toCiIdAndCiNamesMap.put("osV2-214116-3", 12);
    toCiIdAndCiNamesMap.put("osV2-214116-2", 13);
    toCiIdAndCiNamesMap.put("osV2-214117-1", 14);
    toCiIdAndCiNamesMap.put("osV2-214117-3", 15);
    toCiIdAndCiNamesMap.put("osV2-214117-2", 16);

    log.info("toCiIdAndCiNamesMap: {}", gson.toJson(toCiIdAndCiNamesMap));

    List<Integer> toBomCiIdsList = new ArrayList<Integer>(toCiIdAndCiNamesMap.values());
    List<String> toBomCiNameList = new ArrayList<String>(toCiIdAndCiNamesMap.keySet());

    Map<String, Set<String>> fromBomCisAndToBomCisPairs =
        getFromBomCisAndToBomCisPairs(fromBomCiNameList, toBomCiNameList);

    String relationName = "TestRelationName";

    for (String fromBomCiName : fromBomCisAndToBomCisPairs.keySet()) {
      Set<String> toBomCiNameSet = fromBomCisAndToBomCisPairs.get(fromBomCiName);

      for (String toBomCiName : toBomCiNameSet) {
        log.info("{}>> {}>> {}", fromBomCiName, relationName, toBomCiName);
        log.info("{}>> {}>> {}", fromCiIdAndCiNamesMap.get(fromBomCiName), relationName,
            toCiIdAndCiNamesMap.get(toBomCiName));


      }

    }
  }

  @Test(enabled = false)
  private void test2() {

    getBomCiSuffixV2("keyspace-214116-1");
    getBomCiSuffixV2("ring-214116-1");
    getBomCiSuffixV2("keyspace-reaper-214116-1");
    getBomCiSuffixV2("keyspace-order_service_analyze-214116-1");
    getBomCiSuffixV2("keyspace-order_service-214116-1");
    getBomCiSuffixV2("keyspace-pricing_service_analyze-214116-1");
    getBomCiSuffixV2("keyspace-product-214116-1");
    getBomCiSuffixV2("keyspace-systemtraces-214116-1");
    getBomCiSuffixV2("keyspace-product_service_analyze-214116-1");
    getBomCiSuffixV2("keyspace-club_finder-214116-1");
    getBomCiSuffixV2("keyspace-symphony-214116-1");
    getBomCiSuffixV2("keyspace-pricing_service-214116-1");
    getBomCiSuffixV2("keyspace_dbaasuser-214116-1");
    getBomCiSuffixV2("ring-214116-1");

    getBomCiPrefixV2("keyspace-214116-1");
    getBomCiPrefixV2("ring-214116-1");
    getBomCiPrefixV2("keyspace-reaper-214116-1");
    getBomCiPrefixV2("keyspace-order_service_analyze-214116-1");
    getBomCiPrefixV2("keyspace-order_service-214116-1");
    getBomCiPrefixV2("keyspace-pricing_service_analyze-214116-1");
    getBomCiPrefixV2("keyspace-product-214116-1");
    getBomCiPrefixV2("keyspace-systemtraces-214116-1");
    getBomCiPrefixV2("keyspace-product_service_analyze-214116-1");
    getBomCiPrefixV2("keyspace-club_finder-214116-1");
    getBomCiPrefixV2("keyspace-symphony-214116-1");
    getBomCiPrefixV2("keyspace-pricing_service-214116-1");
    getBomCiPrefixV2("keyspace_dbaasuser-214116-1");
    getBomCiPrefixV2("ring-214116-1");



  }

  private String getBomCiSuffixV2(String bomCiName) {

    String[] strArr = bomCiName.split("-");

    StringBuffer bomcCiSuffix = new StringBuffer();
    for (int i = strArr.length - 1; i >= strArr.length - 2; i--) {
      bomcCiSuffix = bomcCiSuffix.append("-").append(strArr[i]);

    }
    log.info("Full suffix: " + bomcCiSuffix);

    StringBuffer bomcCiSuffix2 = new StringBuffer();
    for (int i = strArr.length - 2; i <= strArr.length - 1; i++) {

      bomcCiSuffix2 = bomcCiSuffix2.append("-").append(strArr[i]);

    }
    log.info("Full bomcCiSuffix2: " + bomcCiSuffix2);

    return new String(bomcCiSuffix);

  }

  private String getBomCiPrefixV2(String bomCiName) {

    String bomCiSuffix = getBomCiSuffixV2(bomCiName);
    String bomCiPrefix = bomCiName.substring(0, bomCiName.length() - bomCiSuffix.length());

    log.info("Full Prefix: " + bomCiPrefix);
    return new String(bomCiPrefix);

  }

  @Test(enabled = false)
  private void getCiNamesAndCiIdsMapForCloudCis() {

    try {

      Map<String, Integer> bomComputeCiNamesAndCiIdsMap = new HashMap<String, Integer>();

      bomComputeCiNamesAndCiIdsMap.put("compute-214116-1", 317395);
      bomComputeCiNamesAndCiIdsMap.put("compute-214116-3", 317401);
      bomComputeCiNamesAndCiIdsMap.put("compute-214116-2", 317398);

      Map<String, Integer> ciNamesAndCiIdsMapForCloudCis = new HashMap<String, Integer>();

      Set<Integer> clouidCiIdSet = new HashSet<Integer>();
      for (String bomComputeCiName : bomComputeCiNamesAndCiIdsMap.keySet()) {

        String[] bomComputeCiNameArr = bomComputeCiName.split("-");
        int clouidCiId = new Integer(bomComputeCiNameArr[bomComputeCiNameArr.length - 2]);
        log.info("clouidCiId {} from bomCompute ci name {} " + clouidCiId, bomComputeCiName);
        if (clouidCiId == 0) {
          throw new UnSupportedOperation(
              "bomComputeCiName <" + bomComputeCiName + "> generated 0 clouidCiId");
        }
        clouidCiIdSet.add(clouidCiId);

      }

      log.info("clouidCiIdSet: " + clouidCiIdSet);
      log.info("ciNamesAndCiIdsMapForCloudCis: " + ciNamesAndCiIdsMapForCloudCis);
    } catch (NullPointerException | ArrayIndexOutOfBoundsException e) {
      throw new UnSupportedOperation(
          "Error while processing bomComputeCiNamesAndCiIdsMap for parsing cloud ciId"
              + e.getMessage());
    }
  }


}
