package com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.main;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import com.google.gson.Gson;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.config.IConstants;

public class CMSCIRelationsMappingsProcessorTest {

  private final Logger log = LoggerFactory.getLogger(getClass());
  private Gson gson;
  CMSCIRelationsMappingsProcessor cMSCIRelationsMappingsProcessor;

  @BeforeMethod
  private void init() {
    gson = new Gson();
    String ns = "/TestOrg2/TestTransformtionOperatePhase27";
    String platformName = "guineapig-brown";
    String envName = "dev";
    cMSCIRelationsMappingsProcessor = new CMSCIRelationsMappingsProcessor(ns, platformName,
        IConstants.DESIGN_PHASE, envName, null, 0);


  }


  @Test(enabled = true)
  private void testGetFromManifestCisAndToBomCisPairs() {

    Map<String, Integer> fromManifestCiNamesAndCiIdsMap = new HashMap<String, Integer>();

    fromManifestCiNamesAndCiIdsMap.put("compute", 111111);
   
    fromManifestCiNamesAndCiIdsMap.put("keyspace", 458424);
    fromManifestCiNamesAndCiIdsMap.put("keyspace_dbaasuser", 458437);
    fromManifestCiNamesAndCiIdsMap.put("keyspace-order_service_analyze", 458418);
    fromManifestCiNamesAndCiIdsMap.put("keyspace-pricing_service_analyze", 458410);
    fromManifestCiNamesAndCiIdsMap.put("keyspace-pricing_service", 458416);
    fromManifestCiNamesAndCiIdsMap.put("keyspace-systemtraces", 458420);
    fromManifestCiNamesAndCiIdsMap.put("keyspace-product_service_analyze", 458406);
    fromManifestCiNamesAndCiIdsMap.put("keyspace-order_service", 458414);
    fromManifestCiNamesAndCiIdsMap.put("keyspace-product", 458412);
    fromManifestCiNamesAndCiIdsMap.put("keyspace-reaper", 458404);
    fromManifestCiNamesAndCiIdsMap.put("keyspace-club_finder", 458422);
    fromManifestCiNamesAndCiIdsMap.put("keyspace-symphony", 458408);


    Map<String, Integer> toBomCiNamesAndCiIdsMap = new HashMap<String, Integer>();

    toBomCiNamesAndCiIdsMap.put("compute-214116-1", 2222222);
    toBomCiNamesAndCiIdsMap.put("compute-214116-2", 2222222);
    toBomCiNamesAndCiIdsMap.put("compute-214116-3", 2222222);
    
    toBomCiNamesAndCiIdsMap.put("keyspace-214116-1", 459505);
    toBomCiNamesAndCiIdsMap.put("keyspace-reaper-214116-1", 459535);
    toBomCiNamesAndCiIdsMap.put("keyspace-order_service_analyze-214116-1", 459514);
    toBomCiNamesAndCiIdsMap.put("keyspace-order_service-214116-1", 459520);
    toBomCiNamesAndCiIdsMap.put("keyspace-pricing_service_analyze-214116-1", 459526);
    toBomCiNamesAndCiIdsMap.put("keyspace-product-214116-1", 459523);
    toBomCiNamesAndCiIdsMap.put("keyspace-systemtraces-214116-1", 459511);
    toBomCiNamesAndCiIdsMap.put("keyspace-product_service_analyze-214116-1", 459532);
    toBomCiNamesAndCiIdsMap.put("keyspace-club_finder-214116-1", 459508);
    toBomCiNamesAndCiIdsMap.put("keyspace-symphony-214116-1", 459529);
    toBomCiNamesAndCiIdsMap.put("keyspace-pricing_service-214116-1", 459517);
    toBomCiNamesAndCiIdsMap.put("keyspace_dbaasuser-214116-1", 459502);


    List<String> fromBomCiNamesList = new ArrayList<>(fromManifestCiNamesAndCiIdsMap.keySet());
    List<String> toBomCiNamesList = new ArrayList<>(toBomCiNamesAndCiIdsMap.keySet());

    log.info("jsonified fromBomCiNamesList {}", gson.toJson(fromBomCiNamesList));
    log.info("jsonified toBomCiNamesList {}", gson.toJson(toBomCiNamesList));


    Map<String, Set<String>> fromManifestCisAndToBomCisPairsMap = cMSCIRelationsMappingsProcessor
        .getFromManifestCisAndToBomCisPairs(fromBomCiNamesList, toBomCiNamesList);

    log.info("jsonified fromManifestCisAndToBomCisPairsMap {}",
        gson.toJson(fromManifestCisAndToBomCisPairsMap));

    for (String fromManifestCisNameFromPair : fromManifestCisAndToBomCisPairsMap.keySet()) {

      Set<String> toBomCiNamesSet =
          fromManifestCisAndToBomCisPairsMap.get(fromManifestCisNameFromPair);

      log.info("fromManifestCisNameFromPair{} >> toBomCiNamesSet {}", fromManifestCisNameFromPair,
          gson.toJson(toBomCiNamesSet));

      for (String toBomCiName : toBomCiNamesSet) {
        log.info("{} >> {} ", fromManifestCisNameFromPair, toBomCiName);
        

      }

    }


  }


}
