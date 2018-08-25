package com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.main;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.gson.Gson;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.config.IConstants;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.dal.KloopzCmDal;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.exception.UnSupportedOperation;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.exception.UnSupportedTransformationMappingException;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.model.CmsCIRelationAndRelationAttributesActionMappingsModel;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.util.CircuitconsolidationUtil;

public class CMSCIRelationsMappingsProcessor {


  private final Logger log = LoggerFactory.getLogger(getClass());

  Gson gson = new Gson();
  String ns;
  String platformName;
  String ooPhase;
  String envName;
  String nsForPlatformCiComponents;
  Connection conn;
  KloopzCmDal dal;


  CMSCIRelationsMappingsProcessor(String ns, String platformName, String ooPhase, String envName,
      Connection conn) {
    setNs(ns);
    setPlatformName(platformName);
    setOoPhase(ooPhase);
    setEnvName(envName);
    setConn(conn);
    setNsForPlatformCiComponents(
        CircuitconsolidationUtil.getnsForPlatformCiComponents(ns, platformName, ooPhase, envName));
    setDal(new KloopzCmDal(conn));
  }


  public void setDal(KloopzCmDal dal) {
    this.dal = dal;
  }


  public void setPlatformName(String platformName) {
    this.platformName = platformName;
  }

  public void setOoPhase(String ooPhase) {
    this.ooPhase = ooPhase;
  }

  public void setEnvName(String envName) {
    this.envName = envName;
  }

  public void setNs(String ns) {
    this.ns = ns;
  }


  public void setNsForPlatformCiComponents(String nsForPlatformCiComponents) {
    this.nsForPlatformCiComponents = nsForPlatformCiComponents;
  }

  public void setConn(Connection conn) {
    this.conn = conn;
  }



  /*
   * (CMSCI_RELATION,CREATE_RELATION) (CMSCI_RELATION,DELETE_RELATION)
   * (CMSCI_RELATION_ATTRIBUTE,ADD_RELATION_ATTRIBUTE)
   * 
   */
  public void processCMSCIRelationsMappings(
      List<CmsCIRelationAndRelationAttributesActionMappingsModel> mappingsList) {
    Gson gson = new Gson();

    for (CmsCIRelationAndRelationAttributesActionMappingsModel mapping : mappingsList) {

      String entityType = mapping.getEntityType();
      String action = mapping.getAction();

      if (entityType.equalsIgnoreCase("CMSCI_RELATION")) {
        switch (action) {
          case "CREATE_RELATION":
            process_CREATE_RELATION(mapping);

            break;
          case "DELETE_RELATION":
            process_DELETE_RELATION(mapping);

            break;
          default:
            throw new UnSupportedTransformationMappingException(
                "<action>: " + action + " for <entityType>: " + entityType
                    + "not supported, mapping record: " + gson.toJson(mapping));
        }

      } else if (entityType.equalsIgnoreCase("CMSCI_RELATION_ATTRIBUTE")) {

        switch (action) {
          case "ADD_RELATION_ATTRIBUTE":
            // process_ADD_RELATION_ATTRIBUTE(mapping);
            break;

          default:
            throw new UnSupportedTransformationMappingException(
                "<action>: " + action + " for <entityType>: " + entityType
                    + "not supported, mapping record: " + gson.toJson(mapping));
        }

      } else {
        throw new UnSupportedTransformationMappingException("<entityType>: " + entityType
            + " not supported, mapping record: " + gson.toJson(mapping));

      }



    }


  }

  private void process_CREATE_RELATION(
      CmsCIRelationAndRelationAttributesActionMappingsModel mapping) {


    log.info("\n\n");
    log.info("**************************************************************************");
    log.info("Begin: process_CREATE_RELATION() ");

    if (this.ooPhase.equals(IConstants.DESIGN_PHASE)
        || this.ooPhase.equals(IConstants.TRANSITION_PHASE)) {
      process_CREATE_RELATION_IN_DESIGN_TRANSITION_Phase(mapping);
    } else if (this.ooPhase.equals(IConstants.OPERATE_PHASE)) {
      process_CREATE_RELATION_IN_OPERATE_Phase(mapping);
    } else {

      throw new UnSupportedOperation("ooPhase: <" + this.ooPhase + "> not supported");
    }

    log.info("End: process_CREATE_RELATION() ");
    log.info("**************************************************************************");
    log.info("\n\n");

  }


  private void process_CREATE_RELATION_IN_DESIGN_TRANSITION_Phase(
      CmsCIRelationAndRelationAttributesActionMappingsModel mapping) {


    log.info("\n\n");
    log.info("---------------------------------------------------------------------------");
    log.info("Begin: process_CREATE_RELATION_IN_DESIGN_TRANSITION_Phase() ");

    String targetCMSCIRelationName = mapping.getTargetCmsCiRelationName();
    int targetCMSCIRelationId = mapping.getTargetCmsCiRelationId();
    String targetFromCMSCIClazzName = mapping.getTargetFromCmsCiClazzName();
    int targetFromCMSCIClazzId = mapping.getTargetFromCmsCiClazzId();
    String targetToCMSCIClazzName = mapping.getTargetToCmsCiClazzName();
    int targetToCMSCIClazzId = mapping.getTargetToCmsCiClazzId();
    String comments = IConstants.CIRCUIT_CONSOLIDATION_COMMENTS;


    int ci_state_id = 100;

    List<Integer> fromCiIds = new ArrayList<Integer>();

    if (targetCMSCIRelationName.contains("base.Requires")) {

      fromCiIds = dal.getCiIdsForNsClazzAndPlatformCiName(this.ns, targetFromCMSCIClazzName,
          this.platformName);
      if (fromCiIds.size() != 1) {
        throw new UnSupportedTransformationMappingException(
            "fromCiIds <" + fromCiIds + "> for clazzName <" + targetFromCMSCIClazzName
                + "> and platformName: " + this.platformName);
      }
      log.info("fromCiIds <{}> for base.Requires relation for clazzName: {}", fromCiIds.toString(),
          targetFromCMSCIClazzName);
    } else {
      fromCiIds = dal.getCiIdsForNsAndClazz(nsForPlatformCiComponents, targetFromCMSCIClazzName);
    }

    int nsId = dal.getNsIdForNsPath(this.nsForPlatformCiComponents);

    List<Integer> toCiIds =
        dal.getCiIdsForNsAndClazz(this.nsForPlatformCiComponents, targetToCMSCIClazzName);

    log.info(
        "creating CMSCIRelation targetCMSCIRelationName {} targetCMSCIRelationId {} targetFromCMSCIClazzName {}, targetFromCMSCIClazzId {}, "
            + "targetToCMSCIClazzName {} targetToCMSCIClazzId {}",
        targetCMSCIRelationName, targetCMSCIRelationId, targetFromCMSCIClazzName,
        targetFromCMSCIClazzId, targetToCMSCIClazzName, targetToCMSCIClazzId);

    log.info("creating CMSCIRelation fromCiIds <{}> To toCiIds <{}>", fromCiIds.toString(),
        toCiIds.toString());


    for (int fromCiId : fromCiIds) {
      for (int toCiId : toCiIds) {

        int ci_relation_id = dal.getNext_cm_pk_seqId();
        String relation_goid = fromCiId + "-" + targetCMSCIRelationId + "-" + toCiId;
        dal.createCMSCIRelation(ci_relation_id, nsId, fromCiId, relation_goid,
            targetCMSCIRelationId, toCiId, ci_state_id, comments);
      }

    }

    log.info("End: process_CREATE_RELATION_IN_DESIGN_TRANSITION_Phase() ");
    log.info("---------------------------------------------------------------------------");
    log.info("\n\n");

  }


  private void process_DELETE_RELATION(
      CmsCIRelationAndRelationAttributesActionMappingsModel mapping) {

    String sourceFromCmsCiClazzName = mapping.getSourceFromCmsCiClazzName();
    String sourceCmsCiRelationName = mapping.getSourceCmsCiRelationName();
    String sourceToCmsCiClazzName = mapping.getSourceToCmsCiClazzName();
    int sourceCmsCiRelationId = mapping.getSourceCmsCiRelationId();


    log.info(
        "Begin: process_DELETE_RELATION() **************************************************************************");
    List<Integer> fromCiIds = new ArrayList<Integer>();

    if (sourceCmsCiRelationName.contains("base.Requires")) {

      fromCiIds = dal.getCiIdsForNsClazzAndPlatformCiName(this.ns, sourceFromCmsCiClazzName,
          this.platformName);
      if (fromCiIds.size() != 1) {
        throw new UnSupportedTransformationMappingException(
            "fromCiIds <" + fromCiIds + "> for sourceFromCmsCiClazzName <"
                + sourceFromCmsCiClazzName + "> and platformName: " + this.platformName);
      }
      log.info("fromCiIds <{}> for base.Requires relation for sourceFromCmsCiClazzName: {}",
          fromCiIds.toString(), sourceFromCmsCiClazzName);
    } else {
      fromCiIds = dal.getCiIdsForNsAndClazz(nsForPlatformCiComponents, sourceFromCmsCiClazzName);
    }
    log.info("fromCiIds size: <{}>, fromCiIds: {} ", fromCiIds.size(), fromCiIds.toString());

    List<Integer> toCiIds =
        dal.getCiIdsForNsAndClazz(nsForPlatformCiComponents, mapping.getSourceToCmsCiClazzName());
    log.info("toCiIds size<{}>, toCiIds: {} ", toCiIds.size(), toCiIds.toString());

    dal.deleteCiRelations(sourceFromCmsCiClazzName, fromCiIds, sourceToCmsCiClazzName, toCiIds,
        sourceCmsCiRelationId, sourceCmsCiRelationName, this.nsForPlatformCiComponents);
    log.info(
        "End: process_DELETE_RELATION() **************************************************************************");
  }

  private void process_ADD_RELATION_ATTRIBUTE(
      CmsCIRelationAndRelationAttributesActionMappingsModel mapping) {


    String targetFromCMSCIClazzName = mapping.getTargetFromCmsCiClazzName();
    int targetFromCMSCIClazzId = mapping.getTargetFromCmsCiClazzId();

    String targetToCMSCIClazzName = mapping.getTargetToCmsCiClazzName();
    int targetToCMSCIClazzId = mapping.getTargetToCmsCiClazzId();


    String targetCMSCIRelationName = mapping.getTargetCmsCiRelationName();
    int targetCMSCIRelationId = mapping.getTargetCmsCiRelationId();

    String attributeName = mapping.getAttributeName();
    int attributeId = mapping.getAttributeId();

    String dfValue = mapping.getDfValue();
    String djValue = mapping.getDjValue();

    String owner = IConstants.CIRCUIT_CONSOLIDATION_USER;
    String comments = IConstants.CIRCUIT_CONSOLIDATION_COMMENTS;


    log.info(
        "creating CMSCIRelation attribute attributeName: {} attributeId {} for targetCMSCIRelationName {} targetCMSCIRelationId {}, "
            + "targetFromCMSCIClazzName {}, targetFromCMSCIClazzId {}, targetToCMSCIClazzName{} targetToCMSCIClazzId {} with dfValue {} , djValue {}",
        attributeName, attributeId, targetCMSCIRelationName, targetCMSCIRelationId,
        targetFromCMSCIClazzName, targetFromCMSCIClazzId, targetToCMSCIClazzName,
        targetToCMSCIClazzId, dfValue, djValue);

    List<Integer> ci_relation_ids =
        dal.getCMSCIRelationIds_By_Ns_RelationName_FromClazzToClazz(this.nsForPlatformCiComponents,
            targetCMSCIRelationName, targetFromCMSCIClazzName, targetToCMSCIClazzName);

    log.info("List of ci_relation_ids for transformation mapping for ADD_RELATION_ATTRIBUTE <{}>",
        ci_relation_ids.toString());

    for (int ci_relation_id : ci_relation_ids) {
      log.info("Adding CMSCIRelation attributes for ci_relation_id <{}>", ci_relation_id);
      int ci_rel_attribute_id = dal.getNext_cm_pk_seqId();

      dal.createCMSCIRelationAttribute(ci_rel_attribute_id, ci_relation_id, attributeId, dfValue,
          djValue, owner, comments);
    }


  }

  private void process_CREATE_RELATION_IN_OPERATE_Phase(
      CmsCIRelationAndRelationAttributesActionMappingsModel mapping) {

    log.info("\n\n");
    log.info("---------------------------------------------------------------------------");
    log.info("Begin: process_CREATE_RELATION_IN_OPERATE_Phase() ");

    String targetCMSCIRelationName = mapping.getTargetCmsCiRelationName();
    int targetCMSCIRelationId = mapping.getTargetCmsCiRelationId();
    String targetFromCMSCIClazzName = mapping.getTargetFromCmsCiClazzName();
    int targetFromCMSCIClazzId = mapping.getTargetFromCmsCiClazzId();
    String targetToCMSCIClazzName = mapping.getTargetToCmsCiClazzName();
    int targetToCMSCIClazzId = mapping.getTargetToCmsCiClazzId();
    String comments = IConstants.CIRCUIT_CONSOLIDATION_COMMENTS;


    int ci_state_id = 100;
    int nsId = dal.getNsIdForNsPath(this.nsForPlatformCiComponents);


    Map<String, Integer> fromCiIdsAndCiNamesMap = dal
        .getCiNamesAndCiIdsForNsAndClazz(this.nsForPlatformCiComponents, targetFromCMSCIClazzName);

    log.info("fromCiIdsAndCiNamesMap: {}", gson.toJson(fromCiIdsAndCiNamesMap));

    List<String> fromBomCiNamesList = new ArrayList<String>(fromCiIdsAndCiNamesMap.keySet());


    Map<String, Integer> toCiIdsAndCiNamesMap =
        dal.getCiNamesAndCiIdsForNsAndClazz(this.nsForPlatformCiComponents, targetToCMSCIClazzName);

    log.info("toCiIdsAndCiNamesMap: {}", gson.toJson(toCiIdsAndCiNamesMap));

    List<String> toBomCiNamesList = new ArrayList<String>(toCiIdsAndCiNamesMap.keySet());

    Map<String, Set<String>> fromBomCisAndToBomCisPairs =
        getFromBomCisAndToBomCisPairs(fromBomCiNamesList, toBomCiNamesList);
    log.info("fromBomCisAndToBomCisPairs: {}", gson.toJson(fromBomCisAndToBomCisPairs));

    for (String fromBomCiName : fromBomCisAndToBomCisPairs.keySet()) {
      Set<String> toBomCiNameSet = fromBomCisAndToBomCisPairs.get(fromBomCiName);

      for (String toBomCiName : toBomCiNameSet) {

        int fromCiId = fromCiIdsAndCiNamesMap.get(fromBomCiName);
        int toCiId = toCiIdsAndCiNamesMap.get(toBomCiName);

        log.info(
            "targetFromCMSCIClazzName {} - targetFromCMSCIClazzId {}, targetCMSCIRelationName {}, targetToCMSCIClazzName {} - targetToCMSCIClazzId {}",
            targetFromCMSCIClazzName, targetFromCMSCIClazzId, targetCMSCIRelationName,
            targetToCMSCIClazzName, targetToCMSCIClazzId);
        log.info("creating relation from {} >> {} >> {}", fromBomCiName, targetCMSCIRelationName,
            toBomCiName);
        log.info("creating relation from {} >> {} >> {}", fromCiId, targetCMSCIRelationId, toCiId);


        int ci_relation_id = dal.getNext_cm_pk_seqId();
        String relation_goid = fromCiId + "-" + targetCMSCIRelationId + "-" + toCiId;

        dal.createCMSCIRelation(ci_relation_id, nsId, fromCiId, relation_goid,
            targetCMSCIRelationId, toCiId, ci_state_id, comments);

      }

    }



    log.info("End: process_CREATE_RELATION_IN_OPERATE_Phase() ");
    log.info("---------------------------------------------------------------------------");
    log.info("\n\n");


  }

  private String getBomCiSuffix(String bomCiName) {
    
    String[] strArr= bomCiName.split("-");
   
    StringBuffer bomcCiSuffix=new StringBuffer();
    for (int i = strArr.length-2; i <= strArr.length-1; i++) {
      
      bomcCiSuffix=bomcCiSuffix.append("-").append(strArr[i]);
      
    }
    log.info("Full suffix: "+bomcCiSuffix);
    return new String(bomcCiSuffix);

  }
  
  private String getBomCiPrefix(String bomCiName) {
    
    String bomCiSuffix=getBomCiSuffix(bomCiName);
    String bomCiPrefix=bomCiName.substring(0, bomCiName.length()-bomCiSuffix.length());
    
    log.info("Full Prefix: "+bomCiPrefix);
    return new String(bomCiPrefix);

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

    return fromBomCisAndToBomCisPairsMap;

  }

}
