package com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.main;

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
  KloopzCmDal dal;
  int releaseId;


  CMSCIRelationsMappingsProcessor(String ns, String platformName, String ooPhase, String envName,
      KloopzCmDal dal, int releaseId) {
    setNs(ns);
    setPlatformName(platformName);
    setOoPhase(ooPhase);
    setEnvName(envName);
    setDal(dal);
    setReleaseId(releaseId);
    setNsForPlatformCiComponents(
        CircuitconsolidationUtil.getnsForPlatformCiComponents(ns, platformName, ooPhase, envName));
  }


  public int getReleaseId() {
    return releaseId;
  }


  public void setReleaseId(int releaseId) {
    this.releaseId = releaseId;
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

          case "NO_ACTION":
            log.info("No action for mapping: " + gson.toJson(mapping));
            process_NO_ACTION(mapping);

            break;
          default:
            throw new UnSupportedTransformationMappingException(
                "<action>: " + action + " for <entityType>: " + entityType
                    + "not supported, mapping record: " + gson.toJson(mapping));
        }

      } else if (entityType.equalsIgnoreCase("CMSCI_RELATION_ATTRIBUTE")) {

        switch (action) {
          case "ADD_RELATION_ATTRIBUTE":
            process_ADD_RELATION_ATTRIBUTE(mapping);
            break;

          case "NO_ACTION":
            log.info("Noaction for mapping: " + gson.toJson(mapping));

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

  private void process_NO_ACTION(CmsCIRelationAndRelationAttributesActionMappingsModel mapping) {

    if (this.ooPhase.equalsIgnoreCase(IConstants.DESIGN_PHASE)
        || this.ooPhase.equalsIgnoreCase(IConstants.TRANSITION_PHASE)) {
      process_NO_ACTION_IN_DESIGN_TRANSITION_Phase(mapping);
    } else if (this.ooPhase.equalsIgnoreCase(IConstants.OPERATE_PHASE)) {
      process_NO_ACTION_IN_OPERATE_Phase(mapping);
    } else {

      throw new UnSupportedTransformationMappingException(
          "ooPhase " + this.ooPhase + "not supported");

    }



  }

  private void process_NO_ACTION_IN_DESIGN_TRANSITION_Phase(
      CmsCIRelationAndRelationAttributesActionMappingsModel mapping) {
    log.info("\n\n");
    log.info("**************************************************************************");
    log.info("Begin: process_NO_ACTION_IN_DESIGN_TRANSITION_Phase() ");

    String targetCMSCIRelationName = mapping.getTargetCmsCiRelationName();
    int targetCMSCIRelationId = mapping.getTargetCmsCiRelationId();
    String targetFromCMSCIClazzName = mapping.getTargetFromCmsCiClazzName();
    int targetFromCMSCIClazzId = mapping.getTargetFromCmsCiClazzId();
    String targetToCMSCIClazzName = mapping.getTargetToCmsCiClazzName();
    int targetToCMSCIClazzId = mapping.getTargetToCmsCiClazzId();


    if (targetCMSCIRelationName.equalsIgnoreCase("catalog.WatchedBy")
        || targetCMSCIRelationName.equalsIgnoreCase("manifest.WatchedBy")) {
      log.info("Ignore relation {} fromClazz {} toClazz {}", targetCMSCIRelationName,
          targetFromCMSCIClazzName, targetToCMSCIClazzName);
      log.info("Ignore relationId {} fromClazz {} toClazz {}", targetCMSCIRelationId,
          targetFromCMSCIClazzId, targetToCMSCIClazzId);
      return;
    }



    Map<Integer, String> fromCiIdsCiNamesMap = dal.getCiIdsAndCiNameForNsAndClazzMap(
        this.nsForPlatformCiComponents, targetFromCMSCIClazzName);

    Map<Integer, String> toCiIdsCiNamesMap = dal
        .getCiIdsAndCiNameForNsAndClazzMap(this.nsForPlatformCiComponents, targetToCMSCIClazzName);

    for (int fromCiId : fromCiIdsCiNamesMap.keySet()) {

      for (int toCiId : toCiIdsCiNamesMap.keySet()) {


        if (relationExistsFromCiIdToCiId(this.nsForPlatformCiComponents, targetCMSCIRelationName,
            fromCiId, toCiId)) {
          log.info("targetCMSCIRelationName {} exists fromClazz {} toClazz {} ",
              targetCMSCIRelationName, targetFromCMSCIClazzName, targetToCMSCIClazzName);
          log.info("targetCMSCIRelationId {} exists fromClazzId {} toClazzId {} ",
              targetCMSCIRelationId, targetFromCMSCIClazzId, targetToCMSCIClazzId);
          log.info("targetCMSCIRelationName {} exists fromCiId {} toCiId {} ",
              targetCMSCIRelationName, fromCiId, toCiId);

          log.info("RELATION FOUND!! No Action required");
        } else {

          log.info("RELATION NOT FOUND!! Check if Relation to be created!!");

          log.info("targetCMSCIRelationName {} does not exists fromClazz {} toClazz {} ",
              targetCMSCIRelationName, targetFromCMSCIClazzName, targetToCMSCIClazzName);
          log.info("targetCMSCIRelationId {} does not exists fromClazzId {} toClazzId {} ",
              targetCMSCIRelationId, targetFromCMSCIClazzId, targetToCMSCIClazzId);
          log.info("targetCMSCIRelationName {} does not exists fromCiId {} toCiId {} ",
              targetCMSCIRelationName, fromCiId, toCiId);

          String fromCiName = fromCiIdsCiNamesMap.get(fromCiId);
          String toCiName = toCiIdsCiNamesMap.get(toCiId);
          
          log.info("targetCMSCIRelationName {} does not exists fromCiName {} toCiName {} ",
              targetCMSCIRelationName, fromCiName, toCiName);
          

         

          if (targetCMSCIRelationName.equalsIgnoreCase("manifest.Entrypoint")
              && !toCiName.equalsIgnoreCase("fqdn")) {


            log.info("Ignore relation {} fromClazz {} toClazz {}", targetCMSCIRelationName,
                targetFromCMSCIClazzName, targetToCMSCIClazzName);
            log.info("Ignore relationId {} fromClazz {} toClazz {}", targetCMSCIRelationId,
                targetFromCMSCIClazzId, targetToCMSCIClazzId);

            log.info("Ignore relation {} fromCiName {} toCiName {}", targetCMSCIRelationName,
                fromCiName, toCiName);

            log.info("Ignore relation {} fromCiId {} toCiName {}", targetCMSCIRelationName,
                fromCiId, toCiId);

            log.info("RELATION NOT FOUND!! Relation will be ignored!!");


            return;
          }
          log.info("RELATION NOT FOUND!! Relation will be created!!");
          int nsId = dal.getNsIdForNsPath(this.nsForPlatformCiComponents);
          String comments = IConstants.CIRCUIT_CONSOLIDATION_COMMENTS;
          int ci_state_id = 100;
          createCMSCIRelation(nsId, fromCiId, targetCMSCIRelationId, toCiId, ci_state_id, comments);

          log.info("created relation {} fromClazz {} toClazz {}", targetCMSCIRelationName,
              targetFromCMSCIClazzName, targetToCMSCIClazzName);
          log.info("created relationId {} fromClazz {} toClazz {}", targetCMSCIRelationId,
              targetFromCMSCIClazzId, targetToCMSCIClazzId);

          log.info("created relation {} fromCiName {} toCiName {}", targetCMSCIRelationName,
              fromCiName, toCiName);

          log.info("created relation {} fromCiId {} toCiName {}", targetCMSCIRelationName, fromCiId,
              toCiId);

        }

      }

    }



    log.info("End: process_NO_ACTION_IN_DESIGN_TRANSITION_Phase() ");
    log.info("**************************************************************************");
    log.info("\n\n");

  }


  private void process_NO_ACTION_IN_OPERATE_Phase(
      CmsCIRelationAndRelationAttributesActionMappingsModel mapping) {
    log.info("\n\n");
    log.info("**************************************************************************");
    log.info("Begin: process_NO_ACTION_IN_OPERATE_Phase() ");

    String targetCMSCIRelationName = mapping.getTargetCmsCiRelationName();
    int targetCMSCIRelationId = mapping.getTargetCmsCiRelationId();
    String targetFromCMSCIClazzName = mapping.getTargetFromCmsCiClazzName();
    int targetFromCMSCIClazzId = mapping.getTargetFromCmsCiClazzId();
    String targetToCMSCIClazzName = mapping.getTargetToCmsCiClazzName();
    int targetToCMSCIClazzId = mapping.getTargetToCmsCiClazzId();


    // begins logic

    // TODO: In progress

    Map<String, Integer> fromCiIdsAndCiNamesMap = new HashMap<String, Integer>();
    Map<String, Integer> toCiIdsAndCiNamesMap = new HashMap<String, Integer>();

    if (targetFromCMSCIClazzName.contains("bom.oneops.1.")) {

      fromCiIdsAndCiNamesMap = dal.getCiNamesAndCiIdsMapForNsAndClazz(
          this.nsForPlatformCiComponents, targetFromCMSCIClazzName);

      // TODO: logic yet to be implemented
    } else if (targetFromCMSCIClazzName.contains("manifest.")) {

      String transitonPhaseNsPath = CircuitconsolidationUtil.getnsForPlatformCiComponents(this.ns,
          this.platformName, IConstants.TRANSITION_PHASE, this.envName);
      fromCiIdsAndCiNamesMap =
          dal.getCiNamesAndCiIdsMapForNsAndClazz(transitonPhaseNsPath, targetFromCMSCIClazzName);

      log.info("fromCiIdsAndCiNamesMap: {}", gson.toJson(fromCiIdsAndCiNamesMap));

      toCiIdsAndCiNamesMap = dal.getCiNamesAndCiIdsMapForNsAndClazz(this.nsForPlatformCiComponents,
          targetToCMSCIClazzName);

      log.info("toCiIdsAndCiNamesMap: {}", gson.toJson(toCiIdsAndCiNamesMap));


      List<Integer> fromManifestCiIdsList = new ArrayList<Integer>(fromCiIdsAndCiNamesMap.values());
      List<Integer> toBomCiIdsList = new ArrayList<Integer>(toCiIdsAndCiNamesMap.values());

      for (int fromManifestCiId : fromManifestCiIdsList) {
        for (int toBomCiId : toBomCiIdsList) {
          List<Integer> relations = dal.getCMSCIRelationIds_By_Ns_RelationName_FromCiIdToCiId(
              this.nsForPlatformCiComponents, targetCMSCIRelationName, fromManifestCiId, toBomCiId);
          if (relations.size() != 1) {

            log.info("RELATION DOES NOT EXIST!!! Need to create relation");
            // process_CREATE_RELATION_IN_OPERATE_Phase(mapping);
            
            //TODO: Test this scenario tomorrow
          } else {

            log.info("RELATION DOES EXIST!!! No Action Required");
          }

        }
      }



    } else {
      throw new UnSupportedTransformationMappingException(
          "This scenario was not identified for mapping : " + gson.toJson(mapping));
    }


    // TODO: In progress



    log.info("End: process_NO_ACTION_IN_OPERATE_Phase() ");
    log.info("**************************************************************************");
    log.info("\n\n");

  }

  private boolean relationExistsFromCiIdToCiId(String nsForPlatformCiComponents2,
      String targetCMSCIRelationName, int fromCiId, int toCiId) {

    List<Integer> ci_relation_ids = dal.getCMSCIRelationIds_By_Ns_RelationName_FromCiIdToCiId(
        this.nsForPlatformCiComponents, targetCMSCIRelationName, fromCiId, toCiId);

    if (ci_relation_ids.size() != 0) {
      return true;
    }

    return false;
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
    String sourceFromCmsCiClazzName = mapping.getSourceFromCmsCiClazzName();
    List<Integer> fromCiIds = new ArrayList<Integer>();
    Map<Integer, String> fromCiIdsAndNamesMap = new HashMap<Integer, String>();
    if (targetCMSCIRelationName.contains("base.Requires")) {


      fromCiIdsAndNamesMap = dal.getCiIdsForNsClazzAndPlatformCiName(this.ns,
          targetFromCMSCIClazzName, this.platformName);

      fromCiIds = new ArrayList<>(fromCiIdsAndNamesMap.keySet());

      if (fromCiIds.size() != 1) {
        throw new UnSupportedTransformationMappingException(
            "fromCiIds <" + fromCiIds + "> for clazzName <" + targetFromCMSCIClazzName
                + "> and platformName: " + this.platformName);
      }
      log.info("fromCiIds <{}> for base.Requires relation for clazzName: {}", fromCiIds.toString(),
          targetFromCMSCIClazzName);
    } else {


      fromCiIdsAndNamesMap = dal.getCiIdsAndCiNameForNsAndClazzMap(this.nsForPlatformCiComponents,
          targetFromCMSCIClazzName);
      fromCiIds = new ArrayList<>(fromCiIdsAndNamesMap.keySet());

    }

    int nsId = dal.getNsIdForNsPath(this.nsForPlatformCiComponents);

    /*
     * List<Integer> toCiIds = dal.getCiIdsForNsAndClazz(this.nsForPlatformCiComponents,
     * targetToCMSCIClazzName);
     */

    Map<Integer, String> toCiIdsAndNamesMap = dal
        .getCiIdsAndCiNameForNsAndClazzMap(this.nsForPlatformCiComponents, targetToCMSCIClazzName);
    List<Integer> toCiIds = new ArrayList<>(toCiIdsAndNamesMap.keySet());

    log.info(
        "creating CMSCIRelation targetCMSCIRelationName {} targetCMSCIRelationId {} targetFromCMSCIClazzName {}, targetFromCMSCIClazzId {}, "
            + "targetToCMSCIClazzName {} targetToCMSCIClazzId {}",
        targetCMSCIRelationName, targetCMSCIRelationId, targetFromCMSCIClazzName,
        targetFromCMSCIClazzId, targetToCMSCIClazzName, targetToCMSCIClazzId);

    log.info("creating CMSCIRelation {} fromClazz <{}> To toClazz <{}>", targetCMSCIRelationName,
        targetFromCMSCIClazzName, targetToCMSCIClazzName);
    log.info("creating CMSCIRelation {} fromClazzId <{}> To toClazzId <{}>", targetCMSCIRelationId,
        targetFromCMSCIClazzId, targetToCMSCIClazzId);
    log.info("creating CMSCIRelationId {} fromCiIds <{}> To toCiIds <{}>", targetCMSCIRelationName,
        fromCiIds.toString(), toCiIds.toString());


    for (int fromCiId : fromCiIds) {
      for (int toCiId : toCiIds) {
        String fromCiName = fromCiIdsAndNamesMap.get(fromCiId);
        String toCiName = toCiIdsAndNamesMap.get(toCiId);

        if ((targetCMSCIRelationName.contains("catalog.DependsOn")
            || targetCMSCIRelationName.contains("manifest.DependsOn"))
            && fromCiName.equalsIgnoreCase("fqdn") && toCiName.equalsIgnoreCase("os")) {
          log.info(
              "relation {} from fromCiName {} fromCiId {} >> toCiName {} toCiId {} will be ignored as an exceptional case",
              targetCMSCIRelationName, fromCiName, fromCiId, toCiName, toCiId);
          continue;

        }

        createCMSCIRelation(nsId, fromCiId, targetCMSCIRelationId, toCiId, ci_state_id, comments);
        log.info("created CMSCIRelationId {} fromCiIds <{}> To toCiIds <{}>",
            targetCMSCIRelationName, fromCiId, toCiId);


      }

    }

    log.info("End: process_CREATE_RELATION_IN_DESIGN_TRANSITION_Phase() ");
    log.info("---------------------------------------------------------------------------");
    log.info("\n\n");

  }


  private void createCMSCIRelation(int nsId, int fromCiId, int targetCMSCIRelationId, int toCiId,
      int ci_state_id, String comments) {

    int ci_relation_id = dal.getNext_cm_pk_seqId();
    String relation_goid = fromCiId + "-" + targetCMSCIRelationId + "-" + toCiId;

    dal.createCMSCIRelation(ci_relation_id, nsId, fromCiId, relation_goid, targetCMSCIRelationId,
        toCiId, ci_state_id, comments);

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

      Map<Integer, String> fromCiIdsAndNamesMap = dal.getCiIdsForNsClazzAndPlatformCiName(this.ns,
          sourceFromCmsCiClazzName, this.platformName);
      fromCiIds = new ArrayList<>(fromCiIdsAndNamesMap.keySet());

      if (fromCiIds.size() != 1) {
        throw new UnSupportedTransformationMappingException(
            "fromCiIds <" + fromCiIds + "> for sourceFromCmsCiClazzName <"
                + sourceFromCmsCiClazzName + "> and platformName: " + this.platformName);
      }
      log.info("fromCiIds <{}> for base.Requires relation for sourceFromCmsCiClazzName: {}",
          fromCiIds.toString(), sourceFromCmsCiClazzName);
    } else {
      fromCiIds =
          dal.getCiIdsForNsAndClazz(this.nsForPlatformCiComponents, sourceFromCmsCiClazzName);
    }
    log.info("fromCiIds size: <{}>, fromCiIds: {} ", fromCiIds.size(), fromCiIds.toString());

    List<Integer> toCiIds = dal.getCiIdsForNsAndClazz(this.nsForPlatformCiComponents,
        mapping.getSourceToCmsCiClazzName());
    log.info("toCiIds size<{}>, toCiIds: {} ", toCiIds.size(), toCiIds.toString());

    dal.deleteCiRelations(sourceFromCmsCiClazzName, fromCiIds, sourceToCmsCiClazzName, toCiIds,
        sourceCmsCiRelationId, sourceCmsCiRelationName, this.nsForPlatformCiComponents);
    log.info(
        "End: process_DELETE_RELATION() **************************************************************************");
  }

  private void process_ADD_RELATION_ATTRIBUTE(
      CmsCIRelationAndRelationAttributesActionMappingsModel mapping) {

    log.info("\n\n");
    log.info("**************************************************************************");
    log.info("Begin: process_ADD_RELATION_ATTRIBUTE() ");


    if (this.ooPhase.equals(IConstants.DESIGN_PHASE)
        || this.ooPhase.equals(IConstants.TRANSITION_PHASE)) {
      process_ADD_RELATION_ATTRIBUTE_DESIGN_AND_TRANSITION_PHASE(mapping);
    } else if (this.ooPhase.equals(IConstants.OPERATE_PHASE)) {
      process_ADD_RELATION_ATTRIBUTE_OPERATE_PHASE(mapping);
    }

    log.info("End: process_ADD_RELATION_ATTRIBUTE() ");
    log.info("**************************************************************************");
    log.info("\n\n");

  }

  private void process_ADD_RELATION_ATTRIBUTE_DESIGN_AND_TRANSITION_PHASE(
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
        "creating CMSCIRelation attribute attributeName: {} attributeId: {} , with dfValue {} & djValue {}",
        attributeName, attributeId, dfValue, djValue);

    log.info("fromClazz {} relationName {} toClazz", targetFromCMSCIClazzName,
        targetCMSCIRelationName, targetToCMSCIClazzName);
    log.info("fromClazzId {} relationId {} toClazzId", targetFromCMSCIClazzId,
        targetCMSCIRelationId, targetToCMSCIClazzId);

    List<Integer> ci_relation_ids =
        dal.getCMSCIRelationIds_By_Ns_RelationName_FromClazzToClazz(this.nsForPlatformCiComponents,
            targetCMSCIRelationName, targetFromCMSCIClazzName, targetToCMSCIClazzName);

    log.info("List of ci_relation_ids for transformation mapping for ADD_RELATION_ATTRIBUTE <{}>",
        ci_relation_ids.toString());

    for (int ci_relation_id : ci_relation_ids) {
      int ci_rel_attribute_id = dal.getNext_cm_pk_seqId();
      log.info("Adding new ci_rel_attribute_id <{}> to ci_relation_id <{}>", ci_rel_attribute_id,
          ci_relation_id);

      dal.createCMSCIRelationAttribute(ci_rel_attribute_id, ci_relation_id, attributeId, dfValue,
          djValue, owner, comments);
    }


  }

  private void process_ADD_RELATION_ATTRIBUTE_OPERATE_PHASE(
      CmsCIRelationAndRelationAttributesActionMappingsModel mapping) {

    log.info("\n\n");
    log.info("--------------------------------------------------------------------");
    log.info("Begin: process_ADD_RELATION_ATTRIBUTE_OPERATE_PHASE() ");


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
        "creating CMSCIRelation attribute attributeName: {} attributeId: {} , with dfValue {} & djValue {}",
        attributeName, attributeId, dfValue, djValue);

    log.info("fromClazz {} relationName {} toClazz", targetFromCMSCIClazzName,
        targetCMSCIRelationName, targetToCMSCIClazzName);
    log.info("fromClazzId {} relationId {} toClazzId", targetFromCMSCIClazzId,
        targetCMSCIRelationId, targetToCMSCIClazzId);

    Map<String, Map<String, String>> bomCiRelations = dal
        .getCMSCIRelationIdsMap_By_Ns_RelationName_FromClazzToClazz(this.nsForPlatformCiComponents,
            targetCMSCIRelationName, targetFromCMSCIClazzName, targetToCMSCIClazzName);

    log.info("existing bomCiRelations {}", gson.toJson(bomCiRelations));


    if (targetCMSCIRelationName.equals("base.RealizedAs")
        && attributeName.equals("last_manifest_rfc")) {

      String attribValue = Integer.toString(dal.getNext_dj_pk_seq());
      dfValue = attribValue;
      djValue = attribValue;

      for (Map<String, String> bomCiRelationsMap : bomCiRelations.values()) {

        int ci_rel_attribute_id = dal.getNext_cm_pk_seqId();
        int ci_relation_id = new Integer(bomCiRelationsMap.get("ci_relation_id"));

        dal.createCMSCIRelationAttribute(ci_rel_attribute_id, ci_relation_id, attributeId, dfValue,
            djValue, owner, comments);

      }


    } else if ((targetCMSCIRelationName.equals("base.DeployedTo")
        || targetCMSCIRelationName.equals("base.RealizedAs")) && attributeName.equals("priority")) {
      for (Map<String, String> bomCiRelationsMap : bomCiRelations.values()) {

        String ci_rel_attribute_Value = getAttributeValueFromReferenceCiRelationForOperatePhase(
            attributeId, attributeName, targetCMSCIRelationName, targetFromCMSCIClazzName,
            targetToCMSCIClazzName, bomCiRelationsMap);


        dfValue = ci_rel_attribute_Value;
        djValue = ci_rel_attribute_Value;

        int ci_rel_attribute_id = dal.getNext_cm_pk_seqId();
        int ci_relation_id = new Integer(bomCiRelationsMap.get("ci_relation_id"));


        dal.createCMSCIRelationAttribute(ci_rel_attribute_id, ci_relation_id, attributeId, dfValue,
            djValue, owner, comments);


      }
    } else {

      throw new UnSupportedTransformationMappingException("attributeName <" + attributeName
          + "> not supported for targetCMSCIRelationName <" + targetCMSCIRelationName + "> ");

    }


    log.info("End: process_ADD_RELATION_ATTRIBUTE_OPERATE_PHASE() ");
    log.info("--------------------------------------------------------------------");
    log.info("\n\n");

  }

  private String getAttributeValueFromReferenceCiRelationForOperatePhase(int attributeId,
      String attributeName, String relation_name, String fromClazz, String toClazz,
      Map<String, String> bomCiRelationsMap) {

    Map<String, Map<String, String>> referenceBomCiRelationsForComputeClazz =
        getReferenceBomCiRelationsForComputeClazz(attributeId, attributeName, relation_name,
            fromClazz, toClazz);

    String fromCiName = bomCiRelationsMap.get("fromCiName");
    String toCiName = bomCiRelationsMap.get("toCiName");


    String fromCiNameSuffix;
    String toCiNameSuffix;

    if (fromClazz.contains("bom.oneops.1")) {
      fromCiNameSuffix = getBomCiSuffix(fromCiName);
    } else {
      fromCiNameSuffix = "No-Suffix";
    }

    if (toClazz.contains("bom.oneops.1")) {
      toCiNameSuffix = getBomCiSuffix(toCiName);
    } else {
      toCiNameSuffix = "No-Suffix";
    }


    for (Map<String, String> referenceBomCiRelationsMap : referenceBomCiRelationsForComputeClazz
        .values()) {

      String referenceFromCiName = referenceBomCiRelationsMap.get("fromCiName");
      String referenceToCiName = referenceBomCiRelationsMap.get("toCiName");

      String referenceFromCiNameSuffix = "";
      String referenceToCiNameSuffix = "";

      if (fromClazz.contains("bom.oneops.1")) {
        referenceFromCiNameSuffix = getBomCiSuffix(referenceFromCiName);
      } else {
        referenceFromCiNameSuffix = "No-Suffix";
      }

      if (toClazz.contains("bom.oneops.1")) {
        referenceToCiNameSuffix = getBomCiSuffix(referenceToCiName);
      } else {
        referenceToCiNameSuffix = "No-Suffix";;
      }



      log.info("fromCiName {} >> referenceFromCiName {} ", fromCiName, referenceFromCiName);
      log.info("fromCiNameSuffix {} >> referenceFromCiNameSuffix {} ", fromCiNameSuffix,
          referenceFromCiNameSuffix);

      log.info("toCiName {} >> referenceToCiName {} ", toCiName, referenceToCiName);
      log.info("toCiNameSuffix {} >> referenceToCiNameSuffix {} ", toCiNameSuffix,
          referenceToCiNameSuffix);

      if (fromCiNameSuffix.equals(referenceFromCiNameSuffix)
          && toCiNameSuffix.equals(referenceToCiNameSuffix)) {

        log.info("bomCiRelationsMap {} will refer to referenceBomCiRelationsMap {}",
            gson.toJson(bomCiRelationsMap), gson.toJson(referenceBomCiRelationsMap));

        int referenceCiRelationId = new Integer(referenceBomCiRelationsMap.get("ci_relation_id"));

        log.info("referenceCiRelationId {} ", referenceCiRelationId);
        String relationAttribValue =
            dal.getCIRelationAttrValueByCiRelIdAndAttribName(referenceCiRelationId, attributeName);


        log.info("value returned will be {}", relationAttribValue);

        return relationAttribValue;


      }

    }

    log.error(
        "No reference attrubuteValue found for attributeId {} attributeName {}, relation_name {}, fromClazz {}, toClazz {}",
        attributeId, attributeName, relation_name, fromClazz, toClazz);


    throw new UnSupportedOperation("No reference attributeValue found ");


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

    Map<String, Integer> fromCiIdsAndCiNamesMap = new HashMap<String, Integer>();

    if (targetCMSCIRelationName.equals("base.RealizedAs")) {
      String transitonPhaseNsPath = CircuitconsolidationUtil.getnsForPlatformCiComponents(this.ns,
          this.platformName, IConstants.TRANSITION_PHASE, this.envName);
      fromCiIdsAndCiNamesMap =
          dal.getCiNamesAndCiIdsMapForNsAndClazz(transitonPhaseNsPath, targetFromCMSCIClazzName);

    } else {
      fromCiIdsAndCiNamesMap = dal.getCiNamesAndCiIdsMapForNsAndClazz(
          this.nsForPlatformCiComponents, targetFromCMSCIClazzName);
    }


    log.info("fromCiIdsAndCiNamesMap: {}", gson.toJson(fromCiIdsAndCiNamesMap));

    List<String> fromBomCiNamesList = new ArrayList<String>(fromCiIdsAndCiNamesMap.keySet());


    Map<String, Integer> toCiIdsAndCiNamesMap = new HashMap<String, Integer>();

    if (targetCMSCIRelationName.equals("base.DeployedTo")) {

      toCiIdsAndCiNamesMap = getCiNamesAndCiIdsMapForCloudCis(fromBomCiNamesList);

    } else {
      toCiIdsAndCiNamesMap = dal.getCiNamesAndCiIdsMapForNsAndClazz(this.nsForPlatformCiComponents,
          targetToCMSCIClazzName);
    }

    log.info("toCiIdsAndCiNamesMap: {}", gson.toJson(toCiIdsAndCiNamesMap));

    List<String> toBomCiNamesList = new ArrayList<String>(toCiIdsAndCiNamesMap.keySet());

    Map<String, Set<String>> fromBomCisAndToBomCisPairs = new HashMap<String, Set<String>>();

    if (targetCMSCIRelationName.equals("base.RealizedAs")) {
      fromBomCisAndToBomCisPairs =
          getFromManifestCisAndToBomCisPairs(fromBomCiNamesList, toBomCiNamesList);

    } else if (targetCMSCIRelationName.equals("base.DeployedTo")) {
      fromBomCisAndToBomCisPairs =
          getFromBomCisAndToCloudCisPairs(fromBomCiNamesList, toCiIdsAndCiNamesMap);
    }

    else {
      fromBomCisAndToBomCisPairs =
          getFromBomCisAndToBomCisPairs(fromBomCiNamesList, toBomCiNamesList);

    }

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


        if (targetCMSCIRelationName.contains("bom.DependsOn")) {

          String fromBomCiNamePrefix = getBomCiPrefix(fromBomCiName);
          String toBomCiNamePrefix = getBomCiPrefix(toBomCiName);

          if (fromBomCiNamePrefix.equalsIgnoreCase("fqdn")
              && toBomCiNamePrefix.equalsIgnoreCase("os")) {

            log.info(
                "relation {} fromBomCiName {} fromCiId {} >> toBomCiName {} toCiId {} will be ignored as an exceptional case",
                targetCMSCIRelationName, fromBomCiName, fromCiId, toBomCiName, toCiId);
            continue;

          }

        }

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

  private Map<String, Integer> getCiNamesAndCiIdsMapForCloudCis(List<String> fromBomCiNamesList) {

    Map<String, Integer> ciNamesAndCiIdsMapForCloudCis = new HashMap<String, Integer>();

    try {

      Set<Integer> clouidCiIdSet = new HashSet<Integer>();
      for (String bomCiName : fromBomCiNamesList) {

        int clouidCiId = getcloudCiIdFromBomCiName(bomCiName);

        clouidCiIdSet.add(clouidCiId);

      }

      log.info("clouidCiIdSet {}: ", gson.toJson(clouidCiIdSet));

      for (int cloudCiId : clouidCiIdSet) {

        String cloudCiName = dal.getClouidCiNameAndClazzByCiId(cloudCiId);
        ciNamesAndCiIdsMapForCloudCis.put(cloudCiName, cloudCiId);
      }

      log.info("ciNamesAndCiIdsMapForCloudCis: " + gson.toJson(ciNamesAndCiIdsMapForCloudCis));
      return ciNamesAndCiIdsMapForCloudCis;
    } catch (NullPointerException | ArrayIndexOutOfBoundsException e) {
      throw new UnSupportedOperation(
          "Error while processing bomComputeCiNamesAndCiIdsMap for parsing cloud ciId"
              + e.getMessage());
    }


  }


  private String getBomCiSuffix(String bomCiName) {

    log.info("input bomCiName {} for suffix", bomCiName);
    String[] strArr = bomCiName.split("-");

    StringBuffer bomcCiSuffix = new StringBuffer();
    for (int i = strArr.length - 2; i <= strArr.length - 1; i++) {

      bomcCiSuffix = bomcCiSuffix.append("-").append(strArr[i]);

    }
    log.info("Full suffix: " + bomcCiSuffix);
    return new String(bomcCiSuffix);

  }

  private String getBomCiPrefix(String bomCiName) {

    String bomCiSuffix = getBomCiSuffix(bomCiName);
    String bomCiPrefix = bomCiName.substring(0, bomCiName.length() - bomCiSuffix.length());

    log.info("Full Prefix: " + bomCiPrefix);
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

  private Map<String, Set<String>> getFromManifestCisAndToBomCisPairs(
      List<String> fromBomCiNamesList, List<String> toBomCiNamesList) {

    Map<String, Set<String>> fromBomCisAndToBomCisPairsMap = new HashMap<String, Set<String>>();


    for (String fromBomCiName : fromBomCiNamesList) {

      log.info("fromBomCiName: " + fromBomCiName);
      String fromBomCiNameSuffix = fromBomCiName;

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

  private Map<String, Set<String>> getFromBomCisAndToCloudCisPairs(List<String> fromBomCiNamesList,
      Map<String, Integer> toCiIdsAndCiNamesMap) {


    Map<String, Set<String>> fromBomCisAndToBomCisPairsMap = new HashMap<String, Set<String>>();


    for (String fromBomCiName : fromBomCiNamesList) {

      log.info("fromBomCiName: " + fromBomCiName);
      int cloudCiIdFromBomCiName = getcloudCiIdFromBomCiName(fromBomCiName);

      Set<String> set = new HashSet<String>();

      for (String toBomCiName : toCiIdsAndCiNamesMap.keySet()) {

        int toCloudCiId = toCiIdsAndCiNamesMap.get(toBomCiName);

        if (cloudCiIdFromBomCiName == toCloudCiId) {

          set.add(toBomCiName);
          fromBomCisAndToBomCisPairsMap.put(fromBomCiName, set);
          continue;
        }

      }
      fromBomCisAndToBomCisPairsMap.put(fromBomCiName, set);
    }

    return fromBomCisAndToBomCisPairsMap;

  }

  private int getcloudCiIdFromBomCiName(String bomCiName) {


    String[] bomComputeCiNameArr = bomCiName.split("-");
    int clouidCiId = new Integer(bomComputeCiNameArr[bomComputeCiNameArr.length - 2]);
    log.info("clouidCiId {} from bomCompute ci name {} " + clouidCiId, bomCiName);
    if (clouidCiId == 0) {
      throw new UnSupportedOperation("bomComputeCiName <" + bomCiName + "> generated 0 clouidCiId");
    }

    return clouidCiId;

  }



  private Map<String, Map<String, String>> getReferenceBomCiRelationsForComputeClazz(
      int attributeId, String attributeName, String relation_name, String fromClazz,
      String toClazz) {

    Map<String, Map<String, String>> cMSCIRelationIdsMap =
        new HashMap<String, Map<String, String>>();

    String sourceFromClazzNameAsReference = fromClazz.replace(".1.Os", ".1.Compute");
    String sourceToClazzNameAsReference = toClazz.replace(".1.Os", ".1.Compute");

    log.info("referenceBomCi relation {} >> {} >> {}", sourceFromClazzNameAsReference,
        relation_name, sourceToClazzNameAsReference);
    log.info("referenceBomCi attributeName {}, attributeId {}", attributeName, attributeId);


    cMSCIRelationIdsMap = dal.getCMSCIRelationIdsMap_By_Ns_RelationName_FromClazzToClazz(
        this.nsForPlatformCiComponents, relation_name, sourceFromClazzNameAsReference,
        sourceToClazzNameAsReference);
    log.info("cMSCIRelationIdsMap {}", gson.toJson(cMSCIRelationIdsMap));

    return cMSCIRelationIdsMap;
  }



}
