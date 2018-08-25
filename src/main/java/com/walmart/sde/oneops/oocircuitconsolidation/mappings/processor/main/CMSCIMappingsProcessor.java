package com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.main;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.gson.Gson;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.config.IConstants;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.dal.KloopzCmDal;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.exception.UnSupportedOperation;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.exception.UnSupportedTransformationMappingException;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.model.CmsCiAndCmsCiAttributesActionMappingsModel;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.util.CircuitconsolidationUtil;

public class CMSCIMappingsProcessor {

  private final Logger log = LoggerFactory.getLogger(getClass());

  String ns;
  String platformName;
  String ooPhase;
  String envName;
  String nsForPlatformCiComponents;
  Connection conn;
  KloopzCmDal dal;


  CMSCIMappingsProcessor(String ns, String platformName, String ooPhase, String envName,
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



  public void setNs(String ns) {
    this.ns = ns;
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

  public void setConn(Connection conn) {
    this.conn = conn;
  }


  public void setNsForPlatformCiComponents(String nsForPlatformCiComponents) {
    this.nsForPlatformCiComponents = nsForPlatformCiComponents;
  }

  public void processCMSCIMappings(List<CmsCiAndCmsCiAttributesActionMappingsModel> mappingsList) {
    Gson gson = new Gson();

    // update source property of platform from walmartLabs to oneops
    if (this.ooPhase.equals(IConstants.DESIGN_PHASE)) {
      dal.updatePlatformSourceProperty(this.ns, this.platformName);
    } else if (this.ooPhase.equals(IConstants.TRANSITION_PHASE)) {
      dal.updatePlatformSourceProperty(this.nsForPlatformCiComponents, this.platformName);
    }


    for (CmsCiAndCmsCiAttributesActionMappingsModel mapping : mappingsList) {

      String entityType = mapping.getEntityType();
      String action = mapping.getAction();

      if (entityType.equalsIgnoreCase("CMSCI")) {

        switch (action) {
          case "SWITCH_CMCI_CLAZZID_CLAZZNAME_GOID":
            process_SWITCH_CMCI_CLAZZID_CLAZZNAME_GOID(mapping);
            break;

          case "DELETE_CMSCI":
            process_DELETE_CMSCI(mapping);
            break;

          case "CREATE_CMSCI":
            process_CREATE_CMSCI(mapping);
            break;


          default:
            throw new UnSupportedTransformationMappingException(
                "<action>: " + action + " for <entityType>: " + entityType
                    + "not supported, mapping record: " + gson.toJson(mapping));


        }

      } else if (entityType.equalsIgnoreCase("CMSCI_ATTRIBUTE")) {

        switch (action) {
          case "DELETE_CMSCI_ATTRIBUTE":
            process_DELETE_CMSCI_ATTRIBUTE(mapping);
            break;

          case "SET_DEFAULT_CMSCI_ATTRIBUTE_VALUE":
            processMapping_SET_DEFAULT_ATTRIBUTE_VALUE(mapping);
            break;

          case "CREATE_CMSCI_ATTRIBUTE_WITH_SOURCE_CLAZZ_ATTRIBUTE_VALUE":
            processMapping_CREATE_CMSCI_ATTRIBUTE_WITH_SOURCE_CLAZZ_ATTRIBUTE_VALUE(mapping);
            break;

          case "SWITCH_CMSCI_ATTRIBUTE_ID":
            process_SWITCH_CMSCI_ATTRIBUTE_ID(mapping);
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

  private void process_CREATE_CMSCI(CmsCiAndCmsCiAttributesActionMappingsModel mapping) {
    // TODO Need to check with team if there is a potential issue while fetching Next CiId since
    // this will be a parallel program to OneOps, This may conflict of same CiId is picked up by
    // OneOps core code


    /*
     * <insert id="createCI" parameterType="com.oneops.cms.cm.domain.CmsCI"
     * statementType="CALLABLE"> {call cm_create_ci(#{ciId} ,#{nsId}, #{ciClassId}, #{ciGoid},
     * #{ciName}, #{comments}, #{ciStateId}, #{createdBy})}
     */

    log.info("\n\n");
    log.info("**************************************************************************");
    log.info("Begin: process_CREATE_CMSCI() ");

    int nsId = dal.getNsIdForNsPath(nsForPlatformCiComponents);
    int targetClazzId = mapping.getTargetClassId();
    String targetClazzName = mapping.getTargetClassname();
    

    String comments = IConstants.CIRCUIT_CONSOLIDATION_COMMENTS;
    int ciStateId = 100;
    String createdBy = IConstants.CIRCUIT_CONSOLIDATION_USER;

    if (this.ooPhase.equals(IConstants.DESIGN_PHASE)
        || this.ooPhase.equals(IConstants.TRANSITION_PHASE)) {
      int ciId = dal.getNext_cm_pk_seqId();
      String goid = nsId + "-" + targetClazzId + "-" + ciId;
      String ciName = "os"; // hardcoded value, so far only 1 CI is being created
      dal.createCMSCI(nsId, ciId, targetClazzId, ciName, goid, ciStateId, comments, createdBy);
      log.info("os component created with ciId: {} for targetClazzName {}", ciId,targetClazzName);

    } else if (this.ooPhase.equals(IConstants.OPERATE_PHASE)) {

      Map<Integer, String> bomComputeCiIdAndCiNameMap =
          dal.getCiIdsAndCiNameForNsAndClazz(this.nsForPlatformCiComponents, "bom.Compute");

      for (String bomComputeCiName : bomComputeCiIdAndCiNameMap.values()) {

        if (!bomComputeCiName.contains("compute")) {
          throw new UnSupportedOperation(
              "bomComputeCiName <" + bomComputeCiName + "> is not valid compute ciName");
        }

        String ciName = bomComputeCiName.replace("compute", "os");// hardcoded value, so far only 1
                                                                  // CI is being created
        int ciId = dal.getNext_cm_pk_seqId();
        String goid = nsId + "-" + targetClazzId + "-" + ciId;
        dal.createCMSCI(nsId, ciId, targetClazzId, ciName, goid, ciStateId, comments, createdBy);
        log.info("os component created with ciId: {}", ciId);
        log.info("os component created with ciId: {} for targetClazzName {}", ciId,targetClazzName);
      }


    } else {

      throw new UnSupportedOperation(this.ooPhase + "is not supported");
    }

    log.info("End: process_CREATE_CMSCI() ");
    log.info("**************************************************************************");
    log.info("\n\n");

  }



  private List<String> getPlatformClouds() {
    // TODO Auto-generated method stub

    List<String> cloudsInPlatform = new ArrayList<String>();


    Map<String, Integer> computeCisWith_baseAsRealizedRelation_map =
        dal.getComputeCisDeployedInPlatformByNsPath(this.nsForPlatformCiComponents);

    for (String computeCiName : computeCisWith_baseAsRealizedRelation_map.keySet()) {

      List<Integer> cloudCiIdsFromComputeCiNames = getcloudCiIdsFromComputeCiNames(computeCiName);



    }

    return cloudsInPlatform;
  }



  private List<Integer> getcloudCiIdsFromComputeCiNames(String computeCiName) {
    List<Integer> cloudCiIds = new ArrayList<Integer>();

    log.info("computeCiName: {}", computeCiName);


    return null;
  }



  private void process_DELETE_CMSCI(CmsCiAndCmsCiAttributesActionMappingsModel mapping) {
    // List<Integer> ciIdsToDelete=dal.getCiIdsForNsAndClazz(nsForPlatformCiComponents,
    // mapping.getSourceClassname());
    // log.info("ciIdsToDelete: "+ciIdsToDelete);
    log.info(
        "Begin: process_DELETE_CMSCI() **************************************************************************");
    dal.deleteCmsCisForNsAndClazz(nsForPlatformCiComponents, mapping.getSourceClassname());
    log.info(
        "End: process_DELETE_CMSCI() **************************************************************************");
    // System.exit(0);


  }



  private void process_SWITCH_CMCI_CLAZZID_CLAZZNAME_GOID(
      CmsCiAndCmsCiAttributesActionMappingsModel mapping) {

    String sourceClassName = mapping.getSourceClassname();
    int sourceclassId = mapping.getSourceClassId();
    String targetClassName = mapping.getTargetClassname();
    int targetclassId = mapping.getTargetClassId();

    List<Integer> ciIds = dal.getCiIdsForNsAndClazz(nsForPlatformCiComponents, sourceClassName);
    int nsId = dal.getNsIdForNsPath(nsForPlatformCiComponents);

    log.info("Begin: process_SWITCH_CMCI_CLAZZID_CLAZZNAME_GOID ()");
    log.info("**************************************************************************");

    for (int ciId : ciIds) {

      String goid = nsId + "-" + targetclassId + "-" + ciId;
      dal.cmsCi_update_ciClazzid_clazzname_goid(ciId, sourceClassName, sourceclassId,
          targetClassName, targetclassId, goid);

    }
    log.info("**************************************************************************");
    log.info("End: process_SWITCH_CMCI_CLAZZID_CLAZZNAME_GOID ()");
  }

  private void process_DELETE_CMSCI_ATTRIBUTE(CmsCiAndCmsCiAttributesActionMappingsModel mapping) {

    log.info("**************************************************************************");
    log.info("Begin: process_DELETE_CMSCI_ATTRIBUTE ()");
    // from mappings
    String sourceClazz = mapping.getSourceClassname();
    int sourceClazzId = mapping.getSourceClassId();
    String sourceClazzAttributeName = mapping.getSourceAttributeName();
    int sourceClazzAttributeId = mapping.getSourceAttributeId();

    String targetClazz = mapping.getTargetClassname();
    int targetClazzId = mapping.getTargetClassId();

    dal.deleteCMSCIAttribute(this.nsForPlatformCiComponents, sourceClazz, sourceClazzId,
        sourceClazzAttributeId, sourceClazzAttributeName, targetClazz, targetClazzId);
    log.info("**************************************************************************");
    log.info("End: process_DELETE_CMSCI_ATTRIBUTE ()");
  }

  private void processMapping_CREATE_CMSCI_ATTRIBUTE_WITH_SOURCE_CLAZZ_ATTRIBUTE_VALUE(
      CmsCiAndCmsCiAttributesActionMappingsModel mapping) {
    // from mappings

    log.info("\n\n");
    log.info("**************************************************************************");
    log.info("Begin: processMapping_CREATE_CMSCI_ATTRIBUTE_WITH_SOURCE_CLAZZ_ATTRIBUTE_VALUE ()");

    String sourceClassName = mapping.getSourceClassname();
    int sourceClassId = mapping.getSourceClassId();
    String sourceClazzAttributeName = mapping.getSourceAttributeName();
    int sourceClazzAttributeId = mapping.getSourceAttributeId();


    String targetClassName = mapping.getTargetClassname();
    int targetClassId = mapping.getTargetClassId();
    String targetClazzAttributeName = mapping.getTargetAttributeName();
    int targetClazzAttributeId = mapping.getTargetAttributeId();



    List<Integer> sourceClazzCiIds =
        dal.getCiIdsForNsAndClazz(this.nsForPlatformCiComponents, sourceClassName);
    log.info("sourceClazzCiIds: " + sourceClazzCiIds.toString());

    List<Integer> targetClazzCiIds =
        dal.getCiIdsForNsAndClazz(this.nsForPlatformCiComponents, targetClassName);
    log.info("targetClazzCiIds: " + targetClazzCiIds.toString());

    if ((this.ooPhase.equals(IConstants.DESIGN_PHASE)
        || this.ooPhase.equals(IConstants.TRANSITION_PHASE))) {

      if ((sourceClazzCiIds.size() != 1 || targetClazzCiIds.size() != 1)) {

        throw new UnsupportedOperationException("sourceClazzCiIds: " + sourceClazzCiIds
            + " targetClazzCiIds: " + targetClazzCiIds + " one of the ciIds != 1");
      }
      String cmsCiAttributeValue = dal.getCMSCIAttributeValueByAttribNameAndCiId(
          sourceClazzCiIds.get(0), sourceClazzAttributeId, sourceClazzAttributeName);

      int ciId = targetClazzCiIds.get(0);
      int ci_attribute_id = dal.getNext_cm_pk_seqId();


      log.info(
          "copying cmsCiAttribute sourceClazzAttributeName {} from sourceClassName {} & sourceClassId {} To targetClassName {} & targetClassId {}",
          sourceClazzAttributeName, sourceClassName, sourceClassId, targetClassName, targetClassId);

      log.info("sourceClazzAttributeName{} sourceClazzAttributeId {}", sourceClazzAttributeName,
          sourceClazzAttributeId);
      log.info("targetClazzAttributeName{} targetClazzAttributeId {}", targetClazzAttributeName,
          targetClazzAttributeId);

      dal.createNewCMSCIAttribute(ci_attribute_id, ciId, targetClazzAttributeId,
          cmsCiAttributeValue, IConstants.CIRCUIT_CONSOLIDATION_USER,
          IConstants.CIRCUIT_CONSOLIDATION_COMMENTS);



    } else if (this.ooPhase.equals(IConstants.OPERATE_PHASE)) {

      log.info("copy attribute process for OPERATE Phase");
      if ((sourceClazzCiIds.size() < 1 || targetClazzCiIds.size() < 1)) {

        throw new UnsupportedOperationException("sourceClazzCiIds: " + sourceClazzCiIds
            + " targetClazzCiIds: " + targetClazzCiIds + " one of the ciIds < 1");
      }

      for (int i = 0; i < targetClazzCiIds.size(); i++) {

        String cmsCiAttributeValue = dal.getCMSCIAttributeValueByAttribNameAndCiId(
            sourceClazzCiIds.get(0), sourceClazzAttributeId, sourceClazzAttributeName);

        int ciId = targetClazzCiIds.get(i);
        int ci_attribute_id = dal.getNext_cm_pk_seqId();


        log.info(
            "copying cmsCiAttribute sourceClazzAttributeName {} from sourceClassName {} & sourceClassId {} To targetClassName {} & targetClassId {}",
            sourceClazzAttributeName, sourceClassName, sourceClassId, targetClassName,
            targetClassId);

        log.info("sourceClazzAttributeName{} sourceClazzAttributeId {}", sourceClazzAttributeName,
            sourceClazzAttributeId);
        log.info("targetClazzAttributeName{} targetClazzAttributeId {}", targetClazzAttributeName,
            targetClazzAttributeId);

        dal.createNewCMSCIAttribute(ci_attribute_id, ciId, targetClazzAttributeId,
            cmsCiAttributeValue, IConstants.CIRCUIT_CONSOLIDATION_USER,
            IConstants.CIRCUIT_CONSOLIDATION_COMMENTS);
        log.info("created new ci_attribute_id {} for ciId {} with value from compute to os component", ci_attribute_id, ciId);

      }


    }



    log.info("**************************************************************************");
    log.info("End: processMapping_CREATE_CMSCI_ATTRIBUTE_WITH_SOURCE_CLAZZ_ATTRIBUTE_VALUE ()");
    log.info("\n\n");
  }

  /*
   * This method is created to set default values for attributes of targetClazz, In this scenario
   * the attribute names does not exists in sourceClazz hence values are being set to default
   * 
   */
  private void processMapping_SET_DEFAULT_ATTRIBUTE_VALUE(
      CmsCiAndCmsCiAttributesActionMappingsModel mapping) {
    // from mappings

    String targetClassName = mapping.getTargetClassname();
    int targetClassId = mapping.getTargetClassId();
    String targetAttributeName = mapping.getTargetAttributeName();
    int targetAttributeId = mapping.getTargetAttributeId();
    String targetDefaultValue = mapping.getTargetDefaultValue();

    log.info("Begin: processMapping_SET_DEFAULT_ATTRIBUTE_VALUE ()");
    log.info("**************************************************************************");
    log.info("\n\n");

    List<Integer> ciIds =
        dal.getCiIdsForNsAndClazz(this.nsForPlatformCiComponents, targetClassName);

    log.info(
        "Adding CMSCI Attribute for CiIds {}, targetClassName <{}> targetClassId <{}> , targetAttributeName <{}> targetAttributeId <{}> with default value <{}>",
        ciIds.toString(), targetClassName, targetClassId, targetAttributeName, targetAttributeId,
        targetDefaultValue);


    for (int ciId : ciIds) {
      log.info(
          "Adding CMSCI Attribute for CiId {}, targetClassName <{}> targetClassId <{}> , targetAttributeName <{}> targetAttributeId <{}> with default value <{}>",
          ciId, targetClassName, targetClassId, targetAttributeName, targetAttributeId,
          targetDefaultValue);
      int ci_attribute_id = dal.getNext_cm_pk_seqId();

      dal.createNewCMSCIAttribute(ci_attribute_id, ciId, targetAttributeId, targetDefaultValue,
          IConstants.CIRCUIT_CONSOLIDATION_USER, IConstants.CIRCUIT_CONSOLIDATION_COMMENTS);

    }

    log.info("End: processMapping_SET_DEFAULT_ATTRIBUTE_VALUE ()");
    log.info("**************************************************************************");
    log.info("\n\n");

  }

  private void process_SWITCH_CMSCI_ATTRIBUTE_ID(
      CmsCiAndCmsCiAttributesActionMappingsModel mapping) {
    log.info("\n\n");
    log.info("**************************************************************************");
    log.info("Begin: process_SWITCH_CMSCI_ATTRIBUTE_ID ()");
    int sourceAttributeId = mapping.getSourceAttributeId();
    String sourceAttributeName = mapping.getSourceAttributeName();
    int sourceClazzId = mapping.getSourceClassId();
    String sourceClazzname = mapping.getSourceClassname();


    int targetClassId = mapping.getTargetClassId();
    String targetClazzname = mapping.getTargetClassname();

    int targetAttributeId = mapping.getTargetAttributeId();

    /*
     * dal.switchCMSCIAttribuetId(this.nsForPlatformCiComponents, targetClassId, targetAttributeId,
     * sourceAttributeId);
     */
    dal.switchCMSCIAttribuetId(this.nsForPlatformCiComponents, sourceAttributeId,
        sourceAttributeName, sourceClazzId, sourceClazzname, targetClassId, targetClazzname,
        targetAttributeId);



    log.info("End: process_SWITCH_CMSCI_ATTRIBUTE_ID ()");
    log.info("**************************************************************************");
    log.info("\n\n");
  }



}
