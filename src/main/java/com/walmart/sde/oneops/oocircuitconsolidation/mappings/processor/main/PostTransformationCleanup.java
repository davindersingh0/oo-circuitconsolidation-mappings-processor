package com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.main;

import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.gson.Gson;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.config.IConstants;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.dal.KloopzCmDal;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.exception.UnSupportedOperation;

public class PostTransformationCleanup {

  private final Logger log = LoggerFactory.getLogger(getClass());

  String ooPhase;
  KloopzCmDal dal;
  Gson gson = new Gson();
  String ns;
  String platformName;
  String envName;
  String nsForPlatformCiComponents;


  public void setPostTransformationCleanupProperties(String ooPhase, KloopzCmDal dal, String ns,
      String platformName, String envName, String nsForPlatformCiComponents) {

    this.ooPhase = ooPhase;
    this.dal = dal;
    this.ns = ns;
    this.platformName = platformName;
    this.envName = envName;
    this.nsForPlatformCiComponents = nsForPlatformCiComponents;


  }

  public void cleanUpPlatformToCIRequiresRelation() {


    String platformClazzName;
    String keySpaceClazzName;
    String requiresRelationName;
    String ns_path;
    switch (this.ooPhase) {
      case IConstants.DESIGN_PHASE:
        requiresRelationName = "base.Requires";
        platformClazzName = "catalog.Platform";
        keySpaceClazzName = "catalog.oneops.1.Keyspace";
        ns_path=this.ns;

        break;
      case IConstants.TRANSITION_PHASE:
        requiresRelationName = "manifest.Requires";
        platformClazzName = "manifest.Platform";
        keySpaceClazzName = "manifest.oneops.1.Keyspace";
        ns_path=this.nsForPlatformCiComponents;
        break;
      case IConstants.OPERATE_PHASE:

        log.info("cleanUpPlatformToCIRequiresRelation() not required for Operate Phase");
        return;

      default:
        log.error("ooPhase {} not supported", ooPhase);

        throw new UnSupportedOperation(ooPhase + " not supported");
    }


    Map<Integer, String> platformCiIdsMap =
        dal.getCiIdsForNsClazzAndPlatformCiName(ns_path, platformClazzName, this.platformName);
   

    Map<Integer, String> keySpaceClazzCiIdsAndCiNamesMap =
        dal.getCiIdsAndCiNameForNsAndClazzMap(this.nsForPlatformCiComponents, keySpaceClazzName);

    for (int platformCiId : platformCiIdsMap.keySet()) {

      for (int keyspaceCmsCiId : keySpaceClazzCiIdsAndCiNamesMap.keySet()) {


        List<Integer> ciRelationIdsList = dal.getCMSCIRelationIds_By_RelationNameAndFromCiIdToCiId(
            requiresRelationName, platformCiId, keyspaceCmsCiId);

        updateTemplateAndConstraintAttributeValues(ciRelationIdsList);


      }


    }


  }

  private void updateTemplateAndConstraintAttributeValues(List<Integer> ciRelationIdsList) {

    for (int ciRelationId : ciRelationIdsList) {

      String constraintAttributeName = "constraint";
      String constraintExpectedAttributevalue = "0..*";

      updateCiRelationAttributeValueForRelation(ciRelationId, constraintAttributeName,
          constraintExpectedAttributevalue);

      String templateAttributeName = "template";
      String templateExpectedAttributevalue = "keyspace";

      updateCiRelationAttributeValueForRelation(ciRelationId, templateAttributeName,
          templateExpectedAttributevalue);


    }

  }

  private void updateCiRelationAttributeValueForRelation(int ciRelationId, String attributeName,
      String expectedAttributeValue) {

    String actualAttributeValue =
        dal.getCIRelationAttrValueByCiRelIdAndAttribName(ciRelationId, attributeName);

    if (actualAttributeValue.equalsIgnoreCase(expectedAttributeValue)) {
      log.info("ciRelationId {} already have valid {} value as {}", ciRelationId, attributeName,
          actualAttributeValue);
    } else {
      log.info("ciRelationId <{}> attributeName <{}> value <{}> is not valid", ciRelationId,
          attributeName, actualAttributeValue);
      log.info("updating attributeName {} attribute value to expectedAttributeValue: {}",
          attributeName, expectedAttributeValue);

      dal.updateCMSCiRelationAttributeValue_by_CiRelationIdAndAttributeName(ciRelationId,
          attributeName, expectedAttributeValue);


      log.info("updated attributeName {} attribute value to expectedAttributeValue: {}",
          attributeName, expectedAttributeValue);

    }

  }

  // This method is no more valid, This scenario is not required.
  @Deprecated
  public void cleanUpDependsOnRelation() {

    // getRelationId for base.Requires relation for given platform
    // check if there is any relation where attributes exists.
    // log those attributes
    // delete those attributes


    // String platformClazzName;

    String keySpaceClazzName;
    String dependsOnRelationName;

    switch (this.ooPhase) {
      case IConstants.DESIGN_PHASE:
        dependsOnRelationName = "catalog.DependsOn";
        keySpaceClazzName = "catalog.oneops.1.Keyspace";

        break;
      case IConstants.TRANSITION_PHASE:
        dependsOnRelationName = "manifest.DependsOn";
        keySpaceClazzName = "manifest.oneops.1.Keyspace";
        break;
      case IConstants.OPERATE_PHASE:
        dependsOnRelationName = "bom.DependsOn";
        log.info("cleanUpDependsOnRelation() not required for Operate Phase");

        return;


      default:
        log.error("ooPhase {} not supported", ooPhase);

        throw new UnSupportedOperation(ooPhase + " not supported");
    }

    List<Integer> ciRelationIds = dal.getCMSCIRelationIds_By_Ns_RelationName_FromClazzToClazz(
        this.nsForPlatformCiComponents, dependsOnRelationName, keySpaceClazzName, "%");
    
    log.info("ciRelationIds for cleanUpDependsOnRelation {}:", ciRelationIds);
    
    for (int ciRelationId : ciRelationIds) {

      deleteRelationAttributesIfExists(ciRelationId);


    }


    
  }

  private void deleteRelationAttributesIfExists(int ciRelationId) {

    Map<Integer, Map<String, String>> cmsCiRelationCiAttribIdsAndValuesMap =
        dal.getCmsCiRelationAttributeIdsAndValuesMap(ciRelationId);

    for (int cmsCiRelationCiAttribId : cmsCiRelationCiAttribIdsAndValuesMap.keySet()) {

      log.info(
          "deleting CMSCIRelation attribute for ciRelationId {} with cmsCiRelationCiAttribId <{}> having value as {}",
          ciRelationId, cmsCiRelationCiAttribId,
          gson.toJson(cmsCiRelationCiAttribIdsAndValuesMap.get(cmsCiRelationCiAttribId)));

      dal.deleteCmsCiRelationAttribute_by_cmsCiRelationAttributeId(cmsCiRelationCiAttribId);


    }


  }

}
