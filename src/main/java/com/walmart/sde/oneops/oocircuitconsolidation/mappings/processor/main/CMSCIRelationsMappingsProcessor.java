package com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.main;

import java.sql.Connection;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.gson.Gson;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.exception.UnSupportedTransformationMappingException;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.model.CmsCIRelationAndRelationAttributesActionMappingsModel;

public class CMSCIRelationsMappingsProcessor {


  private final Logger log = LoggerFactory.getLogger(getClass());

  String nsForPlatformCiComponents;
  Connection conn;


  CMSCIRelationsMappingsProcessor(String nsForPlatformCiComponents, Connection conn) {
    setNsForPlatformCiComponents(nsForPlatformCiComponents);
    setConn(conn);
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
      List<CmsCIRelationAndRelationAttributesActionMappingsModel> mappingsList,
      String nsForPlatformCiComponents) {
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
            process_ADD_RELATION_ATTRIBUTE(mapping);
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

    String targetCMSCIRelationName = mapping.getTargetCmsCiRelationName();
    int targetCMSCIRelationId = mapping.getTargetCmsCiRelationId();
    String targetFromCMSCIClazzName = mapping.getTargetFromCmsCiClazzName();
    int targetFromCMSCIClazzId = mapping.getTargetFromCmsCiClazzId();
    String targetToCMSCIClazzName = mapping.getTargetToCmsCiClazzName();
    int targetToCMSCIClazzId = mapping.getTargetToCmsCiClazzId();

    String relationGoid; // create


    // create: create New Relation


  }

  private void process_DELETE_RELATION(
      CmsCIRelationAndRelationAttributesActionMappingsModel mapping) {

    int sourceFromCMSCIClazzId = mapping.getSourceFromCmsCiClazzId();
    String sourceFromCMSCIClazzName = mapping.getSourceFromCmsCiClazzName();

    int sourceToCMSCIClazzId = mapping.getSourceToCmsCiClazzId();
    String sourceToCMSCIClazzName = mapping.getSourceToCmsCiClazzName();

    int sourceCMSCIRelationId = mapping.getSourceCmsCiRelationId();
    String sourceCMSCIRelationName = mapping.getSourceCmsCiRelationName();

    // operations: delete relation, IF a CI is already deleted then relation may not exist

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



    // Operation: Add relation attribute to relation, Add DJ & DF Values

  }

}
