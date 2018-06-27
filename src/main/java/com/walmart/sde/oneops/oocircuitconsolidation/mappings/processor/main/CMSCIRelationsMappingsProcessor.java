package com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.main;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.model.CmsCIRelationAndRelationAttributesActionMappingsModel;

public class CMSCIRelationsMappingsProcessor {


  private final Logger log = LoggerFactory.getLogger(getClass());


  /*
   * (CMSCI_RELATION,CREATE_RELATION) 
   * (CMSCI_RELATION,DELETE_RELATION)
   * (CMSCI_RELATION_ATTRIBUTE,ADD_RELATION_ATTRIBUTE)
   * 
   */
  public void processCMSCIRelationsMappings(
      List<CmsCIRelationAndRelationAttributesActionMappingsModel> mappingsList) {


    for (CmsCIRelationAndRelationAttributesActionMappingsModel mapping : mappingsList) {

      String entityType = mapping.getEntityType();
      String action = mapping.getAction();

      if (entityType.equalsIgnoreCase("CMSCI_RELATION")) {
        switch (action) {
          case "CREATE_RELATION":

            break;
          case "DELETE_RELATION":

            break;
          default:
            // throw exeption, action not supported
            break;
        }

      } else if (entityType.equalsIgnoreCase("CMSCI_RELATION_ATTRIBUTE")) {

        switch (action) {
          case "ADD_RELATION_ATTRIBUTE":

            break;

          default:
            // throw exeption, action not supported
            break;
        }

      } else {
        // throw exception entity type not supported


      }



    }


  }



}
