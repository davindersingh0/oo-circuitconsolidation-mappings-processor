package com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.main;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.model.CmsCiAndCmsCiAttributesActionMappingsModel;

public class CMSCIMappingsProcessor {

  private final Logger log = LoggerFactory.getLogger(getClass());

  /*
   * (CMCI,UPDATE_CMCI_CLAZZID_CLAZZNAME_GOID) 
   * (CMCI_ATTRIBUTE,DELETE_SOURCE_ATTRIBUTE_ID)
   * (CMCI_ATTRIBUTE,SET_DEFAULT_ATTRIBUTE_VALUE) 
   * (CMCI_ATTRIBUTE,UPDATE_SOURCE_ATTRIBUTE_ID)
   * 
   */
  public void processCMSCIMappings(List<CmsCiAndCmsCiAttributesActionMappingsModel> mappingsList) {

    for (CmsCiAndCmsCiAttributesActionMappingsModel mapping : mappingsList) {

      String entityType = mapping.getEntityType();
      String action = mapping.getAction();

      if (entityType.equalsIgnoreCase("CMCI")) {

        switch (action) {
          case "UPDATE_CMCI_CLAZZID_CLAZZNAME_GOID":
            break;

          default:
            // action not supported, throw exception
            break;
        }

      } else if (entityType.equalsIgnoreCase("CMCI_ATTRIBUTE")) {

        switch (action) {
          case "DELETE_SOURCE_ATTRIBUTE_ID":
            break;

          case "SET_DEFAULT_ATTRIBUTE_VALUE":
            break;

          case "UPDATE_SOURCE_ATTRIBUTE_ID":
            break;

          default:
            // action not supported, throw exception
            break;
        }

      } else {

        // throw exception, not supported entityType
      }


    }


  }



}
