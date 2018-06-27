package com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.main;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.gson.Gson;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.exception.UnSupportedTransformationMappingException;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.model.CmsCiAndCmsCiAttributesActionMappingsModel;

public class CMSCIMappingsProcessor {

  private final Logger log = LoggerFactory.getLogger(getClass());

  Gson gson = new Gson();

  /*
   * (CMCI,UPDATE_CMCI_CLAZZID_CLAZZNAME_GOID) (CMCI_ATTRIBUTE,DELETE_SOURCE_ATTRIBUTE_ID)
   * (CMCI_ATTRIBUTE,SET_DEFAULT_ATTRIBUTE_VALUE) (CMCI_ATTRIBUTE,UPDATE_SOURCE_ATTRIBUTE_ID)
   * 
   */
  public void processCMSCIMappings(List<CmsCiAndCmsCiAttributesActionMappingsModel> mappingsList) {

    for (CmsCiAndCmsCiAttributesActionMappingsModel mapping : mappingsList) {

      String entityType = mapping.getEntityType();
      String action = mapping.getAction();

      if (entityType.equalsIgnoreCase("CMCI")) {

        switch (action) {
          case "UPDATE_CMCI_CLAZZID_CLAZZNAME_GOID":
            process_UPDATE_CMCI_CLAZZID_CLAZZNAME_GOID(mapping);
            break;

          default:
            throw new UnSupportedTransformationMappingException("<action>: " + action +" for <entityType>: " + entityType
                +  "not supported, mapping record: " + gson.toJson(mapping));


        }

      } else if (entityType.equalsIgnoreCase("CMCI_ATTRIBUTE")) {

        switch (action) {
          case "DELETE_SOURCE_ATTRIBUTE_ID":
            process_DELETE_SOURCE_ATTRIBUTE_ID(mapping);
            break;

          case "SET_DEFAULT_ATTRIBUTE_VALUE":
            processMapping_SET_DEFAULT_ATTRIBUTE_VALUE(mapping);
            break;

          case "UPDATE_SOURCE_ATTRIBUTE_ID":
            process_UPDATE_SOURCE_ATTRIBUTE_ID(mapping);
            break;

          default:
            throw new UnSupportedTransformationMappingException("<action>: " + action +" for <entityType>: " + entityType
                +  "not supported, mapping record: " + gson.toJson(mapping));
        }

      } else {

        throw new UnSupportedTransformationMappingException("<entityType>: " + entityType
            + " not supported, mapping record: " + gson.toJson(mapping));
      }


    }


  }

  private void process_UPDATE_CMCI_CLAZZID_CLAZZNAME_GOID(
      CmsCiAndCmsCiAttributesActionMappingsModel mapping) {


  }

  private void process_DELETE_SOURCE_ATTRIBUTE_ID(
      CmsCiAndCmsCiAttributesActionMappingsModel mapping) {


  }

  private void processMapping_SET_DEFAULT_ATTRIBUTE_VALUE(
      CmsCiAndCmsCiAttributesActionMappingsModel mapping) {


  }

  private void process_UPDATE_SOURCE_ATTRIBUTE_ID(
      CmsCiAndCmsCiAttributesActionMappingsModel mapping) {

  }


}
