package com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.gson.Gson;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.config.IConstants;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.model.CmsCIRelationAndRelationAttributesActionMappingsModel;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.model.CmsCiAndCmsCiAttributesActionMappingsModel;

public class MappingsCache {

  private final Logger log = LoggerFactory.getLogger(getClass());

  public Map<String, List> createTransformationMappingsCache(Connection conn) {

    log.info("loading transformation mappings...");

    Map<String, List> transformationMappingsMap = new HashMap<String, List>();

    List<CmsCiAndCmsCiAttributesActionMappingsModel> cmsCiMappings = getCMSCiMappings(conn);
    List<CmsCIRelationAndRelationAttributesActionMappingsModel> cmsCiRelationsMappings =
        getCMSCiRelationsMappings(conn);

    log.info("cmsCiMappings: " + new Gson().toJson(cmsCiMappings));
    log.info("cmsCiRelationsMappings: " + new Gson().toJson(cmsCiRelationsMappings));
    
    transformationMappingsMap.put(IConstants.cmsCiMappingsMapKey, cmsCiMappings);
    transformationMappingsMap.put(IConstants.cmsCiRelationsMappingsMapKey, cmsCiRelationsMappings);

    log.info("loaded transformation mappings");

    return transformationMappingsMap;


  }

  private List<CmsCiAndCmsCiAttributesActionMappingsModel> getCMSCiMappings(Connection conn) {

    List<CmsCiAndCmsCiAttributesActionMappingsModel> cmsCiMappingsList =
        new ArrayList<CmsCiAndCmsCiAttributesActionMappingsModel>();

    try {
      Statement statement = conn.createStatement();
      String sqlGetAllCiMappings = "SELECT * FROM kloopzcm.CmsCiAndCmsCiAttributesActionMappings";
      ResultSet resultSet = statement.executeQuery(sqlGetAllCiMappings);


      while (resultSet.next()) {
        CmsCiAndCmsCiAttributesActionMappingsModel mapping =
            new CmsCiAndCmsCiAttributesActionMappingsModel();

        String sourcePack = resultSet.getString(1);
        String sourceClassname = resultSet.getString(2);
        int sourceClassId = resultSet.getInt(3);
        String sourceAttributeName = resultSet.getString(4);
        int sourceAttributeId = resultSet.getInt(5);
        String sourceDefaultValue = resultSet.getString(6);
        String targetPack = resultSet.getString(7);
        String targetClassname = resultSet.getString(8);
        int targetClassId = resultSet.getInt(9);
        String targetAttributeName = resultSet.getString(10);
        int targetAttributeId = resultSet.getInt(11);
        String targetDefaultValue = resultSet.getString(12);
        String action = resultSet.getString(13);
        String entityType = resultSet.getString(14);


        mapping.setSourcePack(sourcePack);
        mapping.setSourceClassname(sourceClassname);
        mapping.setSourceClassId(sourceClassId);
        mapping.setSourceAttributeName(sourceAttributeName);
        mapping.setSourceAttributeId(sourceAttributeId);
        mapping.setSourceDefaultValue(sourceDefaultValue);
        mapping.setTargetPack(targetPack);
        mapping.setTargetClassname(targetClassname);
        mapping.setTargetClassId(targetClassId);
        mapping.setTargetAttributeName(targetAttributeName);
        mapping.setTargetAttributeId(targetAttributeId);
        mapping.setTargetDefaultValue(targetDefaultValue);
        mapping.setAction(action);
        mapping.setEntityType(entityType);


        cmsCiMappingsList.add(mapping);

      }

    } catch (SQLException e) {
      log.error(e.getMessage());
      throw new RuntimeException("Error while reading mappings table: " + e.getMessage());
    }

    return cmsCiMappingsList;
  }

  private List<CmsCIRelationAndRelationAttributesActionMappingsModel> getCMSCiRelationsMappings(
      Connection conn) {

    List<CmsCIRelationAndRelationAttributesActionMappingsModel> cmsCiRelationsMappingsList =
        new ArrayList<CmsCIRelationAndRelationAttributesActionMappingsModel>();



    try {
      Statement statement = conn.createStatement();
      String sqlGetAllCiMappings =
          "SELECT * FROM kloopzcm.CmsCIRelationAndRelationAttributesActionMappings";
      ResultSet resultSet = statement.executeQuery(sqlGetAllCiMappings);

      log.info("MetaData: " + resultSet.getMetaData());


      while (resultSet.next()) {

        CmsCIRelationAndRelationAttributesActionMappingsModel mapping =
            new CmsCIRelationAndRelationAttributesActionMappingsModel();


        String sourcePack = resultSet.getString(1);
        String targetPack = resultSet.getString(2);

        String sourceCmsCiRelationKey = resultSet.getString(3);
        String sourceCmsCiRelationName = resultSet.getString(4);
        int sourceCmsCiRelationId = resultSet.getInt(5);
        String sourceFromCmsCiClazzName = resultSet.getString(6);
        int sourceFromCmsCiClazzId = resultSet.getInt(7);
        String sourceToCmsCiClazzName = resultSet.getString(8);
        int sourceToCmsCiClazzId = resultSet.getInt(9);

        String targetCmsCiRelationKey = resultSet.getString(10);


        String targetCmsCiRelationName = resultSet.getString(11);
        int targetCmsCiRelationId = resultSet.getInt(12);
        String targetFromCmsCiClazzName = resultSet.getString(13);
        int targetFromCmsCiClazzId = resultSet.getInt(14);// need fix
        String targetToCmsCiClazzName = resultSet.getString(15);
        int targetToCmsCiClazzId = resultSet.getInt(16);

        int attributeId = resultSet.getInt(16);
        int relationId = resultSet.getInt(18);
        String attributeName = resultSet.getString(19);
        String dfValue = resultSet.getString(20);
        String djValue = resultSet.getString(21);

        String action = resultSet.getString(22);
        String entityType = resultSet.getString(23);


        // setters

        mapping.setSourcePack(sourcePack);
        mapping.setTargetPack(targetPack);
        mapping.setSourceCmsCiRelationKey(sourceCmsCiRelationKey);
        mapping.setSourceCmsCiRelationName(sourceCmsCiRelationName);
        mapping.setSourceCmsCiRelationId(sourceCmsCiRelationId);
        mapping.setSourceFromCmsCiClazzName(sourceFromCmsCiClazzName);
        mapping.setSourceFromCmsCiClazzId(sourceFromCmsCiClazzId);
        mapping.setSourceToCmsCiClazzName(sourceToCmsCiClazzName);
        mapping.setSourceToCmsCiClazzId(sourceToCmsCiClazzId);
        mapping.setTargetCmsCiRelationKey(targetCmsCiRelationKey);
        mapping.setTargetCmsCiRelationName(targetCmsCiRelationName);
        mapping.setTargetCmsCiRelationId(targetCmsCiRelationId);
        mapping.setTargetFromCmsCiClazzName(targetFromCmsCiClazzName);
        mapping.setTargetFromCmsCiClazzId(targetFromCmsCiClazzId);
        mapping.setTargetToCmsCiClazzName(targetToCmsCiClazzName);
        mapping.setTargetToCmsCiClazzId(targetToCmsCiClazzId);
        mapping.setAttributeId(attributeId);
        mapping.setRelationId(relationId);
        mapping.setAttributeName(attributeName);
        mapping.setDfValue(dfValue);
        mapping.setDjValue(djValue);
        mapping.setAction(action);
        mapping.setEntityType(entityType);

        cmsCiRelationsMappingsList.add(mapping);

      }


    } catch (SQLException e) {
      log.error(e.getMessage());
      throw new RuntimeException("Error while reading mappings table: " + e.getMessage());
    }
    return cmsCiRelationsMappingsList;


  }


}
