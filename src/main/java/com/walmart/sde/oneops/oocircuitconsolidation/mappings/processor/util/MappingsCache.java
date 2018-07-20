package com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
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

  public Map<String, List> createTransformationMappingsCache(Connection conn, String ooPhase) {

    log.info("loading transformation mappings...");

    Map<String, List> transformationMappingsMap = new HashMap<String, List>();

    List<CmsCiAndCmsCiAttributesActionMappingsModel> cmsCiMappings = getCMSCiMappings(conn, ooPhase);
    List<CmsCIRelationAndRelationAttributesActionMappingsModel> cmsCiRelationsMappings =
        getCMSCiRelationsMappings(conn, ooPhase);

    log.info("cmsCiMappings: " + new Gson().toJson(cmsCiMappings));
    log.info("cmsCiRelationsMappings: " + new Gson().toJson(cmsCiRelationsMappings));
    
    transformationMappingsMap.put(IConstants.cmsCiMappingsMapKey, cmsCiMappings);
    transformationMappingsMap.put(IConstants.cmsCiRelationsMappingsMapKey, cmsCiRelationsMappings);

    log.info("loaded transformation mappings");

    return transformationMappingsMap;


  }

  private List<CmsCiAndCmsCiAttributesActionMappingsModel> getCMSCiMappings(Connection conn, String ooPhase) {

    List<CmsCiAndCmsCiAttributesActionMappingsModel> cmsCiMappingsList =
        new ArrayList<CmsCiAndCmsCiAttributesActionMappingsModel>();

    try {
     
      String sqlGetAllCiMappings = "SELECT * FROM kloopzcm.CmsCiAndCmsCiAttributesActionMappings where sourceclassid!=targetclassid and oophase=?;";
      PreparedStatement preparedStement=conn.prepareStatement(sqlGetAllCiMappings);
      preparedStement.setString(1, ooPhase);
      
      log.info("preparedStement: "+preparedStement);
      ResultSet resultSet = preparedStement.executeQuery();


      while (resultSet.next()) {
        CmsCiAndCmsCiAttributesActionMappingsModel mapping =
            new CmsCiAndCmsCiAttributesActionMappingsModel();

        //String ooPhase=resultSet.getString("oophase");
        String sourcePack = resultSet.getString("sourcepack");
        String sourceClassname = resultSet.getString("sourceclassname");
        int sourceClassId = resultSet.getInt("sourceclassid");
        String sourceAttributeName = resultSet.getString("sourceattributename");
        int sourceAttributeId = resultSet.getInt("sourceattributeid");
        String sourceDefaultValue = resultSet.getString("sourcedefaultvalue");
        String targetPack = resultSet.getString("targetpack");
        String targetClassname = resultSet.getString("targetclassname");
        int targetClassId = resultSet.getInt("targetclassid");
        String targetAttributeName = resultSet.getString("targetattributename");
        int targetAttributeId = resultSet.getInt("targetattributeid");
        String targetDefaultValue = resultSet.getString("targetdefaultvalue");
        String action = resultSet.getString("action");
        String entityType = resultSet.getString("entitytype");

        mapping.setOoPhase(ooPhase);
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
      Connection conn, String ooPhase) {

    List<CmsCIRelationAndRelationAttributesActionMappingsModel> cmsCiRelationsMappingsList =
        new ArrayList<CmsCIRelationAndRelationAttributesActionMappingsModel>();



    try {
     
      String sqlGetAllCiMappings =
          "SELECT * FROM kloopzcm.CmsCIRelationAndRelationAttributesActionMappings where oophase=?;";
      log.info("sqlGetAllCiMappings: "+sqlGetAllCiMappings);
    

      PreparedStatement preparedStement=conn.prepareStatement(sqlGetAllCiMappings);
      preparedStement.setString(1, ooPhase);
      
      log.info("preparedStement: "+preparedStement);
      ResultSet resultSet = preparedStement.executeQuery();
      

      while (resultSet.next()) {

        CmsCIRelationAndRelationAttributesActionMappingsModel mapping =
            new CmsCIRelationAndRelationAttributesActionMappingsModel();

        //String ooPhase=resultSet.getString("oophase");
        String sourcePack = resultSet.getString("sourcepack");
        String targetPack = resultSet.getString("targetpack");

        String sourceCmsCiRelationKey = resultSet.getString("sourcecmscirelationkey");
        String sourceCmsCiRelationName = resultSet.getString("sourcecmscirelationname");
        int sourceCmsCiRelationId = resultSet.getInt("sourcecmscirelationid");
        String sourceFromCmsCiClazzName = resultSet.getString("sourcefromcmsciclazzname");
        int sourceFromCmsCiClazzId = resultSet.getInt("sourcefromcmsciclazzid");
        String sourceToCmsCiClazzName = resultSet.getString("sourcetocmsciclazzname");
        int sourceToCmsCiClazzId = resultSet.getInt("sourcetocmsciclazzid");

        String targetCmsCiRelationKey = resultSet.getString("targetcmscirelationkey");


        String targetCmsCiRelationName = resultSet.getString("targetcmscirelationname");
        int targetCmsCiRelationId = resultSet.getInt("targetcmscirelationid");
        String targetFromCmsCiClazzName = resultSet.getString("targetfromcmsciclazzname");
        int targetFromCmsCiClazzId = resultSet.getInt("targetfromcmsciclazzid");// need fix
        String targetToCmsCiClazzName = resultSet.getString("targettocmsciclazzname");
        int targetToCmsCiClazzId = resultSet.getInt("targettocmsciclazzid");

        int attributeId = resultSet.getInt("attributeid");
        int relationId = resultSet.getInt("relationid");
        String attributeName = resultSet.getString("attributename");
        String dfValue = resultSet.getString("dfvalue");
        String djValue = resultSet.getString("djvalue");

        String action = resultSet.getString("action");
        String entityType = resultSet.getString("entitytype");

        
        
        // setters

        mapping.setOoPhase(ooPhase);
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
