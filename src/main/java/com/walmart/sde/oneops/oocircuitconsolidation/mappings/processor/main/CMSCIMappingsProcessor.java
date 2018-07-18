package com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.main;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.gson.Gson;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.config.SqlQueries;
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
  

  CMSCIMappingsProcessor(String nsForPlatformCiComponents, Connection conn) {
    setNsForPlatformCiComponents(nsForPlatformCiComponents);
    setConn(conn);
  }

  CMSCIMappingsProcessor(String ns,  String platformName, String ooPhase, String envName, Connection conn) {
    
    setNs(ns);
    setPlatformName(platformName);
    setOoPhase(ooPhase);
    setEnvName(envName);
    setConn(conn);
    setNsForPlatformCiComponents(CircuitconsolidationUtil.getnsForPlatformCiComponents(ns, platformName, ooPhase, envName));
    
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

  /*
   * (CMCI,UPDATE_CMCI_CLAZZID_CLAZZNAME_GOID) (CMCI_ATTRIBUTE,DELETE_SOURCE_ATTRIBUTE_ID)
   * (CMCI_ATTRIBUTE,SET_DEFAULT_ATTRIBUTE_VALUE) (CMCI_ATTRIBUTE,UPDATE_SOURCE_ATTRIBUTE_ID)
   * 
   */
  public void processCMSCIMappings(List<CmsCiAndCmsCiAttributesActionMappingsModel> mappingsList) {
    Gson gson = new Gson();

    for (CmsCiAndCmsCiAttributesActionMappingsModel mapping : mappingsList) {

      String entityType = mapping.getEntityType();
      String action = mapping.getAction();

      if (entityType.equalsIgnoreCase("CMCI")) {

        switch (action) {
          case "UPDATE_CMCI_CLAZZID_CLAZZNAME_GOID":
            process_UPDATE_CMCI_CLAZZID_CLAZZNAME_GOID(mapping);
            break;

          default:
            throw new UnSupportedTransformationMappingException(
                "<action>: " + action + " for <entityType>: " + entityType
                    + "not supported, mapping record: " + gson.toJson(mapping));


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

  private void process_UPDATE_CMCI_CLAZZID_CLAZZNAME_GOID(
      CmsCiAndCmsCiAttributesActionMappingsModel mapping) {

    //TODO: create a Util function to create GOID
    // from mappings
    String sourceClassName = mapping.getSourceClassname();
    int sourceclassId = mapping.getSourceClassId();
    String targetClassName = mapping.getTargetClassname();
    int targetclassId = mapping.getTargetClassId();

    // from platform
    String sourcegoid;
    // set to platform
    String targetgoid;

    try {



      String selectSQL = SqlQueries.SQL_SELECT_NakedCMSCIByNsAndClazz;
  
      log.info("selectSQL        : "+selectSQL);
      PreparedStatement preparedStatement = conn.prepareStatement(selectSQL);
      preparedStatement.setString(1, this.nsForPlatformCiComponents);
      preparedStatement.setString(2, sourceClassName);
      
      log.info("preparedStatement: "+preparedStatement);
      ResultSet resultSet = preparedStatement.executeQuery();
      
      int numberOfRecords=0;

      int numberOfColumns=resultSet.getMetaData().getColumnCount();
      while (resultSet.next()) {
        numberOfRecords++;
        for(int i=1;i<=numberOfColumns;i++) {
          log.info(resultSet.getMetaData().getColumnLabel(i) +" : " +resultSet.getObject(i));
          
        }
      


      }
      log.info(" SQL_SELECT_NakedCMSCIByNsAndClazz: numberOfRecords: "+numberOfRecords);
    } catch (Exception e) {
      throw new RuntimeException("Error while fetching records" +e.getMessage());
    }
    // operations - UpdateClazzId, UpdateClazzName, UpdateGoid

  }

  private void process_DELETE_SOURCE_ATTRIBUTE_ID(
      CmsCiAndCmsCiAttributesActionMappingsModel mapping) {


    // from mappings
    String sourceClassName = mapping.getSourceClassname();
    int sourceclassId = mapping.getSourceClassId();
    String sourceAttributeName = mapping.getSourceAttributeName();
    int sourceAttributeId = mapping.getSourceAttributeId();

    String targetClassName = mapping.getTargetClassname();
    int targetclassId = mapping.getTargetClassId();

    String sql=SqlQueries.SQL_SELECT_CMSCIATTRIBUTE;
    
        try {
         // String selectSQL = SqlQueries.SQL_SELECT_NakedCMSCIByNsAndClazz;
      
          log.info("SQL_SELECT_CMSCIATTRIBUTE        : "+sql);
          PreparedStatement preparedStatement = conn.prepareStatement(sql);
          
          preparedStatement.setString(1, this.nsForPlatformCiComponents);
          preparedStatement.setInt(2, sourceclassId);
          preparedStatement.setInt(3, sourceAttributeId);
          
          
          // preparedStatement.setString(1, this.nsForPlatformCiComponents);
          
          log.info("preparedStatement: "+preparedStatement);
          ResultSet resultSet = preparedStatement.executeQuery();
          
          int numberOfRecords=0;

          int numberOfColumns=resultSet.getMetaData().getColumnCount();
          while (resultSet.next()) {
            numberOfRecords++;
            for(int i=1;i<=numberOfColumns;i++) {
              log.info(resultSet.getMetaData().getColumnLabel(i) +" : " +resultSet.getObject(i));
              
            }
          


          }
          log.info(" SQL_SELECT_CMSCIATTRIBUTE: numberOfRecords: "+numberOfRecords);

        } catch (Exception e) {
          throw new RuntimeException("Error while fetching records" +e.getMessage());
        }
    
    
    // operation: deleteCIAttribute

  }

  private void processMapping_SET_DEFAULT_ATTRIBUTE_VALUE(
      CmsCiAndCmsCiAttributesActionMappingsModel mapping) {
    // from mappings
    String targetClassName = mapping.getTargetClassname();
    int targetClassId = mapping.getTargetClassId();
    String targetAttributeName = mapping.getTargetAttributeName();
    int targetAttributeId = mapping.getTargetAttributeId();
    String targetDefaultValue = mapping.getTargetDefaultValue();

    
    // sourceAttributeId
    // select * from CM_CI_Attributes where attribute_id=? and ci_id in (select * from cm_ci where ns_id= ? and class_id=?);
     
  // select * from CM_CI_Attributes where attribute_id=? and ci_id in (select * from cm_ci where ns_id= ? and class_id=?);
     //cm_ci ci ,            and ci.class_id = cl.class_id
//     and ci.ns_id = ns.ns_id
//     and ci.ci_state_id = st.ci_state_id
//           from cm_ci ci, md_classes cl, ns_namespaces ns, cm_ci_state st
     /*where ns.ns_path = #{ns}
     and (#{clazz}::varchar is null or cl.class_name = #{clazz})
     and (#{shortClazz}::varchar is null or cl.short_class_name = #{shortClazz})
     and (#{name}::varchar is null or lower(ci.ci_name) = lower(#{name}))
     and ci.class_id = cl.class_id
     and ci.ns_id = ns.ns_id
     and ci.ci_state_id = st.ci_state_id
     */
    
    // operations: create Add New attribute for CMSCI with default value , set for both DJ & DF
    // fields

    
    //getCMSCI for specific class and add the attribute, if multiple classes then add to all of them
    
    String sourceClassName = mapping.getSourceClassname();
    int sourceClassId = mapping.getSourceClassId();
    int sourceAttributeId = mapping.getSourceAttributeId();
    
    String sql =    "select "+
        "ca.ci_attribute_id, "+
        "ca.attribute_id, "+
        "ca.ci_id, "+
        "cla.attribute_name, "+
        "ci.ci_name, "+
        "ns.ns_path "+
        "from cm_ci_attributes ca, md_class_attributes cla, cm_ci ci, ns_namespaces ns "+
        "where "+
        "ca.ci_id=ci.ci_id "+
        "and ci.ns_id = ns.ns_id "+
        "and ca.attribute_id=cla.attribute_id "+
        "and ns.ns_path =? "+
        "and ci.class_id=? ;";
    
    
    try {
      // String selectSQL = SqlQueries.SQL_SELECT_NakedCMSCIByNsAndClazz;
   
       log.info("processMapping_SET_DEFAULT_ATTRIBUTE_VALUE       : "+sql);
       PreparedStatement preparedStatement = conn.prepareStatement(sql);
       
       preparedStatement.setString(1, this.nsForPlatformCiComponents);
       preparedStatement.setInt(2, sourceClassId);
      
       
       
       // preparedStatement.setString(1, this.nsForPlatformCiComponents);
       
       log.info("preparedStatement: "+preparedStatement);
       ResultSet resultSet = preparedStatement.executeQuery();
       
       int numberOfRecords=0;

       int numberOfColumns=resultSet.getMetaData().getColumnCount();
       while (resultSet.next()) {
         numberOfRecords++;
         for(int i=1;i<=numberOfColumns;i++) {
           log.info(resultSet.getMetaData().getColumnLabel(i) +" : " +resultSet.getObject(i));
           
         }
       


       }
       log.info("preparedStatement: "+preparedStatement);
       log.info(" processMapping_SET_DEFAULT_ATTRIBUTE_VALUE: numberOfRecords: "+numberOfRecords);

     } catch (Exception e) {
       throw new RuntimeException("Error while fetching records" +e.getMessage());
     }
 
    
    //TODO: WIP
    
    
    
  }

  private void process_UPDATE_SOURCE_ATTRIBUTE_ID(
      CmsCiAndCmsCiAttributesActionMappingsModel mapping) {

    // from mappings
    
    
    String sourceClassName = mapping.getSourceClassname();
    int sourceClassId = mapping.getSourceClassId();
    int sourceAttributeId = mapping.getSourceAttributeId();
    
    
    String targetClassName = mapping.getTargetClassname();
    int targetClassId = mapping.getTargetClassId();
    String targetAttributeName = mapping.getTargetClassname();
    int targetAttributeId = mapping.getTargetAttributeId();

    //TODO: here "and ci.class_id=? "+ needs to be updated to targetClassID instead of sourceClassId, because we are updating classID before updating attributeIds
    

    String sql =    "select "+
        "ca.ci_attribute_id, "+
        "ca.attribute_id, "+
        "ca.ci_id, "+
        "cla.attribute_name, "+
        "ci.ci_name, "+
        "ns.ns_path "+
        "from cm_ci_attributes ca, md_class_attributes cla, cm_ci ci, ns_namespaces ns "+
        "where "+
        "ca.ci_id=ci.ci_id "+
        "and ci.ns_id = ns.ns_id "+
        "and ca.attribute_id=cla.attribute_id "+
        "and ns.ns_path =? "+
        "and ci.class_id=? "+
        "and ca.attribute_id=?; ";
    

    try {
      // String selectSQL = SqlQueries.SQL_SELECT_NakedCMSCIByNsAndClazz;
   
       log.info("SQL_SELECT_CMSCIATTRIBUTE        : "+sql);
       PreparedStatement preparedStatement = conn.prepareStatement(sql);
       
       preparedStatement.setString(1, this.nsForPlatformCiComponents);
       preparedStatement.setInt(2, sourceClassId);
       preparedStatement.setInt(3, sourceAttributeId);
       
       
       // preparedStatement.setString(1, this.nsForPlatformCiComponents);
       
       log.info("preparedStatement: "+preparedStatement);
       ResultSet resultSet = preparedStatement.executeQuery();
       
       int numberOfRecords=0;

       int numberOfColumns=resultSet.getMetaData().getColumnCount();
       while (resultSet.next()) {
         numberOfRecords++;
         for(int i=1;i<=numberOfColumns;i++) {
           log.info(resultSet.getMetaData().getColumnLabel(i) +" : " +resultSet.getObject(i));
           
         }
       


       }
       log.info(" process_UPDATE_SOURCE_ATTRIBUTE_ID: numberOfRecords: "+numberOfRecords);

     } catch (Exception e) {
       throw new RuntimeException("Error while fetching records" +e.getMessage());
     }
 
    
    
    
  }


}
