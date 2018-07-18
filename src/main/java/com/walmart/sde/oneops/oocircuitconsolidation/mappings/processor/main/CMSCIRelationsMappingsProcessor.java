package com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.main;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.gson.Gson;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.exception.UnSupportedTransformationMappingException;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.model.CmsCIRelationAndRelationAttributesActionMappingsModel;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.util.CircuitconsolidationUtil;

public class CMSCIRelationsMappingsProcessor {


  private final Logger log = LoggerFactory.getLogger(getClass());

  String ns;
  String platformName;
  String ooPhase;
  String envName;
  String nsForPlatformCiComponents;
  Connection conn;


  CMSCIRelationsMappingsProcessor(String ns, String nsForPlatformCiComponents, Connection conn) {
    setNs(ns);
    setNsForPlatformCiComponents(nsForPlatformCiComponents);
    setConn(conn);
  }

  CMSCIRelationsMappingsProcessor(String ns,  String platformName, String ooPhase, String envName, Connection conn) {
    setNs(ns);
    setPlatformName(platformName);
    setOoPhase(ooPhase);
    setEnvName(envName);
    setConn(conn);
    setNsForPlatformCiComponents(CircuitconsolidationUtil.getnsForPlatformCiComponents(ns, platformName, ooPhase, envName));
    
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

  public void setConn(Connection conn) {
    this.conn = conn;
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

    String targetNsPath;

    String relationName = mapping.getSourceCmsCiRelationName();

    switch (relationName) {

      case "base.Requires":
        targetNsPath=this.ns;
        break;
      case "manifest.Requires":
        targetNsPath=this.ns;
        break;

      default:
        targetNsPath=this.nsForPlatformCiComponents;
        break;
    }
    
    if (mapping.getSourceCmsCiRelationName().equalsIgnoreCase("manifest.Requires")) {
      
    }
    
    int sourceFromCMSCIClazzId = mapping.getSourceFromCmsCiClazzId();
    String sourceFromCMSCIClazzName = mapping.getSourceFromCmsCiClazzName();

    int sourceToCMSCIClazzId = mapping.getSourceToCmsCiClazzId();
    String sourceToCMSCIClazzName = mapping.getSourceToCmsCiClazzName();

    int sourceCMSCIRelationId = mapping.getSourceCmsCiRelationId();
    String sourceCMSCIRelationName = mapping.getSourceCmsCiRelationName();

    // operations: delete relation, IF a CI is already deleted then relation may not exist

    /*
     * 
     * insert into cm_ci_relations (ci_relation_id, ns_id, from_ci_id, relation_goid, relation_id,
     * to_ci_id, ci_state_id, comments, last_applied_rfc_id) values (p_ci_relation_id, p_ns_id,
     * p_from_ci_id, p_rel_goid, p_relation_id, p_to_ci_id, p_state_id, p_comments, p_last_rfc_id);
     * 
     * 
     */

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

    String cmsciIdsByClazzAndNsPath_SQL=      "select "+
        "ci.ci_id as ciId, "+
        "ci.ci_name as ciName,"+
        "ci.class_id as ciClassId,"+
        "cl.class_name as ciClassName,"+
        "cl.impl as impl, "+
        "ci.ns_id as nsId, "+
        "ns.ns_path as nsPath, "+
        "ci.ci_goid as ciGoid, "+
        "ci.comments, "+
        "ci.ci_state_id as ciStateId, "+
        "st.state_name as ciState, "+
        "ci.last_applied_rfc_id as lastAppliedRfcId, "+
        "ci.created_by as createdBy, "+
        "ci.updated_by as updatedBy, "+ 
        "ci.created, "+
        "ci.updated "+
    "from cm_ci ci, md_classes cl, ns_namespaces ns, cm_ci_state st "+
    "where ns.ns_path = ? "+
    "and cl.class_name = ? "+
    "and ci.class_id = cl.class_id "+
    "and ci.ns_id = ns.ns_id "+
    "and ci.ci_state_id = st.ci_state_id;";
    
    
    try {



      String sourceFromCmsCiClazzName = mapping.getSourceFromCmsCiClazzName();
  
      log.info("cmsciIdsByClazzAndNsPath_SQL        : "+cmsciIdsByClazzAndNsPath_SQL);
      PreparedStatement preparedStatement = conn.prepareStatement(cmsciIdsByClazzAndNsPath_SQL);
      preparedStatement.setString(1, targetNsPath);
      preparedStatement.setString(2, sourceFromCmsCiClazzName);
      
      log.info("preparedStatement: "+preparedStatement);
      ResultSet resultSet_FromCIds = preparedStatement.executeQuery();
      
      int numberOfRecords=0;

      int numberOfColumns=resultSet_FromCIds.getMetaData().getColumnCount();
      List<Integer> fromCiIds=new ArrayList<Integer>();
      while(resultSet_FromCIds.next()) {
        
        fromCiIds.add(resultSet_FromCIds.getInt(1));
        numberOfRecords++;
      }
      
      log.info(" cmsciIdsByClazzAndNsPath_SQL: numberOfRecords: "+numberOfRecords);


     
  

    
    String cmsciIdsByClazzAndNsPath_SQL2=      "select "+
        "ci.ci_id as ciId, "+
        "ci.ci_name as ciName,"+
        "ci.class_id as ciClassId,"+
        "cl.class_name as ciClassName,"+
        "cl.impl as impl, "+
        "ci.ns_id as nsId, "+
        "ns.ns_path as nsPath, "+
        "ci.ci_goid as ciGoid, "+
        "ci.comments, "+
        "ci.ci_state_id as ciStateId, "+
        "st.state_name as ciState, "+
        "ci.last_applied_rfc_id as lastAppliedRfcId, "+
        "ci.created_by as createdBy, "+
        "ci.updated_by as updatedBy, "+ 
        "ci.created, "+
        "ci.updated "+
    "from cm_ci ci, md_classes cl, ns_namespaces ns, cm_ci_state st "+
    "where ns.ns_path = ? "+
    "and cl.class_name = ? "+
    "and ci.class_id = cl.class_id "+
    "and ci.ns_id = ns.ns_id "+
    "and ci.ci_state_id = st.ci_state_id;";
    
    


      String sourceToCmsCiClazzName = mapping.getSourceToCmsCiClazzName();
  
      log.info("cmsciIdsByClazzAndNsPath_SQL2        : "+cmsciIdsByClazzAndNsPath_SQL2);
      preparedStatement = conn.prepareStatement(cmsciIdsByClazzAndNsPath_SQL2);
      preparedStatement.setString(1, this.nsForPlatformCiComponents);
      preparedStatement.setString(2, sourceToCmsCiClazzName);
      
      log.info("preparedStatement: "+preparedStatement);
      ResultSet resultSet_ToCids = preparedStatement.executeQuery();
      
      numberOfColumns=resultSet_ToCids.getMetaData().getColumnCount();
      numberOfRecords=0;
      List<Integer> toCiIds=new ArrayList<Integer>();
      
      while(resultSet_ToCids.next()) {
        
        toCiIds.add(resultSet_ToCids.getInt(1));
        numberOfRecords++;
      }
      
      log.info(" cmsciIdsByClazzAndNsPath_SQL: numberOfRecords: "+numberOfRecords);
         
    
      String relationsToDeleteSQL =
          "select * from cm_ci_relations ci_rel, ns_namespaces ns "
          + "where ns.ns_path =? "
          + " and ci_rel.relation_id=? "
          + " and ci_rel.from_ci_id =? "
          + " and ci_rel.to_ci_id=?";
  
          
      int numberOfRelationsToDelete=0;
      List<Integer> ci_relation_idList= new ArrayList<Integer>();
      for (int fromCiid : fromCiIds) {

        for(int toCiid: toCiIds) {
          
          
          preparedStatement = conn.prepareStatement(relationsToDeleteSQL);
          preparedStatement.setString(1, targetNsPath);
          preparedStatement.setInt(2, mapping.getSourceCmsCiRelationId());
          preparedStatement.setInt(3, fromCiid);
          preparedStatement.setInt(4, toCiid);
          log.info("preparedStatement: "+preparedStatement);
          ResultSet resultSet_relationsToDelete = preparedStatement.executeQuery();
          
          int numberOfRelations=0;
          while(resultSet_relationsToDelete.next()) {
            numberOfRelations++;
            numberOfRelationsToDelete++;
            ci_relation_idList.add(resultSet_relationsToDelete.getInt("ci_relation_id"));
            
          }
          log.info("numberOfRelations: "+numberOfRelations);
        }
        
      }

     log.info("ci_relation_idList: "+ci_relation_idList.toString());
      log.info("numberOfRelationsToDelete: "+numberOfRelationsToDelete);
      
      
    } catch (Exception e) {
      throw new RuntimeException("Error while fetching records" +e.getMessage());
    }
    


    
    
   // delete from cm_ci_relations where ci_relation_id = ?;
    
 /*   FromCIs: 
    
    select * from cm_ci_relations ci_rel, ns_namespaces ns  
    where 
    ns.ns_path ='/TestOrg2/guineapigs1/_design/guineapig-brown';
    where fromCId =? & toCid=?*/
        
        
    
    
    
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
