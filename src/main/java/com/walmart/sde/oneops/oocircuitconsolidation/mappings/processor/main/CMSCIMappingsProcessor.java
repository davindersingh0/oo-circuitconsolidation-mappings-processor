package com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.main;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.gson.Gson;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.config.IConstants;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.config.SqlQueries;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.dal.KloopzCmDal;
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

  /*
   * (CMCI,UPDATE_CMCI_CLAZZID_CLAZZNAME_GOID) (CMCI_ATTRIBUTE,DELETE_SOURCE_ATTRIBUTE_ID)
   * (CMCI_ATTRIBUTE,SET_DEFAULT_ATTRIBUTE_VALUE) (CMCI_ATTRIBUTE,UPDATE_SOURCE_ATTRIBUTE_ID)
   * 
   * 
   * NEW Scenarios: (SWITCH_CMCI_CLAZZID_CLAZZNAME_GOID,CMSCI) - covered (DELETE_CMSCI,CMSCI)
   * (CREATE_CMSCI,CMSCI)
   * 
   * (DELETE_CMSCI_ATTRIBUTE,CMSCI_ATTRIBUTE) - covered
   * (SET_DEFAULT_CMSCI_ATTRIBUTE_VALUE,CMSCI_ATTRIBUTE) - covered
   * (SWITCH_CMSCI_ATTRIBUTE_ID,CMSCI_ATTRIBUTE)- covered
   * 
   */
  public void processCMSCIMappings(List<CmsCiAndCmsCiAttributesActionMappingsModel> mappingsList) {
    Gson gson = new Gson();

    //update source property of platform from walmartLabs to oneops
    dal.updatePlatformSourceProperty(ns, platformName);
    
    
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


    int nsId = dal.getNsIdForNsPath(nsForPlatformCiComponents);
    int ciId = dal.getNext_cm_pk_seqId();

    int targetClazzId = mapping.getTargetClassId();
    String ciName = "os"; // hardcoded value, so far only 1 CI is being created

    String goid = nsId + "-" + targetClazzId + "-" + ciId;

    String comments = IConstants.CIRCUIT_CONSOLIDATION_COMMENTS;
    int ciStateId = 100;
    String createdBy = IConstants.CIRCUIT_CONSOLIDATION_USER;

    dal.createCMSCI(nsId, ciId, targetClazzId, ciName, goid, ciStateId, comments, createdBy);


  }



  private void process_DELETE_CMSCI(CmsCiAndCmsCiAttributesActionMappingsModel mapping) {
    // List<Integer> ciIdsToDelete=dal.getCiIdsForNsAndClazz(nsForPlatformCiComponents,
    // mapping.getSourceClassname());
    // log.info("ciIdsToDelete: "+ciIdsToDelete);
    dal.deleteCmsCisForNsAndClazz(nsForPlatformCiComponents, mapping.getSourceClassname());
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

    for (int ciId : ciIds) {

      String goid = nsId + "-" + targetclassId + "-" + ciId;
      dal.cmsCi_update_ciClazzid_clazzname_goid(ciId, sourceClassName, sourceclassId,
          targetClassName, targetclassId, goid);

    }

  }

  private void process_DELETE_CMSCI_ATTRIBUTE(CmsCiAndCmsCiAttributesActionMappingsModel mapping) {


    // from mappings
    String sourceClazz = mapping.getSourceClassname();
    int sourceClazzId = mapping.getSourceClassId();
    String sourceClazzAttributeName = mapping.getSourceAttributeName();
    int sourceClazzAttributeId = mapping.getSourceAttributeId();

    String targetClazz = mapping.getTargetClassname();
    int targetClazzId = mapping.getTargetClassId();

    dal.deleteCMSCIAttribute(this.nsForPlatformCiComponents, sourceClazz, sourceClazzId,
        sourceClazzAttributeId, sourceClazzAttributeName, targetClazz, targetClazzId);

  }

  @Deprecated
  private void process_DELETE_SOURCE_ATTRIBUTE_IDV2(
      CmsCiAndCmsCiAttributesActionMappingsModel mapping) {


    // from mappings
    String sourceClassName = mapping.getSourceClassname();
    int sourceclassId = mapping.getSourceClassId();
    String sourceAttributeName = mapping.getSourceAttributeName();
    int sourceAttributeId = mapping.getSourceAttributeId();

    String targetClassName = mapping.getTargetClassname();
    int targetclassId = mapping.getTargetClassId();

    String sql = SqlQueries.SQL_SELECT_CMSCIATTRIBUTE;

    try {
      // String selectSQL = SqlQueries.SQL_SELECT_NakedCMSCIByNsAndClazz;

      log.info("SQL_SELECT_CMSCIATTRIBUTE        : " + sql);
      PreparedStatement preparedStatement = conn.prepareStatement(sql);

      preparedStatement.setString(1, this.nsForPlatformCiComponents);
      preparedStatement.setInt(2, sourceclassId);
      preparedStatement.setInt(3, sourceAttributeId);


      // preparedStatement.setString(1, this.nsForPlatformCiComponents);

      log.info("preparedStatement: " + preparedStatement);
      ResultSet resultSet = preparedStatement.executeQuery();

      int numberOfRecords = 0;

      int numberOfColumns = resultSet.getMetaData().getColumnCount();
      while (resultSet.next()) {
        numberOfRecords++;
        for (int i = 1; i <= numberOfColumns; i++) {
          log.info(resultSet.getMetaData().getColumnLabel(i) + " : " + resultSet.getObject(i));

        }



      }
      log.info(" SQL_SELECT_CMSCIATTRIBUTE: numberOfRecords: " + numberOfRecords);

    } catch (Exception e) {
      throw new RuntimeException("Error while fetching records" + e.getMessage());
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

      dal.createNewCMSCIAttributeWithDefaultValue(ci_attribute_id, ciId, targetAttributeId,
          targetDefaultValue, IConstants.CIRCUIT_CONSOLIDATION_USER,
          IConstants.CIRCUIT_CONSOLIDATION_COMMENTS);

    }

   

  }

  @Deprecated
  private void processMapping_SET_DEFAULT_ATTRIBUTE_VALUE_V2(
      CmsCiAndCmsCiAttributesActionMappingsModel mapping) {
    // from mappings
    String targetClassName = mapping.getTargetClassname();
    int targetClassId = mapping.getTargetClassId();
    String targetAttributeName = mapping.getTargetAttributeName();
    int targetAttributeId = mapping.getTargetAttributeId();
    String targetDefaultValue = mapping.getTargetDefaultValue();


    // sourceAttributeId
    // select * from CM_CI_Attributes where attribute_id=? and ci_id in (select * from cm_ci where
    // ns_id= ? and class_id=?);

    // select * from CM_CI_Attributes where attribute_id=? and ci_id in (select * from cm_ci where
    // ns_id= ? and class_id=?);
    // cm_ci ci , and ci.class_id = cl.class_id
    // and ci.ns_id = ns.ns_id
    // and ci.ci_state_id = st.ci_state_id
    // from cm_ci ci, md_classes cl, ns_namespaces ns, cm_ci_state st
    /*
     * where ns.ns_path = #{ns} and (#{clazz}::varchar is null or cl.class_name = #{clazz}) and
     * (#{shortClazz}::varchar is null or cl.short_class_name = #{shortClazz}) and (#{name}::varchar
     * is null or lower(ci.ci_name) = lower(#{name})) and ci.class_id = cl.class_id and ci.ns_id =
     * ns.ns_id and ci.ci_state_id = st.ci_state_id
     */

    // operations: create Add New attribute for CMSCI with default value , set for both DJ & DF
    // fields


    // getCMSCI for specific class and add the attribute, if multiple classes then add to all of
    // them

    String sourceClassName = mapping.getSourceClassname();
    int sourceClassId = mapping.getSourceClassId();
    int sourceAttributeId = mapping.getSourceAttributeId();

    String sql = "select " + "ca.ci_attribute_id, " + "ca.attribute_id, " + "ca.ci_id, "
        + "cla.attribute_name, " + "ci.ci_name, " + "ns.ns_path "
        + "from cm_ci_attributes ca, md_class_attributes cla, cm_ci ci, ns_namespaces ns "
        + "where " + "ca.ci_id=ci.ci_id " + "and ci.ns_id = ns.ns_id "
        + "and ca.attribute_id=cla.attribute_id " + "and ns.ns_path =? " + "and ci.class_id=? ;";


    try {
      // String selectSQL = SqlQueries.SQL_SELECT_NakedCMSCIByNsAndClazz;

      log.info("processMapping_SET_DEFAULT_ATTRIBUTE_VALUE       : " + sql);
      PreparedStatement preparedStatement = conn.prepareStatement(sql);

      preparedStatement.setString(1, this.nsForPlatformCiComponents);
      preparedStatement.setInt(2, sourceClassId);



      // preparedStatement.setString(1, this.nsForPlatformCiComponents);

      log.info("preparedStatement: " + preparedStatement);
      ResultSet resultSet = preparedStatement.executeQuery();

      int numberOfRecords = 0;

      int numberOfColumns = resultSet.getMetaData().getColumnCount();
      while (resultSet.next()) {
        numberOfRecords++;
        for (int i = 1; i <= numberOfColumns; i++) {
          log.info(resultSet.getMetaData().getColumnLabel(i) + " : " + resultSet.getObject(i));

        }



      }
      log.info("preparedStatement: " + preparedStatement);
      log.info(" processMapping_SET_DEFAULT_ATTRIBUTE_VALUE: numberOfRecords: " + numberOfRecords);

    } catch (Exception e) {
      throw new RuntimeException("Error while fetching records" + e.getMessage());
    }


    // TODO: WIP


  }


  private void process_SWITCH_CMSCI_ATTRIBUTE_ID(
      CmsCiAndCmsCiAttributesActionMappingsModel mapping) {

    int sourceAttributeId = mapping.getSourceAttributeId();

    int targetClassId = mapping.getTargetClassId();
    int targetAttributeId = mapping.getTargetAttributeId();

    dal.switchCMSCIAttribuetId(this.nsForPlatformCiComponents, targetClassId, targetAttributeId,
        sourceAttributeId);

  }


  @Deprecated
  private void process_SWITCH_CMSCI_ATTRIBUTE_IDV2(
      CmsCiAndCmsCiAttributesActionMappingsModel mapping) {

    // from mappings


    String sourceClassName = mapping.getSourceClassname();
    int sourceClassId = mapping.getSourceClassId();
    int sourceAttributeId = mapping.getSourceAttributeId();


    String targetClassName = mapping.getTargetClassname();
    int targetClassId = mapping.getTargetClassId();
    String targetAttributeName = mapping.getTargetClassname();
    int targetAttributeId = mapping.getTargetAttributeId();

    // TODO: here "and ci.class_id=? "+ needs to be updated to targetClassID instead of
    // sourceClassId, because we are updating classID before updating attributeIds


    String sql = "select " + "ca.ci_attribute_id, " + "ca.attribute_id, " + "ca.ci_id, "
        + "cla.attribute_name, " + "ci.ci_name, " + "ns.ns_path "
        + "from cm_ci_attributes ca, md_class_attributes cla, cm_ci ci, ns_namespaces ns "
        + "where " + "ca.ci_id=ci.ci_id " + "and ci.ns_id = ns.ns_id "
        + "and ca.attribute_id=cla.attribute_id " + "and ns.ns_path =? " + "and ci.class_id=? "
        + "and ca.attribute_id=?; ";


    try {
      // String selectSQL = SqlQueries.SQL_SELECT_NakedCMSCIByNsAndClazz;

      log.info("SQL_SELECT_CMSCIATTRIBUTE        : " + sql);
      PreparedStatement preparedStatement = conn.prepareStatement(sql);

      preparedStatement.setString(1, this.nsForPlatformCiComponents);
      preparedStatement.setInt(2, targetClassId);
      preparedStatement.setInt(3, sourceAttributeId);


      // preparedStatement.setString(1, this.nsForPlatformCiComponents);

      log.info("preparedStatement: " + preparedStatement);
      ResultSet resultSet = preparedStatement.executeQuery();

      int numberOfRecords = 0;

      int numberOfColumns = resultSet.getMetaData().getColumnCount();
      while (resultSet.next()) {
        numberOfRecords++;
        for (int i = 1; i <= numberOfColumns; i++) {
          log.info(resultSet.getMetaData().getColumnLabel(i) + " : " + resultSet.getObject(i));

        }

      }
      log.info(" process_UPDATE_SOURCE_ATTRIBUTE_ID: numberOfRecords: " + numberOfRecords);
      System.exit(0);

    } catch (Exception e) {
      throw new RuntimeException("Error while fetching records" + e.getMessage());
    }



  }

  

}
