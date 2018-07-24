package com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.main;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.gson.Gson;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.config.IConstants;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.dal.KloopzCmDal;
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
  KloopzCmDal dal;


  CMSCIRelationsMappingsProcessor(String ns, String platformName, String ooPhase, String envName,
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
    String comments = IConstants.CIRCUIT_CONSOLIDATION_COMMENTS;


    int ci_state_id = 100;


    List<Integer> fromCiIds = new ArrayList<Integer>();
    if (targetFromCMSCIClazzName.contains(".Platform")) {
      fromCiIds = dal.getCiIdsForNsAndClazz(this.ns, targetFromCMSCIClazzName);
    } else {
      fromCiIds =
          dal.getCiIdsForNsAndClazz(this.nsForPlatformCiComponents, targetFromCMSCIClazzName);

    }
    int nsId = dal.getNsIdForNsPath(this.nsForPlatformCiComponents);

    List<Integer> toCiIds =
        dal.getCiIdsForNsAndClazz(this.nsForPlatformCiComponents, targetToCMSCIClazzName);

    log.info(
        "creating CMSCIRelation targetCMSCIRelationName {} targetCMSCIRelationId {} targetFromCMSCIClazzName {}, targetFromCMSCIClazzId {}, targetToCMSCIClazzName {} targetToCMSCIClazzId",
        targetCMSCIRelationName, targetCMSCIRelationId, targetFromCMSCIClazzName,
        targetFromCMSCIClazzId, targetToCMSCIClazzName, targetToCMSCIClazzId);

    log.info("creating CMSCIRelation fromCiIds <{}> To toCiIds <{}>", fromCiIds.toString(),
        toCiIds.toString());



    for (int fromCiId : fromCiIds) {
      for (int toCiId : toCiIds) {

        int ci_relation_id = dal.getNext_cm_pk_seqId();
        String relation_goid = fromCiId + "-" + targetCMSCIRelationId + "-" + toCiId;
        dal.createCMSCIRelation(ci_relation_id, nsId, fromCiId, relation_goid,
            targetCMSCIRelationId, toCiId, ci_state_id, comments);


      }


    }


  }


  @Deprecated
  private void process_CREATE_RELATIONV2(
      CmsCIRelationAndRelationAttributesActionMappingsModel mapping) {

    String targetCMSCIRelationName = mapping.getTargetCmsCiRelationName();
    int targetCMSCIRelationId = mapping.getTargetCmsCiRelationId();
    String targetFromCMSCIClazzName = mapping.getTargetFromCmsCiClazzName();
    int targetFromCMSCIClazzId = mapping.getTargetFromCmsCiClazzId();
    String targetToCMSCIClazzName = mapping.getTargetToCmsCiClazzName();
    int targetToCMSCIClazzId = mapping.getTargetToCmsCiClazzId();

    String relationGoid; // create


    // create: create New Relation


    // TODO: working on create relation:

    // {call cm_create_relation(#{ciRelationId}, #{nsId}, #{fromCiId}, #{relationId}, #{toCiId},
    // #{relationGoid}, #{comments}, #{relationStateId})}



    // get from CIs for given clazz, get toCiz For given clazz, create relations in loop to ensure
    // all fromCIs have relation with ToCis

    String cmsciIdsByClazzAndNsPath_SQL = "select " + "ci.ci_id as ciId, " + "ci.ci_name as ciName,"
        + "ci.class_id as ciClassId," + "cl.class_name as ciClassName," + "cl.impl as impl, "
        + "ci.ns_id as nsId, " + "ns.ns_path as nsPath, " + "ci.ci_goid as ciGoid, "
        + "ci.comments, " + "ci.ci_state_id as ciStateId, " + "st.state_name as ciState, "
        + "ci.last_applied_rfc_id as lastAppliedRfcId, " + "ci.created_by as createdBy, "
        + "ci.updated_by as updatedBy, " + "ci.created, " + "ci.updated "
        + "from cm_ci ci, md_classes cl, ns_namespaces ns, cm_ci_state st "
        + "where ns.ns_path = ? " + "and cl.class_name = ? " + "and ci.class_id = cl.class_id "
        + "and ci.ns_id = ns.ns_id " + "and ci.ci_state_id = st.ci_state_id;";



    String targetNsPath;

    if (targetFromCMSCIClazzName.contains(".Platform")) {
      targetNsPath = this.ns;
    } else {
      targetNsPath = this.nsForPlatformCiComponents;

    }

    try {


      log.info("cmsciIdsByClazzAndNsPath_SQL        : " + cmsciIdsByClazzAndNsPath_SQL);
      PreparedStatement preparedStatement = conn.prepareStatement(cmsciIdsByClazzAndNsPath_SQL);
      preparedStatement.setString(1, targetNsPath);
      preparedStatement.setString(2, targetFromCMSCIClazzName);

      log.info("preparedStatement: " + preparedStatement);
      ResultSet resultSet_FromCIds = preparedStatement.executeQuery();

      int numberOfRecords = 0;


      List<Integer> fromCiIds = new ArrayList<Integer>();
      while (resultSet_FromCIds.next()) {

        fromCiIds.add(resultSet_FromCIds.getInt(1));
        numberOfRecords++;
      }

      log.info(" cmsciIdsByClazzAndNsPath_SQL: numberOfRecords: " + numberOfRecords);


      // ToClazzBlock

      if (targetToCMSCIClazzName.contains(".Platform")) {
        targetNsPath = this.ns;
      } else {
        targetNsPath = this.nsForPlatformCiComponents;

      }

      log.info("cmsciIdsByClazzAndNsPath_SQL        : " + cmsciIdsByClazzAndNsPath_SQL);
      preparedStatement = conn.prepareStatement(cmsciIdsByClazzAndNsPath_SQL);
      preparedStatement.setString(1, targetNsPath);
      preparedStatement.setString(2, targetToCMSCIClazzName);

      log.info("preparedStatement: " + preparedStatement);
      ResultSet resultSet_ToCIds = preparedStatement.executeQuery();

      numberOfRecords = 0;


      List<Integer> toCiIds = new ArrayList<Integer>();
      while (resultSet_ToCIds.next()) {

        toCiIds.add(resultSet_ToCIds.getInt(1));
        numberOfRecords++;
      }

      log.info(" cmsciIdsByClazzAndNsPath_SQL: numberOfRecords: " + numberOfRecords);


      for (int fromCiId : fromCiIds) {

        for (int toCiId : toCiIds) {

          String relationName = mapping.getSourceCmsCiRelationName();
          String targetNsPath_createRelation;
          if (relationName.contains(".Requires")) {
            targetNsPath_createRelation = this.ns;
          } else {
            targetNsPath_createRelation = this.nsForPlatformCiComponents;
          }

          // {ciRelationId}, #{nsId}, #{fromCiId}, #{relationId}, #{toCiId}, #{relationGoid},
          // #{comments}, #{relationStateId}

          String INSERT_createRelation = "";

          log.info("creating relation for : ");

          log.info("targetNsPath_createRelation: " + targetNsPath_createRelation);
          log.info("relationName: " + relationName);
          log.info("targetCMSCIRelationId: " + targetCMSCIRelationId);
          log.info("fromCiId: " + fromCiId);
          log.info("toCiId: " + toCiId);

          log.info("INSERT_createRelation    : " + INSERT_createRelation);
          // preparedStatement = conn.prepareStatement(INSERT_createRelation);
          // preparedStatement.setString(1, targetNsPath);
          // preparedStatement.setString(2, targetToCMSCIClazzName);

          // log.info("preparedStatement: "+preparedStatement);

          // create relation

        }

      }



    } catch (Exception e) {
      throw new RuntimeException("Error while fetching records" + e.getMessage());
    }



  }

  private void process_DELETE_RELATION(
      CmsCIRelationAndRelationAttributesActionMappingsModel mapping) {

    List<Integer> fromCiIds = new ArrayList<Integer>();

    if (mapping.getSourceCmsCiRelationName().contains(".Requires")) {
      fromCiIds = dal.getCiIdsForNsAndClazz(ns, mapping.getSourceFromCmsCiClazzName());
    } else {
      fromCiIds = dal.getCiIdsForNsAndClazz(nsForPlatformCiComponents,
          mapping.getSourceFromCmsCiClazzName());
    }
    log.info("fromCiIds size<{}>, fromCiIds: {} ", fromCiIds.size(), fromCiIds.toString());

    List<Integer> toCiIds =
        dal.getCiIdsForNsAndClazz(nsForPlatformCiComponents, mapping.getSourceToCmsCiClazzName());
    log.info("toCiIds size<{}>, toCiIds: {} ", toCiIds.size(), toCiIds.toString());

    dal.deleteCiRelations(mapping.getSourceFromCmsCiClazzName(), fromCiIds,
        mapping.getSourceToCmsCiClazzName(), toCiIds, mapping.getSourceCmsCiRelationId(),
        mapping.getSourceCmsCiRelationName(), nsForPlatformCiComponents);

  }

  @Deprecated
  private void process_DELETE_RELATIONV2(
      CmsCIRelationAndRelationAttributesActionMappingsModel mapping) {

    String targetNsPath;

    String relationName = mapping.getSourceCmsCiRelationName();

    switch (relationName) {

      case "base.Requires":
        targetNsPath = this.ns;
        break;
      case "manifest.Requires":
        targetNsPath = this.ns;
        break;

      default:
        targetNsPath = this.nsForPlatformCiComponents;
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

    String sql = "select " + "ca.ci_attribute_id, " + "ca.attribute_id, " + "ca.ci_id, "
        + "cla.attribute_name, " + "ci.ci_name, " + "ns.ns_path "
        + "from cm_ci_attributes ca, md_class_attributes cla, cm_ci ci, ns_namespaces ns "
        + "where " + "ca.ci_id=ci.ci_id " + "and ci.ns_id = ns.ns_id "
        + "and ca.attribute_id=cla.attribute_id " + "and ns.ns_path =? " + "and ci.class_id=? "
        + "and ca.attribute_id=?; ";

    String cmsciIdsByClazzAndNsPath_SQL = "select " + "ci.ci_id as ciId, " + "ci.ci_name as ciName,"
        + "ci.class_id as ciClassId," + "cl.class_name as ciClassName," + "cl.impl as impl, "
        + "ci.ns_id as nsId, " + "ns.ns_path as nsPath, " + "ci.ci_goid as ciGoid, "
        + "ci.comments, " + "ci.ci_state_id as ciStateId, " + "st.state_name as ciState, "
        + "ci.last_applied_rfc_id as lastAppliedRfcId, " + "ci.created_by as createdBy, "
        + "ci.updated_by as updatedBy, " + "ci.created, " + "ci.updated "
        + "from cm_ci ci, md_classes cl, ns_namespaces ns, cm_ci_state st "
        + "where ns.ns_path = ? " + "and cl.class_name = ? " + "and ci.class_id = cl.class_id "
        + "and ci.ns_id = ns.ns_id " + "and ci.ci_state_id = st.ci_state_id;";


    try {



      String sourceFromCmsCiClazzName = mapping.getSourceFromCmsCiClazzName();

      log.info("cmsciIdsByClazzAndNsPath_SQL        : " + cmsciIdsByClazzAndNsPath_SQL);
      PreparedStatement preparedStatement = conn.prepareStatement(cmsciIdsByClazzAndNsPath_SQL);
      preparedStatement.setString(1, targetNsPath);
      preparedStatement.setString(2, sourceFromCmsCiClazzName);

      log.info("preparedStatement: " + preparedStatement);
      ResultSet resultSet_FromCIds = preparedStatement.executeQuery();

      int numberOfRecords = 0;

      int numberOfColumns = resultSet_FromCIds.getMetaData().getColumnCount();
      List<Integer> fromCiIds = new ArrayList<Integer>();
      while (resultSet_FromCIds.next()) {

        fromCiIds.add(resultSet_FromCIds.getInt(1));
        numberOfRecords++;
      }

      log.info(" cmsciIdsByClazzAndNsPath_SQL: numberOfRecords: " + numberOfRecords);



      String cmsciIdsByClazzAndNsPath_SQL2 = "select " + "ci.ci_id as ciId, "
          + "ci.ci_name as ciName," + "ci.class_id as ciClassId," + "cl.class_name as ciClassName,"
          + "cl.impl as impl, " + "ci.ns_id as nsId, " + "ns.ns_path as nsPath, "
          + "ci.ci_goid as ciGoid, " + "ci.comments, " + "ci.ci_state_id as ciStateId, "
          + "st.state_name as ciState, " + "ci.last_applied_rfc_id as lastAppliedRfcId, "
          + "ci.created_by as createdBy, " + "ci.updated_by as updatedBy, " + "ci.created, "
          + "ci.updated " + "from cm_ci ci, md_classes cl, ns_namespaces ns, cm_ci_state st "
          + "where ns.ns_path = ? " + "and cl.class_name = ? " + "and ci.class_id = cl.class_id "
          + "and ci.ns_id = ns.ns_id " + "and ci.ci_state_id = st.ci_state_id;";



      String sourceToCmsCiClazzName = mapping.getSourceToCmsCiClazzName();

      log.info("cmsciIdsByClazzAndNsPath_SQL2        : " + cmsciIdsByClazzAndNsPath_SQL2);
      preparedStatement = conn.prepareStatement(cmsciIdsByClazzAndNsPath_SQL2);
      preparedStatement.setString(1, this.nsForPlatformCiComponents);
      preparedStatement.setString(2, sourceToCmsCiClazzName);

      log.info("preparedStatement: " + preparedStatement);
      ResultSet resultSet_ToCids = preparedStatement.executeQuery();

      numberOfColumns = resultSet_ToCids.getMetaData().getColumnCount();
      numberOfRecords = 0;
      List<Integer> toCiIds = new ArrayList<Integer>();

      while (resultSet_ToCids.next()) {

        toCiIds.add(resultSet_ToCids.getInt(1));
        numberOfRecords++;
      }

      log.info(" cmsciIdsByClazzAndNsPath_SQL: numberOfRecords: " + numberOfRecords);


      String relationsToDeleteSQL = "select * from cm_ci_relations ci_rel, ns_namespaces ns "
          + "where ns.ns_path =? " + " and ci_rel.relation_id=? " + " and ci_rel.from_ci_id =? "
          + " and ci_rel.to_ci_id=?";


      int numberOfRelationsToDelete = 0;
      List<Integer> ci_relation_idList = new ArrayList<Integer>();
      for (int fromCiid : fromCiIds) {

        for (int toCiid : toCiIds) {


          preparedStatement = conn.prepareStatement(relationsToDeleteSQL);
          preparedStatement.setString(1, targetNsPath);
          preparedStatement.setInt(2, mapping.getSourceCmsCiRelationId());
          preparedStatement.setInt(3, fromCiid);
          preparedStatement.setInt(4, toCiid);
          log.info("preparedStatement: " + preparedStatement);
          ResultSet resultSet_relationsToDelete = preparedStatement.executeQuery();

          int numberOfRelations = 0;
          while (resultSet_relationsToDelete.next()) {
            numberOfRelations++;
            numberOfRelationsToDelete++;
            ci_relation_idList.add(resultSet_relationsToDelete.getInt("ci_relation_id"));

          }
          log.info("numberOfRelations: " + numberOfRelations);
        }

      }

      log.info("ci_relation_idList: " + ci_relation_idList.toString());
      log.info("numberOfRelationsToDelete: " + numberOfRelationsToDelete);


    } catch (Exception e) {
      throw new RuntimeException("Error while fetching records" + e.getMessage());
    }



    // delete from cm_ci_relations where ci_relation_id = ?;

    /*
     * FromCIs:
     * 
     * select * from cm_ci_relations ci_rel, ns_namespaces ns where ns.ns_path
     * ='/TestOrg2/guineapigs1/_design/guineapig-brown'; where fromCId =? & toCid=?
     */



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

    String owner = IConstants.CIRCUIT_CONSOLIDATION_USER;
    String comments = IConstants.CIRCUIT_CONSOLIDATION_COMMENTS;



    log.info(
        "creating CMSCIRelation attribute attributeName: {} attributeId {} for targetCMSCIRelationName {} targetCMSCIRelationId {}, "
            + "targetFromCMSCIClazzName {}, targetFromCMSCIClazzId {}, targetToCMSCIClazzName{} targetToCMSCIClazzId {} with dfValue {} , djValue {}",
        attributeName, attributeId, targetCMSCIRelationName, targetCMSCIRelationId,
        targetFromCMSCIClazzName, targetFromCMSCIClazzId, targetToCMSCIClazzName,
        targetToCMSCIClazzId, dfValue, djValue);

    List<Integer> ci_relation_ids =
        dal.getCMSCIRelationIds_By_Ns_RelationName_FromClazzToClazz(this.nsForPlatformCiComponents,
            targetCMSCIRelationName, targetFromCMSCIClazzName, targetToCMSCIClazzName);

    log.info("List of ci_relation_ids for transformation mapping for ADD_RELATION_ATTRIBUTE <{}>",
        ci_relation_ids.toString());

    for (int ci_relation_id : ci_relation_ids) {
      log.info("Adding CMSCIRelation attributes for ci_relation_id <{}>", ci_relation_id);
      int ci_rel_attribute_id = dal.getNext_cm_pk_seqId();
      dal.createCMSCIRelationAttribute(ci_rel_attribute_id, ci_relation_id, attributeId, dfValue,
          djValue, owner, comments);
    }


  }



}
