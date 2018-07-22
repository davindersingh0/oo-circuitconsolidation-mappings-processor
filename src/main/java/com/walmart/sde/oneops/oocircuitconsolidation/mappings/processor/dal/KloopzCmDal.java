package com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.dal;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.config.IConstants;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.config.SqlQueries;

public class KloopzCmDal {

  private final Logger log = LoggerFactory.getLogger(getClass());

  Connection conn;


  public KloopzCmDal(Connection conn) {
    this.conn = conn;

  }

  public Connection getConn() {
    return conn;
  }


  public void setConn(Connection conn) {
    this.conn = conn;
  }


  public List<Integer> getCiIdsForNsAndClazz(String ns, String clazz) {

    List<Integer> ciIds = new ArrayList<Integer>();
    try {

      log.info("SQL_SELECT_NakedCMSCIByNsAndClazz        : "
          + SqlQueries.SQL_SELECT_NakedCMSCIByNsAndClazz);
      PreparedStatement preparedStatement_SELECT_NakedCMSCIByNsAndClazz =
          conn.prepareStatement(SqlQueries.SQL_SELECT_NakedCMSCIByNsAndClazz);
      preparedStatement_SELECT_NakedCMSCIByNsAndClazz.setString(1, ns);
      preparedStatement_SELECT_NakedCMSCIByNsAndClazz.setString(2, clazz);

      log.info("preparedStatement_SELECT_NakedCMSCIByNsAndClazz: "
          + preparedStatement_SELECT_NakedCMSCIByNsAndClazz);
      ResultSet resultSet = preparedStatement_SELECT_NakedCMSCIByNsAndClazz.executeQuery();

      int numberOfRecords = 0;
      while (resultSet.next()) {
        ciIds.add(resultSet.getInt("ciId"));
        numberOfRecords++;

      }

      log.info("numberOfRecords: " + numberOfRecords);
    } catch (Exception e) {
      throw new RuntimeException("Error while fetching records" + e.getMessage());
    }
    return ciIds;
  }

  public List<Integer> deleteCmsCisForNsAndClazz(String nsForPlatformCiComponents, String clazz) {

    List<Integer> ciIds = new ArrayList<Integer>();

    try {

      String SQL_deleteCmsCisForNsAndClazz = "DELETE from cm_ci where ci_id in "
          + "( SELECT ci.ci_id FROM cm_ci ci, md_classes cl, ns_namespaces ns, cm_ci_state st "
          + "where ns.ns_path = ? " + "and cl.class_name = ? " + "and ci.class_id = cl.class_id "
          + "and ci.ns_id = ns.ns_id " + "and ci.ci_state_id = st.ci_state_id );";


      log.info("SQL_deleteCmsCisForNsAndClazz: " + SQL_deleteCmsCisForNsAndClazz);
      PreparedStatement preparedStatement = conn.prepareStatement(SQL_deleteCmsCisForNsAndClazz);
      preparedStatement.setString(1, nsForPlatformCiComponents);
      preparedStatement.setString(2, clazz);
      log.info("preparedStatement: " + preparedStatement);
      int NumberOfCmsCiDeleted = preparedStatement.executeUpdate();
      log.info("NumberOfCmsCiDeleted <{}> for nsForPlatformCiComponents <{}> and clazz <{}>",
          NumberOfCmsCiDeleted, nsForPlatformCiComponents, clazz);


    } catch (Exception e) {
      throw new RuntimeException("Error while fetching records" + e.getMessage());
    }
    return ciIds;
  }

  public void deleteCiRelations(String fromClazz, List<Integer> fromCiIds, String toClazz,
      List<Integer> toCiIds, int sourceCmsCiRelationId, String relationName,
      String nsForPlatformCiComponents) {
    int NumberOfCmsCiRelationsDeleted = 0;
    try {
      for (int fromCiId : fromCiIds) {

        for (int toCiId : toCiIds) {

          String SQL_DELETE_CMSCIRelations =
              "delete from cm_ci_relations ci_rel where ns_id = (select ns_id from ns_namespaces ns where ns.ns_path=?) "
                  + " and ci_rel.relation_id=? " + " and ci_rel.from_ci_id =? "
                  + " and ci_rel.to_ci_id=?;";


          log.info("SQL_DELETE_CMSCIRelations: " + SQL_DELETE_CMSCIRelations);
          PreparedStatement preparedStatement = conn.prepareStatement(SQL_DELETE_CMSCIRelations);
          preparedStatement.setString(1, nsForPlatformCiComponents);
          preparedStatement.setInt(2, sourceCmsCiRelationId);
          preparedStatement.setInt(3, fromCiId);
          preparedStatement.setInt(4, toCiId);

          log.info("preparedStatement: " + preparedStatement);
          int deletedRecordsCount = preparedStatement.executeUpdate();
          if (deletedRecordsCount != 0) {
            NumberOfCmsCiRelationsDeleted++;
            log.info(
                "CmsCiRelation deleted for nsForPlatformCiComponents <{}>, fromCiId <{}>, toCiId <{}>",
                nsForPlatformCiComponents, fromCiId, toCiId);
          } else {
            log.info(
                "ignored CmsCiRelation to delete for nsForPlatformCiComponents <{}>, fromCiId <{}>, toCiId <{}>",
                nsForPlatformCiComponents, fromCiId, toCiId);
          }

        }

      }

      log.info(
          "NumberOfCmsCiRelationsDeleted <{}>, for nsForPlatformCiComponents <{}>, fromClazz {}, toClazz {}, relationName {}",
          NumberOfCmsCiRelationsDeleted, nsForPlatformCiComponents, fromClazz, toClazz,
          relationName);
    } catch (Exception e) {
      throw new RuntimeException("Error while fetching records" + e.getMessage());
    }

  }


  public int getNsIdForNsPath(String nsForPlatformCiComponents) {

    List<Integer> nsIds = new ArrayList<Integer>();
    try {

      String SQL_SELECT_nsIdForNsPath = "select * from ns_namespaces where ns_path=?";
      log.info("SQL_SELECT_nsIdForNsPath: " + SQL_SELECT_nsIdForNsPath);

      PreparedStatement preparedStatement = conn.prepareStatement(SQL_SELECT_nsIdForNsPath);
      preparedStatement.setString(1, nsForPlatformCiComponents);
      log.info("preparedStatement: " + preparedStatement);
      ResultSet resultSet = preparedStatement.executeQuery();

      int numberOfRecords = 0;
      while (resultSet.next()) {
        nsIds.add(resultSet.getInt("ns_id"));
        numberOfRecords++;

      }

      log.info("numberOfRecords {} for nsForPlatformCiComponents {} : nsIds {}", numberOfRecords,
          nsForPlatformCiComponents, nsIds.toString());
      if (numberOfRecords == 0 || numberOfRecords > 1) {
        throw new RuntimeException("numberOfRecords " + numberOfRecords
            + " invalid for nsForPlatformCiComponents :" + nsForPlatformCiComponents);
      }
    } catch (Exception e) {
      throw new RuntimeException("Error while fetching records" + e.getMessage());
    }


    return nsIds.get(0);
  }


  public void cmsCi_update_ciClazzid_clazzname_goid(int ciId, String fromClazz, int fromClazzId,
      String toClazz, int toClazzId, String goid) {

    try {

      String SQL_UPDATE_CMSCI_ciClazzid_clazzname_goid =
          "UPDATE cm_ci set class_id=? , ci_goid=? , comments=?, updated_by=?, updated = now() where ci_id=?;";


      log.info("SQL_UPDATE_CMSCI_ciClazzid_clazzname_goid: "
          + SQL_UPDATE_CMSCI_ciClazzid_clazzname_goid);

      PreparedStatement preparedStatement =
          conn.prepareStatement(SQL_UPDATE_CMSCI_ciClazzid_clazzname_goid);

      preparedStatement.setInt(1, toClazzId);
      preparedStatement.setString(2, goid);
      preparedStatement.setString(3, IConstants.CIRCUIT_CONSOLIDATION_COMMENTS);
      preparedStatement.setString(4, IConstants.CIRCUIT_CONSOLIDATION_USER);
      preparedStatement.setInt(5, ciId);


      log.info("preparedStatement: " + preparedStatement);

      int numberOfCmsCiUpdatedForGoid = preparedStatement.executeUpdate();


      log.info(
          "numberOfCmsCiUpdatedForGoid <{}> for ciId <{}> fromClazz <{}> fromClazzId <{}> To toClazz <{}>, toClazzId <{}> with goid <{}>",
          numberOfCmsCiUpdatedForGoid, ciId, fromClazz, fromClazzId, toClazz, toClazzId, goid);


    } catch (Exception e) {
      throw new RuntimeException("Error while fetching records" + e.getMessage());
    }

  }

  public void switchCMSCIAttribuetId(String nsForPlatformCiComponents, int updatedTargetClazzId,
      int targetAttributeId, int sourceAttributeId) {

    String SQL_UPDATE_CMSCI_switchAttributeId =
        "UPDATE cm_ci_attributes set attribute_id=? where ci_attribute_id in "
            + "(SELECT ci_attribute_id from cm_ci_attributes ca, md_class_attributes cla, cm_ci ci, ns_namespaces ns "
            + "where " + "ca.ci_id=ci.ci_id " + "and ci.ns_id = ns.ns_id "
            + "and ca.attribute_id=cla.attribute_id " + "and ns.ns_path =? " + "and ci.class_id=? "
            + "and ca.attribute_id=?); ";


    try {

      log.info("SQL_UPDATE_CMSCI_switchAttributeId        : " + SQL_UPDATE_CMSCI_switchAttributeId);
      PreparedStatement preparedStatement =
          conn.prepareStatement(SQL_UPDATE_CMSCI_switchAttributeId);

      preparedStatement.setInt(1, targetAttributeId);
      preparedStatement.setString(2, nsForPlatformCiComponents);
      preparedStatement.setInt(3, updatedTargetClazzId);
      preparedStatement.setInt(4, sourceAttributeId);


      log.info("preparedStatement: " + preparedStatement);

      int numberOfUpdatedRecords = preparedStatement.executeUpdate();
      log.info("numberOfUpdatedRecords for SQL_UPDATE_CMSCI_switchAttributeId: {} ",
          numberOfUpdatedRecords);

    } catch (Exception e) {
      throw new RuntimeException("Error while fetching records" + e.getMessage());
    }

  }

  public void deleteCMSCIAttribute(String nsForPlatformCiComponents, String sourceClazz,
      int sourceClazzId, int sourceClazzAttributeId, String sourceClazzAttributeName,
      String targetClazz, int targetClazzId) {


    log.info(
        "deleting CMSCIAttribute from attributes targetClazz <{}> targetClazzId <{}> sourceClazzAttributeId <{}> sourceClazzAttributeName <{}> belonging to sourceClazz <{}> sourceClazzId <{}>",
        targetClazz, targetClazzId, sourceClazzAttributeId, sourceClazzAttributeName, sourceClazz,
        sourceClazzId);


    String SQL_DELETE_CMSCIATTRIBUTE =
        "DELETE from cm_ci_attributes where ci_attribute_id in ( select ca.ci_attribute_id "
            + "from cm_ci_attributes ca, md_class_attributes cla, cm_ci ci, ns_namespaces ns "
            + "where " + "ca.ci_id=ci.ci_id " + "and ci.ns_id = ns.ns_id "
            + "and ca.attribute_id=cla.attribute_id " + "and ns.ns_path =? " + "and ci.class_id=? "
            + "and ca.attribute_id=?); ";


    try {

      log.info("SQL_DELETE_CMSCIATTRIBUTE        : " + SQL_DELETE_CMSCIATTRIBUTE);
      PreparedStatement preparedStatement = conn.prepareStatement(SQL_DELETE_CMSCIATTRIBUTE);

      preparedStatement.setString(1, nsForPlatformCiComponents);
      preparedStatement.setInt(2, targetClazzId);
      preparedStatement.setInt(3, sourceClazzAttributeId);

      log.info("preparedStatement: " + preparedStatement);
      int numberOfRecords = preparedStatement.executeUpdate();

      log.info("Number of CMSCI attributes deleted: {}", numberOfRecords);

    } catch (Exception e) {
      throw new RuntimeException("Error while fetching records" + e.getMessage());
    }



  }

  public int getNext_cm_pk_seqId() {

    try {

      String SQL_SELECT_NextCiId = "SELECT nextval('cm_pk_seq');";

      log.info("SQL_SELECT_NextCiId: " + SQL_SELECT_NextCiId);
      PreparedStatement preparedStatement = conn.prepareStatement(SQL_SELECT_NextCiId);

      log.info("preparedStatement: " + preparedStatement);
      ResultSet resultSet = preparedStatement.executeQuery();

      while (resultSet.next()) {
        int nextVal = resultSet.getInt("nextval");
        log.info("nextVal: " + nextVal);
        return nextVal;

      }
    } catch (Exception e) {
      throw new RuntimeException("Error while fetching records" + e.getMessage());
    }
    throw new RuntimeException("Unable to retrive next CiId");

  }

  public void createCMSCI(int nsId, int ciId, int targetClazzId, String ciName, String goid,
      int ciStateId, String comments, String createdBy) {

    /*
     * insert into cm_ci (ci_id, ns_id, class_id, ci_name, ci_goid, comments, ci_state_id,
     * last_applied_rfc_id, created_by) values (p_ci_id, p_ns_id, p_class_id, p_ci_name, p_goid,
     * p_comments, p_state_id, p_last_rfc_id, p_created_by);
     * 
     * insert into cms_ci_event_queue(event_id, source_pk, source_name, event_type_id) values
     * (nextval('event_pk_seq'), p_ci_id, 'cm_ci' , 100);
     * 
     * insert into cm_ci_log(log_id, log_time, log_event, ci_id, ci_name, class_id, class_name,
     * comments, ci_state_id, ci_state_id_old, created_by) values (nextval('log_pk_seq'), now(),
     * 100, p_ci_id, p_ci_name, p_class_id, l_class_name, p_comments, p_state_id, p_state_id,
     * p_created_by);
     */

    try {

      /*
       * String SQL_INSERT_CreateNewCMSCI =
       * "insert into cm_ci (ci_id, ns_id, class_id, ci_name, ci_goid, comments, ci_state_id, last_applied_rfc_id, created_by) "
       * +
       * "values (p_ci_id, p_ns_id, p_class_id, p_ci_name, p_goid, p_comments, p_state_id, p_last_rfc_id, p_created_by);"
       * ;
       */
      String SQL_INSERT_CreateNewCMSCI =
          "INSERT INTO cm_ci (ci_id, ns_id, class_id, ci_name, ci_goid, comments, ci_state_id, created_by) "
              + "VALUES (?, ?, ?, ?, ?, ?, ?, ?);";

      log.info("SQL_INSERT_CreateNewCMSCI: " + SQL_INSERT_CreateNewCMSCI);
      PreparedStatement preparedStatement = conn.prepareStatement(SQL_INSERT_CreateNewCMSCI);

      preparedStatement.setInt(1, ciId);
      preparedStatement.setInt(2, nsId);
      preparedStatement.setInt(3, targetClazzId);
      preparedStatement.setString(4, ciName);
      preparedStatement.setString(5, goid);
      preparedStatement.setString(6, comments);
      preparedStatement.setInt(7, ciStateId);
      preparedStatement.setString(8, createdBy);

      log.info("preparedStatement: " + preparedStatement);

      int numberOfInserts = preparedStatement.executeUpdate();
      log.info("numberOfInserts: {}", numberOfInserts);


    } catch (Exception e) {
      throw new RuntimeException("Error while creating new CMSCI " + e.getMessage());
    }

  }

  public void createNewCMSCIAttributeWithDefaultValue(int ci_attribute_id, int ci_id,
      int attribute_id, String targetDefaultValue, String owner,
      String comments) {
    
    
    // TODO Auto-generated method stub
/*    insert into cm_ci_attributes (ci_attribute_id, ci_id, attribute_id, df_attribute_value, dj_attribute_value, owner, comments)
    values (nextval('cm_pk_seq'), p_ci_id, p_attribute_id, p_df_value, p_dj_value, p_owner, p_comments)
    returning ci_attribute_id into out_ci_attr_id;

    insert into cm_ci_attribute_log(log_id, log_time, log_event, ci_id, ci_attribute_id, attribute_id, attribute_name, comments, owner, dj_attribute_value, dj_attribute_value_old, df_attribute_value, df_attribute_value_old) 
    values (nextval('log_pk_seq'), now(), 100, p_ci_id, out_ci_attr_id, p_attribute_id, l_attribute_name, p_comments, p_owner, p_dj_value, p_dj_value, p_df_value, p_df_value);

    if p_event = true then
        insert into cms_ci_event_queue(event_id, source_pk, source_name, event_type_id)
        values (nextval('event_pk_seq'), p_ci_id, 'cm_ci' , 200);
    */
    

    try {

      String SQL_INSERT_AddNewCMSCIAttributeWithDefaultValue =
          "INSERT INTO cm_ci_attributes (ci_attribute_id, ci_id, attribute_id, df_attribute_value, dj_attribute_value, owner, comments) "
              + "VALUES (?, ?, ?, ?, ?, ?, ?);";

      log.info("SQL_INSERT_AddNewCMSCIAttributeWithDefaultValue: "
          + SQL_INSERT_AddNewCMSCIAttributeWithDefaultValue);
      PreparedStatement preparedStatement =
          conn.prepareStatement(SQL_INSERT_AddNewCMSCIAttributeWithDefaultValue);

      preparedStatement.setInt(1, ci_attribute_id);
      preparedStatement.setInt(2, ci_id);
      preparedStatement.setInt(3, attribute_id);
      preparedStatement.setString(4, targetDefaultValue);
      preparedStatement.setString(5, targetDefaultValue);
      preparedStatement.setString(6, owner);
      preparedStatement.setString(7, comments);


      log.info("preparedStatement: " + preparedStatement);

    } catch (Exception e) {
      throw new RuntimeException("Error while creating new CMSCI " + e.getMessage());
    }

  }
  
}
