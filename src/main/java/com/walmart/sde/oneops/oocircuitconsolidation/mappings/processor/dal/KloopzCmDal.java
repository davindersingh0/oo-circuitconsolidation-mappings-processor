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

      log.info("numberOfRecords {} for nsForPlatformCiComponents {} : nsIds {}", numberOfRecords, nsForPlatformCiComponents, nsIds.toString());
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

  public void switchCMSCIAttribuetId(String nsForPlatformCiComponents, int updatedTargetClazzId, int targetAttributeId, int sourceAttributeId) {
  
    String SQL_UPDATE_CMSCI_switchAttributeId =  "UPDATE cm_ci_attributes set attribute_id=? where ci_attribute_id in "
        + "(SELECT ci_attribute_id from cm_ci_attributes ca, md_class_attributes cla, cm_ci ci, ns_namespaces ns "
        + "where "+
        "ca.ci_id=ci.ci_id "+
        "and ci.ns_id = ns.ns_id "+
        "and ca.attribute_id=cla.attribute_id "+
        "and ns.ns_path =? "+
        "and ci.class_id=? "+
        "and ca.attribute_id=?); ";
        

    try {
    
       log.info("SQL_UPDATE_CMSCI_switchAttributeId        : "+SQL_UPDATE_CMSCI_switchAttributeId);
       PreparedStatement preparedStatement = conn.prepareStatement(SQL_UPDATE_CMSCI_switchAttributeId);
       
       preparedStatement.setInt(1, targetAttributeId);
       preparedStatement.setString(2, nsForPlatformCiComponents);
       preparedStatement.setInt(3, updatedTargetClazzId);
       preparedStatement.setInt(4, sourceAttributeId);

       
       log.info("preparedStatement: "+preparedStatement);
       
       int numberOfUpdatedRecords = preparedStatement.executeUpdate();
       log.info("numberOfUpdatedRecords for SQL_UPDATE_CMSCI_switchAttributeId: {} ",numberOfUpdatedRecords);

       } catch (Exception e) {
       throw new RuntimeException("Error while fetching records" +e.getMessage());
     }
    
  }

}
