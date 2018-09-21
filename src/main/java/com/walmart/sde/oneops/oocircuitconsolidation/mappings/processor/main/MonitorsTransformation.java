package com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.main;

import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.gson.Gson;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.config.IConstants;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.dal.KloopzCmDal;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.exception.UnSupportedOperation;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.exception.UnSupportedTransformationMappingException;

public class MonitorsTransformation {


  private final Logger log = LoggerFactory.getLogger(getClass());

  private String ooPhase;
  private KloopzCmDal dal;
  private Gson gson = new Gson();
  private String ns;
  private String platformName;
  private String envName;
  private String nsForPlatformCiComponents;

  private String monitoredCiToMonitorCiRelationName;
  private String monitorCmsCiClazzName;


  public void setMonitorsTransformationProperties(String ooPhase, KloopzCmDal dal, String ns,
      String platformName, String envName, String nsForPlatformCiComponents) {

    this.ooPhase = ooPhase;
    this.dal = dal;
    this.ns = ns;
    this.platformName = platformName;
    this.envName = envName;
    this.nsForPlatformCiComponents = nsForPlatformCiComponents;


  }

  public void transformMonitors() {



    switch (this.ooPhase) {
      case IConstants.DESIGN_PHASE:

        this.monitoredCiToMonitorCiRelationName = "catalog.WatchedBy";
        this.monitorCmsCiClazzName = "catalog.Monitor";

        process_designPhaseMonitors();


        break;
      case IConstants.TRANSITION_PHASE:
        log.error("ooPhase {} not supported", ooPhase);
        this.monitoredCiToMonitorCiRelationName = "manifest.WatchedBy";
        this.monitorCmsCiClazzName = "manifest.Monitor";
        process_TransitionPhaseMonitors();
        break;

      case IConstants.OPERATE_PHASE:

        log.error("ooPhase {} not supported", ooPhase);
        return;

      default:
        log.error("ooPhase {} not supported", ooPhase);
        throw new UnSupportedOperation(ooPhase + " not supported");
    }

  }

  private void process_designPhaseMonitors() {
    process_dPhase_apache_cassandra_ci_monitors();
    process_dPhase_compute_ci_monitors();
    process_dPhase_os_ci_monitors();

  }

  private void process_TransitionPhaseMonitors() {
    process_tPhase_apache_cassandra_ci_monitors();
    process_tPhase_compute_ci_monitors();
    process_tPhase_os_ci_monitors();

  }

  private void process_dPhase_os_ci_monitors() {
    String[] createMonitorsArr = {"cpu", "load", "disk", "mem", "network"};
    String monitoredCiName = "os";
    String monitoredCiClazzName = "catalog.oneops.1.Os";
    createMonitorForCi(monitoredCiName, createMonitorsArr);
    createRelationsForNewlyCreatedMonitors(createMonitorsArr, monitoredCiClazzName);


  }

  private void process_dPhase_compute_ci_monitors() {
    String monitoredCiName = "compute";
    String monitoredCiClazzName = "catalog.oneops.1.Compute";
    String[] createMonitorsArr = {"ssh"};
    String[] deleteMonitorsArr = {"cpu", "load", "disk", "mem", "crondprocess", "postfixprocess",
        "sshdprocess", "socketconnection", "network"};

    deleteMonitorsForCi(monitoredCiName, deleteMonitorsArr);

    createMonitorForCi(monitoredCiName, createMonitorsArr);
    createRelationsForNewlyCreatedMonitors(createMonitorsArr, monitoredCiClazzName);


  }



  private void process_dPhase_apache_cassandra_ci_monitors() {
    String monitoredCiName = "apache_cassandra";

    String[] deleteMonitorsArr = {"Log", "PendingCompactions", "CommitLogSize", "CheckSSTCount",
        "RecentReadLatency", "WriteOperations", "RecentWriteLatency"};
    deleteMonitorsForCi(monitoredCiName, deleteMonitorsArr);


  }


  private void process_tPhase_os_ci_monitors() {
    String[] createMonitorsArr = {"cpu", "load", "disk", "mem", "network"};
    String monitoredCiName = "os";
    String monitoredCiClazzName = "manifest.oneops.1.Os";
    createMonitorForCi(monitoredCiName, createMonitorsArr);
    createRelationsForNewlyCreatedMonitors(createMonitorsArr, monitoredCiClazzName);


  }

  private void process_tPhase_compute_ci_monitors() {
    String monitoredCiName = "compute";
    String monitoredCiClazzName = "manifest.oneops.1.Compute";
    String[] createMonitorsArr = {"ssh"};
    String[] deleteMonitorsArr = {"cpu", "load", "disk", "mem", "crondprocess", "postfixprocess",
        "sshdprocess", "socketconnection", "network"};

    deleteMonitorsForCi(monitoredCiName, deleteMonitorsArr);

    createMonitorForCi(monitoredCiName, createMonitorsArr);
    createRelationsForNewlyCreatedMonitors(createMonitorsArr, monitoredCiClazzName);

  }

  private void process_tPhase_apache_cassandra_ci_monitors() {
    String monitoredCiName = "apache_cassandra";

    String[] deleteMonitorsArr = {"Log", "PendingCompactions", "CommitLogSize", "CheckSSTCount",
        "RecentReadLatency", "WriteOperations", "RecentWriteLatency"};
    deleteMonitorsForCi(monitoredCiName, deleteMonitorsArr);


  }


  private void deleteMonitorsForCi(String monitoredCiName, String[] monitorNamesArr) {
    for (String monitorName : monitorNamesArr) {

      String monitorCiName = getMonitorCiName(monitoredCiName, monitorName);
      log.info("deleting monitorCiName {} for monitoredCiName {}", monitorCiName, monitoredCiName);

      dal.deleteCmsCibyNsAndCiName(this.nsForPlatformCiComponents, monitorCiName);

    }

  }

  public void createMonitorForCi(String monitoredCiName, String[] monitorNamesArr) {
    log.info("\n\n***************************************************************************");
    log.info("Begin : createMonitorForCi()");

    int nsId = dal.getNsIdForNsPath(this.nsForPlatformCiComponents);
    int targetClazzId = dal.getClazzIdForClazzName(this.monitorCmsCiClazzName);
    String comments = IConstants.CIRCUIT_CONSOLIDATION_COMMENTS;
    int ciStateId = 100;
    String createdBy = IConstants.CIRCUIT_CONSOLIDATION_USER;

    for (String monitorName : monitorNamesArr) {

      log.info("-------------------------------------------------------------------------");
      String monitorCiName = getMonitorCiName(monitoredCiName, monitorName);
      log.info("creating monitorCiName {} for monitoredCiName {}", monitorCiName, monitoredCiName);
      int ciId = dal.getNext_cm_pk_seqId();
      int lastAppliedRfcId = dal.getNext_dj_pk_seq();
      String goid = nsId + "-" + targetClazzId + "-" + ciId;
      dal.createCMSCI(nsId, ciId, targetClazzId, monitorCiName, goid, ciStateId, comments,
          lastAppliedRfcId, createdBy);

      log.info("-------------------------------------------------------------------------");
    }


    log.info("\n\nEnd : createMonitorForCi()");
    log.info("***************************************************************************\n");
  }

  public String getMonitorCiName(String monitoredCiName, String monitorName) {

    String monitorCiName = this.platformName + "-" + monitoredCiName + "-" + monitorName;
    return monitorCiName;
  }


  private void createRelationsForNewlyCreatedMonitors(String[] createMonitorsArr,
      String monitoredCiClazzName) {

    log.info("\n\n***************************************************************************");
    log.info("Begin : createRelationsForNewlyCreatedMonitors()");

    Map<String, Integer> monitoredCiNamesAndCiIdsMap = dal
        .getCiNamesAndCiIdsMapForNsAndClazz(this.nsForPlatformCiComponents, monitoredCiClazzName);


    log.info("monitoredCiNamesAndCiIdsMap: {}", gson.toJson(monitoredCiNamesAndCiIdsMap));

    Map<String, Integer> monitorCiNamesAndCiIdsMap = dal.getCiNamesAndCiIdsMapForNsAndClazz(
        this.nsForPlatformCiComponents, this.monitorCmsCiClazzName);

    log.info("monitorCiNamesAndCiIdsMap: {}", gson.toJson(monitorCiNamesAndCiIdsMap));

    int targetClazzId = dal.getClazzIdForClazzName(this.monitorCmsCiClazzName);

    int relation_id = dal.getMdRelationIdForMdRelationName(this.monitoredCiToMonitorCiRelationName);

    String comments = IConstants.CIRCUIT_CONSOLIDATION_COMMENTS;
    int ci_state_id = 100;
    int nsId = dal.getNsIdForNsPath(this.nsForPlatformCiComponents);

    for (String monitoredCiName : monitoredCiNamesAndCiIdsMap.keySet()) {
      log.info("monitoredCiName: {}", monitoredCiName);
      int monitoredCiId = monitoredCiNamesAndCiIdsMap.get(monitoredCiName);
      log.info("monitoredCiId: {}", monitoredCiId);

      for (String monitorName : createMonitorsArr) {
        log.info("-------------------------------------------------------------------------");
        String monitorCiName = getMonitorCiName(monitoredCiName, monitorName);
        log.info("monitorCiName: {}", monitorCiName);

        int monitorCiId = monitorCiNamesAndCiIdsMap.get(monitorCiName);

        log.info(
            "creating relation from monitoredCiName {} >> relationName {} >> to monitorCiName {}",
            monitoredCiName, this.monitoredCiToMonitorCiRelationName, monitorCiName);

        log.info("creating relation from monitoredCiId {} >> relationId {} >> to monitorCiId {}",
            monitoredCiId, targetClazzId, monitorCiId);
        int ci_relation_id = dal.getNext_cm_pk_seqId();
        String relation_goid = monitoredCiId + "-" + relation_id + "-" + monitorCiId;

        dal.createCMSCIRelation(ci_relation_id, nsId, monitoredCiId, relation_goid, relation_id,
            monitorCiId, ci_state_id, comments);
        log.info("-------------------------------------------------------------------------");

      }

    }

    log.info("\n\nEnd : createRelationsForNewlyCreatedMonitors()");
    log.info("***************************************************************************\n");

  }


  public void deleteAllMonitors() {


    switch (this.ooPhase) {
      case IConstants.DESIGN_PHASE:
        dal.deleteCmsCisForNsAndClazz(nsForPlatformCiComponents, "catelog.Monitor");

        break;
      case IConstants.TRANSITION_PHASE:
        dal.deleteCmsCisForNsAndClazz(nsForPlatformCiComponents, "manifest.Monitor");

        break;
      case IConstants.OPERATE_PHASE:

        log.info(
            "ooPhase {} do not have monitors, Hence no action required for deleteAllMonitors() method ",
            this.ooPhase);

        break;

      default:
        throw new UnSupportedTransformationMappingException(
            "ooPhase " + this.ooPhase + "not supported");

    }



  }

}
