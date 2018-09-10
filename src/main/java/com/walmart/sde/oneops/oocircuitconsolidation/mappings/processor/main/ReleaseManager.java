package com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.main;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.config.IConstants;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.dal.KloopzCmDal;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.exception.UnSupportedOperation;
import com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.util.CircuitconsolidationUtil;

public class ReleaseManager {

  private final Logger log = LoggerFactory.getLogger(getClass());

  String ns;
  String platformName;
  String ooPhase;
  String envName;
  KloopzCmDal dal;

  ReleaseManager(String ns, String platformName, String ooPhase, String envName, KloopzCmDal dal) {

    this.ns = ns;
    this.platformName = platformName;
    this.ooPhase = ooPhase;
    this.envName = envName;
    this.dal = dal;

  }

  public int create_dj_release(int parentReleaseId) {

    int release_id = dal.getNext_dj_pk_seq();

    String releaseNsPath = CircuitconsolidationUtil.getNsForRelease(this.ns, this.platformName,
        this.ooPhase, this.envName);

    // TODO: Operate Phase reelease ID have Transition phase ReleaseId as Parent ReleaseId
    // Transition Phase Release Id have Design Phase ReleaseId as parent ReleaseId,
    // However Transition Phase releaseId did not match with releaseId of Design Phase when checked
    // via
    // http://cmsapi:8080/adapter/rest/dj/simple/cis?nsPath=/TestOrg2/ms-oneops-a-cass/dev/bom/oneops-apache-cassandra/1&ciClassName=%


    int ns_id = dal.getNsIdForNsPath(releaseNsPath);
    String release_name = releaseNsPath + release_id;
    String created_by = IConstants.CIRCUIT_CONSOLIDATION_USER;
    String commited_by = IConstants.CIRCUIT_CONSOLIDATION_USER;
    int release_state_id = 200; // 200=closed

    String release_type = IConstants.CIRCUIT_CONSOLIDATION_COMMENTS;
    String description = IConstants.CIRCUIT_CONSOLIDATION_COMMENTS;
    int revision = 1; // Hard coded

    dal.create_dj_release(release_id, ns_id, parentReleaseId, release_name, created_by, commited_by,
        release_state_id, release_type, description, revision);

    log.info("ReleaseId <{}> created for ooPhase <{}> for releaseNsPath <{}>", release_id,
        this.ooPhase, releaseNsPath);
    return release_id;
  }

  public int getLastApplied_dj_releaseForPhase(String ooPhase) {

    String releaseNsPath =
        CircuitconsolidationUtil.getNsForRelease(this.ns, this.platformName, ooPhase, this.envName);

    int lastAppliedReleaseId = dal.getLastAppliedDjReleaseIdForNsReleasePath(releaseNsPath);

    log.info("lastAppliedReleaseId {} for ooPhase {} , platformName {} , envName {}, ns {}",
        lastAppliedReleaseId, ooPhase, this.platformName, this.envName, this.ns);

    if (lastAppliedReleaseId == 0) {

      switch (ooPhase) {
        case IConstants.OPERATE_PHASE:
          releaseNsPath = CircuitconsolidationUtil.getNsForRelease(this.ns, this.platformName,
              IConstants.TRANSITION_PHASE, this.envName);
          lastAppliedReleaseId = dal.getLastAppliedDjReleaseIdForNsReleasePath(releaseNsPath);

          log.info(
              "lastAppliedReleaseId ooPhase Operate phase was 0, fetched last lastAppliedReleaseId {} for Transition Phase",
              lastAppliedReleaseId);

          return lastAppliedReleaseId;

        case IConstants.TRANSITION_PHASE:
          releaseNsPath = CircuitconsolidationUtil.getNsForRelease(this.ns, this.platformName,
              IConstants.DESIGN_PHASE, this.envName);
          lastAppliedReleaseId = dal.getLastAppliedDjReleaseIdForNsReleasePath(releaseNsPath);
          log.info(
              "lastAppliedReleaseId ooPhase Transition phase was 0, fetched last lastAppliedReleaseId {} for Design Phase",
              lastAppliedReleaseId);

          return lastAppliedReleaseId;

        default:
          throw new UnSupportedOperation(
              "getLastApplied_dj_releaseForPhase method not supported for ooPhase: " + ooPhase);

      }
    }

    if (lastAppliedReleaseId == 0) {
      throw new UnSupportedOperation("lastAppliedReleaseId <" + lastAppliedReleaseId
          + "> for ooPhase " + ooPhase + " not supported");
    }

    return lastAppliedReleaseId;

  }


}
