package com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.config;

public interface SqlQueries {

  //final query
  String SQL_SELECT_NakedCMSCIByNsAndClazz=      "select "+
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



  String SQL_SELECT_CMSCIATTRIBUTE =    "select "+
      "ca.ci_attribute_id, "+
      "ca.attribute_id, "+
      "ca.ci_id, "+
      "cla.attribute_name, "+
      "ci.ci_name, "+
      "ns.ns_path, "+
      "cla.attribute_id "+
      "from cm_ci_attributes ca, md_class_attributes cla, cm_ci ci, ns_namespaces ns "+
      "where "+
      "ca.ci_id=ci.ci_id "+
      "and ci.ns_id = ns.ns_id "+
      "and ca.attribute_id=cla.attribute_id "+
      "and ns.ns_path =? "+
      "and ci.class_id=? "+
      "and ca.attribute_id=?; ";
}


