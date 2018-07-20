package com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.model;

import java.io.Serializable;

public class CmsCIRelationAndRelationAttributesActionMappingsModel implements Serializable {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  private String ooPhase;
  private String sourcePack;
  private String sourceCmsCiRelationKey;
  private String sourceCmsCiRelationName;
  private int sourceCmsCiRelationId;
  private String sourceFromCmsCiClazzName;
  private int sourceFromCmsCiClazzId;
  private String sourceToCmsCiClazzName;
  private int sourceToCmsCiClazzId;



  private String targetPack;
  private String targetCmsCiRelationKey;
  private String targetCmsCiRelationName;
  private int targetCmsCiRelationId;
  private String targetFromCmsCiClazzName;
  private int targetFromCmsCiClazzId;
  private String targetToCmsCiClazzName;
  private int targetToCmsCiClazzId;

  // CI Relation Attributes
  private int attributeId;
  private int relationId;
  private String attributeName;
  private String dfValue;
  private String djValue;
  
  private String action;
  private String entityType;

  
  public String getOoPhase() {
    return ooPhase;
  }

  public void setOoPhase(String ooPhase) {
    this.ooPhase = ooPhase;
  }

  public String getSourcePack() {
    return sourcePack;
  }

  public void setSourcePack(String sourcePack) {
    this.sourcePack = sourcePack;
  }

  public String getSourceCmsCiRelationKey() {
    return sourceCmsCiRelationKey;
  }

  public void setSourceCmsCiRelationKey(String sourceCmsCiRelationKey) {
    this.sourceCmsCiRelationKey = sourceCmsCiRelationKey;
  }

  public String getSourceCmsCiRelationName() {
    return sourceCmsCiRelationName;
  }

  public void setSourceCmsCiRelationName(String sourceCmsCiRelationName) {
    this.sourceCmsCiRelationName = sourceCmsCiRelationName;
  }

  public int getSourceCmsCiRelationId() {
    return sourceCmsCiRelationId;
  }

  public void setSourceCmsCiRelationId(int sourceCmsCiRelationId) {
    this.sourceCmsCiRelationId = sourceCmsCiRelationId;
  }

  public String getSourceFromCmsCiClazzName() {
    return sourceFromCmsCiClazzName;
  }

  public void setSourceFromCmsCiClazzName(String sourceFromCmsCiClazzName) {
    this.sourceFromCmsCiClazzName = sourceFromCmsCiClazzName;
  }

  public int getSourceFromCmsCiClazzId() {
    return sourceFromCmsCiClazzId;
  }

  public void setSourceFromCmsCiClazzId(int sourceFromCmsCiClazzId) {
    this.sourceFromCmsCiClazzId = sourceFromCmsCiClazzId;
  }

  public String getTargetPack() {
    return targetPack;
  }

  public void setTargetPack(String targetPack) {
    this.targetPack = targetPack;
  }

  public String getTargetCmsCiRelationKey() {
    return targetCmsCiRelationKey;
  }

  public void setTargetCmsCiRelationKey(String targetCmsCiRelationKey) {
    this.targetCmsCiRelationKey = targetCmsCiRelationKey;
  }

  public String getTargetCmsCiRelationName() {
    return targetCmsCiRelationName;
  }

  public void setTargetCmsCiRelationName(String targetCmsCiRelationName) {
    this.targetCmsCiRelationName = targetCmsCiRelationName;
  }

  public int getTargetCmsCiRelationId() {
    return targetCmsCiRelationId;
  }

  public void setTargetCmsCiRelationId(int targetCmsCiRelationId) {
    this.targetCmsCiRelationId = targetCmsCiRelationId;
  }

  public String getTargetFromCmsCiClazzName() {
    return targetFromCmsCiClazzName;
  }

  public void setTargetFromCmsCiClazzName(String targetFromCmsCiClazzName) {
    this.targetFromCmsCiClazzName = targetFromCmsCiClazzName;
  }

  public int getTargetFromCmsCiClazzId() {
    return targetFromCmsCiClazzId;
  }

  public void setTargetFromCmsCiClazzId(int targetFromCmsCiClazzId) {
    this.targetFromCmsCiClazzId = targetFromCmsCiClazzId;
  }

  public String getAction() {
    return action;
  }

  public void setAction(String action) {
    this.action = action;
  }

  public String getEntityType() {
    return entityType;
  }

  public void setEntityType(String entityType) {
    this.entityType = entityType;
  }

  public String getSourceToCmsCiClazzName() {
    return sourceToCmsCiClazzName;
  }

  public void setSourceToCmsCiClazzName(String sourceToCmsCiClazzName) {
    this.sourceToCmsCiClazzName = sourceToCmsCiClazzName;
  }

  public int getSourceToCmsCiClazzId() {
    return sourceToCmsCiClazzId;
  }

  public void setSourceToCmsCiClazzId(int sourceToCmsCiClazzId) {
    this.sourceToCmsCiClazzId = sourceToCmsCiClazzId;
  }

  public String getTargetToCmsCiClazzName() {
    return targetToCmsCiClazzName;
  }

  public void setTargetToCmsCiClazzName(String targetToCmsCiClazzName) {
    this.targetToCmsCiClazzName = targetToCmsCiClazzName;
  }

  public int getTargetToCmsCiClazzId() {
    return targetToCmsCiClazzId;
  }

  public void setTargetToCmsCiClazzId(int targetToCmsCiClazzId) {
    this.targetToCmsCiClazzId = targetToCmsCiClazzId;
  }

  public int getAttributeId() {
    return attributeId;
  }

  public void setAttributeId(int attributeId) {
    this.attributeId = attributeId;
  }

  public int getRelationId() {
    return relationId;
  }

  public void setRelationId(int relationId) {
    this.relationId = relationId;
  }

  public String getAttributeName() {
    return attributeName;
  }

  public void setAttributeName(String attributeName) {
    this.attributeName = attributeName;
  }

  public String getDfValue() {
    return dfValue;
  }

  public void setDfValue(String dfValue) {
    this.dfValue = dfValue;
  }

  public String getDjValue() {
    return djValue;
  }

  public void setDjValue(String djValue) {
    this.djValue = djValue;
  }

}
