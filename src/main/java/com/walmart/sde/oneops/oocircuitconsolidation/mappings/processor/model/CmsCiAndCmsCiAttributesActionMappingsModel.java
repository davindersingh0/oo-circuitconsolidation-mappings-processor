package com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.model;

import java.io.Serializable;

public class CmsCiAndCmsCiAttributesActionMappingsModel implements Serializable{
  
  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  private String sourcePack;
  private String sourceClassname;
  private int sourceClassId;
  
  private String sourceAttributeName;
  private int sourceAttributeId;
  private String sourceDefaultValue;

  
  private String targetPack;
  private String targetClassname;
  private int targetClassId;
  
  private String targetAttributeName;
    private int targetAttributeId;
  private String targetDefaultValue;
  
  private String action;
  private String entityType;


  public String getEntityType() {
    return entityType;
  }

  public void setEntityType(String entityType) {
    this.entityType = entityType;
  }

  public String getSourcePack() {
    return sourcePack;
  }

  public void setSourcePack(String sourcePack) {
    this.sourcePack = sourcePack;
  }

  public String getSourceClassname() {
    return sourceClassname;
  }

  public void setSourceClassname(String sourceClassname) {
    this.sourceClassname = sourceClassname;
  }

  public String getTargetPack() {
    return targetPack;
  }

  public void setTargetPack(String targetPack) {
    this.targetPack = targetPack;
  }

  public String getTargetClassname() {
    return targetClassname;
  }

  public void setTargetClassname(String targetClassname) {
    this.targetClassname = targetClassname;
  }

  public int getSourceAttributeId() {
    return sourceAttributeId;
  }

  public void setSourceAttributeId(int sourceAttributeId) {
    this.sourceAttributeId = sourceAttributeId;
  }

  public int getSourceClassId() {
    return sourceClassId;
  }

  public void setSourceClassId(int sourceClassId) {
    this.sourceClassId = sourceClassId;
  }

  public String getSourceAttributeName() {
    return sourceAttributeName;
  }

  public void setSourceAttributeName(String sourceAttributeName) {
    this.sourceAttributeName = sourceAttributeName;
  }

  public String getSourceDefaultValue() {
    return sourceDefaultValue;
  }

  public void setSourceDefaultValue(String sourceDefaultValue) {
    this.sourceDefaultValue = sourceDefaultValue;
  }

  public int getTargetAttributeId() {
    return targetAttributeId;
  }

  public void setTargetAttributeId(int targetAttributeId) {
    this.targetAttributeId = targetAttributeId;
  }

  public int getTargetClassId() {
    return targetClassId;
  }

  public void setTargetClassId(int targetClassId) {
    this.targetClassId = targetClassId;
  }

  public String getTargetAttributeName() {
    return targetAttributeName;
  }

  public void setTargetAttributeName(String targetAttributeName) {
    this.targetAttributeName = targetAttributeName;
  }

  public String getTargetDefaultValue() {
    return targetDefaultValue;
  }

  public void setTargetDefaultValue(String targetDefaultValue) {
    this.targetDefaultValue = targetDefaultValue;
  }

  public String getAction() {
    return action;
  }

  public void setAction(String action) {
    this.action = action;
  }



}