package com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.exception;


public final class UnSupportedTransformationMappingException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public UnSupportedTransformationMappingException() {
    super();
  }

  public UnSupportedTransformationMappingException(Exception e) {
    super(e);
  }

  public UnSupportedTransformationMappingException(String message) {
    super(message);
  }

  public UnSupportedTransformationMappingException(String message, Exception e) {
    super(message, e);
  }

}


