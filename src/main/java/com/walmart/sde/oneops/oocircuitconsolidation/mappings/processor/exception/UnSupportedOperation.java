package com.walmart.sde.oneops.oocircuitconsolidation.mappings.processor.exception;



public final class UnSupportedOperation extends RuntimeException {
  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  public UnSupportedOperation() {
    super();
  }

  public UnSupportedOperation(Exception e) {
    super(e);
  }

  public UnSupportedOperation(String message) {
    super(message);
  }

  public UnSupportedOperation(String message, Exception e) {
    super(message, e);
  }

}


