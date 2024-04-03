package com.solace.spring.cloud.stream.binder.util;

import java.util.Set;

public class SolaceBatchAcknowledgementException extends SolaceAcknowledgmentException {

  private final Set<Integer> failedMessageIndexes;

  public SolaceBatchAcknowledgementException(Set<Integer> failedMessageIndexes, String message,
      Throwable cause) {
    super(message, cause);
    this.failedMessageIndexes = failedMessageIndexes;
  }

  public Set<Integer> getFailedMessageIndexes() {
    return failedMessageIndexes;
  }
}
