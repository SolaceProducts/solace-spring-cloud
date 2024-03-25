package com.solace.spring.cloud.stream.binder.inbound.acknowledge;

import com.solace.spring.cloud.stream.binder.util.SolaceBatchAcknowledgementException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.integration.acks.AcknowledgmentCallback;

/**
 * Acknowledgment callback for a batch of messages.
 */
class JCSMPBatchAcknowledgementCallback implements AcknowledgmentCallback {

  private final List<JCSMPAcknowledgementCallback> acknowledgementCallbacks;
  private boolean acknowledged = false;
  private boolean autoAckEnabled = true;

  private static final Log logger = LogFactory.getLog(JCSMPBatchAcknowledgementCallback.class);

  JCSMPBatchAcknowledgementCallback(List<JCSMPAcknowledgementCallback> acknowledgementCallbacks) {
    this.acknowledgementCallbacks = acknowledgementCallbacks;
  }

  @Override
  public void acknowledge(Status status) {
    // messageContainer.isAcknowledged() might be async set which is why we also need a local ack variable
    if (isAcknowledged()) {
      if (logger.isDebugEnabled()) {
        logger.debug("Batch message is already acknowledged");
      }
      return;
    }

    Set<Integer> failedMessageIndexes = new HashSet<>();
    Throwable firstEncounteredException = null;
    for (int msgIdx = 0; msgIdx < acknowledgementCallbacks.size(); msgIdx++) {
      JCSMPAcknowledgementCallback messageAcknowledgementCallback = acknowledgementCallbacks.get(msgIdx);
      try {
        messageAcknowledgementCallback.acknowledge(status);
      } catch (Exception e) {
        logger.error(String.format("Failed to acknowledge XMLMessage %s",
            messageAcknowledgementCallback.getMessageContainer().getMessage().getMessageId()), e);
        failedMessageIndexes.add(msgIdx);
        if (firstEncounteredException == null) {
          firstEncounteredException = e;
        }
      }
    }

    if (firstEncounteredException != null) {
      throw new SolaceBatchAcknowledgementException(failedMessageIndexes,
          "Failed to acknowledge batch message", firstEncounteredException);
    }

    acknowledged = true;
  }

  @Override
  public boolean isAcknowledged() {
    if (acknowledged) {
      return true;
		} else if (acknowledgementCallbacks.stream().allMatch(JCSMPAcknowledgementCallback::isAcknowledged)) {
      if (logger.isTraceEnabled()) {
        logger.trace("All messages in batch are already acknowledged, marking batch as acknowledged");
      }
      acknowledged = true;
      return true;
    } else {
      return false;
    }
  }

  @Override
  public void noAutoAck() {
    autoAckEnabled = false;
  }

  @Override
  public boolean isAutoAck() {
    return autoAckEnabled;
  }

  boolean isErrorQueueEnabled() {
    return acknowledgementCallbacks.get(0).isErrorQueueEnabled();
  }

  /**
   * Send the message batch to the error queue and acknowledge the message.
   *
   * @return {@code true} if successful, {@code false} if {@code errorQueueInfrastructure} is not
   * defined or batch is already acknowledged.
   */
  boolean republishToErrorQueue() {
    if (!isErrorQueueEnabled()) {
      return false;
    }

    if (isAcknowledged()) {
      if (logger.isDebugEnabled()) {
        logger.debug("Batch message is already acknowledged");
      }
      return false;
    }

    Set<Integer> failedMessageIndexes = new HashSet<>();
    Throwable firstEncounteredException = null;
    AtomicInteger numAcked = new AtomicInteger(0);
    for (int msgIdx = 0; msgIdx < acknowledgementCallbacks.size(); msgIdx++) {
      JCSMPAcknowledgementCallback messageAcknowledgementCallback = acknowledgementCallbacks.get(
          msgIdx);
      try {
        if (messageAcknowledgementCallback.republishToErrorQueue()) {
          numAcked.getAndIncrement();
        }
      } catch (Exception e) {
        failedMessageIndexes.add(msgIdx);
        if (firstEncounteredException == null) {
          firstEncounteredException = e;
        }
      }
    }

    if (firstEncounteredException != null) {
      throw new SolaceBatchAcknowledgementException(failedMessageIndexes,
          "Failed to send batch message to error queue", firstEncounteredException);
    }

    return true;
  }
}
