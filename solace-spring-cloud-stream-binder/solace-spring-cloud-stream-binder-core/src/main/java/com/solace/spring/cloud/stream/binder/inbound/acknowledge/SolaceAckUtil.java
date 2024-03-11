package com.solace.spring.cloud.stream.binder.inbound.acknowledge;

import com.solace.spring.cloud.stream.binder.util.SolaceAcknowledgmentException;
import org.springframework.integration.acks.AcknowledgmentCallback;

/**
 * Utility methods for acting on Solace implementation of {@link AcknowledgmentCallback}.
 */
public class SolaceAckUtil {

  /**
   * Verify if the error queue is enabled for the related message consumer. Beneficial to a consumer
   * in client-ack mode. The application may simply want to verify if error queue is enabled without
   * actually moving the message to error queue.
   *
   * @param acknowledgmentCallback the AcknowledgmentCallback.
   * @return true if the error queue is enabled for the associated message consumer.
   */
  public static boolean isErrorQueueEnabled(AcknowledgmentCallback acknowledgmentCallback) {
    if (acknowledgmentCallback instanceof JCSMPAcknowledgementCallback jcsmpAcknowledgementCallback) {
      return jcsmpAcknowledgementCallback.isErrorQueueEnabled();
    } else if (acknowledgmentCallback instanceof JCSMPBatchAcknowledgementCallback batchAcknowledgementCallback) {
      return batchAcknowledgementCallback.isErrorQueueEnabled();
    }

    return false;
  }

  /**
   * Send the related message to the error queue if enabled for associated message consumer. This
   * could be specially useful for a consumer in client-ack mode
   *
   * @param acknowledgmentCallback the AcknowledgmentCallback.
   * @return return true if error queue is enabled for the associated message consumer and the
   * message is successfully published to the error queue.
   * @throws SolaceAcknowledgmentException failed to send message to error queue.
   */
  public static boolean republishToErrorQueue(AcknowledgmentCallback acknowledgmentCallback) {
    if (!acknowledgmentCallback.isAcknowledged()
        && acknowledgmentCallback instanceof JCSMPAcknowledgementCallback jcsmpAcknowledgementCallback
        && jcsmpAcknowledgementCallback.isErrorQueueEnabled()) {
      try {
        if (jcsmpAcknowledgementCallback.republishToErrorQueue()) {
          return true;
        }
      } catch (Exception e) {
        throw new SolaceAcknowledgmentException(
            String.format("Failed to send XMLMessage %s to error queue",
                jcsmpAcknowledgementCallback.getMessageContainer().getMessage().getMessageId()), e);
      }
    } else if (!acknowledgmentCallback.isAcknowledged()
        && acknowledgmentCallback instanceof JCSMPBatchAcknowledgementCallback batchAcknowledgementCallback
        && batchAcknowledgementCallback.isErrorQueueEnabled()) {
      return batchAcknowledgementCallback.republishToErrorQueue();
    }

    return false;
  }
}