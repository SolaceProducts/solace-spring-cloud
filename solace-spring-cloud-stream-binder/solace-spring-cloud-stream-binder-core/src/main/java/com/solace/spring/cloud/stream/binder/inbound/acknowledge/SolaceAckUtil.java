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
   * @param ackCallback the AcknowledgmentCallback.
   * @return return true if error queue is enabled for the associated message consumer and the
   * message is successfully published to the error queue.
   * @throws SolaceAcknowledgmentException failed to send message to error queue.
   */
  public static boolean republishToErrorQueue(AcknowledgmentCallback ackCallback) {
    if (ackCallback.isAcknowledged()) {
      return false;
    }

    if (ackCallback instanceof JCSMPAcknowledgementCallback jcsmpAckCallback) {
      return jcsmpAckCallback.republishToErrorQueue();
    } else if (ackCallback instanceof JCSMPBatchAcknowledgementCallback jcsmpBatchAckCallback) {
      return jcsmpBatchAckCallback.republishToErrorQueue();
    }

    return false;
  }
}