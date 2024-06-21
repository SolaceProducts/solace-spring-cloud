package com.solace.spring.cloud.stream.binder.inbound.acknowledge;

import com.solace.spring.cloud.stream.binder.util.ErrorQueueInfrastructure;
import com.solace.spring.cloud.stream.binder.util.FlowReceiverContainer;
import com.solace.spring.cloud.stream.binder.util.MessageContainer;
import com.solace.spring.cloud.stream.binder.util.SolaceAcknowledgmentException;
import com.solacesystems.jcsmp.XMLMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.integration.acks.AcknowledgmentCallback;
import org.springframework.lang.Nullable;

class JCSMPAcknowledgementCallback implements AcknowledgmentCallback {
  private final MessageContainer messageContainer;
  private final FlowReceiverContainer flowReceiverContainer;
  private final ErrorQueueInfrastructure errorQueueInfrastructure;
  private boolean acknowledged = false;
  private boolean autoAckEnabled = true;

  private static final Logger LOGGER = LoggerFactory.getLogger(JCSMPAcknowledgementCallback.class);

  JCSMPAcknowledgementCallback(MessageContainer messageContainer,
      FlowReceiverContainer flowReceiverContainer,
      @Nullable ErrorQueueInfrastructure errorQueueInfrastructure) {
    this.messageContainer = messageContainer;
    this.flowReceiverContainer = flowReceiverContainer;
    this.errorQueueInfrastructure = errorQueueInfrastructure;
  }

  @Override
  public void acknowledge(Status status) {
    // messageContainer.isAcknowledged() might be async set which is why we also need a local ack variable
    if (acknowledged || messageContainer.isAcknowledged()) {
      LOGGER.debug("{} {} is already acknowledged", XMLMessage.class.getSimpleName(),
              messageContainer.getMessage().getMessageId());
      return;
    }

    try {
      switch (status) {
        case ACCEPT:
          flowReceiverContainer.acknowledge(messageContainer);
          break;
        case REJECT:
          if (republishToErrorQueue()) {
            break;
          } else {
            flowReceiverContainer.reject(messageContainer);
          }
          break;
        case REQUEUE:
          LOGGER.debug("{} {}: Will be re-queued onto queue {}",
              XMLMessage.class.getSimpleName(), messageContainer.getMessage().getMessageId(),
              flowReceiverContainer.getEndpointName());
          flowReceiverContainer.requeue(messageContainer);
      }
    } catch (SolaceAcknowledgmentException e) {
      throw e;
    } catch (Exception e) {
      throw new SolaceAcknowledgmentException(String.format("Failed to acknowledge XMLMessage %s",
          messageContainer.getMessage().getMessageId()), e);
    }

    acknowledged = true;
  }

  /**
   * Send the message to the error queue and acknowledge the message.
   *
   * @return {@code true} if successful, {@code false} if {@code errorQueueInfrastructure} is not
   * defined.
   */
   boolean republishToErrorQueue() {
    if (errorQueueInfrastructure == null) {
      return false;
    }

    LOGGER.debug("{} {}: Will be republished onto error queue {}",
            XMLMessage.class.getSimpleName(), messageContainer.getMessage().getMessageId(),
            errorQueueInfrastructure.getErrorQueueName());

     try {
      errorQueueInfrastructure.createCorrelationKey(messageContainer, flowReceiverContainer).handleError();
    } catch (Exception e) {
      throw new SolaceAcknowledgmentException(
          String.format("Failed to send XMLMessage %s to error queue",
              messageContainer.getMessage().getMessageId()), e);
    }
    return true;
  }

  @Override
  public boolean isAcknowledged() {
    return acknowledged || messageContainer.isAcknowledged();
  }

  @Override
  public void noAutoAck() {
    autoAckEnabled = false;
  }

  @Override
  public boolean isAutoAck() {
    return autoAckEnabled;
  }

  MessageContainer getMessageContainer() {
    return messageContainer;
  }

  boolean isErrorQueueEnabled() {
     return  errorQueueInfrastructure != null;
  }
}