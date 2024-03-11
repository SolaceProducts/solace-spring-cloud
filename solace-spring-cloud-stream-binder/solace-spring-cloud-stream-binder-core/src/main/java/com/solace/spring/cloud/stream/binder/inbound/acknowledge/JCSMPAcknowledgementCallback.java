package com.solace.spring.cloud.stream.binder.inbound.acknowledge;

import com.solace.spring.cloud.stream.binder.util.ErrorQueueInfrastructure;
import com.solace.spring.cloud.stream.binder.util.FlowReceiverContainer;
import com.solace.spring.cloud.stream.binder.util.MessageContainer;
import com.solace.spring.cloud.stream.binder.util.SolaceAcknowledgmentException;
import com.solace.spring.cloud.stream.binder.util.SolaceStaleMessageException;
import com.solacesystems.jcsmp.XMLMessage;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.integration.acks.AcknowledgmentCallback;
import org.springframework.lang.Nullable;

class JCSMPAcknowledgementCallback implements AcknowledgmentCallback {
  private final MessageContainer messageContainer;
  private final FlowReceiverContainer flowReceiverContainer;
  private final ErrorQueueInfrastructure errorQueueInfrastructure;
  private boolean acknowledged = false;
  private boolean autoAckEnabled = true;

  private static final Log logger = LogFactory.getLog(JCSMPAcknowledgementCallback.class);

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
      if (logger.isDebugEnabled()) {
        logger.debug(
            String.format("%s %s is already acknowledged", XMLMessage.class.getSimpleName(),
                messageContainer.getMessage().getMessageId()));
      }
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
          if (messageContainer.isStale()) {
            throw new SolaceStaleMessageException(String.format(
                "Message container %s (XMLMessage %s) is stale",
                messageContainer.getId(), messageContainer.getMessage().getMessageId()));
          } else {
            if (logger.isDebugEnabled()) {
              logger.debug(String.format("%s %s: Will be re-queued onto queue %s",
                  XMLMessage.class.getSimpleName(), messageContainer.getMessage().getMessageId(),
                  flowReceiverContainer.getQueueName()));
            }
            flowReceiverContainer.requeue(messageContainer);
          }
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
   boolean republishToErrorQueue() throws SolaceStaleMessageException {
    if (errorQueueInfrastructure == null) {
      return false;
    }

    if (logger.isDebugEnabled()) {
      logger.debug(String.format("%s %s: Will be republished onto error queue %s",
          XMLMessage.class.getSimpleName(), messageContainer.getMessage().getMessageId(),
          errorQueueInfrastructure.getErrorQueueName()));
    }

    if (messageContainer.isStale()) {
      throw new SolaceStaleMessageException(
          String.format("Cannot republish failed message container %s " +
                  "(XMLMessage %s) to error queue %s. Message is stale and will be redelivered.",
              messageContainer.getId(), messageContainer.getMessage().getMessageId(),
              errorQueueInfrastructure.getErrorQueueName()));
    }

    errorQueueInfrastructure.createCorrelationKey(messageContainer, flowReceiverContainer)
        .handleError();
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