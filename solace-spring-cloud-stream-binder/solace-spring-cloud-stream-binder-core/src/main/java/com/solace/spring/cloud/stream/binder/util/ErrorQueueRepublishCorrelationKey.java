package com.solace.spring.cloud.stream.binder.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ErrorQueueRepublishCorrelationKey {
	private final ErrorQueueInfrastructure errorQueueInfrastructure;
	private final MessageContainer messageContainer;
	private final FlowReceiverContainer flowReceiverContainer;
	private long errorQueueDeliveryAttempt = 0;

	private static final Logger LOGGER = LoggerFactory.getLogger(ErrorQueueRepublishCorrelationKey.class);

	public ErrorQueueRepublishCorrelationKey(ErrorQueueInfrastructure errorQueueInfrastructure,
											 MessageContainer messageContainer,
											 FlowReceiverContainer flowReceiverContainer) {
		this.errorQueueInfrastructure = errorQueueInfrastructure;
		this.messageContainer = messageContainer;
		this.flowReceiverContainer = flowReceiverContainer;
	}

	public void handleSuccess() {
		flowReceiverContainer.acknowledge(messageContainer);
	}

	public void handleError() {
		while (true) {
			if (messageContainer.isStale()) {
				throw new IllegalStateException(String.format(
						"Cannot republish failed message container %s (XMLMessage %s) to error queue %s. Message is stale",
						messageContainer.getId(),
						messageContainer.getMessage().getMessageId(),
						errorQueueInfrastructure.getErrorQueueName()), null);
			} else if (errorQueueDeliveryAttempt >= errorQueueInfrastructure.getMaxDeliveryAttempts()) {
				fallback();
				break;
			} else {
				errorQueueDeliveryAttempt++;
				LOGGER.info("Republishing XMLMessage {} to error queue {} - attempt {} of {}",
						messageContainer.getMessage().getMessageId(),
						errorQueueInfrastructure.getErrorQueueName(),
						errorQueueDeliveryAttempt,
						errorQueueInfrastructure.getMaxDeliveryAttempts());
				try {
					errorQueueInfrastructure.send(messageContainer, this);
					break;
				} catch (Exception e) {
					LOGGER.warn("Could not send XMLMessage {} to error queue {}",
							messageContainer.getMessage().getMessageId(), errorQueueInfrastructure.getErrorQueueName());
				}
			}
		}
	}

	private void fallback() {
		LOGGER.info("Exceeded max error queue delivery attempts. XMLMessage {} will be re-queued onto queue {}",
				messageContainer.getMessage().getMessageId(), flowReceiverContainer.getEndpointName());
		flowReceiverContainer.requeue(messageContainer);
	}

	public String getSourceMessageId() {
		return messageContainer.getMessage().getMessageId();
	}
	public String getErrorQueueName() {
		return errorQueueInfrastructure.getErrorQueueName();
	}

	long getErrorQueueDeliveryAttempt() {
		return errorQueueDeliveryAttempt;
	}
}
