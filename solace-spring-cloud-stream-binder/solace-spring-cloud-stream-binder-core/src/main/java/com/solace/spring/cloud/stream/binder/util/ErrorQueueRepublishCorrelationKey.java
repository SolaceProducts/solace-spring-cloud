package com.solace.spring.cloud.stream.binder.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ErrorQueueRepublishCorrelationKey {
	private final ErrorQueueInfrastructure errorQueueInfrastructure;
	private final MessageContainer messageContainer;
	private final FlowReceiverContainer flowReceiverContainer;
	private long errorQueueDeliveryAttempt = 0;

	private static final Log logger = LogFactory.getLog(ErrorQueueRepublishCorrelationKey.class);

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
				throw new IllegalStateException(String.format("Message container %s (XMLMessage %s) is stale",
						messageContainer.getId(), messageContainer.getMessage().getMessageId()), null);
			} else if (errorQueueDeliveryAttempt >= errorQueueInfrastructure.getMaxDeliveryAttempts()) {
				fallback();
				break;
			} else {
				errorQueueDeliveryAttempt++;
				logger.info(String.format("Republishing XMLMessage %s to error queue %s - attempt %s of %s",
						messageContainer.getMessage().getMessageId(), errorQueueInfrastructure.getErrorQueueName(),
						errorQueueDeliveryAttempt, errorQueueInfrastructure.getMaxDeliveryAttempts()));
				try {
					errorQueueInfrastructure.send(messageContainer, this);
					break;
				} catch (Exception e) {
					logger.warn(String.format("Could not send XMLMessage %s to error queue %s",
							messageContainer.getMessage().getMessageId(),
							errorQueueInfrastructure.getErrorQueueName()));
				}
			}
		}
	}

	private void fallback() {
			logger.info(String.format(
					"Exceeded max error queue delivery attempts. XMLMessage %s will be re-queued onto queue %s",
					messageContainer.getMessage().getMessageId(), flowReceiverContainer.getEndpointName()));
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
