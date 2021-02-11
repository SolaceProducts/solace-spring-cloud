package com.solace.spring.cloud.stream.binder.util;

import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.XMLMessage;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ErrorQueueRepublishCorrelationKey {
	private final ErrorQueueInfrastructure errorQueueInfrastructure;
	private final MessageContainer messageContainer;
	private final FlowReceiverContainer flowReceiverContainer;
	private final boolean hasTemporaryQueue;
	private final RetryableTaskService retryableTaskService;
	private long errorQueueDeliveryAttempt = 0;

	private static final Log logger = LogFactory.getLog(ErrorQueueRepublishCorrelationKey.class);

	public ErrorQueueRepublishCorrelationKey(ErrorQueueInfrastructure errorQueueInfrastructure,
											 MessageContainer messageContainer,
											 FlowReceiverContainer flowReceiverContainer,
											 boolean hasTemporaryQueue,
											 RetryableTaskService retryableTaskService) {
		this.errorQueueInfrastructure = errorQueueInfrastructure;
		this.messageContainer = messageContainer;
		this.flowReceiverContainer = flowReceiverContainer;
		this.hasTemporaryQueue = hasTemporaryQueue;
		this.retryableTaskService = retryableTaskService;
	}

	public void handleSuccess() throws SolaceStaleMessageException {
		flowReceiverContainer.acknowledge(messageContainer);
	}

	public void handleError() throws SolaceStaleMessageException {
		while (true) {
			if (errorQueueDeliveryAttempt >= errorQueueInfrastructure.getMaxDeliveryAttempts()) {
				fallback();
				break;
			} else if (messageContainer.isStale()) {
				throw new SolaceStaleMessageException(String.format("Message container %s (XMLMessage %s) is stale",
						messageContainer.getId(), messageContainer.getMessage().getMessageId()));
			} else {
				errorQueueDeliveryAttempt++;
				logger.info(String.format("Republishing XMLMessage %s to error queue %s - attempt %s of %s",
						messageContainer.getMessage().getMessageId(), errorQueueInfrastructure.getErrorQueueName(),
						errorQueueDeliveryAttempt, errorQueueInfrastructure.getMaxDeliveryAttempts()));
				try {
					errorQueueInfrastructure.send(messageContainer, this);
					break;
				} catch (JCSMPException e) {
					logger.warn(String.format("Could not send XMLMessage %s to error queue %s",
							messageContainer.getMessage().getMessageId(),
							errorQueueInfrastructure.getErrorQueueName()));
				}
			}
		}
	}

	private void fallback() throws SolaceStaleMessageException {
		if (hasTemporaryQueue) {
			logger.info(String.format(
					"Exceeded max error queue delivery attempts and cannot requeue %s %s since this flow is " +
							"bound to a temporary queue, failed message will be discarded",
					XMLMessage.class.getSimpleName(), messageContainer.getMessage().getMessageId()));
			flowReceiverContainer.acknowledge(messageContainer);
		} else {
			logger.info(String.format(
					"Exceeded max error queue delivery attempts. XMLMessage %s will be re-queued onto queue %s",
					messageContainer.getMessage().getMessageId(), flowReceiverContainer.getQueueName()));
			retryableTaskService.submit(() -> {
				try {
					flowReceiverContainer.acknowledgeRebind(messageContainer);
					return true;
				} catch (JCSMPException e) {
					logger.warn(String.format("failed to rebind queue %s. Will retry",
							flowReceiverContainer.getQueueName()), e);
					return false;
				} catch (SolaceStaleMessageException e) {
					// Nothing to do.
					logger.debug(String.format("Message container %s (XMLMessage %s) is stale",
							messageContainer.getId(), messageContainer.getMessage().getMessageId()), e);
					return true;
				}
			});
		}
	}

	public String getSourceMessageId() {
		return messageContainer.getMessage().getMessageId();
	}
	public String getErrorQueueName() {
		return errorQueueInfrastructure.getErrorQueueName();
	}
}
