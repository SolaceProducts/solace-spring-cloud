package com.solace.spring.cloud.stream.binder.util;

import com.solacesystems.jcsmp.JCSMPException;
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

	public void handleError(boolean skipSyncFallbackAttempt) throws SolaceStaleMessageException {
		while (true) {
			if (messageContainer.isStale()) {
				throw new SolaceStaleMessageException(String.format("Message container %s (XMLMessage %s) is stale",
						messageContainer.getId(), messageContainer.getMessage().getMessageId()));
			} else if (errorQueueDeliveryAttempt >= errorQueueInfrastructure.getMaxDeliveryAttempts()) {
				fallback(skipSyncFallbackAttempt);
				break;
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

	private void fallback(boolean skipSyncAttempt) throws SolaceStaleMessageException {
		if (hasTemporaryQueue) {
			logger.info(String.format(
					"Exceeded max error queue delivery attempts and cannot requeue XMLMessage %s since queue %s is " +
							"temporary. Failed message will be discarded.",
					messageContainer.getMessage().getMessageId(), flowReceiverContainer.getQueueName()));
			flowReceiverContainer.acknowledge(messageContainer);
		} else {
			logger.info(String.format(
					"Exceeded max error queue delivery attempts. XMLMessage %s will be re-queued onto queue %s",
					messageContainer.getMessage().getMessageId(), flowReceiverContainer.getQueueName()));

			RetryableRebindTask rebindTask = new RetryableRebindTask(flowReceiverContainer, messageContainer,
					retryableTaskService);
			try {
				if (skipSyncAttempt || !rebindTask.run(0)) {
					retryableTaskService.submit(rebindTask);
				}
			} catch (InterruptedException interruptedException) {
				logger.info(String.format("Interrupt received while rebinding to queue %s with message %s",
						flowReceiverContainer.getQueueName(), messageContainer.getMessage().getMessageId()));
			}
		}
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
