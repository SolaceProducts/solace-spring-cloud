package com.solace.spring.cloud.stream.binder.util;

import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.XMLMessage;
import com.solacesystems.jcsmp.impl.flow.FlowHandle;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.integration.acks.AcknowledgmentCallback;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;

public class JCSMPAcknowledgementCallbackFactory {
	private final FlowReceiverContainer flowReceiverContainer;
	private final boolean hasTemporaryQueue;
	private ErrorQueueInfrastructure errorQueueInfrastructure;

	public JCSMPAcknowledgementCallbackFactory(FlowReceiverContainer flowReceiverContainer, boolean hasTemporaryQueue) {
		this.flowReceiverContainer = flowReceiverContainer;
		this.hasTemporaryQueue = hasTemporaryQueue;
	}

	public void setErrorQueueInfrastructure(ErrorQueueInfrastructure errorQueueInfrastructure) {
		this.errorQueueInfrastructure = errorQueueInfrastructure;
	}

	public AcknowledgmentCallback createCallback(MessageContainer messageContainer) {
		return new JCSMPAcknowledgementCallback(messageContainer, flowReceiverContainer, hasTemporaryQueue,
				errorQueueInfrastructure);
	}

	static class JCSMPAcknowledgementCallback implements AcknowledgmentCallback {
		private final MessageContainer messageContainer;
		private final FlowReceiverContainer flowReceiverContainer;
		private final boolean hasTemporaryQueue;
		private final ErrorQueueInfrastructure errorQueueInfrastructure;
		private final XMLMessageMapper xmlMessageMapper = new XMLMessageMapper();
		private boolean acknowledged = false;
		private boolean autoAckEnabled = true;

		private static final Log logger = LogFactory.getLog(JCSMPAcknowledgementCallback.class);

		JCSMPAcknowledgementCallback(MessageContainer messageContainer, FlowReceiverContainer flowReceiverContainer,
									 boolean hasTemporaryQueue,
									 @Nullable ErrorQueueInfrastructure errorQueueInfrastructure) {
			this.messageContainer = messageContainer;
			this.flowReceiverContainer = flowReceiverContainer;
			this.hasTemporaryQueue = hasTemporaryQueue;
			this.errorQueueInfrastructure = errorQueueInfrastructure;
		}

		@Override
		public void acknowledge(Status status) {
			if (acknowledged) {
				logger.info(String.format("%s %s is already acknowledged", XMLMessage.class.getSimpleName(),
						messageContainer.getMessage().getMessageId()));
				return;
			}

			try {
				switch (status) {
					case ACCEPT:
						messageContainer.getMessage().ackMessage();
						break;
					case REJECT:
						if (republishToErrorQueue()) {
							break;
						} else if (!hasTemporaryQueue) {
							acknowledge(Status.REQUEUE);
						} else {
							logger.info(String.format(
									"Cannot %s %s %s since this flow is bound to a temporary queue, failed message " +
											"will be discarded",
									Status.REQUEUE, XMLMessage.class.getSimpleName(),
									messageContainer.getMessage().getMessageId()));
							messageContainer.getMessage().ackMessage();
						}
						break;
					case REQUEUE:
						if (hasTemporaryQueue) {
							throw new UnsupportedOperationException(String.format(
									"Cannot %s XMLMessage %s, this operation is not supported with temporary queues",
									status, messageContainer.getMessage().getMessageId()));
						} else {
							logger.info(String.format("%s %s: Will be re-queued onto queue %s",
									XMLMessage.class.getSimpleName(), messageContainer.getMessage().getMessageId(),
									flowReceiverContainer.getQueueName()));
							flowReceiverContainer.rebind(messageContainer.getFlowId());
						}
				}

				acknowledged = true;
			} catch (Exception e) {
				if (!(e instanceof SolaceAcknowledgmentException)) {
					throw new SolaceAcknowledgmentException(String.format("Failed to acknowledge XMLMessage %s",
							messageContainer.getMessage().getMessageId()), e);
				}
			}
		}

		/**
		 * Send the message to the error queue and acknowledge the message.
		 * @return {@code true} if successful, {@code false} if {@code errorQueueInfrastructure} is not defined.
		 */
		private boolean republishToErrorQueue() {
			if (errorQueueInfrastructure == null) {
				return false;
			}

			String queueName = flowReceiverContainer.getQueueName();
			String errorQueueName = SolaceProvisioningUtil.getErrorQueueName(queueName);

			logger.info(String.format("%s %s: Will be republished onto error queue %s",
					XMLMessage.class.getSimpleName(), messageContainer.getMessage().getMessageId(),
					errorQueueName));

			FlowReceiver flowReceiver = flowReceiverContainer.get();
			long expectedFlowId = messageContainer.getFlowId();
			Long actualFlowId = flowReceiver != null ? ((FlowHandle) flowReceiver).getFlowId() : null;
			if (flowReceiver == null || expectedFlowId != actualFlowId) {
				throw new IllegalStateException(String.format("Cannot republish failed %s %s to error queue %s , " +
								"flow %s is closed (active flow: %s), message will be redelivered",
						XMLMessage.class.getSimpleName(), messageContainer.getMessage().getMessageId(),
						errorQueueName, expectedFlowId, actualFlowId));
			}

			Message<?> springMessage = xmlMessageMapper.map(messageContainer.getMessage(), null);
			errorQueueInfrastructure.send(errorQueueName, springMessage);
			messageContainer.getMessage().ackMessage();
			return true;
		}

		@Override
		public boolean isAcknowledged() {
			return acknowledged;
		}

		@Override
		public void noAutoAck() {
			autoAckEnabled = false;
		}

		@Override
		public boolean isAutoAck() {
			return autoAckEnabled;
		}
	}
}
