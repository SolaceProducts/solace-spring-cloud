package com.solace.spring.cloud.stream.binder.util;

import org.springframework.integration.acks.AcknowledgmentCallback;

public class JCSMPAcknowledgementCallbackFactory {
	/**
	 *
	 * @param messageContainer The message.
	 * @param flowReceiverContainer The message's flow receiver's container.
	 * @param hasTemporaryQueue Is the message's flow receiver bound to a temporary queue?
	 * @return a new acknowledgment callback.
	 */
	public AcknowledgmentCallback createCallback(MessageContainer messageContainer,
												 FlowReceiverContainer flowReceiverContainer,
												 boolean hasTemporaryQueue) {
		return new JCSMPAcknowledgementCallback(messageContainer, flowReceiverContainer, hasTemporaryQueue);
	}

	static class JCSMPAcknowledgementCallback implements AcknowledgmentCallback {
		private final MessageContainer messageContainer;
		private final FlowReceiverContainer flowReceiverContainer;
		private final boolean hasTemporaryQueue;
		private boolean acknowledged = false;
		private boolean autoAckEnabled = true;

		JCSMPAcknowledgementCallback(MessageContainer messageContainer, FlowReceiverContainer flowReceiverContainer,
									 boolean hasTemporaryQueue) {
			this.messageContainer = messageContainer;
			this.flowReceiverContainer = flowReceiverContainer;
			this.hasTemporaryQueue = hasTemporaryQueue;
		}

		@Override
		public void acknowledge(Status status) {
			try {
				switch (status) {
					case ACCEPT:
						messageContainer.getMessage().ackMessage();
						break;
					case REJECT:
						messageContainer.getMessage().ackMessage();
						break;
					case REQUEUE:
						if (hasTemporaryQueue) {
							throw new UnsupportedOperationException(String.format(
									"Cannot %s XMLMessage %s, this operation is not supported with temporary queues",
									status, messageContainer.getMessage().getMessageId()));
						} else {
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
