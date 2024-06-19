package com.solace.spring.cloud.stream.binder.inbound.acknowledge;

import com.solace.spring.cloud.stream.binder.util.ErrorQueueInfrastructure;
import com.solace.spring.cloud.stream.binder.util.SolaceAcknowledgmentException;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.transaction.RollbackException;
import com.solacesystems.jcsmp.transaction.TransactedSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.integration.acks.AcknowledgmentCallback;

class TransactedJCSMPAcknowledgementCallback implements AcknowledgmentCallback {
	private final ThreadLocal<TransactedSession> transactedSessionThreadLocal = new ThreadLocal<>();
	private final ErrorQueueInfrastructure errorQueueInfrastructure;
	private boolean acknowledged = false;
	private static final Logger LOGGER = LoggerFactory.getLogger(TransactedJCSMPAcknowledgementCallback.class);

	TransactedJCSMPAcknowledgementCallback(TransactedSession transactedSession,
										   ErrorQueueInfrastructure errorQueueInfrastructure) {
		this.transactedSessionThreadLocal.set(transactedSession);
		this.errorQueueInfrastructure = errorQueueInfrastructure;
	}

	@Override
	public void acknowledge(Status status) {
		if (acknowledged) {
			LOGGER.debug("transaction is already resolved");
			return;
		}

		TransactedSession transactedSession = transactedSessionThreadLocal.get();
		if (transactedSession == null) {
			throw new UnsupportedOperationException("Transactions must be resolved on the message handler's thread");
		}

		try {
			switch (status) {
				case ACCEPT -> {
					try {
						transactedSession.commit();
					} catch (JCSMPException e) {
						if (!(e instanceof RollbackException)) {
							try {
								LOGGER.debug("Rolling back transaction");
								transactedSession.rollback();
							} catch (JCSMPException e1) {
								e.addSuppressed(e1);
							}
						}

						throw e;
					}
				}
				case REJECT -> {
					if (!republishToErrorQueue()) {
						transactedSession.rollback();
					}
				}
				case REQUEUE -> transactedSession.rollback();
			}
		} catch (Exception e) {
			throw new SolaceAcknowledgmentException("Failed to resolve transaction", e);
		}

		acknowledged = true;
	}

	/**
	 * Send the message to the error queue and acknowledge the message.
	 *
	 * @return {@code true} if successful, {@code false} if {@code errorQueueInfrastructure} is not
	 * defined.
	 */
	private boolean republishToErrorQueue() {
		return false; //TODO
//		if (errorQueueInfrastructure == null) {
//			return false;
//		}
//
//		LOGGER.debug("{} {}: Will be republished onto error queue {}",
//				XMLMessage.class.getSimpleName(), messageContainer.getMessage().getMessageId(),
//				errorQueueInfrastructure.getErrorQueueName());
//
//		try {
//			errorQueueInfrastructure.createCorrelationKey(messageContainer, flowReceiverContainer).handleError();
//		} catch (Exception e) {
//			throw new SolaceAcknowledgmentException(
//					String.format("Failed to send XMLMessage %s to error queue",
//							messageContainer.getMessage().getMessageId()), e);
//		}
//		return true;
	}

	@Override
	public boolean isAcknowledged() {
		return acknowledged;
	}
}
