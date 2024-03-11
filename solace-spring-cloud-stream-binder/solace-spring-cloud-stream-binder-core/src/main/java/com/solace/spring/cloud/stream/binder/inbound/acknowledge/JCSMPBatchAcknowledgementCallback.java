package com.solace.spring.cloud.stream.binder.inbound.acknowledge;

import com.solace.spring.cloud.stream.binder.util.MessageContainer;
import com.solace.spring.cloud.stream.binder.util.SolaceBatchAcknowledgementException;
import com.solace.spring.cloud.stream.binder.util.SolaceStaleMessageException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.integration.acks.AcknowledgmentCallback;

/**
 * Acknowledgment callback for a batch of messages.
 */
class JCSMPBatchAcknowledgementCallback implements AcknowledgmentCallback {
	private final List<JCSMPAcknowledgementCallback> acknowledgementCallbacks;
	private boolean acknowledged = false;
	private boolean autoAckEnabled = true;

	private static final Log logger = LogFactory.getLog(JCSMPBatchAcknowledgementCallback.class);

	JCSMPBatchAcknowledgementCallback(List<JCSMPAcknowledgementCallback> acknowledgementCallbacks) {
		this.acknowledgementCallbacks = acknowledgementCallbacks;
	}

	@Override
	public void acknowledge(Status status) {
		// messageContainer.isAcknowledged() might be async set which is why we also need a local ack variable
		if (isAcknowledged()) {
			if (logger.isDebugEnabled()) {
				logger.debug("Batch message is already acknowledged");
			}
			return;
		}

		boolean hasAtLeastOneSuccessAck = false;
		Set<Integer> failedMessageIndexes = new HashSet<>();
		Throwable firstEncounteredException = null;
		boolean allStaleExceptions = false;
		for (int msgIdx = 0; msgIdx < acknowledgementCallbacks.size(); msgIdx++) {
			JCSMPAcknowledgementCallback messageAcknowledgementCallback = acknowledgementCallbacks.get(msgIdx);
			try {
				messageAcknowledgementCallback.acknowledge(status);
				if (!hasAtLeastOneSuccessAck) {
					hasAtLeastOneSuccessAck = messageAcknowledgementCallback.getMessageContainer().isAcknowledged();
				}
			} catch (Exception e) {
				boolean isStale = logPotentialStaleException(e, messageAcknowledgementCallback.getMessageContainer());
				failedMessageIndexes.add(msgIdx);
				if (firstEncounteredException == null) {
					firstEncounteredException = e;
				}
				if (allStaleExceptions || msgIdx == 0) {
					allStaleExceptions = isStale;
				}
			}
		}

		if (firstEncounteredException != null) {
			throw new SolaceBatchAcknowledgementException(failedMessageIndexes, allStaleExceptions,
					"Failed to acknowledge batch message", firstEncounteredException);
		}

		acknowledged = true;
	}

	/**
	 * Log the acknowledgment exception.
	 * @param e exception
	 * @param messageContainer message container
	 * @return true if exception is caused by a {@link SolaceStaleMessageException}
	 */
	private boolean logPotentialStaleException(Throwable e, MessageContainer messageContainer) {
		boolean isStale = ExceptionUtils.indexOfType(e, SolaceStaleMessageException.class) > -1;
		if (isStale) {
			if (logger.isDebugEnabled()) {
				String debugLogMsg = String.format("Failed to acknowledge stale XMLMessage %s",
						messageContainer.getMessage().getMessageId());
				if (logger.isTraceEnabled()) {
					logger.debug(debugLogMsg, e);
				} else {
					logger.debug(debugLogMsg);
				}
			}
		} else {
			logger.error(String.format("Failed to acknowledge XMLMessage %s",
					messageContainer.getMessage().getMessageId()), e);
		}
		return isStale;
	}

	@Override
	public boolean isAcknowledged() {
		if (acknowledged) {
			return true;
		} else if (acknowledgementCallbacks.stream().allMatch(JCSMPAcknowledgementCallback::isAcknowledged)) {
			if (logger.isTraceEnabled()) {
				logger.trace("All messages in batch are already acknowledged, marking batch as acknowledged");
			}
			acknowledged = true;
			return true;
		} else {
			return false;
		}
	}

	@Override
	public void noAutoAck() {
		autoAckEnabled = false;
	}

	@Override
	public boolean isAutoAck() {
		return autoAckEnabled;
	}

	boolean isErrorQueueEnabled() {
		return acknowledgementCallbacks.get(0).isErrorQueueEnabled();
	}

	/**
	 * Send the message batch to the error queue and acknowledge the message.
	 *
	 * @return {@code true} if successful, {@code false} if {@code errorQueueInfrastructure} is not
	 * defined or batch is already acknowledged.
	 */
	boolean republishToErrorQueue() {
		if(!isErrorQueueEnabled()) {
			return false;
		}

		if (isAcknowledged()) {
			if (logger.isDebugEnabled()) {
				logger.debug("Batch message is already acknowledged");
			}
			return false;
		}

		Set<Integer> failedMessageIndexes = new HashSet<>();
		Throwable firstEncounteredException = null;
		boolean allStaleExceptions = false;
		AtomicInteger numAcked = new AtomicInteger(0);
		for (int msgIdx = 0; msgIdx < acknowledgementCallbacks.size(); msgIdx++) {
			JCSMPAcknowledgementCallback messageAcknowledgementCallback = acknowledgementCallbacks.get(
					msgIdx);
			try {
				if (messageAcknowledgementCallback.republishToErrorQueue()) {
					numAcked.getAndIncrement();
				}
			} catch (Exception e) {
				boolean isStale = logPotentialStaleException(e,
						messageAcknowledgementCallback.getMessageContainer());
				failedMessageIndexes.add(msgIdx);
				if (firstEncounteredException == null) {
					firstEncounteredException = e;
				}
				if (allStaleExceptions || msgIdx == 0) {
					allStaleExceptions = isStale;
				}
			}
		}

		if (firstEncounteredException != null) {
			throw new SolaceBatchAcknowledgementException(failedMessageIndexes, allStaleExceptions,
					"Failed to send batch message to error queue", firstEncounteredException);
		}

		return true;
	}
}
