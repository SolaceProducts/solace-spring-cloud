package com.solace.spring.cloud.stream.binder.inbound.acknowledge;

import com.solace.spring.cloud.stream.binder.util.FlowReceiverContainer;
import com.solace.spring.cloud.stream.binder.util.MessageContainer;
import com.solace.spring.cloud.stream.binder.util.RetryableRebindTask;
import com.solace.spring.cloud.stream.binder.util.RetryableTaskService;
import com.solace.spring.cloud.stream.binder.util.SolaceBatchAcknowledgementException;
import com.solace.spring.cloud.stream.binder.util.SolaceStaleMessageException;
import com.solace.spring.cloud.stream.binder.util.UnboundFlowReceiverContainerException;
import com.solacesystems.jcsmp.JCSMPException;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.integration.acks.AcknowledgmentCallback;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Acknowledgment callback for a batch of messages.
 */
class JCSMPBatchAcknowledgementCallback implements AcknowledgmentCallback {
	private final List<JCSMPAcknowledgementCallback> acknowledgementCallbacks;
	private final FlowReceiverContainer flowReceiverContainer;
	private final RetryableTaskService taskService;
	private boolean acknowledged = false;
	private boolean autoAckEnabled = true;

	private static final Log logger = LogFactory.getLog(JCSMPBatchAcknowledgementCallback.class);

	JCSMPBatchAcknowledgementCallback(List<JCSMPAcknowledgementCallback> acknowledgementCallbacks,
									  FlowReceiverContainer flowReceiverContainer,
									  RetryableTaskService taskService) {
		this.acknowledgementCallbacks = acknowledgementCallbacks;
		this.flowReceiverContainer = flowReceiverContainer;
		this.taskService = taskService;
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
				if (msgIdx < acknowledgementCallbacks.size() - 1) {
					messageAcknowledgementCallback.doAsyncRebindIfNecessary();
				}
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

		if (logger.isTraceEnabled()) {
			logger.trace("Beginning to wait for async rebinds to complete if necessary");
		}

		Set<UUID> flowsReceiversToRebind = new HashSet<>();

		for (int msgIdx = 0; msgIdx < acknowledgementCallbacks.size(); msgIdx++) {
			JCSMPAcknowledgementCallback messageAcknowledgementCallback = acknowledgementCallbacks.get(msgIdx);
			try {
				messageAcknowledgementCallback.awaitRebindIfNecessary(1, TimeUnit.MILLISECONDS);
			} catch (ExecutionException | InterruptedException exception) {
				if (exception instanceof ExecutionException && (exception.getCause() instanceof JCSMPException ||
						exception.getCause() instanceof UnboundFlowReceiverContainerException)) {
					UUID flowReceiverReferenceId = messageAcknowledgementCallback.getMessageContainer()
							.getFlowReceiverReferenceId();
					if (!flowsReceiversToRebind.add(flowReceiverReferenceId) && logger.isDebugEnabled()) {
						logger.debug(String.format("Failed to rebind flow container %s . " +
										"Sending new async rebind task in case one isn't already active",
								flowReceiverReferenceId), exception);
					}
					continue;
				}

				Throwable e = exception instanceof ExecutionException ? exception.getCause() : exception;
				boolean isStale = logPotentialStaleException(e, messageAcknowledgementCallback.getMessageContainer());
				failedMessageIndexes.add(msgIdx);
				if (firstEncounteredException == null) {
					firstEncounteredException = e;
				}
				if (allStaleExceptions || (msgIdx == 0 && !hasAtLeastOneSuccessAck)) {
					allStaleExceptions = isStale;
				}
			} catch (TimeoutException e) {
				UUID flowReceiverReferenceId = messageAcknowledgementCallback.getMessageContainer()
						.getFlowReceiverReferenceId();
				if (!flowsReceiversToRebind.add(flowReceiverReferenceId) && logger.isTraceEnabled()) {
					logger.trace(String.format("Timed out while waiting for rebind of flow receiver container %s, " +
							"sending new async rebind task in case one isn't already active", flowReceiverReferenceId),
							e);
				}

			}
		}

		// in case there isn't an active rebind
		for (UUID flowReceiverReferenceId : flowsReceiversToRebind) {
			taskService.submit(new RetryableRebindTask(flowReceiverContainer, flowReceiverReferenceId, taskService));
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
}
