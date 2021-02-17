package com.solace.spring.cloud.stream.binder.util;

import com.solacesystems.jcsmp.JCSMPException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Objects;
import java.util.StringJoiner;

public class RetryableRebindTask implements RetryableTaskService.RetryableTask {
	private final FlowReceiverContainer flowReceiverContainer;
	private final MessageContainer messageContainer;
	private final RetryableTaskService taskService;

	private static final Log logger = LogFactory.getLog(RetryableRebindTask.class);

	public RetryableRebindTask(FlowReceiverContainer flowReceiverContainer, MessageContainer messageContainer,
							   RetryableTaskService taskService) {
		this.flowReceiverContainer = flowReceiverContainer;
		this.messageContainer = messageContainer;
		this.taskService = taskService;
	}

	@Override
	public boolean run(int attempt) throws InterruptedException {
		try {
			flowReceiverContainer.acknowledgeRebind(messageContainer);
			return true;
		} catch (JCSMPException | UnboundFlowReceiverContainerException e) {
			if (messageContainer.isStale() && !flowReceiverContainer.isBound()) {
				logger.warn(String.format(
						"failed to rebind queue %s and flow container %s is now unbound. Attempting to bind.",
						flowReceiverContainer.getId(), flowReceiverContainer.getQueueName()), e);
				taskService.submit(new RetryableBindTask(flowReceiverContainer));
				return true;
			} else {
				logger.warn(String.format("failed to rebind flow container %s queue %s. Will retry",
						flowReceiverContainer.getId(), flowReceiverContainer.getQueueName()), e);
				return false;
			}
		} catch (SolaceStaleMessageException e) {
			logger.info(String.format("Message container %s (XMLMessage %s) is stale and was already redelivered",
					messageContainer.getId(), messageContainer.getMessage().getMessageId()), e);
			return true;
		}
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", RetryableRebindTask.class.getSimpleName() + "[", "]")
				.add("flowReceiverContainer=" + flowReceiverContainer.getId())
				.add("messageContainer=" + messageContainer.getId())
				.toString();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		RetryableRebindTask that = (RetryableRebindTask) o;
		return Objects.equals(flowReceiverContainer, that.flowReceiverContainer) &&
				Objects.equals(messageContainer, that.messageContainer) &&
				Objects.equals(taskService, that.taskService);
	}

	@Override
	public int hashCode() {
		return Objects.hash(flowReceiverContainer, messageContainer, taskService);
	}
}
