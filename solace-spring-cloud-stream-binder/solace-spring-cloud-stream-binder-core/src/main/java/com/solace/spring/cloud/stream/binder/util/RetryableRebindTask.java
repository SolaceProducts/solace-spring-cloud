package com.solace.spring.cloud.stream.binder.util;

import com.solacesystems.jcsmp.JCSMPException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Objects;
import java.util.StringJoiner;
import java.util.UUID;

public class RetryableRebindTask implements RetryableTaskService.RetryableTask {
	private final FlowReceiverContainer flowReceiverContainer;
	private final UUID flowReceiverContainerId;
	private final RetryableTaskService taskService;

	private static final Log logger = LogFactory.getLog(RetryableRebindTask.class);

	public RetryableRebindTask(FlowReceiverContainer flowReceiverContainer, UUID flowReceiverContainerId, RetryableTaskService taskService) {
		this.flowReceiverContainer = flowReceiverContainer;
		this.flowReceiverContainerId = flowReceiverContainerId;
		this.taskService = taskService;
	}

	@Override
	public boolean run(int attempt) throws InterruptedException {
		try {
			return flowReceiverContainer.rebind(flowReceiverContainerId, true) != null;
		} catch (JCSMPException | UnboundFlowReceiverContainerException e) {
			if (!flowReceiverContainer.isBound()) {
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
		}
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", RetryableRebindTask.class.getSimpleName() + "[", "]")
				.add("flowReceiverContainer=" + flowReceiverContainer)
				.add("flowReceiverContainerId=" + flowReceiverContainerId)
				.add("taskService=" + taskService)
				.toString();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		RetryableRebindTask that = (RetryableRebindTask) o;
		return Objects.equals(flowReceiverContainer, that.flowReceiverContainer) &&
				Objects.equals(flowReceiverContainerId, that.flowReceiverContainerId) &&
				Objects.equals(taskService, that.taskService);
	}

	@Override
	public int hashCode() {
		return Objects.hash(flowReceiverContainer, flowReceiverContainerId, taskService);
	}
}
