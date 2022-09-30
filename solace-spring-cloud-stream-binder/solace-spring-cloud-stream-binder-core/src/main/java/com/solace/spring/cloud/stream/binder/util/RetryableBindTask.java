package com.solace.spring.cloud.stream.binder.util;

import com.solacesystems.jcsmp.JCSMPException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.locks.Lock;

public class RetryableBindTask implements RetryableTaskService.RetryableTask {
	private final FlowReceiverContainer flowReceiverContainer;

	private static final Log logger = LogFactory.getLog(RetryableBindTask.class);

	public RetryableBindTask(FlowReceiverContainer flowReceiverContainer) {
		this.flowReceiverContainer = flowReceiverContainer;
	}

	@Override
	public boolean run(int attempt) {
		try {
			flowReceiverContainer.bind();
			return true;
		} catch (JCSMPException e) {
			logger.warn(String.format("failed to bind queue %s. Will retry", flowReceiverContainer.getQueueName()), e);
			return false;
		}
	}

	@Override
	public Lock getBlockLock() {
		return flowReceiverContainer.getRebindBlockLock();
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", RetryableBindTask.class.getSimpleName() + "[", "]")
				.add("flowReceiverContainer=" + flowReceiverContainer.getId())
				.toString();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		RetryableBindTask that = (RetryableBindTask) o;
		return Objects.equals(flowReceiverContainer.getId(), that.flowReceiverContainer.getId());
	}

	@Override
	public int hashCode() {
		return Objects.hash(flowReceiverContainer);
	}
}
