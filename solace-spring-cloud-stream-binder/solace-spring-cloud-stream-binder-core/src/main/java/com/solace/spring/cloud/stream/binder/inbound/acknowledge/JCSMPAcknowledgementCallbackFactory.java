package com.solace.spring.cloud.stream.binder.inbound.acknowledge;

import com.solace.spring.cloud.stream.binder.util.ErrorQueueInfrastructure;
import com.solace.spring.cloud.stream.binder.util.FlowReceiverContainer;
import com.solace.spring.cloud.stream.binder.util.MessageContainer;
import com.solace.spring.cloud.stream.binder.util.RetryableTaskService;
import org.springframework.integration.acks.AcknowledgmentCallback;

import java.util.List;
import java.util.stream.Collectors;

public class JCSMPAcknowledgementCallbackFactory {
	private final FlowReceiverContainer flowReceiverContainer;
	private final boolean hasTemporaryQueue;
	private final RetryableTaskService taskService;
	private ErrorQueueInfrastructure errorQueueInfrastructure;

	public JCSMPAcknowledgementCallbackFactory(FlowReceiverContainer flowReceiverContainer, boolean hasTemporaryQueue,
											   RetryableTaskService taskService) {
		this.flowReceiverContainer = flowReceiverContainer;
		this.hasTemporaryQueue = hasTemporaryQueue;
		this.taskService = taskService;
	}

	public void setErrorQueueInfrastructure(ErrorQueueInfrastructure errorQueueInfrastructure) {
		this.errorQueueInfrastructure = errorQueueInfrastructure;
	}

	public AcknowledgmentCallback createCallback(MessageContainer messageContainer) {
		return createJCSMPCallback(messageContainer);
	}

	public AcknowledgmentCallback createBatchCallback(List<MessageContainer> messageContainers) {
		return new JCSMPBatchAcknowledgementCallback(messageContainers.stream()
				.map(this::createJCSMPCallback)
				.collect(Collectors.toList()), flowReceiverContainer, taskService);
	}

	private JCSMPAcknowledgementCallback createJCSMPCallback(MessageContainer messageContainer) {
		return new JCSMPAcknowledgementCallback(messageContainer, flowReceiverContainer, hasTemporaryQueue,
				taskService, errorQueueInfrastructure);
	}

}
