package com.solace.spring.cloud.stream.binder.inbound.acknowledge;

import com.solace.spring.cloud.stream.binder.util.ErrorQueueInfrastructure;
import com.solace.spring.cloud.stream.binder.util.FlowReceiverContainer;
import com.solace.spring.cloud.stream.binder.util.MessageContainer;
import java.util.List;
import org.springframework.integration.acks.AcknowledgmentCallback;

public class JCSMPAcknowledgementCallbackFactory {
	private final FlowReceiverContainer flowReceiverContainer;
	private ErrorQueueInfrastructure errorQueueInfrastructure;

	public JCSMPAcknowledgementCallbackFactory(FlowReceiverContainer flowReceiverContainer) {
		this.flowReceiverContainer = flowReceiverContainer;
	}

	public void setErrorQueueInfrastructure(ErrorQueueInfrastructure errorQueueInfrastructure) {
		this.errorQueueInfrastructure = errorQueueInfrastructure;
	}

	public AcknowledgmentCallback createCallback(MessageContainer messageContainer) {
		return createJCSMPCallback(messageContainer);
	}

	public AcknowledgmentCallback createBatchCallback(List<MessageContainer> messageContainers) {
		return new JCSMPBatchAcknowledgementCallback(messageContainers.stream()
				.map(this::createJCSMPCallback).toList());
	}

	private JCSMPAcknowledgementCallback createJCSMPCallback(MessageContainer messageContainer) {
		return new JCSMPAcknowledgementCallback(messageContainer, flowReceiverContainer,
				errorQueueInfrastructure);
	}

}
