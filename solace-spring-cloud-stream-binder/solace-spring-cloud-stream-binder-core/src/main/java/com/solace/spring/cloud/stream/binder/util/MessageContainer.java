package com.solace.spring.cloud.stream.binder.util;

import com.solacesystems.jcsmp.BytesXMLMessage;

import java.util.StringJoiner;
import java.util.UUID;

public class MessageContainer {
	private final UUID id = UUID.randomUUID();
	private final BytesXMLMessage message;
	private final UUID flowReceiverReferenceId;
	private boolean acknowledged;

	MessageContainer(BytesXMLMessage message, UUID flowReceiverReferenceId) {
		this.message = message;
		this.flowReceiverReferenceId = flowReceiverReferenceId;
	}

	public UUID getId() {
		return id;
	}

	public BytesXMLMessage getMessage() {
		return message;
	}

	public UUID getFlowReceiverReferenceId() {
		return flowReceiverReferenceId;
	}

	public boolean isAcknowledged() {
		return acknowledged;
	}

	void setAcknowledged(boolean acknowledged) {
		this.acknowledged = acknowledged;
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", MessageContainer.class.getSimpleName() + "[", "]")
				.add("id=" + id)
				.add("message=" + message)
				.add("flowReceiverReferenceId=" + flowReceiverReferenceId)
				.add("acknowledged=" + acknowledged)
				.toString();
	}
}
