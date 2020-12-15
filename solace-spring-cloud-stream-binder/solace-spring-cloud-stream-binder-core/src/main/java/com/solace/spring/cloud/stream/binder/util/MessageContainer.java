package com.solace.spring.cloud.stream.binder.util;

import com.solacesystems.jcsmp.BytesXMLMessage;

import java.util.StringJoiner;
import java.util.UUID;

public class MessageContainer {
	private final UUID id = UUID.randomUUID();
	private final BytesXMLMessage message;
	private final long flowId;
	private boolean acknowledged;

	MessageContainer(BytesXMLMessage message, long flowId) {
		this.message = message;
		this.flowId = flowId;
	}

	public UUID getId() {
		return id;
	}

	public BytesXMLMessage getMessage() {
		return message;
	}

	public long getFlowId() {
		return flowId;
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
				.add("flowId=" + flowId)
				.add("acknowledged=" + acknowledged)
				.toString();
	}
}
