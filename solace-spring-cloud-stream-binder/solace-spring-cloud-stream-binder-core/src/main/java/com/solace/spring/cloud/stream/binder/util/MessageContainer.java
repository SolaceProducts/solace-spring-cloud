package com.solace.spring.cloud.stream.binder.util;

import com.solacesystems.jcsmp.BytesXMLMessage;

import java.util.StringJoiner;

public class MessageContainer {
	private final BytesXMLMessage message;
	private final long flowId;

	MessageContainer(BytesXMLMessage message, long flowId) {
		this.message = message;
		this.flowId = flowId;
	}

	public BytesXMLMessage getMessage() {
		return message;
	}

	public long getFlowId() {
		return flowId;
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", MessageContainer.class.getSimpleName() + "[", "]")
				.add("message=" + message)
				.add("flowId=" + flowId)
				.toString();
	}
}
