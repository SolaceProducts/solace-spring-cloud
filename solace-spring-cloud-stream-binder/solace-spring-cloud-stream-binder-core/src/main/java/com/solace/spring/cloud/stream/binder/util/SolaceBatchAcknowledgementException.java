package com.solace.spring.cloud.stream.binder.util;

import java.util.Set;

public class SolaceBatchAcknowledgementException extends SolaceAcknowledgmentException {
	private final Set<Integer> failedMessageIndexes;
	private final boolean allStaleExceptions;

	public SolaceBatchAcknowledgementException(Set<Integer> failedMessageIndexes, boolean allStaleExceptions,
											   String message, Throwable cause) {
		super(message, cause);
		this.failedMessageIndexes = failedMessageIndexes;
		this.allStaleExceptions = allStaleExceptions;
	}

	public Set<Integer> getFailedMessageIndexes() {
		return failedMessageIndexes;
	}

	public boolean isAllStaleExceptions() {
		return allStaleExceptions;
	}
}
