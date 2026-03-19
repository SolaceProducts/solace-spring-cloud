package com.solace.spring.cloud.stream.binder.util;

public sealed class SolaceAcknowledgmentException extends RuntimeException permits SolaceBatchAcknowledgementException {
	public SolaceAcknowledgmentException(String message, Throwable cause) {
		super(message, cause);
	}
}
