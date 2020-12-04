package com.solace.spring.cloud.stream.binder.util;

public class SolaceAcknowledgmentException extends RuntimeException {
	public SolaceAcknowledgmentException(String message, Throwable cause) {
		super(message, cause);
	}
}
