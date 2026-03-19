package com.solace.spring.cloud.stream.binder.util;

public final class ClosedChannelBindingException extends RuntimeException {
	public ClosedChannelBindingException(String message) {
		super(message);
	}
}
