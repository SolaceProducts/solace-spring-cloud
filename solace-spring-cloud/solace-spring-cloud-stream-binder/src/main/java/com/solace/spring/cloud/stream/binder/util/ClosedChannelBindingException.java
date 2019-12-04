package com.solace.spring.cloud.stream.binder.util;

public class ClosedChannelBindingException extends RuntimeException {
	public ClosedChannelBindingException(String message) {
		super(message);
	}
}
