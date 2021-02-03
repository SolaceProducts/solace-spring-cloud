package com.solace.spring.cloud.stream.binder.util;

public class SolaceStaleMessageException extends Exception {
	public SolaceStaleMessageException(String message) {
		super(message);
	}
}
