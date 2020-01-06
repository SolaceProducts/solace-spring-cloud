package com.solace.spring.cloud.stream.binder.util;

public class SolaceMessageConversionException extends RuntimeException {
	public SolaceMessageConversionException(String message) {
		super(message);
	}

	public SolaceMessageConversionException(Throwable throwable) {
		super(throwable);
	}

	public SolaceMessageConversionException(String message, Throwable throwable) {
		super(message, throwable);
	}
}
