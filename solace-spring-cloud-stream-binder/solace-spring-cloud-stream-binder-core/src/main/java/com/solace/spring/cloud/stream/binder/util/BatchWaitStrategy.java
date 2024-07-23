package com.solace.spring.cloud.stream.binder.util;

public enum BatchWaitStrategy {
	/**
	 * Adheres to the {@code batchTimeout} consumer config option.
	 */
	RESPECT_TIMEOUT,
	/**
	 * Immediately collects the batch once no more messages are available on the endpoint.
	 */
	IMMEDIATE
}
