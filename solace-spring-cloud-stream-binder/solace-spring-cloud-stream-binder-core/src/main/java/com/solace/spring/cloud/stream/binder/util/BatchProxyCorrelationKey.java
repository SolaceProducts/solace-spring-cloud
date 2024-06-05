package com.solace.spring.cloud.stream.binder.util;

import java.util.concurrent.atomic.AtomicInteger;

public class BatchProxyCorrelationKey {
	private final Object targetCorrelationKey;
	private final AtomicInteger numRemaining;

	public BatchProxyCorrelationKey(Object targetCorrelationKey, int numRemaining) {
		this.targetCorrelationKey = targetCorrelationKey;
		this.numRemaining = new AtomicInteger(numRemaining);
	}

	/**
	 * Retrieve the target correlation key after all successes have been received.
	 * @return the target correlation key if after being invoked the required number of times. {@code null} otherwise.
	 */
	public Object getCorrelationKeyForSuccess() {
		return numRemaining.decrementAndGet() == 0 ? targetCorrelationKey : null;
	}

	/**
	 * Returns the real correlation key in event of failure.
	 * @return the target correlation key
	 */
	public Object getCorrelationKeyForFailure() {
		numRemaining.decrementAndGet();
		return targetCorrelationKey;
	}
}
