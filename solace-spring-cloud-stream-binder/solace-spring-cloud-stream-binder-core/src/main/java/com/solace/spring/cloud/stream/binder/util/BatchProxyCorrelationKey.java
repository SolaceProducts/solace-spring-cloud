package com.solace.spring.cloud.stream.binder.util;

import java.util.concurrent.atomic.AtomicInteger;

public class BatchProxyCorrelationKey {
	private final Object targetCorrelationKey;
	private final AtomicInteger numRemaining;
	private static final int RETURNED_KEY = -1;

	public BatchProxyCorrelationKey(Object targetCorrelationKey, int numRemaining) {
		this.targetCorrelationKey = targetCorrelationKey;
		this.numRemaining = new AtomicInteger(numRemaining);
	}

	/**
	 * Retrieve the target correlation key after all successes have been received.
	 * @return the target correlation key if after being invoked the required number of times and if it hasn't been
	 * returned before. {@code null} otherwise.
	 */
	public Object getCorrelationKeyForSuccess() {
		if (numRemaining.updateAndGet(i -> i > RETURNED_KEY ? i - 1 : i) == 0) {
			return targetCorrelationKey;
		} else {
			return null;
		}
	}

	/**
	 * Returns the real correlation key in event of failure.
	 * @return the target correlation key if it hasn't been returned before. {@code null} otherwise.
	 */
	public Object getCorrelationKeyForFailure() {
		if (numRemaining.getAndSet(RETURNED_KEY) != RETURNED_KEY) {
			return targetCorrelationKey;
		} else {
			return null;
		}
	}
}
