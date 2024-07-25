package com.solace.spring.cloud.stream.binder.test.spring;

import com.solace.spring.cloud.stream.binder.util.BatchWaitStrategy;
import org.springframework.lang.Nullable;

public enum MessageLayout {
	SINGLE(false),
	BATCHED_TIMEOUT(true, BatchWaitStrategy.RESPECT_TIMEOUT),
	BATCHED_IMMEDIATE(true, BatchWaitStrategy.IMMEDIATE);

	private final boolean isBatched;
	@Nullable private final BatchWaitStrategy waitStrategy;

	MessageLayout(boolean isBatched) {
		this(isBatched, null);
	}

	MessageLayout(boolean isBatched, @Nullable BatchWaitStrategy waitStrategy) {
		this.isBatched = isBatched;
		this.waitStrategy = waitStrategy;
	}

	public boolean isBatched() {
		return isBatched;
	}

	public BatchWaitStrategy getWaitStrategy() {
		if (waitStrategy == null || !isBatched) {
			throw new UnsupportedOperationException("Message layout is not batched");
		}
		return waitStrategy;
	}
}
