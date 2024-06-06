package com.solace.spring.cloud.stream.binder.util;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class BatchProxyCorrelationKeyTest {
	@Test
	public void testSuccess() {
		Object target = new Object();
		BatchProxyCorrelationKey correlationKey = new BatchProxyCorrelationKey(target, 3);
		assertThat(correlationKey.getCorrelationKeyForSuccess()).isNull();
		assertThat(correlationKey.getCorrelationKeyForSuccess()).isNull();
		assertThat(correlationKey.getCorrelationKeyForSuccess()).isEqualTo(target);

		// ensure subsequent calls always returns null
		assertThat(correlationKey.getCorrelationKeyForSuccess()).isNull();
		assertThat(correlationKey.getCorrelationKeyForFailure()).isNull();
	}

	@Test
	public void testFailed() {
		Object target = new Object();
		BatchProxyCorrelationKey correlationKey = new BatchProxyCorrelationKey(target, 3);
		assertThat(correlationKey.getCorrelationKeyForSuccess()).isNull();
		assertThat(correlationKey.getCorrelationKeyForFailure()).isEqualTo(target);

		// ensure subsequent calls always returns null
		assertThat(correlationKey.getCorrelationKeyForSuccess()).isNull();
		assertThat(correlationKey.getCorrelationKeyForSuccess()).isNull();
		assertThat(correlationKey.getCorrelationKeyForSuccess()).isNull();
		assertThat(correlationKey.getCorrelationKeyForFailure()).isNull();
	}
}
