package com.solace.spring.cloud.stream.binder.properties;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SolaceConsumerPropertiesTest {
	@ParameterizedTest
	@ValueSource(ints = {1, 256})
	public void testSetBatchMaxSize(int batchMaxSize) {
		SolaceConsumerProperties consumerProperties = new SolaceConsumerProperties();
		consumerProperties.setBatchMaxSize(batchMaxSize);
		assertEquals(batchMaxSize, consumerProperties.getBatchMaxSize());
	}

	@ParameterizedTest
	@ValueSource(ints = {-1, 0})
	public void testFailSetBatchMaxSize(int batchMaxSize) {
		assertThrows(IllegalArgumentException.class, () -> new SolaceConsumerProperties()
				.setBatchMaxSize(batchMaxSize));
	}

	@ParameterizedTest
	@ValueSource(ints = {0, 1, 1000})
	public void testSetBatchTimeout(int batchTimeout) {
		SolaceConsumerProperties consumerProperties = new SolaceConsumerProperties();
		consumerProperties.setBatchTimeout(batchTimeout);
		assertEquals(batchTimeout, consumerProperties.getBatchTimeout());
	}

	@ParameterizedTest
	@ValueSource(ints = {-1})
	public void testFailSetBatchTimeout(int batchTimeout) {
		assertThrows(IllegalArgumentException.class, () -> new SolaceConsumerProperties()
				.setBatchTimeout(batchTimeout));
	}
}
