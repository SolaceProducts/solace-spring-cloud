package com.solace.spring.cloud.stream.binder.properties;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

	@ParameterizedTest
	@ValueSource(ints = {1, 1000})
	public void testSetFlowRebindBackOffInitialInterval(int time) {
		SolaceConsumerProperties consumerProperties = new SolaceConsumerProperties();
		consumerProperties.setFlowRebindBackOffInitialInterval(time);
		assertEquals(time, consumerProperties.getFlowRebindBackOffInitialInterval());
	}

	@ParameterizedTest
	@ValueSource(ints = {-1, 0})
	public void testFailSetFlowRebindBackOffInitialInterval(int time) {
		assertThrows(IllegalArgumentException.class, () -> new SolaceConsumerProperties()
				.setFlowRebindBackOffInitialInterval(time));
	}

	@ParameterizedTest
	@ValueSource(ints = {1, 1000})
	public void testSetFlowRebindBackOffMaxInterval(int time) {
		SolaceConsumerProperties consumerProperties = new SolaceConsumerProperties();
		consumerProperties.setFlowRebindBackOffMaxInterval(time);
		assertEquals(time, consumerProperties.getFlowRebindBackOffMaxInterval());
	}

	@ParameterizedTest
	@ValueSource(ints = {-1, 0})
	public void testFailSetFlowRebindBackOffMaxInterval(int time) {
		assertThrows(IllegalArgumentException.class, () -> new SolaceConsumerProperties()
				.setFlowRebindBackOffMaxInterval(time));
	}

	@ParameterizedTest
	@ValueSource(doubles = {1.0, 1.5, 1000})
	public void testSetFlowRebindBackOffMultiplier(double multiplier) {
		SolaceConsumerProperties consumerProperties = new SolaceConsumerProperties();
		consumerProperties.setFlowRebindBackOffMultiplier(multiplier);
		assertEquals(multiplier, consumerProperties.getFlowRebindBackOffMultiplier());
	}

	@ParameterizedTest
	@ValueSource(doubles = {-1000, -1.5, -1, -0.5, 0, 0.5})
	public void testFailSetFlowRebindBackOffMultiplier(double multiplier) {
		assertThrows(IllegalArgumentException.class, () -> new SolaceConsumerProperties()
				.setFlowRebindBackOffMultiplier(multiplier));
	}

	@Test
	void testDefaultHeaderExclusionsListIsEmpty() {
		assertTrue(new SolaceConsumerProperties().getHeaderExclusions().isEmpty());
	}
}
