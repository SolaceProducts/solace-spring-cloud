package com.solace.spring.cloud.stream.binder.test.spring;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.springframework.integration.support.MessageBuilder;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.IntStream;

public final class MessageGenerator {
	public static MessageBuilder<?> generateMessage(Function<Integer, ?> payloadGenerator,
													Function<Integer, Map<String, Object>> headersGenerator,
													BatchingConfig batchingConfig) {
		if (batchingConfig.isEnabled()) {
			return IntStream.range(0, batchingConfig.getNumberOfMessages())
					.mapToObj(i -> new ImmutablePair<>(payloadGenerator.apply(i), headersGenerator.apply(i)))
					.collect(new BatchedMessageCollector<>(ImmutablePair::getLeft, ImmutablePair::getRight));
		} else {
			return MessageBuilder.withPayload(payloadGenerator.apply(0))
					.copyHeaders(headersGenerator.apply(0));
		}
	}

	public static class BatchingConfig {
		private boolean enabled;
		private int numberOfMessages = 256;

		public boolean isEnabled() {
			return enabled;
		}

		public BatchingConfig setEnabled(boolean enabled) {
			this.enabled = enabled;
			return this;
		}

		public int getNumberOfMessages() {
			return numberOfMessages;
		}

		public BatchingConfig setNumberOfMessages(int numberOfMessages) {
			this.numberOfMessages = numberOfMessages;
			return this;
		}
	}
}
