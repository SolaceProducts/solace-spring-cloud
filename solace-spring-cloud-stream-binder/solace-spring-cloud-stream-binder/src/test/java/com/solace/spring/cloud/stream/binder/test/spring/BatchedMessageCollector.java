package com.solace.spring.cloud.stream.binder.test.spring;

import com.solace.spring.cloud.stream.binder.messaging.SolaceBinderHeaders;
import org.springframework.integration.support.MessageBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

public class BatchedMessageCollector<T, V> implements Collector<T, MessageBuilder<List<V>>, MessageBuilder<List<V>>> {
	private final Function<T, V> getPayload;
	private final Function<T, Map<String, Object>> getHeaders;

	public BatchedMessageCollector(Function<T, V> getPayload, Function<T, Map<String, Object>> getHeaders) {
		this.getPayload = getPayload;
		this.getHeaders = getHeaders;
	}


	@Override
	public Supplier<MessageBuilder<List<V>>> supplier() {
		return () -> MessageBuilder.<List<V>>withPayload(new ArrayList<>())
				.setHeader(SolaceBinderHeaders.BATCHED_HEADERS, new ArrayList<Map<String, Object>>());
	}

	@Override
	public BiConsumer<MessageBuilder<List<V>>, T> accumulator() {
		return (builder, elem) -> {
			builder.getPayload().add(getPayload.apply(elem));
			@SuppressWarnings("unchecked")
			List<Map<String, Object>> batchedHeaders = (List<Map<String, Object>>) builder.getHeaders()
					.get(SolaceBinderHeaders.BATCHED_HEADERS);
			batchedHeaders.add(getHeaders.apply(elem));
		};
	}

	@Override
	public BinaryOperator<MessageBuilder<List<V>>> combiner() {
		return (left, right) -> {
			@SuppressWarnings("unchecked")
			List<Map<String, Object>> leftBatchedHeaders = (List<Map<String, Object>>) left.getHeaders()
					.get(SolaceBinderHeaders.BATCHED_HEADERS);
			@SuppressWarnings("unchecked")
			List<Map<String, Object>> rightBatchedHeaders = (List<Map<String, Object>>) right.getHeaders()
					.get(SolaceBinderHeaders.BATCHED_HEADERS);
			leftBatchedHeaders.addAll(rightBatchedHeaders);
			left.getPayload().addAll(right.getPayload());
			return left;
		};
	}

	@Override
	public Function<MessageBuilder<List<V>>, MessageBuilder<List<V>>> finisher() {
		return b -> b;
	}

	@Override
	public Set<Characteristics> characteristics() {
		return Set.of();
	}
}
