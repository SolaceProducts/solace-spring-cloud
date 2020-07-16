package com.solace.spring.cloud.stream.binder.test.util;

import java.util.function.Function;

@FunctionalInterface
public interface ThrowingFunction<T,R> extends Function<T,R> {

	@Override
	default R apply(T t) {
		try {
			return getThrows(t);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	R getThrows(T t) throws Exception;
}
