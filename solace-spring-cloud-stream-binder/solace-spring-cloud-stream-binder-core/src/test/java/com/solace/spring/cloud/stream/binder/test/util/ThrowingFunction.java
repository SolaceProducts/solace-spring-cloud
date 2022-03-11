package com.solace.spring.cloud.stream.binder.test.util;


import java.util.function.Function;

@FunctionalInterface
public interface ThrowingFunction<T, R> extends Function<T, R> {

	@Override
	default R apply(T t) {
		try {
			return applyThrows(t);
		} catch (Throwable e) {
			throw new RuntimeException(e);
		}
	}

	R applyThrows(T t) throws Throwable;
}
