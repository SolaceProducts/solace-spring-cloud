package com.solace.spring.cloud.stream.binder.test.util;

import java.util.function.Supplier;

@FunctionalInterface
public interface ThrowingSupplier<T> extends Supplier<T> {

	@Override
	default T get() {
		try {
			return getThrows();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	T getThrows() throws Exception;
}
