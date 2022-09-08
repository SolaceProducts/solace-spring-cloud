package com.solace.spring.cloud.stream.binder.util;

@FunctionalInterface
public interface InterruptableConsumer<T> {
	void accept(T t) throws InterruptedException;
}
