package com.solace.spring.cloud.stream.binder.messaging;

public interface HeaderMeta<T> {
	Class<T> getType();
	boolean isReadable();
	boolean isWritable();
	Scope getScope();

	enum Scope {
		LOCAL, WIRE
	}
}
