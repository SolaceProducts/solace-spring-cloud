package com.solace.spring.cloud.stream.binder.messaging;

import com.solacesystems.jcsmp.XMLMessage;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SolaceBinderHeaderMeta<T> implements HeaderMeta<T> {
	public static final Map<String, SolaceBinderHeaderMeta<?>> META = Stream.of(new Object[][] {
			{SolaceBinderHeaders.MESSAGE_VERSION, new SolaceBinderHeaderMeta<>(Integer.class, true, false, Scope.WIRE)},
			{SolaceBinderHeaders.RAW_MESSAGE, new SolaceBinderHeaderMeta<>(XMLMessage.class, true, false, Scope.LOCAL)},
			{SolaceBinderHeaders.SERIALIZED_PAYLOAD, new SolaceBinderHeaderMeta<>(Boolean.class, false, false, Scope.WIRE)},
			{SolaceBinderHeaders.SERIALIZED_HEADERS, new SolaceBinderHeaderMeta<>(String.class, false, false, Scope.WIRE)},
			{SolaceBinderHeaders.SERIALIZED_HEADERS_ENCODING, new SolaceBinderHeaderMeta<>(String.class, false, false, Scope.WIRE)}
	}).collect(Collectors.toMap(d -> (String) d[0], d -> (SolaceBinderHeaderMeta<?>) d[1]));

	private final Class<T> type;
	private final boolean readable;
	private final boolean writable;
	private final Scope scope;

	public SolaceBinderHeaderMeta(Class<T> type, boolean readable, boolean writable, Scope scope) {
		this.type = type;
		this.readable = readable;
		this.writable = writable;
		this.scope = scope;
	}

	@Override
	public Class<T> getType() {
		return type;
	}

	@Override
	public boolean isReadable() {
		return readable;
	}

	@Override
	public boolean isWritable() {
		return writable;
	}

	@Override
	public Scope getScope() {
		return scope;
	}
}
