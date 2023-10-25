package com.solace.spring.cloud.stream.binder.messaging;

import com.solace.spring.cloud.stream.binder.util.CorrelationData;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SolaceBinderHeaderMeta<T> implements HeaderMeta<T> {
	public static final Map<String, SolaceBinderHeaderMeta<?>> META = Stream.of(new Object[][] {
			{SolaceBinderHeaders.PARTITION_KEY, new SolaceBinderHeaderMeta<>(String.class, false, true, Scope.WIRE)},
			{SolaceBinderHeaders.MESSAGE_VERSION, new SolaceBinderHeaderMeta<>(Integer.class, true, false, Scope.WIRE)},
			{SolaceBinderHeaders.SERIALIZED_PAYLOAD, new SolaceBinderHeaderMeta<>(Boolean.class, false, false, Scope.WIRE)},
			{SolaceBinderHeaders.SERIALIZED_HEADERS, new SolaceBinderHeaderMeta<>(String.class, false, false, Scope.WIRE)},
			{SolaceBinderHeaders.SERIALIZED_HEADERS_ENCODING, new SolaceBinderHeaderMeta<>(String.class, false, false, Scope.WIRE)},
			{SolaceBinderHeaders.CONFIRM_CORRELATION, new SolaceBinderHeaderMeta<>(CorrelationData.class, false, false, Scope.LOCAL)},
			{SolaceBinderHeaders.NULL_PAYLOAD, new SolaceBinderHeaderMeta<>(Boolean.class, true, false, Scope.LOCAL)},
			{SolaceBinderHeaders.BATCHED_HEADERS, new SolaceBinderHeaderMeta<>(List.class, true, false, Scope.LOCAL)},
			{SolaceBinderHeaders.TARGET_DESTINATION_TYPE, new SolaceBinderHeaderMeta<>(String.class, false, false, Scope.LOCAL)}
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

	/**
	 * The readable property is only used by tests and doesn't necessarily reflect whether a header can be read by an application or not
	 */
	@Override
	public boolean isReadable() {
		return readable;
	}

	/**
	 * The writable property is only used by tests and doesn't necessarily reflect whether a header can be written by an application or not
	 */
	@Override
	public boolean isWritable() {
		return writable;
	}

	@Override
	public Scope getScope() {
		return scope;
	}
}
