package com.solace.spring.cloud.stream.binder.messaging;

import com.solacesystems.jcsmp.SDTStream;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SolaceBinderHeaderMeta<T> implements HeaderMeta<T> {
	public static final Map<String, SolaceBinderHeaderMeta<?>> META = Stream.of(new Object[][] {
			{SolaceBinderHeaders.MESSAGE_VERSION, new SolaceBinderHeaderMeta<>(Integer.class, true, false)},
			{SolaceBinderHeaders.SERIALIZED_PAYLOAD, new SolaceBinderHeaderMeta<>(Boolean.class, false, false)},
			{SolaceBinderHeaders.SERIALIZED_HEADERS, new SolaceBinderHeaderMeta<>(SDTStream.class, false, false)}
	}).collect(Collectors.toMap(d -> (String) d[0], d -> (SolaceBinderHeaderMeta<?>) d[1]));

	private final Class<T> type;
	private final boolean readable;
	private final boolean writable;

	public SolaceBinderHeaderMeta(Class<T> type, boolean readable, boolean writable) {
		this.type = type;
		this.readable = readable;
		this.writable = writable;
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
}
