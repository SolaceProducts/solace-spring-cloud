package com.solace.spring.cloud.stream.binder.test.junit.param.provider;

import com.solace.spring.cloud.stream.binder.messaging.HeaderMeta;
import com.solace.spring.cloud.stream.binder.messaging.SolaceBinderHeaderMeta;
import com.solace.spring.cloud.stream.binder.messaging.SolaceBinderHeaders;
import com.solace.spring.cloud.stream.binder.messaging.SolaceHeaderMeta;
import com.solace.spring.cloud.stream.binder.messaging.SolaceHeaders;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class SolaceSpringHeaderArgumentsProvider implements ArgumentsProvider {
	private static final Map<Class<?>, Map<String, ? extends HeaderMeta<?>>> ARGS;

	static {
		ARGS = new HashMap<>();
		ARGS.put(SolaceHeaders.class, SolaceHeaderMeta.META);
		ARGS.put(SolaceBinderHeaders.class, SolaceBinderHeaderMeta.META);
	}

	@Override
	public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
		return ARGS.entrySet().stream().map(e -> Arguments.of(e.getKey(), e.getValue()));
	}

	public static class ClassesOnly implements ArgumentsProvider {
		@Override
		public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
			return ARGS.keySet().stream().map(Arguments::of);
		}
	}

	public static class MetaOnly implements ArgumentsProvider {
		@Override
		public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
			return ARGS.values().stream().map(Arguments::of);
		}
	}
}
