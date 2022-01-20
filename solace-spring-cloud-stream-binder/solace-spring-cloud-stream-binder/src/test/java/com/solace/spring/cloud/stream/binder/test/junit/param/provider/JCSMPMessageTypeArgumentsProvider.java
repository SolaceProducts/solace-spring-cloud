package com.solace.spring.cloud.stream.binder.test.junit.param.provider;

import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.MapMessage;
import com.solacesystems.jcsmp.StreamMessage;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.XMLContentMessage;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

import java.util.stream.Stream;

public class JCSMPMessageTypeArgumentsProvider implements ArgumentsProvider {
	@Override
	public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
		return Stream.of(TextMessage.class,
						BytesMessage.class,
						XMLContentMessage.class,
						MapMessage.class,
						StreamMessage.class)
				.map(Arguments::of);
	}
}
