package com.solace.spring.cloud.stream.binder.util;

import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Values;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class StaticMessageHeaderMapAccessorTest {
	@Test
	void testGet() {
		assertThat(StaticMessageHeaderMapAccessor.get(Map.ofEntries(
				Map.entry("test-key", "foobar"),
				Map.entry("other-key", new Object())
		), "test-key", String.class)).isEqualTo("foobar");
	}

	@CartesianTest(name = "[{index}] definedEntry={0}")
	void testGetMissing(@Values(booleans = {false, true}) boolean definedEntry) {
		Map<String, Object> headers = new HashMap<>();
		headers.put("other-key", new Object());
		if (definedEntry) {
			headers.put("test-key", null);
		}
		assertThat(StaticMessageHeaderMapAccessor.get(headers, "test-key", String.class)).isNull();
	}

	@Test
	void testGetThrow() {
		assertThatThrownBy(() -> StaticMessageHeaderMapAccessor.get(Map.of("test-key", new Object()),
				"test-key", String.class))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("Incorrect type specified for header 'test-key'. Expected [class java.lang.String] but actual type is [class java.lang.Object]");
	}
}
