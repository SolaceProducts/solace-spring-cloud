package com.solace.spring.cloud.stream.binder.util;

import java.util.Map;

/**
 * Utility class to perform generic header accessing operations without needing to create a
 * {@link org.springframework.messaging.MessageHeaders MessageHeaders} object.
 */
public final class StaticMessageHeaderMapAccessor {
	public static <T> T get(Map<String, Object> headers, String key, Class<T> type) {
		Object value = headers.get(key);
		if (value == null) {
			return null;
		}
		if (!type.isAssignableFrom(value.getClass())) {
			throw new IllegalArgumentException(String.format(
					"Incorrect type specified for header '%s'. Expected [%s] but actual type is [%s]",
					key, type, value.getClass()));
		}

		@SuppressWarnings("unchecked")
		T toReturn = (T) value;

		return toReturn;
	}
}
