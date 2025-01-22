package com.solace.spring.cloud.stream.binder.util;

public enum SmfMessageHeaderWriteCompatibility {
	/**
	 * Only headers which are natively supported by SMF are allowed to be written.
	 * Unsupported types will throw an exception.
	 */
	NATIVE_ONLY,

	/**
	 * Non-native and serializable headers will be serialized to a byte array then encoded into a string with the
	 * corresponding {@link com.solace.spring.cloud.stream.binder.messaging.SolaceBinderHeaders#SERIALIZED_HEADERS} and
	 * {@link com.solace.spring.cloud.stream.binder.messaging.SolaceBinderHeaders#SERIALIZED_HEADERS_ENCODING} headers
	 * set accordingly.
	 */
	SERIALIZE_AND_ENCODE_NON_NATIVE_TYPES
}
