package com.solace.spring.cloud.stream.binder.util;

public enum SmfMessagePayloadWriteCompatibility {
	/**
	 * Only payloads which are natively supported by SMF are allowed to be written.
	 * Unsupported types will throw an exception.
	 */
	NATIVE_ONLY,

	/**
	 * Non-native and serializable payloads will be serialized into a byte array with the corresponding
	 * {@link com.solace.spring.cloud.stream.binder.messaging.SolaceBinderHeaders#SERIALIZED_PAYLOAD} header set
	 * accordingly.
	 * Native payloads will be written as usual.
	 */
	SERIALIZE_NON_NATIVE_TYPES
}
