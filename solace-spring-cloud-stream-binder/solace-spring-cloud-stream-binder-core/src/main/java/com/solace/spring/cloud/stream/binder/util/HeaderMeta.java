package com.solace.spring.cloud.stream.binder.util;

sealed interface HeaderMeta<T> permits SolaceBinderHeaderMeta, SolaceHeaderMeta {
	/**
	 * The type of header.
	 * @return header type
	 */
	Class<T> getType();

	/**
	 * Whether applications can directly read the header's value.
	 * @return is readable
	 */
	boolean isReadable();

	/**
	 * Whether applications can directly set the header's value.
	 * @return is writable
	 */
	boolean isWritable();

	/**
	 * The scope of the header.
	 * @return scope
	 */
	Scope getScope();

	enum Scope {
		/**
		 * The header is only present on a message within the given application.
		 */
		LOCAL,
		/**
		 * The header is read/written from/to the wire message.
		 */
		WIRE
	}
}
