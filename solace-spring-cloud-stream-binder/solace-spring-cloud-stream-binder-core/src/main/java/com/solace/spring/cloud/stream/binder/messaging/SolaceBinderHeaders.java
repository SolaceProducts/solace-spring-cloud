package com.solace.spring.cloud.stream.binder.messaging;

import com.solacesystems.jcsmp.XMLMessage;
import org.springframework.messaging.Message;

/**
 * <p>Solace-defined Spring headers to get/set Solace Spring Cloud Stream Binder properties
 * from/to Spring {@link Message Message} headers.</p>
 * <br>
 * <p>These can be used for:</p>
 * <ul>
 *     <li>Getting/Setting Solace Binder metadata</li>
 *     <li>Directive actions for the binder when producing/consuming messages</li>
 * </ul>
 * <br>
 * <p><b>Header Access Control:</b></p>
 * <p>Be aware that each header has an expected usage scenario.
 * Using headers outside of their intended access-control scenario is not supported.</p>
 */
public final class SolaceBinderHeaders {
	/**
	 * The prefix used for all headers in this class.
	 */
	static final String PREFIX = SolaceHeaders.PREFIX + "scst_";

	/**
	 * <p><b>Acceptable Value Type:</b> {@link Integer}</p>
	 * <p><b>Access:</b> Read</p>
	 * <p><b>Default Value: </b>{@code 1}</p>
	 * <br>
	 * <p>A static number set by the publisher to indicate the Spring Cloud Stream Solace message version.</p>
	 */
	public static final String MESSAGE_VERSION = PREFIX + "messageVersion";

	/**
	 * <p><b>Acceptable Value Type:</b> {@link XMLMessage}</p>
	 * <p><b>Access:</b> Read</p>
	 * <br>
	 * <p>The raw Solace message.</p>
	 */
	public static final String RAW_MESSAGE = PREFIX + "rawMessage";

	/**
	 * <p><b>Acceptable Value Type:</b> {@link Boolean}</p>
	 * <p><b>Access:</b> Internal Binder Use Only</p>
	 * <br>
	 * <p>Is {@code true} if a Solace Spring Cloud Stream binder has serialized the payload before publishing
	 * it to a broker. Is undefined otherwise.</p>
	 */
	public static final String SERIALIZED_PAYLOAD = PREFIX + "serializedPayload";

	/**
	 * <p><b>Acceptable Value Type:</b> {@link String}</p>
	 * <p><b>Access:</b> Internal Binder Use Only</p>
	 * <br>
	 * <p>A JSON String array of header names where each entry indicates that that header’s value was serialized by a
	 * Solace Spring Cloud Stream binder before publishing it to a broker.</p>
	 */
	public static final String SERIALIZED_HEADERS = PREFIX + "serializedHeaders";

	/**
	 * <p><b>Acceptable Value Type:</b> {@link String}</p>
	 * <p><b>Access:</b> Internal Binder Use Only</p>
	 * <p><b>Default Value: </b>{@code "base64"}</p>
	 * <br>
	 * <p>The encoding algorithm used to encode the headers indicated by {@link #SERIALIZED_HEADERS}.</p>
	 */
	public static final String SERIALIZED_HEADERS_ENCODING = PREFIX + "serializedHeadersEncoding";
}
