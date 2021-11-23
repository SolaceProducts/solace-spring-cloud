package com.solace.spring.cloud.stream.binder.messaging;

import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.ReplicationGroupMessageId;
import org.springframework.messaging.Message;

/**
 * <p>Solace-defined Spring headers to get/set Solace message properties from/to
 * Spring {@link Message Message} headers.</p>
 * <br>
 * <p><b>Header Access Control:</b></p>
 * <p>Be aware that each header has an expected usage scenario.
 * Using headers outside of their intended access-control scenario is not supported.</p>
 */
public final class SolaceHeaders {
	/**
	 * The prefix used for all headers in this class.
	 */
	static final String PREFIX = "solace_";

	/**
	 * <p><b>Acceptable Value Type:</b> {@link String}</p>
	 * <p><b>Access:</b> Read/Write</p>
	 * <br>
	 * <p>The message ID (a string for an application-specific message identifier).</p>
	 * <p>This is the {@code JMSMessageID} header field if publishing/consuming to/from JMS.</p>
	 */
	public static final String APPLICATION_MESSAGE_ID = PREFIX + "applicationMessageId";

	/**
	 * <p><b>Acceptable Value Type:</b> {@link String}</p>
	 * <p><b>Access:</b> Read/Write</p>
	 * <br>
	 * <p>The application message type.</p>
	 * <p>This is the {@code JMSType} header field if publishing/consuming to/from JMS.</p>
	 */
	public static final String APPLICATION_MESSAGE_TYPE = PREFIX + "applicationMessageType";

	/**
	 * <p><b>Acceptable Value Type:</b> {@link String}</p>
	 * <p><b>Access:</b> Read/Write</p>
	 * <br>
	 * <p>The correlation ID.</p>
	 */
	public static final String CORRELATION_ID = PREFIX + "correlationId";

	/**
	 * <p><b>Acceptable Value Type:</b> {@link Destination}</p>
	 * <p><b>Access:</b> Read</p>
	 * <br>
	 * <p>The destination this message was published to.</p>
	 */
	public static final String DESTINATION = PREFIX + "destination";

	/**
	 * <p><b>Acceptable Value Type:</b> {@link Boolean}</p>
	 * <p><b>Access:</b> Read</p>
	 * <br>
	 * <p>Whether one or more messages have been discarded prior to the current message.</p>
	 */
	public static final String DISCARD_INDICATION = PREFIX + "discardIndication";

	/**
	 * <p><b>Acceptable Value Type:</b> {@link Boolean}</p>
	 * <p><b>Access:</b> Read/Write</p>
	 * <br>
	 * <p>Whether the message is eligible to be moved to a Dead Message Queue.</p>
	 */
	public static final String DMQ_ELIGIBLE = PREFIX + "dmqEligible";

	/**
	 * <p><b>Acceptable Value Type:</b> {@link Long}</p>
	 * <p><b>Access:</b> Read/Write</p>
	 * <br>
	 * <p>The UTC time (in milliseconds, from midnight, January 1, 1970 UTC) when the message is supposed to
	 * expire.</p>
	 */
	public static final String EXPIRATION = PREFIX + "expiration";

	/**
	 * <p><b>Acceptable Value Type:</b> {@link String}</p>
	 * <p><b>Access:</b> Read/Write</p>
	 * <br>
	 * <p>The HTTP content encoding header value from interaction with an HTTP client.</p>
	 */
	public static final String HTTP_CONTENT_ENCODING = PREFIX + "httpContentEncoding";

	/**
	 * <p><b>Acceptable Value Type:</b> {@link Boolean}</p>
	 * <p><b>Access:</b> Read/Write</p>
	 * <br>
	 * <p>Indicates that this message is a reply</p>
	 */
	public static final String IS_REPLY = PREFIX + "isReply";

	/**
	 * <p><b>Acceptable Value Type:</b> {@link Integer}</p>
	 * <p><b>Access:</b> Read/Write</p>
	 * <br>
	 * <p>Priority value in the range of 0–255, or -1 if it is not set.</p>
	 */
	public static final String PRIORITY = PREFIX + "priority";

	/**
	 * <p><b>Acceptable Value Type:</b> {@link ReplicationGroupMessageId}</p>
	 * <p><b>Access:</b> Read</p>
	 * <br>
	 * <p>The replication group message ID (Specifies a Replication Group Message ID as a replay start location.).</p>
	 */

	public static final String REPLICATION_GROUP_MESSAGE_ID = PREFIX + "replicationGroupMessageId";

	/**
	 * <p><b>Acceptable Value Type:</b> {@link Long}</p>
	 * <p><b>Access:</b> Read</p>
	 * <br>
	 * <p>The receive timestamp (in milliseconds, from midnight, January 1, 1970 UTC).</p>
	 */
	public static final String RECEIVE_TIMESTAMP = PREFIX + "receiveTimestamp";

	/**
	 * <p><b>Acceptable Value Type:</b> {@link Boolean}</p>
	 * <p><b>Access:</b> Read</p>
	 * <br>
	 * <p>Indicates if the message has been delivered by the broker to the API before.</p>
	 */
	public static final String REDELIVERED = PREFIX + "redelivered";

	/**
	 * <p><b>Acceptable Value Type:</b> {@link Destination}</p>
	 * <p><b>Access:</b> Read/Write</p>
	 * <br>
	 * <p>The replyTo destination for the message.</p>
	 */
	public static final String REPLY_TO = PREFIX + "replyTo";

	/**
	 * <p><b>Acceptable Value Type:</b> {@link String}</p>
	 * <p><b>Access:</b> Read/Write</p>
	 * <br>
	 * <p>The Sender ID for the message.</p>
	 */
	public static final String SENDER_ID = PREFIX + "senderId";

	/**
	 * <p><b>Acceptable Value Type:</b> {@link Long}</p>
	 * <p><b>Access:</b> Read/Write</p>
	 * <br>
	 * <p>The send timestamp (in milliseconds, from midnight, January 1, 1970 UTC).</p>
	 */
	public static final String SENDER_TIMESTAMP = PREFIX + "senderTimestamp";

	/**
	 * <p><b>Acceptable Value Type:</b> {@link Long}</p>
	 * <p><b>Access:</b> Read/Write</p>
	 * <br>
	 * <p>The sequence number.</p>
	 */
	public static final String SEQUENCE_NUMBER = PREFIX + "sequenceNumber";

	/**
	 * <p><b>Acceptable Value Type:</b> {@link Long}</p>
	 * <p><b>Access:</b> Read/Write</p>
	 * <br>
	 * <p>The number of milliseconds before the message is discarded or moved to a Dead Message Queue.</p>
	 */
	public static final String TIME_TO_LIVE = PREFIX + "timeToLive";

	/**
	 * <p><b>Acceptable Value Type:</b> {@code byte[]}</p>
	 * <p><b>Access:</b> Read/Write</p>
	 * <br>
	 * <p>When an application sends a message, it can optionally attach application-specific data along
	 * with the message, such as user data.</p>
	 */
	public static final String USER_DATA = PREFIX + "userData";
}
