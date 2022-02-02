package com.solace.spring.cloud.stream.binder.messaging;

import com.solace.spring.cloud.stream.binder.util.SolaceDeliveryCountException;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.ReplicationGroupMessageId;
import com.solacesystems.jcsmp.XMLMessage;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SolaceHeaderMeta<T> implements HeaderMeta<T> {
	public static final Map<String, SolaceHeaderMeta<?>> META = Stream.of(new Object[][] {
			{SolaceHeaders.APPLICATION_MESSAGE_ID, new SolaceHeaderMeta<>(String.class, XMLMessage::getApplicationMessageId, XMLMessage::setApplicationMessageId)},
			{SolaceHeaders.APPLICATION_MESSAGE_TYPE, new SolaceHeaderMeta<>(String.class, XMLMessage::getApplicationMessageType, XMLMessage::setApplicationMessageType)},
			{SolaceHeaders.CORRELATION_ID, new SolaceHeaderMeta<>(String.class, XMLMessage::getCorrelationId, XMLMessage::setCorrelationId)},
			{SolaceHeaders.DELIVERY_COUNT, new SolaceHeaderMeta<>(Integer.class, SolaceHeaderMeta::getDeliveryCount, null)},
			{SolaceHeaders.DESTINATION, new SolaceHeaderMeta<>(Destination.class, XMLMessage::getDestination, null)},
			{SolaceHeaders.DISCARD_INDICATION, new SolaceHeaderMeta<>(Boolean.class, XMLMessage::getDiscardIndication, null)},
			{SolaceHeaders.DMQ_ELIGIBLE, new SolaceHeaderMeta<>(Boolean.class, XMLMessage::isDMQEligible, XMLMessage::setDMQEligible, true)},
			{SolaceHeaders.EXPIRATION, new SolaceHeaderMeta<>(Long.class, XMLMessage::getExpiration, XMLMessage::setExpiration)},
			{SolaceHeaders.HTTP_CONTENT_ENCODING, new SolaceHeaderMeta<>(String.class, XMLMessage::getHTTPContentEncoding, XMLMessage::setHTTPContentEncoding)},
			{SolaceHeaders.IS_REPLY, new SolaceHeaderMeta<>(Boolean.class, XMLMessage::isReplyMessage, XMLMessage::setAsReplyMessage)},
			{SolaceHeaders.PRIORITY, new SolaceHeaderMeta<>(Integer.class, XMLMessage::getPriority, XMLMessage::setPriority)},
			{SolaceHeaders.RECEIVE_TIMESTAMP, new SolaceHeaderMeta<>(Long.class, XMLMessage::getReceiveTimestamp, null)},
			{SolaceHeaders.REDELIVERED, new SolaceHeaderMeta<>(Boolean.class, XMLMessage::getRedelivered, null)},
			{SolaceHeaders.REPLICATION_GROUP_MESSAGE_ID, new SolaceHeaderMeta<>(ReplicationGroupMessageId.class, XMLMessage::getReplicationGroupMessageId, null)},
			{SolaceHeaders.REPLY_TO, new SolaceHeaderMeta<>(Destination.class, XMLMessage::getReplyTo, XMLMessage::setReplyTo)},
			{SolaceHeaders.SENDER_ID, new SolaceHeaderMeta<>(String.class, XMLMessage::getSenderId, XMLMessage::setSenderId)},
			{SolaceHeaders.SENDER_TIMESTAMP, new SolaceHeaderMeta<>(Long.class, XMLMessage::getSenderTimestamp, XMLMessage::setSenderTimestamp)},
			{SolaceHeaders.SEQUENCE_NUMBER, new SolaceHeaderMeta<>(Long.class, XMLMessage::getSequenceNumber, XMLMessage::setSequenceNumber)},
			{SolaceHeaders.TIME_TO_LIVE, new SolaceHeaderMeta<>(Long.class, XMLMessage::getTimeToLive, XMLMessage::setTimeToLive)},
			{SolaceHeaders.USER_DATA, new SolaceHeaderMeta<>(byte[].class, XMLMessage::getUserData, XMLMessage::setUserData)}
	}).collect(Collectors.toMap(d -> (String) d[0], d -> (SolaceHeaderMeta<?>) d[1]));

	private final Class<T> type;
	private final Function<XMLMessage, T> readAction;
	private final BiConsumer<XMLMessage, T> writeAction;
	private final T defaultValueOverride;
	private final boolean hasDefaultValueOverride;

	private SolaceHeaderMeta(Class<T> type, Function<XMLMessage, T> readAction, BiConsumer<XMLMessage, T> writeAction,
							 T defaultValueOverride) {
		this.type = type;
		this.readAction = readAction;
		this.writeAction = writeAction;
		this.defaultValueOverride = defaultValueOverride;
		this.hasDefaultValueOverride = true;
	}

	private SolaceHeaderMeta(Class<T> type, Function<XMLMessage, T> readAction, BiConsumer<XMLMessage, T> writeAction) {
		this.type = type;
		this.readAction = readAction;
		this.writeAction = writeAction;
		this.defaultValueOverride = null;
		this.hasDefaultValueOverride = false;
	}

	@Override
	public Class<T> getType() {
		return type;
	}

	@Override
	public boolean isReadable() {
		return readAction != null;
	}

	@Override
	public boolean isWritable() {
		return writeAction != null;
	}

	@Override
	public Scope getScope() {
		return Scope.WIRE;
	}

	public Function<XMLMessage, T> getReadAction() {
		return readAction;
	}

	public BiConsumer<XMLMessage, Object> getWriteAction() {
		return (msg, value) -> {
			if (type.isInstance(value)) {
				@SuppressWarnings("unchecked") T castedValue = (T) value;
				writeAction.accept(msg, castedValue);
			} else {
				throw new IllegalArgumentException(String.format("Expected type %s, but got %s", type,
						value.getClass()));
			}
		};
	}

	private static Integer getDeliveryCount(XMLMessage xmlMessage) {
		try {
			return xmlMessage.getDeliveryCount();
		} catch (UnsupportedOperationException e) {
			throw new SolaceDeliveryCountException(e);
		}
	}

	/**
	 * Get the overridden default value. Overridden default values may be {@code null}.
	 * Check {@link #hasOverriddenDefaultValue()} to be sure.
	 * @return new default value or null if does not exist
	 */
	public T getDefaultValueOverride() {
		return hasDefaultValueOverride ? defaultValueOverride : null;
	}

	public boolean hasOverriddenDefaultValue() {
		return hasDefaultValueOverride;
	}
}
