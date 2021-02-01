package com.solace.spring.cloud.stream.binder.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.solace.spring.cloud.stream.binder.messaging.SolaceBinderHeaderMeta;
import com.solace.spring.cloud.stream.binder.messaging.SolaceBinderHeaders;
import com.solace.spring.cloud.stream.binder.messaging.SolaceHeaderMeta;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solacesystems.common.util.ByteArray;
import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.MapMessage;
import com.solacesystems.jcsmp.SDTMap;
import com.solacesystems.jcsmp.SDTStream;
import com.solacesystems.jcsmp.StreamMessage;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.XMLContentMessage;
import com.solacesystems.jcsmp.XMLMessage;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.StaticMessageHeaderAccessor;
import org.springframework.integration.acks.AcknowledgmentCallback;
import org.springframework.integration.support.DefaultMessageBuilderFactory;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.util.MimeType;
import org.springframework.util.SerializationUtils;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class XMLMessageMapper {
	private static final Log logger = LogFactory.getLog(XMLMessageMapper.class);
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
	static final int MESSAGE_VERSION = 1;
	static final Encoder DEFAULT_ENCODING = Encoder.BASE64;

	private final ObjectWriter stringSetWriter = OBJECT_MAPPER.writerFor(new TypeReference<Set<String>>(){});
	private final ObjectReader stringSetReader = OBJECT_MAPPER.readerFor(new TypeReference<Set<String>>(){});

	public XMLMessage mapError(Message<?> message, SolaceConsumerProperties consumerProperties) {
		XMLMessage xmlMessage = map(message);
		if (consumerProperties.getErrorMsgDmqEligible() != null) {
			xmlMessage.setDMQEligible(consumerProperties.getErrorMsgDmqEligible());
		}
		if (consumerProperties.getErrorMsgTtl() != null) {
			xmlMessage.setTimeToLive(consumerProperties.getErrorMsgTtl());
		}
		return xmlMessage;
	}

	public XMLMessage map(Message<?> message) {
		return map(message, Collections.emptyList());
	}

	public XMLMessage map(Message<?> message, Collection<String> excludedHeaders) {
		XMLMessage xmlMessage;
		Object payload = message.getPayload();
		SDTMap metadata = map(message.getHeaders(), excludedHeaders);
		rethrowableCall(metadata::putInteger, SolaceBinderHeaders.MESSAGE_VERSION, MESSAGE_VERSION);

		if (payload instanceof byte[]) {
			BytesMessage bytesMessage = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
			bytesMessage.setData((byte[]) payload);
			xmlMessage = bytesMessage;
		} else if (payload instanceof String) {
			TextMessage textMessage = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
			textMessage.setText((String) payload);
			xmlMessage = textMessage;
		} else if (payload instanceof SDTStream) {
			StreamMessage streamMessage = JCSMPFactory.onlyInstance().createMessage(StreamMessage.class);
			streamMessage.setStream((SDTStream) payload);
			xmlMessage = streamMessage;
		} else if (payload instanceof SDTMap) {
			MapMessage mapMessage = JCSMPFactory.onlyInstance().createMessage(MapMessage.class);
			mapMessage.setMap((SDTMap) payload);
			xmlMessage = mapMessage;
		} else if (payload instanceof Serializable) {
			BytesMessage bytesMessage = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
			bytesMessage.setData(rethrowableCall(SerializationUtils::serialize, payload));
			rethrowableCall(metadata::putBoolean, SolaceBinderHeaders.SERIALIZED_PAYLOAD, true);
			xmlMessage = bytesMessage;
		} else {
			String msg = String.format(
					"Invalid payload received. Expected %s. Received: %s",
					String.join(", ",
							byte[].class.getSimpleName(),
							String.class.getSimpleName(),
							SDTStream.class.getSimpleName(),
							SDTMap.class.getSimpleName(),
							Serializable.class.getSimpleName()
					), payload.getClass().getName());
			SolaceMessageConversionException exception = new SolaceMessageConversionException(msg);
			logger.warn(msg, exception);
			throw exception;
		}

		MimeType contentType = StaticMessageHeaderAccessor.getContentType(message);
		if (contentType != null) {
			xmlMessage.setHTTPContentType(contentType.toString());
		}

		// Copy Solace properties from Spring Message to JCSMP XMLMessage
		for (Map.Entry<String, SolaceHeaderMeta<?>> header : SolaceHeaderMeta.META.entrySet()) {
			if (!header.getValue().isWritable()) {
				continue;
			}

			Object value = message.getHeaders().get(header.getKey());
			if (value != null) {
				if (!header.getValue().getType().isInstance(value)) {
					String msg = String.format(
							"Message %s has an invalid value type for header %s. Expected %s but received %s.",
							message.getHeaders().getId(), header.getKey(), header.getValue().getType(),
							value.getClass());
					SolaceMessageConversionException exception = new SolaceMessageConversionException(msg);
					logger.warn(msg, exception);
					throw exception;
				}
			} else if (header.getValue().hasOverriddenDefaultValue()) {
				value = header.getValue().getDefaultValueOverride();
			} else {
				continue;
			}

			try {
				header.getValue().getWriteAction().accept(xmlMessage, value);
			} catch (Exception e) {
				String msg = String.format("Could not set %s property from header %s of message %s",
						XMLMessage.class.getSimpleName(), header.getKey(), message.getHeaders().getId());
				SolaceMessageConversionException exception = new SolaceMessageConversionException(msg, e);
				logger.warn(msg, exception);
				throw exception;
			}
		}

		xmlMessage.setProperties(metadata);
		xmlMessage.setDeliveryMode(DeliveryMode.PERSISTENT);
		return xmlMessage;
	}

	public Message<?> map(XMLMessage xmlMessage, AcknowledgmentCallback acknowledgmentCallback) throws SolaceMessageConversionException {
		return map(xmlMessage, acknowledgmentCallback, false);
	}

	public Message<?> map(XMLMessage xmlMessage, AcknowledgmentCallback acknowledgmentCallback, boolean setRawMessageHeader) throws SolaceMessageConversionException {
		SDTMap metadata = xmlMessage.getProperties();

		Object payload;
		if (xmlMessage instanceof BytesMessage) {
			payload = ((BytesMessage) xmlMessage).getData();
			if (metadata != null &&
					metadata.containsKey(SolaceBinderHeaders.SERIALIZED_PAYLOAD) &&
					rethrowableCall(metadata::getBoolean, SolaceBinderHeaders.SERIALIZED_PAYLOAD)) {
				payload = rethrowableCall(SerializationUtils::deserialize, (byte[]) payload);
			}
		} else if (xmlMessage instanceof TextMessage) {
			payload = ((TextMessage) xmlMessage).getText();
		} else if (xmlMessage instanceof MapMessage) {
			payload = ((MapMessage) xmlMessage).getMap();
		} else if (xmlMessage instanceof StreamMessage) {
			payload = ((StreamMessage) xmlMessage).getStream();
		} else if (xmlMessage instanceof XMLContentMessage) {
			payload = ((XMLContentMessage) xmlMessage).getXMLContent();
		} else {
			String msg = String.format("Invalid message format received. Expected %s. Received: %s",
					String.join(", ",
							BytesMessage.class.getSimpleName(),
							TextMessage.class.getSimpleName(),
							MapMessage.class.getSimpleName(),
							StreamMessage.class.getSimpleName(),
							XMLContentMessage.class.getSimpleName()
					), xmlMessage.getClass());
			SolaceMessageConversionException exception = new SolaceMessageConversionException(msg);
			logger.warn(msg, exception);
			throw exception;
		}

		if (payload == null) {
			String msg = String.format("XMLMessage %s has no payload", xmlMessage.getMessageId());
			SolaceMessageConversionException exception = new SolaceMessageConversionException(msg);
			logger.warn(msg, exception);
			throw exception;
		}

		MessageBuilder<?> builder = new DefaultMessageBuilderFactory()
				.withPayload(payload)
				.copyHeaders(map(metadata))
				.setHeader(IntegrationMessageHeaderAccessor.ACKNOWLEDGMENT_CALLBACK, acknowledgmentCallback)
				.setHeaderIfAbsent(MessageHeaders.CONTENT_TYPE, xmlMessage.getHTTPContentType())
				.setHeaderIfAbsent(IntegrationMessageHeaderAccessor.DELIVERY_ATTEMPT, new AtomicInteger(0));

		for (Map.Entry<String, SolaceHeaderMeta<?>> header : SolaceHeaderMeta.META.entrySet()) {
			if (!header.getValue().isReadable()) {
				continue;
			}
			builder.setHeaderIfAbsent(header.getKey(), header.getValue().getReadAction().apply(xmlMessage));
		}

		if (setRawMessageHeader) {
			builder.setHeader(SolaceBinderHeaders.RAW_MESSAGE, xmlMessage);
		}

		return builder.build();
	}

	SDTMap map(MessageHeaders headers, Collection<String> excludedHeaders) {
		SDTMap metadata = JCSMPFactory.onlyInstance().createMap();
		Set<String> serializedHeaders = new HashSet<>();
		for (Map.Entry<String,Object> header : headers.entrySet()) {
			if (header.getKey().equalsIgnoreCase(IntegrationMessageHeaderAccessor.ACKNOWLEDGMENT_CALLBACK) ||
					header.getKey().equalsIgnoreCase(BinderHeaders.TARGET_DESTINATION) ||
					SolaceHeaderMeta.META.containsKey(header.getKey()) ||
					SolaceBinderHeaderMeta.META.containsKey(header.getKey())) {
				continue;
			}
			if (excludedHeaders.contains(header.getKey())){
				continue;
			}

			addSDTMapObject(metadata, serializedHeaders, header.getKey(), header.getValue());
		}

		if (!serializedHeaders.isEmpty()) {
			rethrowableCall(metadata::putString, SolaceBinderHeaders.SERIALIZED_HEADERS,
					rethrowableCall(stringSetWriter::writeValueAsString, serializedHeaders));
			rethrowableCall(metadata::putString, SolaceBinderHeaders.SERIALIZED_HEADERS_ENCODING,
					DEFAULT_ENCODING.getName());
		}
		return metadata;
	}

	MessageHeaders map(SDTMap metadata) {
		if (metadata == null) {
			return new MessageHeaders(Collections.emptyMap());
		}

		Map<String,Object> headers = new HashMap<>();

		// Deserialize headers
		if (metadata.containsKey(SolaceBinderHeaders.SERIALIZED_HEADERS)) {
			Encoder encoder = null;
			if (metadata.containsKey(SolaceBinderHeaders.SERIALIZED_HEADERS_ENCODING)) {
				String encoding = rethrowableCall(metadata::getString, SolaceBinderHeaders.SERIALIZED_HEADERS_ENCODING);
				encoder = Encoder.getByName(encoding);
				if (encoder == null) {
					String msg = String.format("%s encoding is not supported", encoding);
					SolaceMessageConversionException exception = new SolaceMessageConversionException(msg);
					logger.warn(msg, exception);
					throw exception;
				}
			}

			Set<String> serializedHeaders = rethrowableCall(stringSetReader::readValue,
					rethrowableCall(metadata::getString, SolaceBinderHeaders.SERIALIZED_HEADERS));

			for (String headerName : serializedHeaders) {
				if (metadata.containsKey(headerName)) {
					byte[] serializedValue = encoder != null ?
							encoder.decode(rethrowableCall(metadata::getString, headerName)) :
							rethrowableCall(metadata::getBytes, headerName);
					Object value = SerializationUtils.deserialize(serializedValue);
					if (value instanceof ByteArray) { // Just in case...
						value = ((ByteArray) value).getBuffer();
					}
					headers.put(headerName, value);
				}
			}
		}

		metadata.keySet().stream()
				.filter(h -> !headers.containsKey(h))
				.filter(h -> !SolaceBinderHeaderMeta.META.containsKey(h))
				.filter(h -> !SolaceHeaderMeta.META.containsKey(h))
				.forEach(h -> {
					Object value = rethrowableCall(metadata::get, h);
					if (value instanceof ByteArray) {
						value = ((ByteArray) value).getBuffer();
					}
					headers.put(h, value);
				});

		if (metadata.containsKey(SolaceBinderHeaders.MESSAGE_VERSION)) {
			int messageVersion = rethrowableCall(metadata::getInteger, SolaceBinderHeaders.MESSAGE_VERSION);
			headers.put(SolaceBinderHeaders.MESSAGE_VERSION, messageVersion);
		}

		return new MessageHeaders(headers);
	}

	/**
	 * Wrapper function which converts Serializable objects to byte[] if they aren't naturally supported by the SDTMap
	 */
	private void addSDTMapObject(SDTMap sdtMap, Set<String> serializedHeaders, String key, Object object)
			throws SolaceMessageConversionException {
		rethrowableCall((k, o) -> {
			try {
				sdtMap.putObject(k, o);
			} catch (IllegalArgumentException e) {
				if (o instanceof Serializable) {
					rethrowableCall(sdtMap::putString, k,
							DEFAULT_ENCODING.encode(rethrowableCall(SerializationUtils::serialize, o)));

					serializedHeaders.add(k);
				} else {
					throw e;
				}
			}
		}, key, object);
	}

	private <T,R> R rethrowableCall(ThrowingFunction<T,R> function, T var) {
		return function.apply(var);
	}

	private <T,U> void rethrowableCall(ThrowingBiConsumer<T,U> consumer, T var0, U var1) {
		consumer.accept(var0, var1);
	}

	private <R> R rethrowableCall(ThrowingSupplier<R> supplier) {
		return supplier.get();
	}

	@FunctionalInterface
	private interface ThrowingFunction<T,R> extends Function<T,R> {

		@Override
		default R apply(T t) {
			try {
				return applyThrows(t);
			} catch (Exception e) {
				SolaceMessageConversionException wrappedException = new SolaceMessageConversionException(e);
				logger.warn(wrappedException);
				throw wrappedException;
			}
		}

		R applyThrows(T t) throws Exception;
	}

	@FunctionalInterface
	private interface ThrowingBiConsumer<T,U> extends BiConsumer<T,U> {

		@Override
		default void accept(T t, U u) {
			try {
				applyThrows(t, u);
			} catch (Exception e) {
				SolaceMessageConversionException wrappedException = new SolaceMessageConversionException(e);
				logger.warn(wrappedException);
				throw wrappedException;
			}
		}

		void applyThrows(T t, U u) throws Exception;
	}

	@FunctionalInterface
	private interface ThrowingSupplier<T> extends Supplier<T> {

		@Override
		default T get() {
			try {
				return applyThrows();
			} catch (Exception e) {
				SolaceMessageConversionException wrappedException = new SolaceMessageConversionException(e);
				logger.warn(wrappedException);
				throw wrappedException;
			}
		}

		T applyThrows() throws Exception;
	}

	enum Encoder {
		BASE64("base64", Base64.getEncoder()::encodeToString, Base64.getDecoder()::decode);

		private final String name;
		private final ThrowingFunction<byte[], String> encodeFnc;
		private final ThrowingFunction<String, byte[]> decodeFnc;

		private static final Map<String, Encoder> nameMap = new HashMap<>();
		static {
			Arrays.stream(Encoder.values()).forEach(e -> nameMap.put(e.getName(), e));
		}

		Encoder(String name, ThrowingFunction<byte[], String> encodeFnc, ThrowingFunction<String, byte[]> decodeFnc) {
			this.name = name;
			this.encodeFnc = encodeFnc;
			this.decodeFnc = decodeFnc;
		}

		public String encode(byte[] input) {
			return input != null ? encodeFnc.apply(input) : null;
		}

		public byte[] decode(String input) {
			return input != null ? decodeFnc.apply(input) : null;
		}

		public String getName() {
			return name;
		}

		public static Encoder getByName(String name) {
			return nameMap.get(name);
		}
	}
}
