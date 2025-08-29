package com.solace.spring.cloud.stream.binder.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.solace.spring.cloud.stream.binder.messaging.HeaderMeta;
import com.solace.spring.cloud.stream.binder.messaging.SolaceBinderHeaderMeta;
import com.solace.spring.cloud.stream.binder.messaging.SolaceBinderHeaders;
import com.solace.spring.cloud.stream.binder.messaging.SolaceHeaderMeta;
import com.solace.spring.cloud.stream.binder.messaging.SolaceHeaders;
import com.solace.spring.cloud.stream.binder.properties.SmfMessageReaderProperties;
import com.solace.spring.cloud.stream.binder.properties.SmfMessageWriterProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceProducerProperties;
import com.solace.spring.cloud.stream.binder.test.util.SerializableFoo;
import com.solace.spring.cloud.stream.binder.test.util.ThrowingFunction;
import com.solacesystems.common.util.ByteArray;
import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.MapMessage;
import com.solacesystems.jcsmp.ReplicationGroupMessageId;
import com.solacesystems.jcsmp.SDTException;
import com.solacesystems.jcsmp.SDTMap;
import com.solacesystems.jcsmp.SDTStream;
import com.solacesystems.jcsmp.StreamMessage;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.XMLContentMessage;
import com.solacesystems.jcsmp.XMLMessage;
import java.util.LinkedHashMap;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.MapAssert;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.function.ThrowingConsumer;
import org.junit.jupiter.api.function.ThrowingSupplier;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junitpioneer.jupiter.cartesian.CartesianArgumentsSource;
import org.junitpioneer.jupiter.cartesian.CartesianParameterArgumentsProvider;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Values;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.StaticMessageHeaderAccessor;
import org.springframework.integration.acks.AcknowledgmentCallback;
import org.springframework.integration.support.DefaultMessageBuilderFactory;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;
import org.springframework.util.SerializationUtils;

import java.lang.reflect.Array;
import java.lang.reflect.Parameter;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class XMLMessageMapperTest {
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
	private final ObjectWriter objectWriter = OBJECT_MAPPER.writer();
	private final ObjectReader objectReader = OBJECT_MAPPER.reader();

	@Spy
	private final XMLMessageMapper xmlMessageMapper = new XMLMessageMapper();

	private static final Logger LOGGER = LoggerFactory.getLogger(XMLMessageMapperTest.class);
	private static final Set<String> JMS_INVALID_HEADER_NAMES = new HashSet<>(Arrays.asList("~ab;c", "NULL",
			"TRUE", "FALSE", "NOT", "AND", "OR", "BETWEEN", "LIKE", "IN", "IS", "ESCAPE", "JMSX_abc", "JMS_abc"));

	static {
		assertTrue(JMS_INVALID_HEADER_NAMES.stream().anyMatch(h -> !Character.isJavaIdentifierStart(h.charAt(0))));
		assertTrue(JMS_INVALID_HEADER_NAMES.stream().map(CharSequence::chars)
				.anyMatch(c -> c.skip(1).anyMatch(c1 -> !Character.isJavaIdentifierPart(c1))));
		assertTrue(JMS_INVALID_HEADER_NAMES.stream().anyMatch(h -> h.startsWith("JMSX")));
		assertTrue(JMS_INVALID_HEADER_NAMES.stream().anyMatch(h -> h.startsWith("JMS_")));
	}

	@CartesianTest
	<T, MT extends XMLMessage> void testMapSpringMessageToXMLMessage(
			@CartesianTest.Enum SmfMessagePayloadWriteCompatibility payloadWriteCompatibility,
			@CartesianArgumentsSource(SpringMessageTypeCartesianArgumentsProvider.class)
			SpringMessageTypeProvider<T, MT> springMessageTypeProvider) {
		Message<?> testSpringMessage = new DefaultMessageBuilderFactory()
				.withPayload(springMessageTypeProvider.createPayload())
				.setHeader("test-header-1", "test-header-val-1")
				.setHeader("test-header-2", "test-header-val-2")
				.setHeader(MessageHeaders.CONTENT_TYPE, springMessageTypeProvider.mimeType().toString())
				.build();

		SmfMessageWriterProperties writerProperties = new SmfMessageWriterProperties(new SolaceProducerProperties());
		writerProperties.setPayloadTypeCompatibility(payloadWriteCompatibility);

		if (SmfMessagePayloadWriteCompatibility.NATIVE_ONLY.equals(payloadWriteCompatibility) &&
				testSpringMessage.getPayload() instanceof SerializableFoo) {
			assertThatThrownBy(() -> xmlMessageMapper.mapToSmf(testSpringMessage, writerProperties))
					.isInstanceOf(SolaceMessageConversionException.class)
					.hasMessageContaining("Invalid payload received")
					.hasMessageContaining("Received: %s", SerializableFoo.class.getName());
		} else {
			Assertions.assertThat(xmlMessageMapper.mapToSmf(testSpringMessage, writerProperties))
					.asInstanceOf(InstanceOfAssertFactories.type(springMessageTypeProvider.expectedXmlMessageType()))
					.satisfies(
							m -> Assertions.assertThat(springMessageTypeProvider.extractSmfPayload(m))
									.isEqualTo(testSpringMessage.getPayload()),
							m -> {
								if (springMessageTypeProvider.serializedPayload()) {
									Assertions.assertThat(m.getProperties().getBoolean(SolaceBinderHeaders.SERIALIZED_PAYLOAD))
											.isTrue();
								}
							},
							m -> validateXMLProperties(m, testSpringMessage)
					);
		}
	}

	@CartesianTest
	<T, MT extends XMLMessage> void testMapSpringMessageToXMLMessageBatch(
			@CartesianTest.Enum SmfMessagePayloadWriteCompatibility payloadWriteCompatibility,
			@CartesianArgumentsSource(SpringMessageTypeCartesianArgumentsProvider.class)
			SpringMessageTypeProvider<T, MT> springMessageTypeProvider) {
		String excludedHeader = "test-excluded-header";
		List<ImmutablePair<T, Map<String, Object>>> messageContents = IntStream.range(0, 256)
				.mapToObj(i -> new ImmutablePair<>(springMessageTypeProvider.createPayload(),
						Map.<String, Object>ofEntries(
								Map.entry("test-header-1", "test-header-val-1"),
								Map.entry("test-header-2", "test-header-val-2"),
								Map.entry("test-header-3", "test-header-val-unique-" + i),
								Map.entry("test-excluded-header", "EXCLUDED VALUE"),
								Map.entry(MessageHeaders.CONTENT_TYPE, springMessageTypeProvider.mimeType().toString())
				)))
				.toList();

		Message<List<T>> batchedMessage = MessageBuilder
				.withPayload(messageContents.stream()
						.map(ImmutablePair::getLeft)
						.toList())
				.setHeader(SolaceBinderHeaders.BATCHED_HEADERS,
						messageContents.stream().map(ImmutablePair::getRight).toList())
				.build();

		SmfMessageWriterProperties serializationProperties = new SmfMessageWriterProperties(new SolaceProducerProperties());
		serializationProperties.setPayloadTypeCompatibility(payloadWriteCompatibility);
		serializationProperties.getHeaderExclusions().add(excludedHeader);

		if (SmfMessagePayloadWriteCompatibility.NATIVE_ONLY.equals(payloadWriteCompatibility) &&
				springMessageTypeProvider.createPayload() instanceof SerializableFoo) {
			assertThatThrownBy(() -> xmlMessageMapper.mapBatchedToSmf(batchedMessage, serializationProperties))
					.isInstanceOf(SolaceMessageConversionException.class)
					.hasMessageContaining("Invalid payload received")
					.hasMessageContaining("Received: %s", SerializableFoo.class.getName());
			return;
		}

		List<XMLMessage> xmlMessages = xmlMessageMapper.mapBatchedToSmf(batchedMessage, serializationProperties);

		Mockito.verify(xmlMessageMapper, Mockito.times(messageContents.size()))
				.mapToSmf(Mockito.any(),
						Mockito.anyMap(),
						Mockito.eq(StaticMessageHeaderAccessor.getId(batchedMessage)),
						Mockito.eq(serializationProperties));

		Assertions.assertThat(xmlMessages).hasSameSizeAs(messageContents);
		Assertions.assertThat(IntStream.range(0, xmlMessages.size())
						.mapToObj(i -> new ImmutablePair<>(xmlMessages.get(i), messageContents.get(i))))
				.allSatisfy(comparison ->
						Assertions.assertThat(comparison.getLeft())
								.asInstanceOf(InstanceOfAssertFactories.type(springMessageTypeProvider.expectedXmlMessageType()))
								.extracting(springMessageTypeProvider::extractSmfPayload)
								.isEqualTo(comparison.getRight().getLeft()))
				.allSatisfy(comparison -> {
					if (springMessageTypeProvider.serializedPayload()) {
						Assertions.assertThat(comparison.getLeft()
										.getProperties()
										.getBoolean(SolaceBinderHeaders.SERIALIZED_PAYLOAD))
								.isTrue();
					}
				})
				.allSatisfy(comparison -> {
					Map<String, Object> expectedHeaders = new HashMap<>(comparison.getRight().getRight());
					expectedHeaders.remove(excludedHeader);
					validateXMLProperties(
							comparison.getLeft(), // XML message
							comparison.getRight().getLeft(), // input Spring message payload
							comparison.getRight().getRight(), // input Spring message headers
							expectedHeaders);
						}
				);
	}

	@Test
	void testMapSpringMessageToXMLMessageBatch_noBatchedHeaders() {
		List<String> payloads = IntStream.range(0, 256).mapToObj(i -> "asdasd").toList();
		Assertions.assertThat(xmlMessageMapper.mapBatchedToSmf(MessageBuilder.withPayload(payloads).build(),
						new SmfMessageWriterProperties(new SolaceProducerProperties())))
				.hasSameSizeAs(payloads)
				.allSatisfy(m -> Assertions.assertThat(m.getProperties().keySet())
						.containsOnly(SolaceBinderHeaders.MESSAGE_VERSION))
				.asInstanceOf(InstanceOfAssertFactories.list(TextMessage.class))
				.extracting(TextMessage::getText)
				.containsExactlyInAnyOrderElementsOf(payloads);
	}

	@Test
	void testMapSpringMessageToXMLMessageBatch_invalidBatchedPayload() {
		assertThatThrownBy(() -> xmlMessageMapper.mapBatchedToSmf(
				MessageBuilder.withPayload("invalid batched payload").build(),
				new SmfMessageWriterProperties(new SolaceProducerProperties())))
				.isInstanceOf(SolaceMessageConversionException.class)
				.rootCause()
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("payload of batched message")
				.hasMessageContaining("to be of type List<?>");
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	void testMapSpringMessageToXMLMessageBatch_invalidBatchedHeadersSize(boolean isEmpty) {
		List<String> payloads = IntStream.range(0, 10).mapToObj(i -> "asdasd").toList();

		Message<List<String>> batchedMessage = MessageBuilder.withPayload(payloads)
				.setHeader(SolaceBinderHeaders.BATCHED_HEADERS,
						isEmpty ? List.of() : IntStream.range(0, payloads.size() / 2).mapToObj(i -> Map.of()).toList())
				.build();

		assertThatThrownBy(() -> xmlMessageMapper.mapBatchedToSmf(batchedMessage,
				new SmfMessageWriterProperties(new SolaceProducerProperties())))
				.isInstanceOf(SolaceMessageConversionException.class)
				.rootCause()
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("must have matching lengths");
	}

	@Test
	void testMapSpringMessageToXMLMessageBatch_doNotReWrapMessageConversionException() {
		Message<?> testSpringMessage = MessageBuilder.withPayload(List.of("foo", "bar")).build();

		SmfMessageWriterProperties serializationProperties = new SmfMessageWriterProperties(new SolaceProducerProperties());
		SolaceMessageConversionException exception = new SolaceMessageConversionException("test");
		Mockito.doThrow(exception).when(xmlMessageMapper).mapToSmf(
				Mockito.any(),
				Mockito.anyMap(),
				Mockito.eq(StaticMessageHeaderAccessor.getId(testSpringMessage)),
				Mockito.eq(serializationProperties));
		assertThatThrownBy(() -> xmlMessageMapper.mapBatchedToSmf(testSpringMessage, serializationProperties))
				.isEqualTo(exception);
	}

	@Test
	void testMapSpringMessageToXMLMessage_WriteSolaceProperties() throws Exception {
		MessageBuilder<?> messageBuilder = new DefaultMessageBuilderFactory()
				.withPayload("")
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE);

		Set<Map.Entry<String, ? extends HeaderMeta<?>>> writeableHeaders = Stream.of(
					SolaceHeaderMeta.META.entrySet().stream(),
					SolaceBinderHeaderMeta.META.entrySet().stream())
				.flatMap(h -> h)
				.filter(h -> h.getValue().isWritable())
				.collect(Collectors.toSet());
		assertNotEquals(0, writeableHeaders.size(), "Test header set was empty");

		for (Map.Entry<String, ? extends HeaderMeta<?>> header : writeableHeaders) {
			messageBuilder.setHeader(header.getKey(), Objects.requireNonNull(switch (header.getKey()) {
				case SolaceHeaders.APPLICATION_MESSAGE_ID,
					 SolaceHeaders.APPLICATION_MESSAGE_TYPE,
					 SolaceHeaders.CORRELATION_ID,
					 SolaceHeaders.HTTP_CONTENT_ENCODING,
					 SolaceHeaders.SENDER_ID,
					 SolaceBinderHeaders.TARGET_DESTINATION_TYPE ->
						RandomStringUtils.randomAlphanumeric(10);
				case SolaceHeaders.DMQ_ELIGIBLE ->
						!(Boolean) ((SolaceHeaderMeta<?>) header.getValue()).getDefaultValueOverride();
				case SolaceHeaders.IS_REPLY ->
						true; //The opposite of what a Solace message defaults to
				case SolaceHeaders.EXPIRATION,
					 SolaceHeaders.SENDER_TIMESTAMP,
					 SolaceHeaders.SEQUENCE_NUMBER,
					 SolaceHeaders.TIME_TO_LIVE ->
						(long) ThreadLocalRandom.current().nextInt(10000);
				case SolaceHeaders.PRIORITY ->
						ThreadLocalRandom.current().nextInt(255);
				case SolaceHeaders.REPLY_TO ->
						JCSMPFactory.onlyInstance().createQueue(RandomStringUtils.randomAlphanumeric(10));
				case SolaceHeaders.USER_DATA ->
						RandomStringUtils.randomAlphanumeric(10).getBytes();
				case SolaceBinderHeaders.BATCHED_HEADERS ->
						List.of(Map.of("foo", "bar"));
				case SolaceBinderHeaders.CONFIRM_CORRELATION ->
						new CorrelationData();
				case SolaceBinderHeaders.PARTITION_KEY -> {
					// This value is overwritten by binder-defined partition key header
					messageBuilder.setHeader(XMLMessage.MessageUserPropertyConstants.QUEUE_PARTITION_KEY,
							RandomStringUtils.randomAlphanumeric(10));
					yield RandomStringUtils.randomAlphanumeric(10);
				}
				default -> {
					fail(String.format("no test for header %s", header.getKey()));
					yield null;
				}
			}));
		}

		Message<?> testSpringMessage = messageBuilder.build();
		XMLMessage xmlMessage = xmlMessageMapper.mapToSmf(testSpringMessage,
				new SmfMessageWriterProperties(new SolaceProducerProperties()));

		for (Map.Entry<String, ? extends HeaderMeta<?>> header : writeableHeaders) {
			Object expectedValue = testSpringMessage.getHeaders().get(header.getKey());
			switch (header.getKey()) {
				case SolaceHeaders.APPLICATION_MESSAGE_ID ->
						assertEquals(expectedValue, xmlMessage.getApplicationMessageId());
				case SolaceHeaders.APPLICATION_MESSAGE_TYPE ->
						assertEquals(expectedValue, xmlMessage.getApplicationMessageType());
				case SolaceHeaders.CORRELATION_ID ->
						assertEquals(expectedValue, xmlMessage.getCorrelationId());
				case SolaceHeaders.DMQ_ELIGIBLE ->
						assertEquals(expectedValue, xmlMessage.isDMQEligible());
				case SolaceHeaders.EXPIRATION ->
						assertEquals(expectedValue, xmlMessage.getExpiration());
				case SolaceHeaders.IS_REPLY ->
						assertEquals(expectedValue, xmlMessage.isReplyMessage());
				case SolaceHeaders.HTTP_CONTENT_ENCODING ->
						assertEquals(expectedValue, xmlMessage.getHTTPContentEncoding());
				case SolaceHeaders.PRIORITY ->
						assertEquals(expectedValue, xmlMessage.getPriority());
				case SolaceHeaders.REPLY_TO ->
						assertEquals(expectedValue, xmlMessage.getReplyTo());
				case SolaceHeaders.SENDER_ID ->
						assertEquals(expectedValue, xmlMessage.getSenderId());
				case SolaceHeaders.SENDER_TIMESTAMP ->
						assertEquals(expectedValue, xmlMessage.getSenderTimestamp());
				case SolaceHeaders.SEQUENCE_NUMBER ->
						assertEquals(expectedValue, xmlMessage.getSequenceNumber());
				case SolaceHeaders.TIME_TO_LIVE ->
						assertEquals(expectedValue, xmlMessage.getTimeToLive());
				case SolaceHeaders.USER_DATA ->
						assertEquals(expectedValue, xmlMessage.getUserData());
				case SolaceBinderHeaders.PARTITION_KEY ->
						assertEquals(expectedValue, xmlMessage.getProperties()
								.getString(XMLMessage.MessageUserPropertyConstants.QUEUE_PARTITION_KEY));
				case SolaceBinderHeaders.BATCHED_HEADERS,
					 SolaceBinderHeaders.CONFIRM_CORRELATION,
					 SolaceBinderHeaders.TARGET_DESTINATION_TYPE ->
						// These Spring headers aren't ever reflected in the SMF message
						assertNull(xmlMessage.getProperties().get(header.getKey()));
				default -> fail(String.format("no test for header %s", header.getKey()));
			}
		}

		validateXMLProperties(xmlMessage, testSpringMessage);
	}

	@Test
	void testMapSpringMessageToXMLMessage_NonWriteableSolaceProperties() throws Exception {
		MessageBuilder<?> messageBuilder = new DefaultMessageBuilderFactory()
				.withPayload("")
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE);

		Set<Map.Entry<String, ? extends HeaderMeta<?>>> nonWriteableHeaders = Stream.of(
					SolaceHeaderMeta.META.entrySet().stream(),
					SolaceBinderHeaderMeta.META.entrySet().stream())
				.flatMap(h -> h)
				.filter(h -> !h.getValue().isWritable())
				.collect(Collectors.toSet());
		assertNotEquals(0, nonWriteableHeaders.size(), "Test header set was empty");

		for (Map.Entry<String, ? extends HeaderMeta<?>> header : nonWriteableHeaders) {
			// Doesn't matter what we set the values to
			messageBuilder.setHeader(header.getKey(), new Object());
		}

		Message<?> testSpringMessage = messageBuilder.build();
		XMLMessage xmlMessage = xmlMessageMapper.mapToSmf(testSpringMessage,
				new SmfMessageWriterProperties(new SolaceProducerProperties()));

		for (Map.Entry<String, ? extends HeaderMeta<?>> header : nonWriteableHeaders) {
			switch (header.getKey()) {
				case SolaceHeaders.REPLICATION_GROUP_MESSAGE_ID:
					assertNull(xmlMessage.getReplicationGroupMessageId());
					break;
				case SolaceHeaders.DELIVERY_COUNT:
					assertThrows(UnsupportedOperationException.class, xmlMessage::getDeliveryCount);
					break;
				case SolaceHeaders.DESTINATION:
					assertNull(xmlMessage.getDestination());
					break;
				case SolaceHeaders.DISCARD_INDICATION:
					assertFalse(xmlMessage.getDiscardIndication());
					break;
				case SolaceHeaders.RECEIVE_TIMESTAMP:
					assertEquals(0, xmlMessage.getReceiveTimestamp());
					break;
				case SolaceHeaders.REDELIVERED:
					assertFalse(xmlMessage.getRedelivered());
					break;
				case SolaceBinderHeaders.MESSAGE_VERSION:
					assertEquals(Integer.valueOf(XMLMessageMapper.MESSAGE_VERSION),
							xmlMessage.getProperties().getInteger(header.getKey()));
					break;
				case SolaceBinderHeaders.SERIALIZED_HEADERS:
					String serializedHeadersJson = xmlMessage.getProperties().getString(header.getKey());
					assertThat(serializedHeadersJson, not(emptyString()));
					assertThat(objectReader.forType(new TypeReference<Set<String>>() {})
							.readValue(serializedHeadersJson), not(empty()));
					break;
				case SolaceBinderHeaders.SERIALIZED_HEADERS_ENCODING:
					assertEquals("base64", xmlMessage.getProperties().getString(header.getKey()));
					break;
				case SolaceBinderHeaders.SERIALIZED_PAYLOAD:
				case SolaceBinderHeaders.BATCHED_HEADERS:
				case SolaceBinderHeaders.CONFIRM_CORRELATION:
				case SolaceBinderHeaders.NULL_PAYLOAD:
				case SolaceBinderHeaders.TARGET_DESTINATION_TYPE:
					assertNull(xmlMessage.getProperties().get(header.getKey()));
					break;
				default:
					fail(String.format("no test for header %s", header.getKey()));
			}
		}

		validateXMLProperties(xmlMessage, testSpringMessage.getPayload(), testSpringMessage.getHeaders(),
				testSpringMessage.getHeaders()
						.entrySet()
						.stream()
						.filter(h -> nonWriteableHeaders
								.stream()
								.map(Map.Entry::getKey)
								.noneMatch(nonWriteableHeader -> h.getKey().equals(nonWriteableHeader))
						)
						.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
		);
	}

	@Test
	void testMapSpringMessageToXMLMessage_WriteUndefinedSolaceHeader() throws Exception {
		String undefinedSolaceHeader1 = "abc1234";
		SerializableFoo undefinedSolaceHeader2 = new SerializableFoo("abc", "123");
		Message<?> testSpringMessage = new DefaultMessageBuilderFactory()
				.withPayload("")
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.setHeader("solace_foo1", undefinedSolaceHeader1)
				.setHeader("solace_foo2", undefinedSolaceHeader2)
				.build();

		XMLMessage xmlMessage = xmlMessageMapper.mapToSmf(testSpringMessage,
				new SmfMessageWriterProperties(new SolaceProducerProperties()));

		assertEquals(undefinedSolaceHeader1, xmlMessage.getProperties().getString("solace_foo1"));
		assertEquals(undefinedSolaceHeader2, SerializationUtils.deserialize(Base64.getDecoder()
				.decode(xmlMessage.getProperties().getString("solace_foo2"))));

		assertEquals("base64", xmlMessage.getProperties().getString(SolaceBinderHeaders.SERIALIZED_HEADERS_ENCODING));
		String serializedHeadersJson = xmlMessage.getProperties().getString(SolaceBinderHeaders.SERIALIZED_HEADERS);
		assertThat(serializedHeadersJson, not(emptyString()));
		Set<String> serializedHeaders = objectReader.forType(new TypeReference<Set<String>>() {})
				.readValue(serializedHeadersJson);
		assertThat(serializedHeaders, not(empty()));
		assertThat(serializedHeaders, hasItem("solace_foo2"));

		validateXMLProperties(xmlMessage, testSpringMessage);
	}

	@Test
	void testMapSpringMessageToXMLMessage_OverrideDefaultSolaceProperties() throws Exception {
		Set<Map.Entry<String, ? extends SolaceHeaderMeta<?>>> overriddenWriteableHeaders = SolaceHeaderMeta.META
				.entrySet()
				.stream()
				.filter(h -> h.getValue().isWritable())
				.filter(h -> h.getValue().hasOverriddenDefaultValue())
				.collect(Collectors.toSet());
		assertNotEquals(0, overriddenWriteableHeaders.size(), "Test header set was empty");

		Message<?> testSpringMessage = new DefaultMessageBuilderFactory()
				.withPayload("")
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();
		XMLMessage xmlMessage = xmlMessageMapper.mapToSmf(testSpringMessage,
				new SmfMessageWriterProperties(new SolaceProducerProperties()));

		for (Map.Entry<String, ? extends HeaderMeta<?>> header : overriddenWriteableHeaders) {
			switch (header.getKey()) {
				case SolaceHeaders.DMQ_ELIGIBLE:
					assertTrue(xmlMessage.isDMQEligible());
					break;
				default:
					fail(String.format("no test for header %s", header.getKey()));
			}
		}

		validateXMLProperties(xmlMessage, testSpringMessage);
	}

	@ParameterizedTest
	@ValueSource(classes = {String.class, MimeType.class})
	void testMapSpringMessageToXMLMessage_contentTypeHeader(
			Class<?> contentTypeClass) {
		Message<?> testSpringMessage = new DefaultMessageBuilderFactory()
				.withPayload("test")
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeType.class.isAssignableFrom(contentTypeClass) ?
						MimeTypeUtils.TEXT_PLAIN : MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		Assertions.assertThat(xmlMessageMapper.mapToSmf(testSpringMessage,
						new SmfMessageWriterProperties(new SolaceProducerProperties())))
				.extracting(XMLMessage::getHTTPContentType)
				.isEqualTo(MimeTypeUtils.TEXT_PLAIN_VALUE);
	}

	@Test
	void testFailMapSpringMessageToXMLMessage_InvalidPayload() {
		Message<?> testSpringMessage = new DefaultMessageBuilderFactory().withPayload(new Object()).build();
		assertThrows(SolaceMessageConversionException.class, () -> xmlMessageMapper.mapToSmf(testSpringMessage,
				new SmfMessageWriterProperties(new SolaceProducerProperties())));
	}

	@Test
	void testFailMapSpringMessageToXMLMessage_InvalidHeaderType() {
		Set<Map.Entry<String, ? extends HeaderMeta<?>>> writeableHeaders = Stream.of(
					SolaceHeaderMeta.META.entrySet().stream(),
					SolaceBinderHeaderMeta.META.entrySet().stream())
				.flatMap(h -> h)
				.filter(h -> h.getValue().isWritable())
				.filter(h -> h.getValue().getScope().equals(HeaderMeta.Scope.WIRE))
				.collect(Collectors.toSet());
		assertNotEquals(0, writeableHeaders.size(), "Test header set was empty");

		for (Map.Entry<String, ? extends HeaderMeta<?>> header : writeableHeaders) {
			Message<?> testSpringMessage = new DefaultMessageBuilderFactory().withPayload("")
					.setHeader(header.getKey(), new Object())
					.build();
			try {
				xmlMessageMapper.mapToSmf(testSpringMessage,
						new SmfMessageWriterProperties(new SolaceProducerProperties()));
				fail(String.format("Expected message mapping to fail for header %s", header.getKey()));
			} catch (SolaceMessageConversionException e) {
				if (header.getValue() instanceof SolaceHeaderMeta<?>) {
					assertEquals(e.getMessage(), String.format(
							"Message %s has an invalid value type for header %s. Expected %s but received %s.",
							testSpringMessage.getHeaders().getId(), header.getKey(), header.getValue().getType(),
							Object.class));
				} else {
					switch (header.getKey()) {
						case SolaceBinderHeaders.PARTITION_KEY ->
								Assertions.assertThat(e).rootCause()
										.isInstanceOf(IllegalArgumentException.class)
										.hasMessageContainingAll(header.getKey(), header.getValue().getType().toString());
						default ->
								fail(String.format("no test for header %s", header.getKey()));
					}
				}
			}
		}
	}

	@Test
	void testMapXMLMessageToErrorXMLMessage() throws Exception {
		SDTMap headers = JCSMPFactory.onlyInstance().createMap();
		headers.putInteger(SolaceBinderHeaders.MESSAGE_VERSION, 1);
		headers.putString("a", "abc");
		TextMessage inputMessage = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
		inputMessage.setText("test-payload");
		inputMessage.setProperties(headers);
		inputMessage.setApplicationMessageId("test");
		inputMessage.setDMQEligible(true);
		inputMessage.setTimeToLive(1L);
		inputMessage.setReadOnly();

		XMLMessage errorMessage = xmlMessageMapper.mapError(inputMessage, new SolaceConsumerProperties());
		assertThat(errorMessage, instanceOf(TextMessage.class));
		assertEquals(inputMessage.getText(), ((TextMessage) errorMessage).getText());
		assertEquals(inputMessage.getProperties(), errorMessage.getProperties());
		assertEquals(inputMessage.getApplicationMessageId(), errorMessage.getApplicationMessageId());
		assertEquals(inputMessage.isDMQEligible(), errorMessage.isDMQEligible());
		assertEquals(inputMessage.getTimeToLive(), errorMessage.getTimeToLive());
		assertFalse(errorMessage.isReadOnly());
	}

	@Test
	void testMapXMLMessageToErrorXMLMessage_WithProperties() {
		TextMessage inputMessage = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
		SolaceConsumerProperties consumerProperties = new SolaceConsumerProperties();
		consumerProperties.setErrorMsgDmqEligible(!inputMessage.isDMQEligible());
		consumerProperties.setErrorMsgTtl(100L);

		XMLMessage xmlMessage = xmlMessageMapper.mapError(inputMessage, consumerProperties);

		assertEquals(consumerProperties.getErrorMsgDmqEligible(), xmlMessage.isDMQEligible());
		assertEquals(consumerProperties.getErrorMsgTtl().longValue(), xmlMessage.getTimeToLive());
	}

	@Test
	void testMapSpringMessageToXMLMessage_WithExcludedHeader() throws SDTException {
		String testPayload = "testPayload";
		SmfMessageWriterProperties serializationProperties = new SmfMessageWriterProperties(new SolaceProducerProperties());
		serializationProperties.getHeaderExclusions()
				.add("io.opentracing.contrib.spring.integration.messaging.OpenTracingChannelInterceptor.SCOPE");
		Message<?> testSpringMessage = new DefaultMessageBuilderFactory().withPayload(testPayload)
				.setHeader(
						"io.opentracing.contrib.spring.integration.messaging.OpenTracingChannelInterceptor.SCOPE",
						"any")
				.build();

		XMLMessage xmlMessage = xmlMessageMapper.mapToSmf(testSpringMessage, serializationProperties);
		Mockito.verify(xmlMessageMapper).mapToSmf(testSpringMessage, serializationProperties);

		assertNull(xmlMessage.getProperties()
				.getMap("io.opentracing.contrib.spring.integration.messaging.OpenTracingChannelInterceptor.SCOPE"));
	}

	@Test
	void testMapSpringMessageToXMLMessage_WithExcludedHeader_ShouldNotMatchPartially() throws SDTException {
		String testPayload = "testPayload";
		SmfMessageWriterProperties serializationProperties = new SmfMessageWriterProperties(new SolaceProducerProperties());
		serializationProperties.getHeaderExclusions()
				.add("io.opentracing.contrib.spring.integration.messaging.OpenTracingChannelInterceptor.SCOPE");
		Message<?> testSpringMessage = new DefaultMessageBuilderFactory().withPayload(testPayload)
				.setHeader(
						"io.opentracing.contrib.spring.integration.messaging.OpenTracingChannelInterceptor",
						"any")
				.build();

		XMLMessage xmlMessage = xmlMessageMapper.mapToSmf(testSpringMessage, serializationProperties);
		Mockito.verify(xmlMessageMapper).mapToSmf(testSpringMessage, serializationProperties);
		assertEquals("any", xmlMessage.getProperties()
				.get("io.opentracing.contrib.spring.integration.messaging.OpenTracingChannelInterceptor"));
	}

	@Test
	void testMapSpringMessageToXMLMessage_WithExcludedHeader_ShouldNotFilterSolaceHeader() throws SDTException {
		MessageBuilder<?> messageBuilder = new DefaultMessageBuilderFactory()
				.withPayload(new SerializableFoo("a", "b"))
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.setHeader("test-serializable-header", new SerializableFoo("a", "b"));

		Set<Map.Entry<String, ? extends HeaderMeta<?>>> writeableHeaders = Stream.of(
				SolaceHeaderMeta.META.entrySet().stream(),
				SolaceBinderHeaderMeta.META.entrySet().stream())
				.flatMap(h -> h)
				.filter(h -> h.getValue().isWritable())
				.filter(h -> h.getValue().getScope().equals(HeaderMeta.Scope.WIRE))
				.collect(Collectors.toSet());
		assertNotEquals(0, writeableHeaders.size(), "Test header set was empty");

		for (Map.Entry<String, ? extends HeaderMeta<?>> header : writeableHeaders) {
			messageBuilder.setHeader(header.getKey(), Objects.requireNonNull(switch (header.getKey()) {
				case SolaceHeaders.APPLICATION_MESSAGE_ID,
					 SolaceHeaders.APPLICATION_MESSAGE_TYPE,
					 SolaceHeaders.CORRELATION_ID,
					 SolaceHeaders.HTTP_CONTENT_ENCODING,
					 SolaceHeaders.SENDER_ID,
					 SolaceBinderHeaders.PARTITION_KEY ->
						RandomStringUtils.randomAlphanumeric(10);
				case SolaceHeaders.DMQ_ELIGIBLE ->
						!(Boolean) ((SolaceHeaderMeta<?>) header.getValue()).getDefaultValueOverride();
				case SolaceHeaders.IS_REPLY -> true; //The opposite of what a Solace message defaults to
				case SolaceHeaders.EXPIRATION,
					 SolaceHeaders.SENDER_TIMESTAMP,
					 SolaceHeaders.SEQUENCE_NUMBER,
					 SolaceHeaders.TIME_TO_LIVE ->
						ThreadLocalRandom.current().nextLong(10000);
				case SolaceHeaders.PRIORITY ->
						ThreadLocalRandom.current().nextInt(255);
				case SolaceHeaders.REPLY_TO ->
						JCSMPFactory.onlyInstance().createQueue(RandomStringUtils.randomAlphanumeric(10));
				case SolaceHeaders.USER_DATA ->
						RandomStringUtils.randomAlphanumeric(10).getBytes();
				default -> {
					fail(String.format("no test for header %s", header.getKey()));
					yield null;
				}
			}));
		}

		SmfMessageWriterProperties serializationProperties = new SmfMessageWriterProperties(
				new SolaceProducerProperties());
		serializationProperties.getHeaderExclusions().addAll(writeableHeaders.stream()
				.map(Map.Entry::getKey)
				.toList());

		Message<?> testSpringMessage = messageBuilder.build();
		XMLMessage xmlMessage = xmlMessageMapper.mapToSmf(testSpringMessage, serializationProperties);

		for (Map.Entry<String, ? extends HeaderMeta<?>> header : writeableHeaders) {
			Object expectedValue = testSpringMessage.getHeaders().get(header.getKey());
			switch (header.getKey()) {
				case SolaceHeaders.APPLICATION_MESSAGE_ID ->
						assertEquals(expectedValue, xmlMessage.getApplicationMessageId());
				case SolaceHeaders.APPLICATION_MESSAGE_TYPE ->
						assertEquals(expectedValue, xmlMessage.getApplicationMessageType());
				case SolaceHeaders.CORRELATION_ID ->
						assertEquals(expectedValue, xmlMessage.getCorrelationId());
				case SolaceHeaders.DMQ_ELIGIBLE ->
						assertEquals(expectedValue, xmlMessage.isDMQEligible());
				case SolaceHeaders.EXPIRATION ->
						assertEquals(expectedValue, xmlMessage.getExpiration());
				case SolaceHeaders.HTTP_CONTENT_ENCODING ->
						assertEquals(expectedValue, xmlMessage.getHTTPContentEncoding());
				case SolaceHeaders.IS_REPLY ->
						assertEquals(expectedValue, xmlMessage.isReplyMessage());
				case SolaceHeaders.PRIORITY ->
					assertEquals(expectedValue, xmlMessage.getPriority());
				case SolaceHeaders.REPLY_TO ->
						assertEquals(expectedValue, xmlMessage.getReplyTo());
				case SolaceHeaders.SENDER_ID ->
						assertEquals(expectedValue, xmlMessage.getSenderId());
				case SolaceHeaders.SENDER_TIMESTAMP ->
						assertEquals(expectedValue, xmlMessage.getSenderTimestamp());
				case SolaceHeaders.SEQUENCE_NUMBER ->
						assertEquals(expectedValue, xmlMessage.getSequenceNumber());
				case SolaceHeaders.TIME_TO_LIVE ->
						assertEquals(expectedValue, xmlMessage.getTimeToLive());
				case SolaceHeaders.USER_DATA ->
						assertEquals(expectedValue, xmlMessage.getUserData());
				case SolaceBinderHeaders.PARTITION_KEY ->
						assertEquals(expectedValue, xmlMessage.getProperties()
								.getString(XMLMessage.MessageUserPropertyConstants.QUEUE_PARTITION_KEY));
				default ->
						fail(String.format("no test for header %s", header.getKey()));
			}
		}

		Assertions.assertThat(SolaceBinderHeaderMeta.META
						.entrySet()
						.stream()
						.filter(e -> SolaceHeaderMeta.Scope.WIRE.equals(e.getValue().getScope()))
						.filter(e -> !e.getValue().isWritable()) // already tested earlier in this test
						.map(Map.Entry::getKey))
				.allSatisfy(h -> Assertions.assertThat(xmlMessage.getProperties().get(h)).isNotNull());

		Mockito.verify(xmlMessageMapper).mapToSmf(testSpringMessage, serializationProperties);
	}

	@CartesianTest
	void testMapSpringMessageToXMLMessage_WithHeaderTypeCompatibility_NativeOnlyValues(
			@CartesianTest.Enum SmfMessageHeaderWriteCompatibility headerTypeCompatibility) {
		Message<?> testSpringMessage = new DefaultMessageBuilderFactory()
				.withPayload("test")
				.setHeader("test-bigint", BigInteger.ONE)
				.setHeader("test-boolean", true)
				.setHeader("test-byte", (byte) 1)
				.setHeader("test-byte[]", new byte[] { 1, 2, 3 })
				.setHeader("test-bytearray", new ByteArray(new byte[] { 1, 2, 3 }))
				.setHeader("test-char", 'a')
				.setHeader("test-destination", JCSMPFactory.onlyInstance().createTopic("test"))
				.setHeader("test-double", 1.0)
				.setHeader("test-float", 1.0f)
				.setHeader("test-integer", 1)
				.setHeader("test-long", 1L)
				.setHeader("test-map", JCSMPFactory.onlyInstance().createMap())
				.setHeader("test-short", (short) 1)
				.setHeader("test-stream", JCSMPFactory.onlyInstance().createStream())
				.setHeader("test-string", "test")
				.setHeader("test-unsupported", new Object())
				.build();

		SmfMessageWriterProperties writerProperties = new SmfMessageWriterProperties(new SolaceProducerProperties());
		writerProperties.setHeaderTypeCompatibility(headerTypeCompatibility);
		writerProperties.getHeaderExclusions().add(MessageHeaders.ID);
		writerProperties.getHeaderExclusions().add("test-unsupported");
		XMLMessage xmlMessage = xmlMessageMapper.mapToSmf(testSpringMessage, writerProperties);

		Assertions.assertThat(xmlMessage.getProperties())
				.satisfies(p -> Assertions.assertThat(p.keySet()).containsExactlyInAnyOrderElementsOf(
						Stream.concat(Stream.of(SolaceBinderHeaders.MESSAGE_VERSION),
										testSpringMessage.getHeaders().keySet().stream())
								.filter(k -> !MessageHeaders.ID.equals(k))
								.filter(k -> !"test-unsupported".equals(k))
								.toList()))
				.satisfies(p -> Assertions.assertThat(p.get("test-bigint")).isEqualTo(BigInteger.ONE),
						p -> Assertions.assertThat(p.getBoolean("test-boolean")).isTrue(),
						p -> Assertions.assertThat(p.getByte("test-byte")).isEqualTo((byte) 1),
						p -> Assertions.assertThat(p.getBytes("test-byte[]")).isEqualTo(new byte[] { 1, 2, 3 }),
						p -> Assertions.assertThat(p.getByteArray("test-bytearray")).isEqualTo(new ByteArray(new byte[] { 1, 2, 3 })),
						p -> Assertions.assertThat(p.getCharacter("test-char")).isEqualTo('a'),
						p -> Assertions.assertThat(p.getDestination("test-destination"))
								.isEqualTo(JCSMPFactory.onlyInstance().createTopic("test")),
						p -> Assertions.assertThat(p.getDouble("test-double")).isEqualTo(1.0),
						p -> Assertions.assertThat(p.getFloat("test-float")).isEqualTo(1.0f),
						p -> Assertions.assertThat(p.getInteger("test-integer")).isEqualTo(1),
						p -> Assertions.assertThat(p.getLong("test-long")).isEqualTo(1L),
						p -> Assertions.assertThat(p.getMap("test-map")).isNotNull(),
						p -> Assertions.assertThat(p.getShort("test-short")).isEqualTo((short) 1),
						p -> Assertions.assertThat(p.getStream("test-stream")).isNotNull(),
						p -> Assertions.assertThat(p.getString("test-string")).isEqualTo("test"));
	}

	@CartesianTest
	void testMapSpringMessageToXMLMessage_WithHeaderTypeCompatibility_SerializableValue(
			@CartesianTest.Enum SmfMessageHeaderWriteCompatibility headerTypeCompatibility) throws Exception {
		Message<?> testSpringMessage = new DefaultMessageBuilderFactory()
				.withPayload("test")
				.setHeader("test-serializable", new SerializableFoo("a", "b"))
				.build();

		SmfMessageWriterProperties writerProperties = new SmfMessageWriterProperties(new SolaceProducerProperties());
		writerProperties.setHeaderTypeCompatibility(headerTypeCompatibility);

		switch (headerTypeCompatibility) {
			case NATIVE_ONLY -> assertThatThrownBy(() -> xmlMessageMapper.mapToSmf(testSpringMessage, writerProperties))
					.isInstanceOf(SolaceMessageConversionException.class)
					.rootCause()
					.isInstanceOf(IllegalArgumentException.class)
					.hasMessage("Invalid type as value - %s", SerializableFoo.class.getSimpleName());
			case SERIALIZE_AND_ENCODE_NON_NATIVE_TYPES -> {
				XMLMessage xmlMessage = xmlMessageMapper.mapToSmf(testSpringMessage, writerProperties);
				Assertions.assertThat(xmlMessage.getProperties().getString("test-serializable"))
						.isEqualTo(Base64.getEncoder().encodeToString(SerializationUtils.serialize(new SerializableFoo("a", "b"))));
			}
			default -> fail("Unexpected header type compatibility: " + headerTypeCompatibility);
		}
	}

	@ParameterizedTest
	@MethodSource("xmlMessageTypeProviders")
	<T, MT extends XMLMessage> void testMapXMLMessageToSpringMessage(
			XmlMessageTypeProvider<T, MT> xmlMessageTypeProvider) throws Throwable {
		MT xmlMessage = JCSMPFactory.onlyInstance().createMessage(xmlMessageTypeProvider.getXmlMessageType());
		T expectedPayload = xmlMessageTypeProvider.createPayload();
		xmlMessageTypeProvider.setXMLMessagePayload(xmlMessage, expectedPayload);

		SDTMap metadata = JCSMPFactory.onlyInstance().createMap();
		metadata.putString(MessageHeaders.CONTENT_TYPE, xmlMessageTypeProvider.getMimeType().toString());
		metadata.putString("test-header-1", "test-header-val-1");
		metadata.putString("test-header-2", "test-header-val-2");
		xmlMessageTypeProvider.injectAdditionalXMLMessageProperties(metadata);
		xmlMessage.setProperties(metadata);

		AcknowledgmentCallback acknowledgmentCallback = Mockito.mock(AcknowledgmentCallback.class);
		SolaceConsumerProperties consumerProperties = new SolaceConsumerProperties();
		SmfMessageReaderProperties smfMessageReaderProperties = new SmfMessageReaderProperties(consumerProperties);
		Message<?> springMessage = xmlMessageMapper.mapToSpring(xmlMessage, acknowledgmentCallback, smfMessageReaderProperties);
		Mockito.verify(xmlMessageMapper).mapToSpring(xmlMessage, acknowledgmentCallback, false, smfMessageReaderProperties);

		validateSpringPayload(springMessage.getPayload(), expectedPayload);
		validateSpringHeaders(springMessage.getHeaders(), xmlMessage);
		assertNull(StaticMessageHeaderAccessor.getSourceData(springMessage));
	}

	@ParameterizedTest
	@MethodSource("xmlMessageTypeProviders")
	<T, MT extends XMLMessage> void testMapXMLMessageToSpringMessageBatch(
			XmlMessageTypeProvider<T, MT> xmlMessageTypeProvider) {
		List<T> expectedPayloads = IntStream.range(0, 256)
				.mapToObj(i -> xmlMessageTypeProvider.createPayload())
				.collect(Collectors.toList());
		List<MT> xmlMessages = expectedPayloads.stream()
				.map((ThrowingFunction<T, MT>) payload -> {
					MT m = JCSMPFactory.onlyInstance().createMessage(xmlMessageTypeProvider.getXmlMessageType());
					xmlMessageTypeProvider.setXMLMessagePayload(m, payload);
					SDTMap metadata = JCSMPFactory.onlyInstance().createMap();
					metadata.putString(MessageHeaders.CONTENT_TYPE, xmlMessageTypeProvider.getMimeType().toString());
					metadata.putString("test-header-1", "test-header-val-1");
					metadata.putString("test-header-2", "test-header-val-2");
					xmlMessageTypeProvider.injectAdditionalXMLMessageProperties(metadata);
					m.setProperties(metadata);
					return m;
				})
				.collect(Collectors.toList());

		AcknowledgmentCallback acknowledgmentCallback = Mockito.mock(AcknowledgmentCallback.class);
		SolaceConsumerProperties consumerProperties = new SolaceConsumerProperties();
		SmfMessageReaderProperties smfMessageReaderProperties = new SmfMessageReaderProperties(consumerProperties);
		Message<List<?>> springMessage = xmlMessageMapper.mapBatchedToSpring(xmlMessages, acknowledgmentCallback, smfMessageReaderProperties);
		Mockito.verify(xmlMessageMapper).mapBatchedToSpring(xmlMessages, acknowledgmentCallback, false, smfMessageReaderProperties);

		validateSpringBatchPayload(springMessage.getPayload(), expectedPayloads);
		validateSpringBatchHeaders(springMessage.getHeaders(), xmlMessages);
		assertNull(StaticMessageHeaderAccessor.getSourceData(springMessage));
	}

	@ParameterizedTest
	@MethodSource("xmlMessageTypeProviders")
	<T, MT extends XMLMessage> void testMapXMLMessageToSpringMessage_WithRawMessageHeader(
			XmlMessageTypeProvider<T, MT> xmlMessageTypeProvider) throws Throwable {
		MT xmlMessage = JCSMPFactory.onlyInstance().createMessage(xmlMessageTypeProvider.getXmlMessageType());
		T expectedPayload = xmlMessageTypeProvider.createPayload();
		xmlMessageTypeProvider.setXMLMessagePayload(xmlMessage, expectedPayload);

		SDTMap metadata = JCSMPFactory.onlyInstance().createMap();
		metadata.putString(MessageHeaders.CONTENT_TYPE, xmlMessageTypeProvider.getMimeType().toString());
		metadata.putString("test-header-1", "test-header-val-1");
		metadata.putString("test-header-2", "test-header-val-2");
		xmlMessageTypeProvider.injectAdditionalXMLMessageProperties(metadata);
		xmlMessage.setProperties(metadata);

		AcknowledgmentCallback acknowledgmentCallback = Mockito.mock(AcknowledgmentCallback.class);
		SolaceConsumerProperties consumerProperties = new SolaceConsumerProperties();
		SmfMessageReaderProperties smfMessageReaderProperties = new SmfMessageReaderProperties(consumerProperties);
		Message<?> springMessage = xmlMessageMapper.mapToSpring(xmlMessage, acknowledgmentCallback, true, smfMessageReaderProperties);

		validateSpringPayload(springMessage.getPayload(), expectedPayload);
		validateSpringHeaders(springMessage.getHeaders(), xmlMessage);
		assertEquals(xmlMessage, StaticMessageHeaderAccessor.getSourceData(springMessage));
	}

	@ParameterizedTest
	@MethodSource("xmlMessageTypeProviders")
	<T, MT extends XMLMessage> void testMapXMLMessageToSpringMessageBatch_WithRawMessageHeader(
			XmlMessageTypeProvider<T, MT> xmlMessageTypeProvider) {
		List<T> expectedPayloads = IntStream.range(0, 256)
				.mapToObj(i -> xmlMessageTypeProvider.createPayload())
				.collect(Collectors.toList());
		List<MT> xmlMessages = expectedPayloads.stream()
				.map((ThrowingFunction<T, MT>) payload -> {
					MT m = JCSMPFactory.onlyInstance().createMessage(xmlMessageTypeProvider.getXmlMessageType());
					xmlMessageTypeProvider.setXMLMessagePayload(m, payload);
					SDTMap metadata = JCSMPFactory.onlyInstance().createMap();
					metadata.putString(MessageHeaders.CONTENT_TYPE, xmlMessageTypeProvider.getMimeType().toString());
					metadata.putString("test-header-1", "test-header-val-1");
					metadata.putString("test-header-2", "test-header-val-2");
					xmlMessageTypeProvider.injectAdditionalXMLMessageProperties(metadata);
					m.setProperties(metadata);
					return m;
				})
				.collect(Collectors.toList());

		AcknowledgmentCallback acknowledgmentCallback = Mockito.mock(AcknowledgmentCallback.class);
		SolaceConsumerProperties consumerProperties = new SolaceConsumerProperties();
		SmfMessageReaderProperties smfMessageReaderProperties = new SmfMessageReaderProperties(consumerProperties);
		Message<List<?>> springMessage = xmlMessageMapper.mapBatchedToSpring(xmlMessages, acknowledgmentCallback, true, smfMessageReaderProperties);

		validateSpringBatchPayload(springMessage.getPayload(), expectedPayloads);
		validateSpringBatchHeaders(springMessage.getHeaders(), xmlMessages);
		assertEquals(xmlMessages, StaticMessageHeaderAccessor.getSourceData(springMessage));
		Assertions.assertThat(springMessage.getHeaders())
				.extractingByKey(SolaceBinderHeaders.BATCHED_HEADERS)
				.asList()
				.allSatisfy(springMessageHeaders -> Assertions.assertThat(springMessageHeaders)
						.asInstanceOf(InstanceOfAssertFactories.map(String.class, Object.class))
						.doesNotContainKey(IntegrationMessageHeaderAccessor.SOURCE_DATA));
	}

	@CartesianTest(name = "[{index}] {0} batchMode={1}")
	<T, MT extends XMLMessage> void testMapXMLMessageToSpringMessage_WithContentTypeHeaderAndHTTPContentType(
			@CartesianArgumentsSource(XmlMessageTypeCartesianProvider.class)
					Named<XmlMessageTypeProvider<T, MT>> namedXmlMessageTypeProvider,
			@Values(booleans = {false, true}) boolean batchMode) throws Throwable {
		XmlMessageTypeProvider<T, MT> xmlMessageTypeProvider = namedXmlMessageTypeProvider.getPayload();
		MT xmlMessage = JCSMPFactory.onlyInstance().createMessage(xmlMessageTypeProvider.getXmlMessageType());
		T expectedPayload = xmlMessageTypeProvider.createPayload();
		xmlMessageTypeProvider.setXMLMessagePayload(xmlMessage, expectedPayload);

		SDTMap metadata = JCSMPFactory.onlyInstance().createMap();
		metadata.putString(MessageHeaders.CONTENT_TYPE, xmlMessageTypeProvider.getMimeType().toString());
		xmlMessageTypeProvider.injectAdditionalXMLMessageProperties(metadata);
		xmlMessage.setProperties(metadata);
		xmlMessage.setHTTPContentType(MimeTypeUtils.TEXT_HTML_VALUE);

		AcknowledgmentCallback acknowledgmentCallback = Mockito.mock(AcknowledgmentCallback.class);
		SolaceConsumerProperties consumerProperties = new SolaceConsumerProperties();
		SmfMessageReaderProperties smfMessageReaderProperties = new SmfMessageReaderProperties(consumerProperties);
		Message<?> springMessage;
		MessageHeaders springMessageHeaders;
		if (batchMode) {
			List<MT> xmlMessages = Collections.singletonList(xmlMessage);
			springMessage = xmlMessageMapper.mapBatchedToSpring(xmlMessages, acknowledgmentCallback, smfMessageReaderProperties);
			Mockito.verify(xmlMessageMapper).mapBatchedToSpring(xmlMessages, acknowledgmentCallback, false, smfMessageReaderProperties);
			@SuppressWarnings("unchecked")
			Map<String, Object> messageHeaders = (Map<String, Object>) Objects.requireNonNull(springMessage.getHeaders()
					.get(SolaceBinderHeaders.BATCHED_HEADERS, List.class)).get(0);
			springMessageHeaders = new MessageHeaders(messageHeaders);
		} else {
			springMessage = xmlMessageMapper.mapToSpring(xmlMessage, acknowledgmentCallback, smfMessageReaderProperties);
			Mockito.verify(xmlMessageMapper).mapToSpring(xmlMessage, acknowledgmentCallback, false, smfMessageReaderProperties);
			springMessageHeaders = springMessage.getHeaders();
		}

		assertEquals(metadata.getString(MessageHeaders.CONTENT_TYPE),
				springMessageHeaders.get(MessageHeaders.CONTENT_TYPE));

		if (batchMode) {
			validateSpringBatchHeaders(springMessage.getHeaders(), Collections.singletonList(xmlMessage));
		} else {
			validateSpringHeaders(springMessage.getHeaders(), xmlMessage);
		}
	}

	@ParameterizedTest(name = "[{index}] batchMode={0}")
	@ValueSource(booleans = {false, true})
	void testMapXMLMessageToSpringMessage_ReadSolaceProperties(boolean batchMode) throws Exception {
		Set<Map.Entry<String, ? extends HeaderMeta<?>>> readableHeaders = Stream.of(
					SolaceHeaderMeta.META.entrySet().stream(),
					SolaceBinderHeaderMeta.META.entrySet().stream())
				.flatMap(h -> h)
				.filter(h -> h.getValue().isReadable())
				.collect(Collectors.toSet());
		assertNotEquals(0, readableHeaders.size(), "Test header set was empty");

		XMLMessage defaultXmlMessage = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
		TextMessage xmlMessage = Mockito.mock(TextMessage.class); // Some properties are read-only. Need to mock
		SDTMap metadata = JCSMPFactory.onlyInstance().createMap();

		for (Map.Entry<String, ? extends HeaderMeta<?>> header : readableHeaders) {
			if (!HeaderMeta.Scope.WIRE.equals(header.getValue().getScope())) continue;
			switch (header.getKey()) {
				case SolaceHeaders.APPLICATION_MESSAGE_ID:
					Mockito.when(xmlMessage.getApplicationMessageId()).thenReturn(header.getKey());
					break;
				case SolaceHeaders.APPLICATION_MESSAGE_TYPE:
					Mockito.when(xmlMessage.getApplicationMessageType()).thenReturn(header.getKey());
					break;
				case SolaceHeaders.CORRELATION_ID:
					Mockito.when(xmlMessage.getCorrelationId()).thenReturn(header.getKey());
					break;
				case SolaceHeaders.DELIVERY_COUNT:
					Mockito.when(xmlMessage.getDeliveryCount()).thenThrow(new UnsupportedOperationException("Feature is disabled"));
					break;
				case SolaceHeaders.DESTINATION:
					Mockito.when(xmlMessage.getDestination())
							.thenReturn(JCSMPFactory.onlyInstance().createQueue(header.getKey()));
					break;
				case SolaceHeaders.DISCARD_INDICATION:
					Mockito.when(xmlMessage.getDiscardIndication())
							.thenReturn(!defaultXmlMessage.getDiscardIndication());
					break;
				case SolaceHeaders.DMQ_ELIGIBLE:
					Mockito.when(xmlMessage.isDMQEligible()).thenReturn(!defaultXmlMessage.isDMQEligible());
					break;
				case SolaceHeaders.EXPIRATION:
					Mockito.when(xmlMessage.getExpiration()).thenReturn(ThreadLocalRandom.current().nextLong());
					break;
				case SolaceHeaders.IS_REPLY:
					Mockito.when(xmlMessage.isReplyMessage()).thenReturn(!defaultXmlMessage.isReplyMessage());
					break;
				case SolaceHeaders.HTTP_CONTENT_ENCODING:
					Mockito.when(xmlMessage.getHTTPContentEncoding()).thenReturn(header.getKey());
					break;
				case SolaceHeaders.PRIORITY:
					Mockito.when(xmlMessage.getPriority()).thenReturn(ThreadLocalRandom.current().nextInt());
					break;
				case SolaceHeaders.RECEIVE_TIMESTAMP:
					Mockito.when(xmlMessage.getReceiveTimestamp()).thenReturn(ThreadLocalRandom.current().nextLong());
					break;
				case SolaceHeaders.REDELIVERED:
					Mockito.when(xmlMessage.getRedelivered()).thenReturn(!defaultXmlMessage.getRedelivered());
					break;
				case SolaceHeaders.REPLICATION_GROUP_MESSAGE_ID:
					Mockito.when(xmlMessage.getReplicationGroupMessageId()).thenReturn(Mockito.mock(ReplicationGroupMessageId.class));
					break;
				case SolaceHeaders.REPLY_TO:
					Mockito.when(xmlMessage.getReplyTo())
							.thenReturn(JCSMPFactory.onlyInstance().createQueue(header.getKey()));
					break;
				case SolaceHeaders.SENDER_ID:
					Mockito.when(xmlMessage.getSenderId()).thenReturn(header.getKey());
					break;
				case SolaceHeaders.SENDER_TIMESTAMP:
					Mockito.when(xmlMessage.getSenderTimestamp()).thenReturn(ThreadLocalRandom.current().nextLong());
					break;
				case SolaceHeaders.SEQUENCE_NUMBER:
					Mockito.when(xmlMessage.getSequenceNumber()).thenReturn(ThreadLocalRandom.current().nextLong());
					break;
				case SolaceHeaders.TIME_TO_LIVE:
					Mockito.when(xmlMessage.getTimeToLive()).thenReturn(ThreadLocalRandom.current().nextLong());
					break;
				case SolaceHeaders.USER_DATA:
					Mockito.when(xmlMessage.getUserData()).thenReturn(header.getKey().getBytes());
					break;
				case SolaceBinderHeaders.MESSAGE_VERSION:
					metadata.putInteger(header.getKey(), ThreadLocalRandom.current().nextInt());
					break;
				default:
					fail(String.format("no test for header %s", header.getKey()));
			}
		}

		Mockito.when(xmlMessage.getProperties()).thenReturn(metadata);
		Mockito.when(xmlMessage.getText()).thenReturn("testPayload");
		metadata.putString(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE);

		AcknowledgmentCallback acknowledgmentCallback = Mockito.mock(AcknowledgmentCallback.class);
		SolaceConsumerProperties consumerProperties = new SolaceConsumerProperties();
		SmfMessageReaderProperties smfMessageReaderProperties = new SmfMessageReaderProperties(consumerProperties);
		Message<?> springMessage;
		MessageHeaders springMessageHeaders;
		if (batchMode) {
			List<TextMessage> xmlMessages = Collections.singletonList(xmlMessage);
			springMessage = xmlMessageMapper.mapBatchedToSpring(xmlMessages, acknowledgmentCallback, smfMessageReaderProperties);
			Mockito.verify(xmlMessageMapper).mapBatchedToSpring(xmlMessages, acknowledgmentCallback, false, smfMessageReaderProperties);

			@SuppressWarnings("unchecked")
			Map<String, Object> messageHeaders = (Map<String, Object>) Objects.requireNonNull(springMessage.getHeaders()
							.get(SolaceBinderHeaders.BATCHED_HEADERS, List.class)).get(0);
			springMessageHeaders = new MessageHeaders(messageHeaders);
		} else {
			springMessage = xmlMessageMapper.mapToSpring(xmlMessage, acknowledgmentCallback, smfMessageReaderProperties);
			Mockito.verify(xmlMessageMapper).mapToSpring(xmlMessage, acknowledgmentCallback, false, smfMessageReaderProperties);
			springMessageHeaders = springMessage.getHeaders();
		}

		for (Map.Entry<String, ? extends HeaderMeta<?>> header : readableHeaders) {
			Object actualValue = springMessageHeaders.get(header.getKey());
			switch (header.getKey()) {
				case SolaceHeaders.APPLICATION_MESSAGE_ID:
					assertEquals(xmlMessage.getApplicationMessageId(), actualValue);
					break;
				case SolaceHeaders.APPLICATION_MESSAGE_TYPE:
					assertEquals(xmlMessage.getApplicationMessageType(), actualValue);
					break;
				case SolaceHeaders.CORRELATION_ID:
					assertEquals(xmlMessage.getCorrelationId(), actualValue);
					break;
				case SolaceHeaders.DELIVERY_COUNT:
					//For this test, the delivery count feature is disabled
					assertNull(actualValue);
					break;
				case SolaceHeaders.DESTINATION:
					assertEquals(xmlMessage.getDestination(), actualValue);
					break;
				case SolaceHeaders.DISCARD_INDICATION:
					assertEquals(xmlMessage.getDiscardIndication(), actualValue);
					break;
				case SolaceHeaders.DMQ_ELIGIBLE:
					assertEquals(xmlMessage.isDMQEligible(), actualValue);
					break;
				case SolaceHeaders.EXPIRATION:
					assertEquals(xmlMessage.getExpiration(), actualValue);
					break;
				case SolaceHeaders.HTTP_CONTENT_ENCODING:
					assertEquals(xmlMessage.getHTTPContentEncoding(), actualValue);
					break;
				case SolaceHeaders.IS_REPLY:
					assertEquals(xmlMessage.isReplyMessage(), actualValue);
					break;
				case SolaceHeaders.PRIORITY:
					assertEquals(xmlMessage.getPriority(), actualValue);
					break;
				case SolaceHeaders.RECEIVE_TIMESTAMP:
					assertEquals(xmlMessage.getReceiveTimestamp(), actualValue);
					break;
				case SolaceHeaders.REDELIVERED:
					assertEquals(xmlMessage.getRedelivered(), actualValue);
					break;
				case SolaceHeaders.REPLICATION_GROUP_MESSAGE_ID:
					assertEquals(xmlMessage.getReplicationGroupMessageId(), actualValue);
					break;
				case SolaceHeaders.REPLY_TO:
					assertEquals(xmlMessage.getReplyTo(), actualValue);
					break;
				case SolaceHeaders.SENDER_ID:
					assertEquals(xmlMessage.getSenderId(), actualValue);
					break;
				case SolaceHeaders.SENDER_TIMESTAMP:
					assertEquals(xmlMessage.getSenderTimestamp(), actualValue);
					break;
				case SolaceHeaders.SEQUENCE_NUMBER:
					assertEquals(xmlMessage.getSequenceNumber(), actualValue);
					break;
				case SolaceHeaders.TIME_TO_LIVE:
					assertEquals(xmlMessage.getTimeToLive(), actualValue);
					break;
				case SolaceHeaders.USER_DATA:
					assertEquals(xmlMessage.getUserData(), actualValue);
					break;
				case SolaceBinderHeaders.MESSAGE_VERSION:
					assertEquals(xmlMessage.getProperties().get(header.getKey()), actualValue);
					break;
				default:
					if (HeaderMeta.Scope.WIRE.equals(header.getValue().getScope())) {
						fail(String.format("no test for header %s", header.getKey()));
					} else {
						assertNull(actualValue, "Only wire-scoped headers can map to the Spring message");
					}
			}
		}

		if (batchMode) {
			validateSpringBatchHeaders(springMessage.getHeaders(), Collections.singletonList(xmlMessage));
		} else {
			validateSpringHeaders(springMessage.getHeaders(), xmlMessage);
		}
	}

	@ParameterizedTest(name = "[{index}] batchMode={0}")
	@ValueSource(booleans = {false, true})
	void testMapXMLMessageToSpringMessage_NonReadableSolaceProperties(boolean batchMode) throws Exception {
		Set<Map.Entry<String, ? extends HeaderMeta<?>>> nonReadableHeaders = Stream.of(
				SolaceHeaderMeta.META.entrySet().stream(),
				SolaceBinderHeaderMeta.META.entrySet().stream())
				.flatMap(h -> h)
				.filter(h -> !h.getValue().isReadable())
				.collect(Collectors.toSet());
		assertNotEquals(0, nonReadableHeaders.size(), "Test header set was empty");

		TextMessage xmlMessage = Mockito.mock(TextMessage.class);
		SDTMap metadata = JCSMPFactory.onlyInstance().createMap();

		for (Map.Entry<String, ? extends HeaderMeta<?>> header : nonReadableHeaders) {
			switch (header.getKey()) {
				case SolaceBinderHeaders.SERIALIZED_HEADERS:
					metadata.putString(header.getKey(), objectWriter.writeValueAsString(Collections.emptyList()));
					break;
				case SolaceBinderHeaders.SERIALIZED_HEADERS_ENCODING:
					metadata.putString(header.getKey(), "base64");
					break;
				case SolaceBinderHeaders.SERIALIZED_PAYLOAD:
					metadata.putBoolean(header.getKey(), false);
					break;
				case SolaceBinderHeaders.CONFIRM_CORRELATION:
					metadata.putString(header.getKey(), "random_string");
					break;
				case SolaceBinderHeaders.TARGET_DESTINATION_TYPE:
					metadata.putString(header.getKey(), "topic");
					break;
				case SolaceBinderHeaders.PARTITION_KEY:
					metadata.putString(header.getKey(), "partitionKey");
					break;
				default:
					fail(String.format("no test for header %s", header.getKey()));
			}
		}

		Mockito.when(xmlMessage.getProperties()).thenReturn(metadata);
		Mockito.when(xmlMessage.getText()).thenReturn("testPayload");
		Mockito.when(xmlMessage.getDeliveryCount()).thenThrow(new UnsupportedOperationException("Feature is disabled"));
		metadata.putString(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE);

		AcknowledgmentCallback acknowledgmentCallback = Mockito.mock(AcknowledgmentCallback.class);
		SolaceConsumerProperties consumerProperties = new SolaceConsumerProperties();
		SmfMessageReaderProperties smfMessageReaderProperties = new SmfMessageReaderProperties(consumerProperties);

		Message<?> springMessage;
		MessageHeaders springMessageHeaders;
		if (batchMode) {
			List<TextMessage> xmlMessages = Collections.singletonList(xmlMessage);
			springMessage = xmlMessageMapper.mapBatchedToSpring(xmlMessages, acknowledgmentCallback, smfMessageReaderProperties);
			Mockito.verify(xmlMessageMapper).mapBatchedToSpring(xmlMessages, acknowledgmentCallback, false, smfMessageReaderProperties);

			@SuppressWarnings("unchecked")
			Map<String, Object> messageHeaders = (Map<String, Object>) Objects.requireNonNull(springMessage.getHeaders()
					.get(SolaceBinderHeaders.BATCHED_HEADERS, List.class)).get(0);
			springMessageHeaders = new MessageHeaders(messageHeaders);
		} else {
			springMessage = xmlMessageMapper.mapToSpring(xmlMessage, acknowledgmentCallback, smfMessageReaderProperties);
			Mockito.verify(xmlMessageMapper).mapToSpring(xmlMessage, acknowledgmentCallback, false, smfMessageReaderProperties);
			springMessageHeaders = springMessage.getHeaders();
		}

		for (Map.Entry<String, ? extends HeaderMeta<?>> header : nonReadableHeaders) {
			assertThat(springMessageHeaders, not(hasKey(header)));
		}

		SDTMap filteredMetadata = JCSMPFactory.onlyInstance().createMap();
		for (String metadataKey : metadata.keySet()) {
			if (nonReadableHeaders.stream().map(Map.Entry::getKey).noneMatch(metadataKey::equals)) {
				filteredMetadata.putObject(metadataKey, metadata.get(metadataKey));
			}
		}

		if (batchMode) {
			validateSpringBatchHeaders(springMessage.getHeaders(), Collections.singletonList(xmlMessage),
					Collections.singletonList(filteredMetadata));
		} else {
			validateSpringHeaders(springMessage.getHeaders(), xmlMessage, filteredMetadata);
		}
	}

	@ParameterizedTest(name = "[{index}] batchMode={0}")
	@ValueSource(booleans = {false, true})
	void testMapXMLMessageToSpringMessage_ReadLocalSolaceProperties(boolean batchMode) throws Exception {
		Set<Map.Entry<String, ? extends HeaderMeta<?>>> readableLocalHeaders = Stream.of(
				SolaceHeaderMeta.META.entrySet().stream(),
				SolaceBinderHeaderMeta.META.entrySet().stream())
				.flatMap(h -> h)
				.filter(h -> h.getValue().isReadable())
				.filter(h -> HeaderMeta.Scope.LOCAL.equals(h.getValue().getScope()))
				.collect(Collectors.toSet());
		assertNotEquals(0, readableLocalHeaders.size(), "Test header set was empty");

		TextMessage xmlMessage = Mockito.mock(TextMessage.class);
		SDTMap metadata = JCSMPFactory.onlyInstance().createMap();

		for (Map.Entry<String, ? extends HeaderMeta<?>> header : readableLocalHeaders) {
			// Since these properties are local-scoped, their wire-values should be ignored
			metadata.putString(header.getKey(), "test");
		}

		Mockito.when(xmlMessage.getProperties()).thenReturn(metadata);
		Mockito.when(xmlMessage.getText()).thenReturn("testPayload");
		Mockito.when(xmlMessage.getDeliveryCount()).thenThrow(new UnsupportedOperationException("Feature is disabled"));
		metadata.putString(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE);

		AcknowledgmentCallback acknowledgmentCallback = Mockito.mock(AcknowledgmentCallback.class);
		SolaceConsumerProperties consumerProperties = new SolaceConsumerProperties();
		SmfMessageReaderProperties smfMessageReaderProperties = new SmfMessageReaderProperties(consumerProperties);

		Message<?> springMessage;
		MessageHeaders springMessageHeaders;
		if (batchMode) {
			List<TextMessage> xmlMessages = Collections.singletonList(xmlMessage);
			springMessage = xmlMessageMapper.mapBatchedToSpring(xmlMessages, acknowledgmentCallback, smfMessageReaderProperties);
			Mockito.verify(xmlMessageMapper).mapBatchedToSpring(xmlMessages, acknowledgmentCallback, false, smfMessageReaderProperties);

			@SuppressWarnings("unchecked")
			Map<String, Object> messageHeaders = (Map<String, Object>) Objects.requireNonNull(springMessage.getHeaders()
					.get(SolaceBinderHeaders.BATCHED_HEADERS, List.class)).get(0);
			springMessageHeaders = new MessageHeaders(messageHeaders);
		} else {
			springMessage = xmlMessageMapper.mapToSpring(xmlMessage, acknowledgmentCallback, smfMessageReaderProperties);
			Mockito.verify(xmlMessageMapper).mapToSpring(xmlMessage, acknowledgmentCallback, false, smfMessageReaderProperties);
			springMessageHeaders = springMessage.getHeaders();
		}

		for (Map.Entry<String, ? extends HeaderMeta<?>> header : readableLocalHeaders) {
			Object actualValue = springMessageHeaders.get(header.getKey());
			switch (header.getKey()) {
				case SolaceBinderHeaders.BATCHED_HEADERS:
				case SolaceBinderHeaders.NULL_PAYLOAD:
					assertNull(actualValue);
					break;
				default:
					fail(String.format("no test for header %s", header.getKey()));
			}
		}

		SDTMap filteredMetadata = JCSMPFactory.onlyInstance().createMap();
		for (String metadataKey : metadata.keySet()) {
			if (readableLocalHeaders.stream().map(Map.Entry::getKey).noneMatch(metadataKey::equals)) {
				filteredMetadata.putObject(metadataKey, metadata.get(metadataKey));
			}
		}

		if (batchMode) {
			validateSpringBatchHeaders(springMessage.getHeaders(), Collections.singletonList(xmlMessage),
					Collections.singletonList(filteredMetadata));
		} else {
			validateSpringHeaders(springMessage.getHeaders(), xmlMessage, filteredMetadata);
		}
	}

	@ParameterizedTest(name = "[{index}] batchMode={0}")
	@ValueSource(booleans = {false, true})
	void testMapXMLMessageToSpringMessage_deliveryCountFeatureEnabled(boolean batchMode) {
		int deliveryCount = 42;
		TextMessage xmlMessage = Mockito.mock(TextMessage.class);
		Mockito.when(xmlMessage.getText()).thenReturn("testPayload");
		Mockito.when(xmlMessage.getDeliveryCount()).thenReturn(deliveryCount);

		AcknowledgmentCallback acknowledgmentCallback = Mockito.mock(AcknowledgmentCallback.class);
		SolaceConsumerProperties consumerProperties = new SolaceConsumerProperties();
		SmfMessageReaderProperties smfMessageReaderProperties = new SmfMessageReaderProperties(consumerProperties);
		MapAssert<String, Object> headersAssert;
		if (batchMode) {
			headersAssert = Assertions.assertThat(Objects.requireNonNull(xmlMessageMapper
									.mapBatchedToSpring(Collections.singletonList(xmlMessage), acknowledgmentCallback, smfMessageReaderProperties)
									.getHeaders()
					.get(SolaceBinderHeaders.BATCHED_HEADERS, List.class))
					.get(0))
					.asInstanceOf(InstanceOfAssertFactories.map(String.class, Object.class));
		} else {
			headersAssert = Assertions.assertThat(xmlMessageMapper.mapToSpring(xmlMessage, acknowledgmentCallback, smfMessageReaderProperties)
					.getHeaders());
		}
		headersAssert.extractingByKey(SolaceHeaders.DELIVERY_COUNT).isEqualTo(deliveryCount);
	}

	@ParameterizedTest(name = "[{index}] batchMode={0}")
	@ValueSource(booleans = {false, true})
	void testMapXMLMessageToSpringMessage_ReadUndefinedSolaceHeader(boolean batchMode) throws Exception {
		String undefinedSolaceHeader1 = "abc124";
		SerializableFoo undefinedSolaceHeader2 = new SerializableFoo("a", "b");
		TextMessage xmlMessage = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
		xmlMessage.setText("test");
		Set<String> serializedHeaders = Collections.singleton("solace_foo2");
		SDTMap metadata = JCSMPFactory.onlyInstance().createMap();
		metadata.putString("solace_foo1", undefinedSolaceHeader1);
		metadata.putBytes("solace_foo2", SerializationUtils.serialize(undefinedSolaceHeader2));
		metadata.putString(SolaceBinderHeaders.SERIALIZED_HEADERS, objectWriter.writeValueAsString(serializedHeaders));
		metadata.putString(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE);
		xmlMessage.setProperties(metadata);

		AcknowledgmentCallback acknowledgmentCallback = Mockito.mock(AcknowledgmentCallback.class);
		SolaceConsumerProperties consumerProperties = new SolaceConsumerProperties();
		SmfMessageReaderProperties smfMessageReaderProperties = new SmfMessageReaderProperties(consumerProperties);

		Message<?> springMessage;
		MessageHeaders springMessageHeaders;
		if (batchMode) {
			List<TextMessage> xmlMessages = Collections.singletonList(xmlMessage);
			springMessage = xmlMessageMapper.mapBatchedToSpring(xmlMessages, acknowledgmentCallback, smfMessageReaderProperties);
			@SuppressWarnings("unchecked")
			Map<String, Object> messageHeaders = (Map<String, Object>) Objects.requireNonNull(springMessage.getHeaders()
					.get(SolaceBinderHeaders.BATCHED_HEADERS, List.class)).get(0);
			springMessageHeaders = new MessageHeaders(messageHeaders);
		} else {
			springMessage = xmlMessageMapper.mapToSpring(xmlMessage, acknowledgmentCallback, smfMessageReaderProperties);
			springMessageHeaders = springMessage.getHeaders();
		}

		assertEquals(undefinedSolaceHeader1, springMessageHeaders.get("solace_foo1", String.class));
		assertEquals(undefinedSolaceHeader2, springMessageHeaders.get("solace_foo2", SerializableFoo.class));

		if (batchMode) {
			validateSpringBatchHeaders(springMessage.getHeaders(), Collections.singletonList(xmlMessage));
		} else {
			validateSpringHeaders(springMessage.getHeaders(), xmlMessage);
		}
	}

	@ParameterizedTest(name = "[{index}] batchMode={0}")
	@ValueSource(booleans = {false, true})
	void testMapXMLMessageToSpringMessage_WithNullPayload(boolean batchMode) {
		BytesMessage xmlMessage = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
		AcknowledgmentCallback acknowledgmentCallback = Mockito.mock(AcknowledgmentCallback.class);
		SolaceConsumerProperties consumerProperties = new SolaceConsumerProperties();
		SmfMessageReaderProperties smfMessageReaderProperties = new SmfMessageReaderProperties(consumerProperties);

		Message<?> springMessage;
		MessageHeaders springMessageHeaders;
		if (batchMode) {
			List<BytesMessage> xmlMessages = Collections.singletonList(xmlMessage);
			springMessage = xmlMessageMapper.mapBatchedToSpring(xmlMessages, acknowledgmentCallback, smfMessageReaderProperties);
			@SuppressWarnings("unchecked")
			Map<String, Object> messageHeaders = (Map<String, Object>) Objects.requireNonNull(springMessage.getHeaders()
					.get(SolaceBinderHeaders.BATCHED_HEADERS, List.class)).get(0);
			springMessageHeaders = new MessageHeaders(messageHeaders);
		} else {
			springMessage = xmlMessageMapper.mapToSpring(xmlMessage, acknowledgmentCallback, smfMessageReaderProperties);
			springMessageHeaders = springMessage.getHeaders();
		}

		assertEquals(Boolean.TRUE, springMessageHeaders.get(SolaceBinderHeaders.NULL_PAYLOAD, Boolean.class));
		if (batchMode) {
			Assertions.assertThat(springMessage.getHeaders()).doesNotContainKey(SolaceBinderHeaders.NULL_PAYLOAD);
		}
	}

	@ParameterizedTest()
	@MethodSource("nullPayloadXMLMessageProvider")
	void testMapXMLMessageToSpringMessage_WhenPayloadSetAsXMLAttachment(BytesXMLMessage xmlMessage) {
		byte[] payload = "Hello World".getBytes();
		xmlMessage.writeBytes(payload); // Write Payload as XML Attachment
		AcknowledgmentCallback acknowledgmentCallback = Mockito.mock(AcknowledgmentCallback.class);
		SolaceConsumerProperties consumerProperties = new SolaceConsumerProperties();
		SmfMessageReaderProperties smfMessageReaderProperties = new SmfMessageReaderProperties(consumerProperties);

		Message<?> springMessage = xmlMessageMapper.mapToSpring(xmlMessage, acknowledgmentCallback, smfMessageReaderProperties);
		MessageHeaders springMessageHeaders = springMessage.getHeaders();

		assertNull(springMessageHeaders.get(SolaceBinderHeaders.NULL_PAYLOAD, Boolean.class));
		if (xmlMessage instanceof XMLContentMessage) {
			validateSpringPayload(springMessage.getPayload(), new String(payload));
		} else {
			validateSpringPayload(springMessage.getPayload(), payload);
		}
	}

	@ParameterizedTest(name = "[{index}] batchMode={0}")
	@ValueSource(booleans = {false, true})
	void testMapXMLMessageToSpringMessage_WithListPayload(boolean batchMode) throws Exception {
		BytesMessage xmlMessage = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
		List<SerializableFoo> expectedPayload = Collections.singletonList(new SerializableFoo(
				RandomStringUtils.randomAlphanumeric(100), RandomStringUtils.randomAlphanumeric(100)));
		xmlMessage.setData(SerializationUtils.serialize(expectedPayload));
		SDTMap metadata = JCSMPFactory.onlyInstance().createMap();
		metadata.putString(MessageHeaders.CONTENT_TYPE, "application/x-java-serialized-object");
		metadata.putBoolean(SolaceBinderHeaders.SERIALIZED_PAYLOAD, true);
		xmlMessage.setProperties(metadata);
		AcknowledgmentCallback acknowledgmentCallback = Mockito.mock(AcknowledgmentCallback.class);
		SolaceConsumerProperties consumerProperties = new SolaceConsumerProperties();
		SmfMessageReaderProperties smfMessageReaderProperties = new SmfMessageReaderProperties(consumerProperties);

		Message<?> springMessage;
		if (batchMode) {
			springMessage = xmlMessageMapper.mapBatchedToSpring(Collections.singletonList(xmlMessage),
					acknowledgmentCallback, smfMessageReaderProperties);
		} else {
			springMessage = xmlMessageMapper.mapToSpring(xmlMessage, acknowledgmentCallback, smfMessageReaderProperties);
		}

		if (batchMode) {
			validateSpringBatchPayload((List<?>) springMessage.getPayload(),
					Collections.singletonList(expectedPayload));
			validateSpringBatchHeaders(springMessage.getHeaders(), Collections.singletonList(xmlMessage));
		} else {
			validateSpringPayload(springMessage.getPayload(), expectedPayload);
			validateSpringHeaders(springMessage.getHeaders(), xmlMessage);
		}
	}

	@Test
	void testMapMessageHeadersToSDTMap_JMSXGroupID() throws Exception {
		String jmsxGroupID = "partition-key-value";
		SDTMap sdtMap = xmlMessageMapper.mapHeadersToSmf(
				new MessageHeaders(Collections.singletonMap(
						XMLMessage.MessageUserPropertyConstants.QUEUE_PARTITION_KEY, jmsxGroupID)),
				new SmfMessageWriterProperties(new SolaceProducerProperties()));
		assertThat(sdtMap.keySet(), hasItem(XMLMessage.MessageUserPropertyConstants.QUEUE_PARTITION_KEY));
		assertEquals(jmsxGroupID, sdtMap.getString(XMLMessage.MessageUserPropertyConstants.QUEUE_PARTITION_KEY));
	}

	@Test
	void testMapMessageHeadersToSDTMap_Serializable() throws Exception {
		String key = "a";
		SerializableFoo value = new SerializableFoo("abc123", "HOOPLA!");
		Map<String,Object> headers = new HashMap<>();
		headers.put(key, value);
		headers.put(BinderHeaders.TARGET_DESTINATION, "redirected-target");

		SDTMap sdtMap = xmlMessageMapper.mapHeadersToSmf(new MessageHeaders(headers),
				new SmfMessageWriterProperties(new SolaceProducerProperties()));

		assertThat(sdtMap.keySet(), hasItem(key));
		assertThat(sdtMap.keySet(), hasItem(SolaceBinderHeaders.SERIALIZED_HEADERS));
		assertThat(sdtMap.keySet(), hasItem(SolaceBinderHeaders.SERIALIZED_HEADERS_ENCODING));
		assertEquals("base64", sdtMap.getString(SolaceBinderHeaders.SERIALIZED_HEADERS_ENCODING));
		assertThat(sdtMap.keySet(), not(hasItem(BinderHeaders.TARGET_DESTINATION)));
		assertEquals(value, SerializationUtils.deserialize(Base64.getDecoder().decode(sdtMap.getString(key))));
		String serializedHeadersJson = sdtMap.getString(SolaceBinderHeaders.SERIALIZED_HEADERS);
		assertThat(serializedHeadersJson, not(emptyString()));
		Set<String> serializedHeaders = objectReader.forType(new TypeReference<Set<String>>() {})
				.readValue(serializedHeadersJson);
		assertThat(serializedHeaders, hasSize(2));
		assertThat(serializedHeaders, hasItem(key));
		assertThat(serializedHeaders, hasItem(MessageHeaders.ID));
	}

	@Test
	void testMapMessageHeadersToSDTMap_NonSerializable() {
		SolaceMessageConversionException thrown = assertThrows(SolaceMessageConversionException.class,
				() -> xmlMessageMapper.mapHeadersToSmf(new MessageHeaders(Collections.singletonMap("a", new Object())),
						new SmfMessageWriterProperties(new SolaceProducerProperties())));
		assertThat(thrown.getCause(), instanceOf(IllegalArgumentException.class));
		assertThat(thrown.getMessage(), containsString("Invalid type as value - Object"));
	}

	@Test
	void testMapMessageHeadersToSDTMap_NonSerializableToString() throws Exception {
		String key = "a";
		Object value = new Object();
		SmfMessageWriterProperties serializationProperties = new SmfMessageWriterProperties(new SolaceProducerProperties());
		serializationProperties.setNonSerializableHeaderConvertToString(true);
		SDTMap sdtMap = xmlMessageMapper.mapHeadersToSmf(new MessageHeaders(Collections.singletonMap(key, value)),
				serializationProperties);
		assertEquals(value.toString(), sdtMap.get(key));
	}

	@Test
	void testMapMessageHeadersToSDTMap_Null() throws Exception {
		String key = "a";
		Map<String,Object> headers = Collections.singletonMap(key, null);
		SDTMap sdtMap = xmlMessageMapper.mapHeadersToSmf(new MessageHeaders(headers),
				new SmfMessageWriterProperties(new SolaceProducerProperties()));
		assertThat(sdtMap.keySet(), hasItem(key));
		assertNull(sdtMap.get(key));
	}

	@Test
	void testMapMessageHeadersToSDTMap_NonJmsCompatible() throws Exception {
		byte[] value = "test".getBytes(); // byte[] values are not supported by JMS
		Map<String,Object> headers = new HashMap<>();
		JMS_INVALID_HEADER_NAMES.forEach(h -> headers.put(h, value));

		SDTMap sdtMap = xmlMessageMapper.mapHeadersToSmf(new MessageHeaders(headers),
				new SmfMessageWriterProperties(new SolaceProducerProperties()));

		for (String header : JMS_INVALID_HEADER_NAMES) {
			assertThat(sdtMap.keySet(), hasItem(header));
			assertEquals(value, sdtMap.getBytes(header));
		}
	}

	@Test
	void testMapSDTMapToMessageHeaders_Serializable() throws Exception {
		String key = "a";
		SerializableFoo value = new SerializableFoo("abc123", "HOOPLA!");
		SDTMap sdtMap = JCSMPFactory.onlyInstance().createMap();
		sdtMap.putObject(key, SerializationUtils.serialize(value));
		List<String> serializedHeaders = Arrays.asList(key, key);
		sdtMap.putString(SolaceBinderHeaders.SERIALIZED_HEADERS, objectWriter.writeValueAsString(serializedHeaders));

		MessageHeaders messageHeaders = xmlMessageMapper.mapHeadersToSpring(sdtMap,
				new SmfMessageReaderProperties(new SolaceConsumerProperties()));

		assertThat(messageHeaders.keySet(), hasItem(key));
		assertThat(messageHeaders.keySet(), not(hasItem(SolaceBinderHeaders.SERIALIZED_HEADERS)));
		assertEquals(value, messageHeaders.get(key));
		assertNull(messageHeaders.get(SolaceBinderHeaders.SERIALIZED_HEADERS));
	}

	@Test
	void testMapSDTMapToMessageHeaders_Null() throws Exception {
		String key = "a";
		SDTMap sdtMap = JCSMPFactory.onlyInstance().createMap();
		sdtMap.putObject(key, null);

		MessageHeaders messageHeaders = xmlMessageMapper.mapHeadersToSpring(sdtMap,
				new SmfMessageReaderProperties(new SolaceConsumerProperties()));

		assertThat(messageHeaders.keySet(), hasItem(key));
		assertNull(messageHeaders.get(key));
	}

	@ParameterizedTest(name = "[{index}] batchMode={0}")
	@ValueSource(booleans = {false, true})
	void testMapXMLMessageToSpringMessage_WithExcludedHeader(boolean batchMode)
			throws SDTException {
		List<String> excludedHeaders = List.of("headerKey1", "headerKey2", "headerKey5",
				"solace_expiration", "solace_discardIndication", "solace_redelivered",
				"solace_dmqEligible", "solace_priority");
		BytesMessage xmlMessage = Mockito.spy(JCSMPFactory.onlyInstance().createMessage(BytesMessage.class));
		AcknowledgmentCallback acknowledgmentCallback = Mockito.mock(AcknowledgmentCallback.class);
		SolaceConsumerProperties consumerProperties = new SolaceConsumerProperties();
		consumerProperties.setHeaderExclusions(excludedHeaders);
		SmfMessageReaderProperties smfMessageReaderProperties = new SmfMessageReaderProperties(consumerProperties);

		SDTMap metadata = JCSMPFactory.onlyInstance().createMap();
		Mockito.when(xmlMessage.getProperties()).thenReturn(metadata);
		for (int i = 0; i < 10; i++) {
			metadata.putObject("headerKey" + i, UUID.randomUUID().toString());
		}

		Message<?> springMessage;
		MessageHeaders springMessageHeaders;
		if (batchMode) {
			List<BytesMessage> xmlMessages = Collections.singletonList(xmlMessage);
			springMessage = xmlMessageMapper.mapBatchedToSpring(xmlMessages, acknowledgmentCallback, smfMessageReaderProperties);
			@SuppressWarnings("unchecked")
			Map<String, Object> messageHeaders = (Map<String, Object>) Objects.requireNonNull(springMessage.getHeaders()
					.get(SolaceBinderHeaders.BATCHED_HEADERS, List.class)).get(0);
			springMessageHeaders = new MessageHeaders(messageHeaders);
		} else {
			springMessage = xmlMessageMapper.mapToSpring(xmlMessage, acknowledgmentCallback, smfMessageReaderProperties);
			springMessageHeaders = springMessage.getHeaders();
		}

		for (int i = 0; i < 10; i++) {
			String key = "headerKey" + i;
			if (excludedHeaders.contains(key)) {
				assertFalse(springMessageHeaders.containsKey(key));
			} else {
				assertEquals(metadata.get(key), springMessageHeaders.get(key));
			}
		}
	}

	@Test
	void testMapSDTMapToMessageHeaders_WithExcludedHeader() throws Exception {
		SDTMap sdtMap = JCSMPFactory.onlyInstance().createMap();
		for (int i = 0; i < 10; i++) {
			sdtMap.putObject("headerKey" + i, UUID.randomUUID().toString());
		}

		List<String> excludedHeaders = List.of("headerKey1", "headerKey2", "headerKey5");
		SolaceConsumerProperties solaceConsumerProperties = new SolaceConsumerProperties();
		solaceConsumerProperties.setHeaderExclusions(excludedHeaders);
		MessageHeaders messageHeaders = xmlMessageMapper.mapHeadersToSpring(sdtMap,
				new SmfMessageReaderProperties(solaceConsumerProperties));

		for (int i = 0; i < 10; i++) {
			String key = "headerKey" + i;
			if (excludedHeaders.contains(key)) {
				assertFalse(messageHeaders.containsKey(key));
			} else {
				assertEquals(sdtMap.get(key), messageHeaders.get(key));
			}
		}
	}

	@Test
	void testMapSDTMapToMessageHeaders_WithExcludedHeader_canExcludeBinderHeaders() throws Exception {
		SDTMap sdtMap = JCSMPFactory.onlyInstance().createMap();
		sdtMap.putObject(SolaceBinderHeaders.MESSAGE_VERSION, 10);
		sdtMap.putObject(SolaceBinderHeaders.SERIALIZED_HEADERS, "header1, header2");
		sdtMap.putObject("retainedHeader", "test");

		List<String> excludedHeaders = List.of(SolaceBinderHeaders.MESSAGE_VERSION, SolaceBinderHeaders.SERIALIZED_HEADERS);
		SolaceConsumerProperties solaceConsumerProperties = new SolaceConsumerProperties();
		solaceConsumerProperties.setHeaderExclusions(excludedHeaders);
		MessageHeaders messageHeaders = xmlMessageMapper.mapHeadersToSpring(sdtMap,
				new SmfMessageReaderProperties(solaceConsumerProperties));

		sdtMap.keySet().forEach(key -> {
			if (excludedHeaders.contains(key)) {
				assertFalse(messageHeaders.containsKey(key));
			} else {
				try {
					assertEquals(sdtMap.get(key), messageHeaders.get(key));
				} catch (SDTException e) {
					fail(e);
				}
			}
		});
	}

	@Test
	void testMapSDTMapToMessageHeaders_EncodedSerializable() throws Exception {
		String key = "a";
		SerializableFoo value = new SerializableFoo("abc123", "HOOPLA!");
		SDTMap sdtMap = JCSMPFactory.onlyInstance().createMap();
		sdtMap.putString(key, Base64.getEncoder().encodeToString(SerializationUtils.serialize(value)));
		Set<String> serializedHeaders = Collections.singleton(key);
		sdtMap.putString(SolaceBinderHeaders.SERIALIZED_HEADERS, objectWriter.writeValueAsString(serializedHeaders));
		sdtMap.putString(SolaceBinderHeaders.SERIALIZED_HEADERS_ENCODING, "base64");

		MessageHeaders messageHeaders = xmlMessageMapper.mapHeadersToSpring(sdtMap,
				new SmfMessageReaderProperties(new SolaceConsumerProperties()));

		assertThat(messageHeaders.keySet(), hasItem(key));
		assertThat(messageHeaders.keySet(), not(hasItem(SolaceBinderHeaders.SERIALIZED_HEADERS)));
		assertEquals(value, messageHeaders.get(key));
		assertNull(messageHeaders.get(SolaceBinderHeaders.SERIALIZED_HEADERS));
	}

	@Test
	void testMapSDTMapToMessageHeaders_ExtraSerializableHeader() throws Exception {
		String key = "a";
		SDTMap sdtMap = JCSMPFactory.onlyInstance().createMap();
		Set<String> serializedHeaders = Collections.singleton(key);
		sdtMap.putString(SolaceBinderHeaders.SERIALIZED_HEADERS, objectWriter.writeValueAsString(serializedHeaders));

		MessageHeaders messageHeaders = xmlMessageMapper.mapHeadersToSpring(sdtMap,
				new SmfMessageReaderProperties(new SolaceConsumerProperties()));

		assertThat(messageHeaders.keySet(), not(hasItem(key)));
		assertThat(messageHeaders.keySet(), not(hasItem(SolaceBinderHeaders.SERIALIZED_HEADERS)));
	}

	@Test
	void testMapSDTMapToMessageHeaders_NullSerializableHeader() throws Exception {
		String key = "a";
		SDTMap sdtMap = JCSMPFactory.onlyInstance().createMap();
		sdtMap.putObject(key, null);
		Set<String> serializedHeaders = Collections.singleton(key);
		sdtMap.putString(SolaceBinderHeaders.SERIALIZED_HEADERS, objectWriter.writeValueAsString(serializedHeaders));
		sdtMap.putString(SolaceBinderHeaders.SERIALIZED_HEADERS_ENCODING, "base64");

		MessageHeaders messageHeaders = xmlMessageMapper.mapHeadersToSpring(sdtMap,
				new SmfMessageReaderProperties(new SolaceConsumerProperties()));

		assertThat(messageHeaders.keySet(), hasItem(key));
		assertNull(messageHeaders.get(key));
		assertThat(messageHeaders.keySet(), not(hasItem(SolaceBinderHeaders.SERIALIZED_HEADERS)));
	}

	@Test
	void testFailMapSDTMapToMessageHeaders_InvalidEncoding() throws Exception {
		String key = "a";
		SerializableFoo value = new SerializableFoo("abc123", "HOOPLA!");
		SDTMap sdtMap = JCSMPFactory.onlyInstance().createMap();
		sdtMap.putString(key, Base64.getEncoder().encodeToString(SerializationUtils.serialize(value)));
		Set<String> serializedHeaders = Collections.singleton(key);
		sdtMap.putString(SolaceBinderHeaders.SERIALIZED_HEADERS, objectWriter.writeValueAsString(serializedHeaders));
		sdtMap.putString(SolaceBinderHeaders.SERIALIZED_HEADERS_ENCODING, "abc");

		SolaceMessageConversionException exception = assertThrows(SolaceMessageConversionException.class,
				() -> xmlMessageMapper.mapHeadersToSpring(sdtMap, new SmfMessageReaderProperties(new SolaceConsumerProperties())));
		assertThat(exception.getMessage(), containsString("encoding is not supported"));
	}

	@Test
	void testMapSDTMapToMessageHeaders_NonJmsCompatible() throws Exception {
		byte[] value = "test".getBytes(); // byte[] values are not supported by JMS
		SDTMap sdtMap = JCSMPFactory.onlyInstance().createMap();
		for (String header : JMS_INVALID_HEADER_NAMES) {
			sdtMap.putBytes(header, value);
		}

		MessageHeaders messageHeaders = xmlMessageMapper.mapHeadersToSpring(sdtMap,
				new SmfMessageReaderProperties(new SolaceConsumerProperties()));

		for (String header : JMS_INVALID_HEADER_NAMES) {
			assertThat(messageHeaders.keySet(), hasItem(header));
			assertEquals(value, messageHeaders.get(header, byte[].class));
		}
	}

	@Test
	void testMapLoop() throws Exception {
		Message<?> expectedSpringMessage = new DefaultMessageBuilderFactory()
				.withPayload("testPayload")
				.setHeader("test-header-1", "test-header-val-1")
				.setHeader("test-header-2", "test-header-val-2")
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();
		Map<String,Object> springHeaders = new HashMap<>(expectedSpringMessage.getHeaders());

		SDTMap metadata = JCSMPFactory.onlyInstance().createMap();
		metadata.putString("test-header-1", "test-header-val-1");
		metadata.putString("test-header-2", "test-header-val-2");
		metadata.putString(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE);
		XMLMessage expectedXmlMessage = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
		expectedXmlMessage.setProperties(metadata);
		expectedXmlMessage.setHTTPContentType(MimeTypeUtils.TEXT_PLAIN_VALUE);

		SolaceConsumerProperties consumerProperties = new SolaceConsumerProperties();
		SmfMessageReaderProperties smfMessageReaderProperties = new SmfMessageReaderProperties(consumerProperties);
		Message<?> springMessage = expectedSpringMessage;
		XMLMessage xmlMessage;
		int i = 0;
		do {
			LOGGER.info("Iteration {} - Message<?> to XMLMessage:\n{}", i, springMessage);
			xmlMessage = xmlMessageMapper.mapToSmf(springMessage,
					new SmfMessageWriterProperties(new SolaceProducerProperties()));
			validateXMLProperties(xmlMessage, expectedSpringMessage.getPayload(), expectedSpringMessage.getHeaders(),
					springHeaders);

			LOGGER.info("Iteration {} - XMLMessage to Message<?>:\n{}", i, xmlMessage);
			AcknowledgmentCallback acknowledgmentCallback = Mockito.mock(AcknowledgmentCallback.class);
			springMessage = xmlMessageMapper.mapToSpring(xmlMessage, acknowledgmentCallback, smfMessageReaderProperties);
			validateSpringHeaders(springMessage.getHeaders(), expectedXmlMessage);

			// Update the expected default spring headers
			springHeaders.put(MessageHeaders.ID, springMessage.getHeaders().getId());
			springHeaders.put(MessageHeaders.TIMESTAMP, springMessage.getHeaders().getTimestamp());

			i++;
		} while (i < 3);
	}

	private void validateXMLProperties(XMLMessage xmlMessage, Message<?> springMessage)
			throws Exception {
		validateXMLProperties(xmlMessage, springMessage.getPayload(), springMessage.getHeaders());
	}

	private void validateXMLProperties(XMLMessage xmlMessage,
									   Object springMessagePayload,
									   Map<String, Object> springMessageHeaders)
			throws Exception {
		validateXMLProperties(xmlMessage, springMessagePayload, springMessageHeaders, springMessageHeaders);
	}

	private void validateXMLProperties(
			XMLMessage xmlMessage,
			Object springMessagePayload,
			Map<String, Object> springMessageHeaders,
			Map<String, Object> expectedHeaders) throws Exception {

		assertEquals(DeliveryMode.PERSISTENT, xmlMessage.getDeliveryMode());
		Assertions.assertThat(new GenericMessage<>(springMessagePayload, springMessageHeaders))
				.extracting(StaticMessageHeaderAccessor::getContentType)
				.hasToString(xmlMessage.getHTTPContentType());

		SDTMap metadata = xmlMessage.getProperties();

		assertEquals((Integer) XMLMessageMapper.MESSAGE_VERSION, metadata.getInteger(SolaceBinderHeaders.MESSAGE_VERSION));

		Set<String> serializedHeaders = new HashSet<>();

		if (metadata.containsKey(SolaceBinderHeaders.SERIALIZED_HEADERS)) {
			XMLMessageMapper.Encoder encoder = XMLMessageMapper.Encoder
					.getByName(metadata.getString(SolaceBinderHeaders.SERIALIZED_HEADERS_ENCODING));

			Set<String> serializedHeadersSet = objectReader.forType(new TypeReference<Set<String>>(){})
					.readValue(metadata.getString(SolaceBinderHeaders.SERIALIZED_HEADERS));
			Assertions.assertThat(serializedHeadersSet)
					.isNotEmpty()
					.allSatisfy(h -> Assertions.assertThat(metadata.keySet()).contains(h))
					.allSatisfy(h -> {
						Object headerValue = SerializationUtils.deserialize(encoder != null ?
								encoder.decode(metadata.getString(h)) :
								metadata.getBytes(h));

						if (expectedHeaders.containsKey(h)) {
							assertEquals(expectedHeaders.get(h), headerValue);
						} else {
							assertNotNull(headerValue);
						}
					});

			serializedHeaders.addAll(serializedHeadersSet);
		}

		Map<String, SolaceHeaderMeta<?>> readWriteableSolaceHeaders = SolaceHeaderMeta.META
				.entrySet()
				.stream()
				.filter(h -> h.getValue().isWritable())
				.filter(h -> h.getValue().isReadable())
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

		Assertions.assertThat(expectedHeaders)
				.allSatisfy((headerKey, headerValue) -> {
					if (readWriteableSolaceHeaders.containsKey(headerKey)) {
						Object value = readWriteableSolaceHeaders.get(headerKey).getReadAction().apply(xmlMessage);
						assertEquals(headerValue, value);
					} else if (!serializedHeaders.contains(headerKey)) {
						switch (headerKey) {
							case SolaceBinderHeaders.PARTITION_KEY ->
									headerKey = XMLMessage.MessageUserPropertyConstants.QUEUE_PARTITION_KEY;
							case XMLMessage.MessageUserPropertyConstants.QUEUE_PARTITION_KEY ->
									headerValue = expectedHeaders.getOrDefault(SolaceBinderHeaders.PARTITION_KEY, headerValue);
							case SolaceBinderHeaders.BATCHED_HEADERS,
								 SolaceBinderHeaders.CONFIRM_CORRELATION,
								 SolaceBinderHeaders.TARGET_DESTINATION_TYPE -> {
								// These Spring headers aren't ever reflected in the SMF message
								Assertions.assertThat(metadata.keySet()).doesNotContain(headerKey);
								return;
							}
						}
						Assertions.assertThat(metadata.keySet()).contains(headerKey);
						try {
							Assertions.assertThat(metadata.get(headerKey)).isEqualTo(headerValue);
						} catch (SDTException e) {
							throw new RuntimeException(e);
						}
					}
				});
	}

	private void validateSpringHeaders(MessageHeaders messageHeaders, XMLMessage xmlMessage)
			throws SDTException, JsonProcessingException {
		validateSpringHeaders(messageHeaders, xmlMessage, false, xmlMessage.getProperties());
	}

	private void validateSpringHeaders(MessageHeaders messageHeaders, XMLMessage xmlMessage, SDTMap expectedHeaders)
			throws SDTException, JsonProcessingException {
		validateSpringHeaders(messageHeaders, xmlMessage, false, expectedHeaders);
	}
	private void validateSpringBatchHeaders(MessageHeaders batchMessageHeaders, List<? extends XMLMessage> xmlMessages) {
		validateSpringBatchHeaders(batchMessageHeaders, xmlMessages,
				xmlMessages.stream().map(XMLMessage::getProperties).collect(Collectors.toList()));
	}

	private void validateSpringBatchHeaders(MessageHeaders batchMessageHeaders, List<? extends XMLMessage> xmlMessages,
											List<SDTMap> expectedBatchedHeaders) {
		Assertions.assertThat(batchMessageHeaders)
				.hasEntrySatisfying(IntegrationMessageHeaderAccessor.ACKNOWLEDGMENT_CALLBACK, ackCallback ->
						Assertions.assertThat(ackCallback).isNotNull().isInstanceOf(AcknowledgmentCallback.class))
				.hasEntrySatisfying(IntegrationMessageHeaderAccessor.DELIVERY_ATTEMPT, deliveryAttempt ->
						Assertions.assertThat(deliveryAttempt).isNotNull()
								.asInstanceOf(InstanceOfAssertFactories.ATOMIC_INTEGER).hasValue(0))
				.extractingByKey(SolaceBinderHeaders.BATCHED_HEADERS)
				.isNotNull()
				.asInstanceOf(InstanceOfAssertFactories.list(Map.class))
				.hasSameSizeAs(xmlMessages)
				.hasSameSizeAs(expectedBatchedHeaders)
				.map(messageHeaders -> {
					@SuppressWarnings("unchecked")
					MessageHeaders headers = new MessageHeaders((Map<String, Object>) messageHeaders);
					return headers;
				})
				.satisfies(batchedHeaders -> {
					for (int i = 0; i < xmlMessages.size(); i++) {
						validateSpringHeaders(batchedHeaders.get(i), xmlMessages.get(i), true,
								expectedBatchedHeaders.get(i));
					}
				});
	}

	private void validateSpringHeaders(MessageHeaders messageHeaders, XMLMessage xmlMessage, boolean batchMode,
									   SDTMap expectedHeaders)
			throws SDTException, JsonProcessingException {
		List<String> nonReadableBinderHeaderMeta = SolaceBinderHeaderMeta.META
				.entrySet()
				.stream()
				.filter(h -> !h.getValue().isReadable())
				.map(Map.Entry::getKey)
				.toList();

		Assertions.assertThat(nonReadableBinderHeaderMeta)
				.allSatisfy(customHeaderName ->
						Assertions.assertThat(messageHeaders).doesNotContainKey(customHeaderName));

		Set<String> serializedHeaders = new HashSet<>();
		if (expectedHeaders.containsKey(SolaceBinderHeaders.SERIALIZED_HEADERS)) {
			String serializedHeaderJson = expectedHeaders.getString(SolaceBinderHeaders.SERIALIZED_HEADERS);
			Assertions.assertThat(serializedHeaderJson).isNotEmpty();

			Set<String> serializedHeaderSet = objectReader.forType(new TypeReference<Set<String>>() {})
					.readValue(serializedHeaderJson);
			Assertions.assertThat(serializedHeaderSet)
					.isNotEmpty()
					.allSatisfy(h -> Assertions.assertThat(expectedHeaders.keySet()).contains(h))
					.allSatisfy(h -> Assertions.assertThat(messageHeaders.keySet()).contains(h))
					.allSatisfy(h -> Assertions.assertThat(SerializationUtils.deserialize(expectedHeaders.getBytes(h)))
							.isEqualTo(messageHeaders.get(h)));

			serializedHeaders.addAll(serializedHeaderSet);
		}

		Assertions.assertThat(expectedHeaders.keySet())
				.filteredOn(h -> !(nonReadableBinderHeaderMeta.contains(h) || serializedHeaders.contains(h)))
				.allSatisfy(h -> assertEquals(expectedHeaders.get(h), messageHeaders.get(h)));

		if (batchMode) {
			Assertions.assertThat(messageHeaders)
					.doesNotContainKey(IntegrationMessageHeaderAccessor.ACKNOWLEDGMENT_CALLBACK)
					.doesNotContainKey(IntegrationMessageHeaderAccessor.DELIVERY_ATTEMPT);
		} else {
			Assertions.assertThat(messageHeaders)
					.hasEntrySatisfying(IntegrationMessageHeaderAccessor.ACKNOWLEDGMENT_CALLBACK, v ->
							Assertions.assertThat(v)
									.asInstanceOf(InstanceOfAssertFactories.type(AcknowledgmentCallback.class))
									.isNotNull())
					.hasEntrySatisfying(IntegrationMessageHeaderAccessor.DELIVERY_ATTEMPT, v ->
							Assertions.assertThat(v)
									.asInstanceOf(InstanceOfAssertFactories.ATOMIC_INTEGER)
									.hasValue(0));
		}

		Object contentType = messageHeaders.get(MessageHeaders.CONTENT_TYPE);
		assertNotNull(contentType);
		if (!expectedHeaders.containsKey(MessageHeaders.CONTENT_TYPE)) {
			assertEquals(xmlMessage.getHTTPContentType(), (contentType instanceof MimeType ?
					(MimeType) contentType : MimeType.valueOf(contentType.toString())).toString());
		}

		//DeliveryCount feature is assumed disabled
		Assertions.assertThat(messageHeaders).doesNotContainKey(SolaceHeaders.DELIVERY_COUNT);
	}

	private <T> void validateSpringBatchPayload(List<?> payloads, List<T> expectedPayloads) {
		Assertions.assertThat(payloads).hasSameSizeAs(expectedPayloads);
		Assertions.assertThat(IntStream.range(0, payloads.size())
						.mapToObj(i -> new ImmutablePair<>(payloads.get(i), expectedPayloads.get(i)))
						.toList()
				)
				.allSatisfy(p -> validateSpringPayload(p.getLeft(), p.getRight()));
	}

	private <T> void validateSpringPayload(Object payload, T expectedPayload) {
		Assertions.assertThat(payload).isInstanceOf(expectedPayload.getClass());

		@SuppressWarnings("unchecked")
		T springMessagePayload = (T) expectedPayload.getClass().cast(payload);

		if (expectedPayload.getClass().isArray()) {
			if (expectedPayload.getClass().getComponentType().isPrimitive()) {
				assertEquals(Array.getLength(expectedPayload), Array.getLength(springMessagePayload));
				for (int i = 0; i < Array.getLength(springMessagePayload); i++) {
					assertEquals(Array.get(expectedPayload, i), Array.get(springMessagePayload, i));
				}
			} else {
				assertArrayEquals((Object[]) expectedPayload, (Object[]) springMessagePayload);
			}
		} else {
			assertEquals(expectedPayload, springMessagePayload);
		}
	}

	private static final Map<String, SpringMessageTypeProvider<?, ?>> SPRING_MESSAGE_TYPE_PROVIDERS = Map.ofEntries(
			Map.entry("BYTE_ARRAY", new SpringMessageTypeProvider<>(
					BytesMessage.class,
					MimeTypeUtils.APPLICATION_OCTET_STREAM,
					() -> "testPayload".getBytes(StandardCharsets.UTF_8),
					BytesMessage::getData)),
			Map.entry("STRING", new SpringMessageTypeProvider<>(
					TextMessage.class,
					MimeTypeUtils.TEXT_PLAIN,
					() -> "testPayload",
					TextMessage::getText)),
			Map.entry("SERIALIZABLE", new SpringMessageTypeProvider<>(
					BytesMessage.class,
					new MimeType("application", "x-java-serialized-object"),
					() -> new SerializableFoo("abc123", "HOOPLA!"),
					m -> SerializationUtils.deserialize(m.getData()),
					true)),
			Map.entry("SDT_STREAM", new SpringMessageTypeProvider<>(
					StreamMessage.class,
					new MimeType("application", "x-java-serialized-object"),
					() -> {
						SDTStream sdtStream = JCSMPFactory.onlyInstance().createStream();
						sdtStream.writeBoolean(true);
						sdtStream.writeCharacter('s');
						sdtStream.writeMap(JCSMPFactory.onlyInstance().createMap());
						sdtStream.writeStream(JCSMPFactory.onlyInstance().createStream());
						return sdtStream;
					},
					StreamMessage::getStream)),
			Map.entry("SDT_MAP", new SpringMessageTypeProvider<>(
					MapMessage.class,
					new MimeType("application", "x-java-serialized-object"),
					() -> {
						SDTMap sdtMap = JCSMPFactory.onlyInstance().createMap();
						sdtMap.putBoolean("a", true);
						sdtMap.putCharacter("b", 's');
						sdtMap.putMap("c", JCSMPFactory.onlyInstance().createMap());
						sdtMap.putStream("d", JCSMPFactory.onlyInstance().createStream());
						return sdtMap;
					},
					MapMessage::getMap))
	);

	private static Stream<Arguments> springPayloadTypeProviders() {
		return SPRING_MESSAGE_TYPE_PROVIDERS.entrySet()
				.stream()
				.map(e -> Named.of(e.getKey(), e.getValue()))
				.map(Arguments::of);
	}

	private static class SpringMessageTypeCartesianArgumentsProvider implements
			CartesianParameterArgumentsProvider<SpringMessageTypeProvider<?, ?>> {

		@Override
		public Stream<SpringMessageTypeProvider<?, ?>> provideArguments(ExtensionContext context, Parameter parameter) {
			return SPRING_MESSAGE_TYPE_PROVIDERS.values().stream();
		}
	}

	private record SpringMessageTypeProvider<T, MT extends XMLMessage>(
			Class<MT> expectedXmlMessageType,
			MimeType mimeType,
			ThrowingSupplier<T> createPayloadSupplier,
			Function<MT, T> xmlMessagePayloadGetter,
			boolean serializedPayload) {
		public SpringMessageTypeProvider(Class<MT> expectedXmlMessageType,
										 MimeType mimeType,
										 ThrowingSupplier<T> createPayloadSupplier,
										 Function<MT, T> xmlMessagePayloadGetter) {
			this(expectedXmlMessageType, mimeType, createPayloadSupplier, xmlMessagePayloadGetter, false);
		}

		public T createPayload() {
				try {
					return createPayloadSupplier.get();
				} catch (Throwable e) {
					throw new RuntimeException(e);
				}
			}

			public T extractSmfPayload(MT message) {
				return xmlMessagePayloadGetter.apply(message);
			}
		}

	private static Stream<Arguments> xmlMessageTypeProviders() {
		return Stream.of(
				Arguments.of(Named.of("BYTE_ARRAY", new XmlMessageTypeProvider<>(
						BytesMessage.class,
						MimeTypeUtils.APPLICATION_OCTET_STREAM,
						() -> RandomStringUtils.randomAlphabetic(10).getBytes(StandardCharsets.UTF_8),
						BytesMessage::setData))),
				Arguments.of(Named.of("SERIALIZABLE", new XmlMessageTypeProvider<>(
						BytesMessage.class,
						new MimeType("application", "x-java-serialized-object"),
						() -> new SerializableFoo(RandomStringUtils.randomAlphabetic(10),
								RandomStringUtils.randomAlphabetic(10)),
						(msg, p) -> msg.setData(SerializationUtils.serialize(p)),
						props -> props.putBoolean(SolaceBinderHeaders.SERIALIZED_PAYLOAD, true)))),
				Arguments.of(Named.of("STRING", new XmlMessageTypeProvider<>(
						TextMessage.class,
						MimeTypeUtils.TEXT_PLAIN,
						() -> RandomStringUtils.randomAlphabetic(10),
						TextMessage::setText))),
				Arguments.of(Named.of("SDT_MAP", new XmlMessageTypeProvider<>(
						MapMessage.class,
						new MimeType("application", "x-java-serialized-object"),
						() -> {
							SDTMap expectedPayload = JCSMPFactory.onlyInstance().createMap();
							expectedPayload.putBoolean("a", true);
							expectedPayload.putCharacter("b", 's');
							expectedPayload.putMap("c", JCSMPFactory.onlyInstance().createMap());
							expectedPayload.putStream("d", JCSMPFactory.onlyInstance().createStream());
							return expectedPayload;
						},
						MapMessage::setMap))),
				Arguments.of(Named.of("SDT_STREAM", new XmlMessageTypeProvider<>(
						StreamMessage.class,
						new MimeType("application", "x-java-serialized-object"),
						() -> {
							SDTStream expectedPayload = JCSMPFactory.onlyInstance().createStream();
							expectedPayload.writeBoolean(true);
							expectedPayload.writeCharacter('s');
							expectedPayload.writeMap(JCSMPFactory.onlyInstance().createMap());
							expectedPayload.writeStream(JCSMPFactory.onlyInstance().createStream());
							return expectedPayload;
						},
						StreamMessage::setStream))),
				Arguments.of(Named.of("XML_CONTENT", new XmlMessageTypeProvider<>(
						XMLContentMessage.class,
						MimeTypeUtils.TEXT_XML,
						() -> "<a><b>testPayload</b><c>testPayload2</c></a>",
						XMLContentMessage::setXMLContent))));
	}

	private static class XmlMessageTypeCartesianProvider implements CartesianParameterArgumentsProvider<
				Named<XmlMessageTypeProvider<?, ?>>> {

		@SuppressWarnings("unchecked")
		@Override
		public Stream<Named<XmlMessageTypeProvider<?, ?>>> provideArguments(ExtensionContext context, Parameter parameter) {
			return xmlMessageTypeProviders()
					.map(arguments -> arguments.get()[0])
					.map(arg -> (Named<XmlMessageTypeProvider<?, ?>>) arg);
		}
	}

	private static class XmlMessageTypeProvider<T, MT extends XMLMessage> {
		private final Class<MT> xmlMessageType;
		private final MimeType mimeType;
		private final ThrowingSupplier<T> createPayloadSupplier;
		private final BiConsumer<MT, T> setXmlMessagePayloadFnc;
		private final ThrowingConsumer<SDTMap> injectAdditionalXMLMessageProperties;

		private XmlMessageTypeProvider(Class<MT> xmlMessageType,
									   MimeType mimeType,
									   ThrowingSupplier<T> createPayloadSupplier,
									   BiConsumer<MT, T> setXmlMessagePayloadFnc) {
			this(xmlMessageType,
					mimeType,
					createPayloadSupplier,
					setXmlMessagePayloadFnc,
					props -> {});
		}

		private XmlMessageTypeProvider(Class<MT> xmlMessageType,
									   MimeType mimeType,
									   ThrowingSupplier<T> createPayloadSupplier,
									   BiConsumer<MT, T> setXmlMessagePayloadFnc,
									   ThrowingConsumer<SDTMap> injectAdditionalXMLMessageProperties) {
			this.xmlMessageType = xmlMessageType;
			this.mimeType = mimeType;
			this.createPayloadSupplier = createPayloadSupplier;
			this.setXmlMessagePayloadFnc = setXmlMessagePayloadFnc;
			this.injectAdditionalXMLMessageProperties = injectAdditionalXMLMessageProperties;
		}

		public Class<MT> getXmlMessageType() {
			return xmlMessageType;
		}

		public void setXMLMessagePayload(MT message, T payload) {
			setXmlMessagePayloadFnc.accept(message, payload);
		}

		public MimeType getMimeType() {
			return mimeType;
		}

		public T createPayload() {
			try {
				return createPayloadSupplier.get();
			} catch (Throwable e) {
				throw new RuntimeException(e);
			}
		}

		public void injectAdditionalXMLMessageProperties(SDTMap sdtMap) throws Throwable {
			injectAdditionalXMLMessageProperties.accept(sdtMap);
		}
	}

	static Stream<Arguments> nullPayloadXMLMessageProvider() {
		return Stream.of(
				Arguments.of(JCSMPFactory.onlyInstance().createMessage(BytesMessage.class)),
				Arguments.of(JCSMPFactory.onlyInstance().createMessage(XMLContentMessage.class)),
				Arguments.of(JCSMPFactory.onlyInstance().createMessage(TextMessage.class)),
				Arguments.of(JCSMPFactory.onlyInstance().createMessage(MapMessage.class)),
				Arguments.of(JCSMPFactory.onlyInstance().createMessage(StreamMessage.class))
		);
	}

	@Test
	void testApplyHeaderNameMapping_SpringHeadersToSolaceUserPropertyNameMapping() throws SDTException {
		Map<String, String> headerToUserPropertyKeyMapping = new LinkedHashMap<>();
		headerToUserPropertyKeyMapping.put("original-timestamp-header", "timestamp");
		headerToUserPropertyKeyMapping.put("original-app-header", "app");

		Message<?> springMessage = MessageBuilder.withPayload("test")
				.setHeader("original-timestamp-header", "2025-01-01T00:00:00Z")
				.setHeader("original-app-header", "test-app")
				.setHeader("original-unmapped-header", "some-value").build();

		SolaceProducerProperties producerProperties = new SolaceProducerProperties();
		producerProperties.setHeaderNameMapping(headerToUserPropertyKeyMapping);
		SmfMessageWriterProperties writerProperties = new SmfMessageWriterProperties(
				producerProperties);

		XMLMessage xmlMessage = xmlMessageMapper.mapToSmf(springMessage, writerProperties);

		SDTMap properties = xmlMessage.getProperties();
		assertEquals("2025-01-01T00:00:00Z", properties.getString("timestamp"));
		assertEquals("test-app", properties.getString("app"));

		assertEquals("2025-01-01T00:00:00Z", properties.getString("original-timestamp-header"));
		assertEquals("test-app", properties.getString("original-app-header"));
		assertEquals("some-value", properties.getString("original-unmapped-header"));
	}

	//@Test
	void testApplyHeaderNameMapping_DuplicateSpringHeadersToSolaceUserPropertyNameMapping()
			throws SDTException {
		Map<String, String> headerToUserPropertyKeyMapping = new LinkedHashMap<>();
		headerToUserPropertyKeyMapping.put("original-timestamp-header", "timestamp");
		headerToUserPropertyKeyMapping.put("original-app-header", "timestamp");

		Message<?> springMessage = MessageBuilder.withPayload("test")
				.setHeader("original-timestamp-header", "2025-01-01T00:00:00Z")
				.setHeader("original-app-header", "test-app")
				.setHeader("original-unmapped-header", "some-value").build();

		SolaceProducerProperties producerProperties = new SolaceProducerProperties();
		producerProperties.setHeaderNameMapping(headerToUserPropertyKeyMapping);
		SmfMessageWriterProperties writerProperties = new SmfMessageWriterProperties(
				producerProperties);

		XMLMessage xmlMessage = xmlMessageMapper.mapToSmf(springMessage, writerProperties);

		//Log contains warning about duplicate mapping
		SDTMap properties = xmlMessage.getProperties();
		assertEquals("2025-01-01T00:00:00Z", properties.getString("timestamp"));

		assertEquals("2025-01-01T00:00:00Z", properties.getString("original-timestamp-header"));
		assertEquals("test-app", properties.getString("original-app-header"));
		assertEquals("some-value", properties.getString("original-unmapped-header"));
	}

	@Test
	void testApplyHeaderNameMapping_SolaceUserPropertiesToSpringHeadersMapping() throws SDTException {
		Map<String, String> headerToUserPropertyKeyMapping = new LinkedHashMap<>();
		headerToUserPropertyKeyMapping.put("id", "mapped-id-header");
		headerToUserPropertyKeyMapping.put("timestamp", "mapped-timestamp-header");
		headerToUserPropertyKeyMapping.put("app", "mapped-app-header");

		TextMessage xmlMessage = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
		xmlMessage.setText("test");
		SDTMap properties = JCSMPFactory.onlyInstance().createMap();
		properties.putString("id", "12345");
		properties.putString("timestamp", "2025-01-01T00:00:00Z");
		properties.putString("app", "test-app");
		properties.putString("unmapped-user-prop", "some-value");
		xmlMessage.setProperties(properties);

		SolaceConsumerProperties consumerProperties = new SolaceConsumerProperties();
		consumerProperties.setHeaderNameMapping(headerToUserPropertyKeyMapping);
		SmfMessageReaderProperties smfMessageReaderProperties = new SmfMessageReaderProperties(consumerProperties);

		Message<?> springMessage = xmlMessageMapper.mapToSpring(xmlMessage, null, smfMessageReaderProperties);

		assertEquals("12345", springMessage.getHeaders().get("mapped-id-header"));
		assertEquals("2025-01-01T00:00:00Z", springMessage.getHeaders().get("mapped-timestamp-header"));
		assertEquals("test-app", springMessage.getHeaders().get("mapped-app-header"));

		//id and timestamp is Spring Integration reserved headers, original values will not be preserved
		assertNotEquals("12345", springMessage.getHeaders().get("id"));
		assertNotEquals("2025-01-01T00:00:00Z", springMessage.getHeaders().get("timestamp"));
		assertNotNull(springMessage.getHeaders().get("id"));
		assertNotNull(springMessage.getHeaders().get("timestamp"));
		assertEquals("test-app", springMessage.getHeaders().get("app"));
		assertEquals("some-value", springMessage.getHeaders().get("unmapped-user-prop"));
	}

	//@Test
	void testApplyHeaderNameMapping_DuplicateSolaceUserPropertiesToSpringHeadersMapping()
			throws SDTException {
		Map<String, String> headerToUserPropertyKeyMapping = new LinkedHashMap<>();
		headerToUserPropertyKeyMapping.put("timestamp", "mapped-timestamp-header");
		headerToUserPropertyKeyMapping.put("app", "mapped-timestamp-header");

		TextMessage xmlMessage = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
		xmlMessage.setText("test");
		SDTMap properties = JCSMPFactory.onlyInstance().createMap();
		properties.putString("timestamp", "2025-01-01T00:00:00Z");
		properties.putString("app", "test-app");
		properties.putString("unmapped-user-prop", "some-value");
		xmlMessage.setProperties(properties);

		SolaceConsumerProperties consumerProperties = new SolaceConsumerProperties();
		consumerProperties.setHeaderNameMapping(headerToUserPropertyKeyMapping);
		SmfMessageReaderProperties smfMessageReaderProperties = new SmfMessageReaderProperties(consumerProperties);

		Message<?> springMessage = xmlMessageMapper.mapToSpring(xmlMessage, null, smfMessageReaderProperties);

		assertEquals("2025-01-01T00:00:00Z", springMessage.getHeaders().get("mapped-timestamp-header"));

		//id and timestamp is Spring Integration reserved headers, original values will not be preserved
		assertNotEquals("2025-01-01T00:00:00Z", springMessage.getHeaders().get("timestamp"));
		assertNotNull(springMessage.getHeaders().get("timestamp"));
		assertEquals("test-app", springMessage.getHeaders().get("app"));
		assertEquals("some-value", springMessage.getHeaders().get("unmapped-user-prop"));
	}

	@ParameterizedTest(name = "{index} testApplyHeaderNameMapping_ProducerBackwardCompatibility - {1}")
	@MethodSource("publisherBackwardCompatibilityArguments")
	void testApplyHeaderNameMapping_ProducerBackwardCompatibility(
			SolaceProducerProperties producerProperties, String scenario) throws SDTException {
		Message<?> springMessage = MessageBuilder.withPayload("test").setHeader("my-header", "my-value")
				.build();

		SmfMessageWriterProperties writerProperties = new SmfMessageWriterProperties(
				producerProperties);
		XMLMessage xmlMessage = xmlMessageMapper.mapToSmf(springMessage, writerProperties);

		verify(xmlMessageMapper).applyHeaderNameMapping(anyMap(),
				eq(writerProperties.getHeaderNameMapping()));

		SDTMap properties = xmlMessage.getProperties();
		assertEquals("my-value", properties.getString("my-header"));
	}

	private static Stream<Arguments> publisherBackwardCompatibilityArguments() {
		SolaceProducerProperties producerPropertiesWithEmptyHeaderMapping = new SolaceProducerProperties();
		producerPropertiesWithEmptyHeaderMapping.setHeaderNameMapping(
				new LinkedHashMap<>());

		return Stream.of(arguments(new SolaceProducerProperties(), "Default Producer Properties"),
				arguments(producerPropertiesWithEmptyHeaderMapping,
						"Producer Properties with Empty Mapping"));
	}

	@ParameterizedTest(name = "{index} testApplyHeaderNameMapping_ConsumerBackwardCompatibility - {1}")
	@MethodSource("consumerBackwardCompatibilityArguments")
	void testApplyHeaderNameMapping_ConsumerBackwardCompatibility(
			SolaceConsumerProperties consumerProperties, String scenario) throws SDTException {
		TextMessage xmlMessage = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
		xmlMessage.setText("test");
		SDTMap properties = JCSMPFactory.onlyInstance().createMap();
		properties.putString("my-header", "my-value");
		xmlMessage.setProperties(properties);

		SmfMessageReaderProperties smfMessageReaderProperties = new SmfMessageReaderProperties(consumerProperties);

		Message<?> springMessage = xmlMessageMapper.mapToSpring(xmlMessage, null,
				smfMessageReaderProperties);

		verify(xmlMessageMapper).applyHeaderNameMapping(anyMap(), anyMap());

		assertEquals("my-value", springMessage.getHeaders().get("my-header"));
	}

	private static Stream<Arguments> consumerBackwardCompatibilityArguments() {
		SolaceConsumerProperties consumerPropertiesWithEmptyHeaderMapping = new SolaceConsumerProperties();
		consumerPropertiesWithEmptyHeaderMapping.setHeaderNameMapping(
				new LinkedHashMap<>());

		return Stream.of(arguments(new SolaceConsumerProperties(), "Default Consumer Properties"),
				arguments(consumerPropertiesWithEmptyHeaderMapping,
						"Consumer Properties with Empty Mapping"));
	}

	@Test
	void testApplyHeaderNameMapping_MultipleMappingsAndTypes() throws SDTException {
		Map<String, String> headerToUserPropertyKeyMapping = new LinkedHashMap<>();
		headerToUserPropertyKeyMapping.put("string-header", "str");
		headerToUserPropertyKeyMapping.put("number-header", "num");
		headerToUserPropertyKeyMapping.put("boolean-header", "bool");

		// Create test message with different types
		Message<?> springMessage = MessageBuilder.withPayload("test")
				.setHeader("string-header", "string-value").setHeader("number-header", 42)
				.setHeader("boolean-header", true).setHeader("unmapped-header", "unmapped-value").build();

		SolaceProducerProperties producerProperties = new SolaceProducerProperties();
		producerProperties.setHeaderNameMapping(headerToUserPropertyKeyMapping);
		SmfMessageWriterProperties writerProperties = new SmfMessageWriterProperties(
				producerProperties);
		XMLMessage xmlMessage = xmlMessageMapper.mapToSmf(springMessage, writerProperties);

		// Verify all mappings work correctly
		SDTMap properties = xmlMessage.getProperties();
		assertEquals("string-value", properties.getString("str"));
		assertEquals(42, properties.getInteger("num").intValue());
		assertEquals(true, properties.getBoolean("bool"));
		assertEquals("unmapped-value", properties.getString("unmapped-header"));
	}

	@Test
	void testApplyHeaderNameMapping_producerHeaderExclusionWithMapping() throws SDTException {
		Map<String, String> headerToUserPropertyKeyMapping = new LinkedHashMap<>();
		headerToUserPropertyKeyMapping.put("original-timestamp-header", "timestamp");
		headerToUserPropertyKeyMapping.put("original-app-header", "app");

		List<String> excludedHeaders = List.of("original-timestamp-header", "original-app-header");

		Message<?> springMessage = MessageBuilder.withPayload("test")
				.setHeader("original-timestamp-header", "2025-01-01T00:00:00Z")
				.setHeader("original-app-header", "test-app")
				.setHeader("original-unmapped-header", "some-value").build();

		SolaceProducerProperties producerProperties = new SolaceProducerProperties();
		producerProperties.setHeaderNameMapping(headerToUserPropertyKeyMapping);
		producerProperties.setHeaderExclusions(excludedHeaders);
		SmfMessageWriterProperties writerProperties = new SmfMessageWriterProperties(
				producerProperties);

		XMLMessage xmlMessage = xmlMessageMapper.mapToSmf(springMessage, writerProperties);

		SDTMap properties = xmlMessage.getProperties();
		assertEquals("2025-01-01T00:00:00Z", properties.getString("timestamp"));
		assertEquals("test-app", properties.getString("app"));

		//Excluded headers should not be present in the properties
		assertNull(properties.getString("original-timestamp-header"));
		assertNull(properties.getString("original-app-header"));
		assertEquals("some-value", properties.getString("original-unmapped-header"));
	}

	@Test
	void testApplyHeaderNameMapping_consumerHeaderExclusionWithMapping() throws SDTException {
		Map<String, String> headerToUserPropertyKeyMapping = new LinkedHashMap<>();
		headerToUserPropertyKeyMapping.put("timestamp", "mapped-timestamp-header");
		headerToUserPropertyKeyMapping.put("excluded-header", "mapped-excluded-header");

		List<String> excludedHeaders = List.of("excluded-header");

		TextMessage xmlMessage = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
		xmlMessage.setText("test");
		SDTMap properties = JCSMPFactory.onlyInstance().createMap();
		properties.putString("timestamp", "2025-01-01T00:00:00Z");
		properties.putString("unmapped-user-prop", "some-value");
		properties.putString("excluded-header", "some-value");
		xmlMessage.setProperties(properties);

		SolaceConsumerProperties consumerProperties = new SolaceConsumerProperties();
		consumerProperties.setHeaderNameMapping(headerToUserPropertyKeyMapping);
		consumerProperties.setHeaderExclusions(excludedHeaders);
		SmfMessageReaderProperties smfMessageReaderProperties = new SmfMessageReaderProperties(consumerProperties);

		Message<?> springMessage = xmlMessageMapper.mapToSpring(xmlMessage, null, smfMessageReaderProperties);

		assertEquals("2025-01-01T00:00:00Z", springMessage.getHeaders().get("mapped-timestamp-header"));

		//timestamp is Spring Integration reserved headers, original values will not be preserved
		assertNotEquals("2025-01-01T00:00:00Z", springMessage.getHeaders().get("timestamp"));
		assertNotNull(springMessage.getHeaders().get("timestamp"));
		assertEquals("some-value", springMessage.getHeaders().get("unmapped-user-prop"));

		//Excluded header should not be present in the Spring message headers, and won't be mapped
		assertNull(springMessage.getHeaders().get("mapped-excluded-header"));
		assertNull(springMessage.getHeaders().get("excluded-header"));
	}
}
