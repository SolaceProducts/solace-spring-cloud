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
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.test.util.SerializableFoo;
import com.solace.spring.cloud.stream.binder.test.util.ThrowingFunction;
import com.solacesystems.jcsmp.BytesMessage;
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
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.MapAssert;
import org.hamcrest.CoreMatchers;
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
import org.junitpioneer.jupiter.cartesian.CartesianArgumentsProvider;
import org.junitpioneer.jupiter.cartesian.CartesianArgumentsSource;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Values;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.StaticMessageHeaderAccessor;
import org.springframework.integration.acks.AcknowledgmentCallback;
import org.springframework.integration.support.DefaultMessageBuilderFactory;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;
import org.springframework.util.SerializationUtils;
import org.testcontainers.shaded.org.apache.commons.lang.math.RandomUtils;

import java.lang.reflect.Array;
import java.lang.reflect.Parameter;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

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

@ExtendWith(MockitoExtension.class)
public class XMLMessageMapperTest {
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
	private final ObjectWriter objectWriter = OBJECT_MAPPER.writer();
	private final ObjectReader objectReader = OBJECT_MAPPER.reader();

	@Spy
	private final XMLMessageMapper xmlMessageMapper = new XMLMessageMapper();

	private static final Log logger = LogFactory.getLog(XMLMessageMapperTest.class);
	private static final Set<String> JMS_INVALID_HEADER_NAMES = new HashSet<>(Arrays.asList("~ab;c", "NULL",
			"TRUE", "FALSE", "NOT", "AND", "OR", "BETWEEN", "LIKE", "IN", "IS", "ESCAPE", "JMSX_abc", "JMS_abc"));

	static {
		assertTrue(JMS_INVALID_HEADER_NAMES.stream().anyMatch(h -> !Character.isJavaIdentifierStart(h.charAt(0))));
		assertTrue(JMS_INVALID_HEADER_NAMES.stream().map(CharSequence::chars)
				.anyMatch(c -> c.skip(1).anyMatch(c1 -> !Character.isJavaIdentifierPart(c1))));
		assertTrue(JMS_INVALID_HEADER_NAMES.stream().anyMatch(h -> h.startsWith("JMSX")));
		assertTrue(JMS_INVALID_HEADER_NAMES.stream().anyMatch(h -> h.startsWith("JMS_")));
	}

	@Test
	public void testMapSpringMessageToXMLMessage_ByteArray() throws Exception {
		Message<?> testSpringMessage = new DefaultMessageBuilderFactory()
				.withPayload("testPayload".getBytes(StandardCharsets.UTF_8))
				.setHeader("test-header-1", "test-header-val-1")
				.setHeader("test-header-2", "test-header-val-2")
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_OCTET_STREAM_VALUE)
				.build();

		XMLMessage xmlMessage = xmlMessageMapper.map(testSpringMessage, null, false);

		assertThat(xmlMessage, CoreMatchers.instanceOf(BytesMessage.class));
		assertEquals(testSpringMessage.getPayload(), ((BytesMessage) xmlMessage).getData());
		validateXMLProperties(xmlMessage, testSpringMessage);
	}

	@Test
	public void testMapSpringMessageToXMLMessage_String() throws Exception {
		Message<?> testSpringMessage = new DefaultMessageBuilderFactory()
				.withPayload("testPayload")
				.setHeader("test-header-1", "test-header-val-1")
				.setHeader("test-header-2", "test-header-val-2")
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		XMLMessage xmlMessage = xmlMessageMapper.map(testSpringMessage, null, false);

		assertThat(xmlMessage, CoreMatchers.instanceOf(TextMessage.class));
		assertEquals(testSpringMessage.getPayload(), ((TextMessage) xmlMessage).getText());
		validateXMLProperties(xmlMessage, testSpringMessage);
	}

	@Test
	public void testMapSpringMessageToXMLMessage_Serializable() throws Exception {
		Message<?> testSpringMessage = new DefaultMessageBuilderFactory()
				.withPayload(new SerializableFoo("abc123", "HOOPLA!"))
				.setHeader("test-header-1", "test-header-val-1")
				.setHeader("test-header-2", "test-header-val-2")
				.setHeader(MessageHeaders.CONTENT_TYPE, "application/x-java-serialized-object")
				.build();

		XMLMessage xmlMessage = xmlMessageMapper.map(testSpringMessage, null, false);

		assertThat(xmlMessage, CoreMatchers.instanceOf(BytesMessage.class));
		assertEquals(testSpringMessage.getPayload(),
				SerializationUtils.deserialize(((BytesMessage) xmlMessage).getData()));
		assertThat(xmlMessage.getProperties().keySet(),
				hasItem(SolaceBinderHeaders.SERIALIZED_PAYLOAD));
		assertEquals(true, xmlMessage.getProperties().getBoolean(SolaceBinderHeaders.SERIALIZED_PAYLOAD));
		validateXMLProperties(xmlMessage, testSpringMessage);
	}

	@Test
	public void testMapSpringMessageToXMLMessage_STDStream() throws Exception {
		SDTStream sdtStream = JCSMPFactory.onlyInstance().createStream();
		sdtStream.writeBoolean(true);
		sdtStream.writeCharacter('s');
		sdtStream.writeMap(JCSMPFactory.onlyInstance().createMap());
		sdtStream.writeStream(JCSMPFactory.onlyInstance().createStream());
		Message<?> testSpringMessage = new DefaultMessageBuilderFactory()
				.withPayload(sdtStream)
				.setHeader("test-header-1", "test-header-val-1")
				.setHeader("test-header-2", "test-header-val-2")
				.setHeader(MessageHeaders.CONTENT_TYPE, "application/x-java-serialized-object")
				.build();

		XMLMessage xmlMessage = xmlMessageMapper.map(testSpringMessage, null, false);

		assertThat(xmlMessage, CoreMatchers.instanceOf(StreamMessage.class));
		assertEquals(testSpringMessage.getPayload(), ((StreamMessage) xmlMessage).getStream());
		validateXMLProperties(xmlMessage, testSpringMessage);
	}

	@Test
	public void testMapSpringMessageToXMLMessage_STDMap() throws Exception {
		SDTMap sdtMap = JCSMPFactory.onlyInstance().createMap();
		sdtMap.putBoolean("a", true);
		sdtMap.putCharacter("b", 's');
		sdtMap.putMap("c", JCSMPFactory.onlyInstance().createMap());
		sdtMap.putStream("d", JCSMPFactory.onlyInstance().createStream());
		Message<?> testSpringMessage = new DefaultMessageBuilderFactory()
				.withPayload(sdtMap)
				.setHeader("test-header-1", "test-header-val-1")
				.setHeader("test-header-2", "test-header-val-2")
				.setHeader(MessageHeaders.CONTENT_TYPE, "application/x-java-serialized-object")
				.build();

		XMLMessage xmlMessage = xmlMessageMapper.map(testSpringMessage, null, false);

		assertThat(xmlMessage, CoreMatchers.instanceOf(MapMessage.class));
		assertEquals(testSpringMessage.getPayload(), ((MapMessage) xmlMessage).getMap());
		validateXMLProperties(xmlMessage, testSpringMessage);
	}

	@Test
	public void testMapSpringMessageToXMLMessage_WriteSolaceProperties() throws Exception {
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
			Object value;
			switch (header.getKey()) {
				case SolaceHeaders.APPLICATION_MESSAGE_ID:
				case SolaceHeaders.APPLICATION_MESSAGE_TYPE:
				case SolaceHeaders.CORRELATION_ID:
				case SolaceHeaders.HTTP_CONTENT_ENCODING:
				case SolaceHeaders.SENDER_ID:
					value = RandomStringUtils.randomAlphanumeric(10);
					break;
				case SolaceHeaders.DMQ_ELIGIBLE:
					value = !(Boolean) ((SolaceHeaderMeta<?>) header.getValue()).getDefaultValueOverride();
					break;
				case SolaceHeaders.IS_REPLY:
					value = true; //The opposite of what a Solace message defaults to
					break;
				case SolaceHeaders.EXPIRATION:
				case SolaceHeaders.SENDER_TIMESTAMP:
				case SolaceHeaders.SEQUENCE_NUMBER:
				case SolaceHeaders.TIME_TO_LIVE:
					value = (long) RandomUtils.JVM_RANDOM.nextInt(10000);
					break;
				case SolaceHeaders.PRIORITY:
					value = RandomUtils.JVM_RANDOM.nextInt(255);
					break;
				case SolaceHeaders.REPLY_TO:
					value = JCSMPFactory.onlyInstance().createQueue(RandomStringUtils.randomAlphanumeric(10));
					break;
				case SolaceHeaders.USER_DATA:
					value = RandomStringUtils.randomAlphanumeric(10).getBytes();
					break;
				default:
					value = null;
					fail(String.format("no test for header %s", header.getKey()));
			}
			assertNotNull(value);
			messageBuilder.setHeader(header.getKey(), value);
		}

		Message<?> testSpringMessage = messageBuilder.build();
		XMLMessage xmlMessage = xmlMessageMapper.map(testSpringMessage, null, false);

		for (Map.Entry<String, ? extends HeaderMeta<?>> header : writeableHeaders) {
			Object expectedValue = testSpringMessage.getHeaders().get(header.getKey());
			switch (header.getKey()) {
				case SolaceHeaders.APPLICATION_MESSAGE_ID:
					assertEquals(expectedValue, xmlMessage.getApplicationMessageId());
					break;
				case SolaceHeaders.APPLICATION_MESSAGE_TYPE:
					assertEquals(expectedValue, xmlMessage.getApplicationMessageType());
					break;
				case SolaceHeaders.CORRELATION_ID:
					assertEquals(expectedValue, xmlMessage.getCorrelationId());
					break;
				case SolaceHeaders.DMQ_ELIGIBLE:
					assertEquals(expectedValue, xmlMessage.isDMQEligible());
					break;
				case SolaceHeaders.EXPIRATION:
					assertEquals(expectedValue, xmlMessage.getExpiration());
					break;
				case SolaceHeaders.IS_REPLY:
					assertEquals(expectedValue, xmlMessage.isReplyMessage());
					break;
				case SolaceHeaders.HTTP_CONTENT_ENCODING:
					assertEquals(expectedValue, xmlMessage.getHTTPContentEncoding());
					break;
				case SolaceHeaders.PRIORITY:
					assertEquals(expectedValue, xmlMessage.getPriority());
					break;
				case SolaceHeaders.REPLY_TO:
					assertEquals(expectedValue, xmlMessage.getReplyTo());
					break;
				case SolaceHeaders.SENDER_ID:
					assertEquals(expectedValue, xmlMessage.getSenderId());
					break;
				case SolaceHeaders.SENDER_TIMESTAMP:
					assertEquals(expectedValue, xmlMessage.getSenderTimestamp());
					break;
				case SolaceHeaders.SEQUENCE_NUMBER:
					assertEquals(expectedValue, xmlMessage.getSequenceNumber());
					break;
				case SolaceHeaders.TIME_TO_LIVE:
					assertEquals(expectedValue, xmlMessage.getTimeToLive());
					break;
				case SolaceHeaders.USER_DATA:
					assertEquals(expectedValue, xmlMessage.getUserData());
					break;
				default:
					fail(String.format("no test for header %s", header.getKey()));
			}
		}

		validateXMLProperties(xmlMessage, testSpringMessage);
	}

	@Test
	public void testMapSpringMessageToXMLMessage_NonWriteableSolaceProperties() throws Exception {
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
		XMLMessage xmlMessage = xmlMessageMapper.map(testSpringMessage, null, false);

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
					assertEquals(new Integer(XMLMessageMapper.MESSAGE_VERSION),
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
					assertNull(xmlMessage.getProperties().get(header.getKey()));
					break;
				default:
					fail(String.format("no test for header %s", header.getKey()));
			}
		}

		validateXMLProperties(xmlMessage, testSpringMessage,
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
	public void testMapSpringMessageToXMLMessage_WriteUndefinedSolaceHeader() throws Exception {
		String undefinedSolaceHeader1 = "abc1234";
		SerializableFoo undefinedSolaceHeader2 = new SerializableFoo("abc", "123");
		Message<?> testSpringMessage = new DefaultMessageBuilderFactory()
				.withPayload("")
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.setHeader("solace_foo1", undefinedSolaceHeader1)
				.setHeader("solace_foo2", undefinedSolaceHeader2)
				.build();

		XMLMessage xmlMessage = xmlMessageMapper.map(testSpringMessage, null, false);

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
	public void testMapSpringMessageToXMLMessage_OverrideDefaultSolaceProperties() throws Exception {
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
		XMLMessage xmlMessage = xmlMessageMapper.map(testSpringMessage, null, false);

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

	@Test
	public void testFailMapSpringMessageToXMLMessage_InvalidPayload() {
		Message<?> testSpringMessage = new DefaultMessageBuilderFactory().withPayload(new Object()).build();
		assertThrows(SolaceMessageConversionException.class, () -> xmlMessageMapper.map(testSpringMessage,
				null, false));
	}

	@Test
	public void testFailMapSpringMessageToXMLMessage_InvalidHeaderType() {
		Set<Map.Entry<String, ? extends HeaderMeta<?>>> writeableHeaders = Stream.of(
					SolaceHeaderMeta.META.entrySet().stream(),
					SolaceBinderHeaderMeta.META.entrySet().stream())
				.flatMap(h -> h)
				.filter(h -> h.getValue().isWritable())
				.collect(Collectors.toSet());
		assertNotEquals(0, writeableHeaders.size(), "Test header set was empty");

		for (Map.Entry<String, ? extends HeaderMeta<?>> header : writeableHeaders) {
			Message<?> testSpringMessage = new DefaultMessageBuilderFactory().withPayload("")
					.setHeader(header.getKey(), new Object())
					.build();
			try {
				xmlMessageMapper.map(testSpringMessage, null, false);
				fail(String.format("Expected message mapping to fail for header %s", header.getKey()));
			} catch (SolaceMessageConversionException e) {
				assertEquals(e.getMessage(), String.format(
						"Message %s has an invalid value type for header %s. Expected %s but received %s.",
						testSpringMessage.getHeaders().getId(), header.getKey(), header.getValue().getType(),
						Object.class));
			}
		}
	}

	@Test
	public void testMapXMLMessageToErrorXMLMessage() throws Exception {
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
	public void testMapXMLMessageToErrorXMLMessage_WithProperties() {
		TextMessage inputMessage = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
		SolaceConsumerProperties consumerProperties = new SolaceConsumerProperties();
		consumerProperties.setErrorMsgDmqEligible(!inputMessage.isDMQEligible());
		consumerProperties.setErrorMsgTtl(100L);

		XMLMessage xmlMessage = xmlMessageMapper.mapError(inputMessage, consumerProperties);

		assertEquals(consumerProperties.getErrorMsgDmqEligible(), xmlMessage.isDMQEligible());
		assertEquals(consumerProperties.getErrorMsgTtl().longValue(), xmlMessage.getTimeToLive());
	}

	@Test
	public void testMapProducerSpringMessageToXMLMessage_WithExcludedHeader() throws SDTException {
		String testPayload = "testPayload";
		List<String> excludedHeaders = Collections.singletonList("io.opentracing.contrib.spring.integration.messaging.OpenTracingChannelInterceptor.SCOPE");
		Message<?> testSpringMessage = new DefaultMessageBuilderFactory().withPayload(testPayload)
				.setHeader(
						"io.opentracing.contrib.spring.integration.messaging.OpenTracingChannelInterceptor.SCOPE",
						"any")
				.build();

		XMLMessage xmlMessage = xmlMessageMapper.map(testSpringMessage, excludedHeaders, false);
		Mockito.verify(xmlMessageMapper).map(testSpringMessage, excludedHeaders, false);

		assertNull(xmlMessage.getProperties()
				.getMap("io.opentracing.contrib.spring.integration.messaging.OpenTracingChannelInterceptor.SCOPE"));
	}

	@Test
	public void testMapProducerSpringMessageToXMLMessage_WithExcludedHeader_ShouldNotMatchPartially() throws SDTException {
		String testPayload = "testPayload";
		List<String> excludedHeaders = Collections.singletonList("io.opentracing.contrib.spring.integration.messaging.OpenTracingChannelInterceptor.SCOPE");
		Message<?> testSpringMessage = new DefaultMessageBuilderFactory().withPayload(testPayload)
				.setHeader(
						"io.opentracing.contrib.spring.integration.messaging.OpenTracingChannelInterceptor",
						"any")
				.build();

		XMLMessage xmlMessage = xmlMessageMapper.map(testSpringMessage, excludedHeaders, false);
		Mockito.verify(xmlMessageMapper).map(testSpringMessage, excludedHeaders, false);
		assertEquals("any", xmlMessage.getProperties()
				.get("io.opentracing.contrib.spring.integration.messaging.OpenTracingChannelInterceptor"));
	}

	@Test
	public void testMapProducerSpringMessageToXMLMessage_WithExcludedHeader_ShouldNotFilterSolaceHeader() throws SDTException {
		MessageBuilder<?> messageBuilder = new DefaultMessageBuilderFactory()
				.withPayload(new SerializableFoo("a", "b"))
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.setHeader("test-serializable-header", new SerializableFoo("a", "b"));

		Set<Map.Entry<String, ? extends HeaderMeta<?>>> writeableHeaders = Stream.of(
				SolaceHeaderMeta.META.entrySet().stream(),
				SolaceBinderHeaderMeta.META.entrySet().stream())
				.flatMap(h -> h)
				.filter(h -> h.getValue().isWritable())
				.collect(Collectors.toSet());
		assertNotEquals(0, writeableHeaders.size(), "Test header set was empty");

		for (Map.Entry<String, ? extends HeaderMeta<?>> header : writeableHeaders) {
			Object value;
			switch (header.getKey()) {
			case SolaceHeaders.APPLICATION_MESSAGE_ID:
			case SolaceHeaders.APPLICATION_MESSAGE_TYPE:
			case SolaceHeaders.CORRELATION_ID:
			case SolaceHeaders.HTTP_CONTENT_ENCODING:
			case SolaceHeaders.SENDER_ID:
				value = RandomStringUtils.randomAlphanumeric(10);
				break;
			case SolaceHeaders.DMQ_ELIGIBLE:
				value = !(Boolean) ((SolaceHeaderMeta<?>) header.getValue()).getDefaultValueOverride();
				break;
			case SolaceHeaders.IS_REPLY:
				value = true; //The opposite of what a Solace message defaults to
				break;
			case SolaceHeaders.EXPIRATION:
			case SolaceHeaders.SENDER_TIMESTAMP:
			case SolaceHeaders.SEQUENCE_NUMBER:
			case SolaceHeaders.TIME_TO_LIVE:
				value = (long) RandomUtils.JVM_RANDOM.nextInt(10000);
				break;
			case SolaceHeaders.PRIORITY:
				value = RandomUtils.JVM_RANDOM.nextInt(255);
				break;
			case SolaceHeaders.REPLY_TO:
				value = JCSMPFactory.onlyInstance().createQueue(RandomStringUtils.randomAlphanumeric(10));
				break;
			case SolaceHeaders.USER_DATA:
				value = RandomStringUtils.randomAlphanumeric(10).getBytes();
				break;
			default:
				value = null;
				fail(String.format("no test for header %s", header.getKey()));
			}
			assertNotNull(value);
			messageBuilder.setHeader(header.getKey(), value);
		}

		List<String> excludedHeaders = writeableHeaders.stream()
				.map(Map.Entry::getKey)
				.collect(Collectors.toList());

		Message<?> testSpringMessage = messageBuilder.build();
		XMLMessage xmlMessage = xmlMessageMapper.map(testSpringMessage, excludedHeaders, false);

		for (Map.Entry<String, ? extends HeaderMeta<?>> header : writeableHeaders) {
			Object expectedValue = testSpringMessage.getHeaders().get(header.getKey());
			switch (header.getKey()) {
			case SolaceHeaders.APPLICATION_MESSAGE_ID:
				assertEquals(expectedValue, xmlMessage.getApplicationMessageId());
				break;
			case SolaceHeaders.APPLICATION_MESSAGE_TYPE:
				assertEquals(expectedValue, xmlMessage.getApplicationMessageType());
				break;
			case SolaceHeaders.CORRELATION_ID:
				assertEquals(expectedValue, xmlMessage.getCorrelationId());
				break;
			case SolaceHeaders.DMQ_ELIGIBLE:
				assertEquals(expectedValue, xmlMessage.isDMQEligible());
				break;
			case SolaceHeaders.EXPIRATION:
				assertEquals(expectedValue, xmlMessage.getExpiration());
				break;
			case SolaceHeaders.HTTP_CONTENT_ENCODING:
				assertEquals(expectedValue, xmlMessage.getHTTPContentEncoding());
				break;
			case SolaceHeaders.IS_REPLY:
				assertEquals(expectedValue, xmlMessage.isReplyMessage());
				break;
			case SolaceHeaders.PRIORITY:
				assertEquals(expectedValue, xmlMessage.getPriority());
				break;
			case SolaceHeaders.REPLY_TO:
				assertEquals(expectedValue, xmlMessage.getReplyTo());
				break;
			case SolaceHeaders.SENDER_ID:
				assertEquals(expectedValue, xmlMessage.getSenderId());
				break;
			case SolaceHeaders.SENDER_TIMESTAMP:
				assertEquals(expectedValue, xmlMessage.getSenderTimestamp());
				break;
			case SolaceHeaders.SEQUENCE_NUMBER:
				assertEquals(expectedValue, xmlMessage.getSequenceNumber());
				break;
			case SolaceHeaders.TIME_TO_LIVE:
				assertEquals(expectedValue, xmlMessage.getTimeToLive());
				break;
			case SolaceHeaders.USER_DATA:
				assertEquals(expectedValue, xmlMessage.getUserData());
				break;
			default:
				fail(String.format("no test for header %s", header.getKey()));
			}
		}

		for (Map.Entry<String, SolaceBinderHeaderMeta<?>> binderHeaderMetaEntry : SolaceBinderHeaderMeta.META.entrySet()) {
			if (SolaceHeaderMeta.Scope.WIRE.equals(binderHeaderMetaEntry.getValue().getScope())) {
				assertNotNull(xmlMessage.getProperties().get(binderHeaderMetaEntry.getKey()));
			}
		}

		Mockito.verify(xmlMessageMapper).map(testSpringMessage, excludedHeaders, false);
	}

	@ParameterizedTest
	@MethodSource("xmlMessageTypeProviders")
	public <T, MT extends XMLMessage> void testMapXMLMessageToSpringMessage(
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
		Message<?> springMessage = xmlMessageMapper.map(xmlMessage, acknowledgmentCallback);
		Mockito.verify(xmlMessageMapper).map(xmlMessage, acknowledgmentCallback, false);

		validateSpringPayload(springMessage.getPayload(), expectedPayload);
		validateSpringHeaders(springMessage.getHeaders(), xmlMessage);
		assertNull(StaticMessageHeaderAccessor.getSourceData(springMessage));
	}

	@ParameterizedTest
	@MethodSource("xmlMessageTypeProviders")
	public <T, MT extends XMLMessage> void testMapXMLMessageToSpringMessageBatch(
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
		Message<List<?>> springMessage = xmlMessageMapper.mapBatchMessage(xmlMessages, acknowledgmentCallback);
		Mockito.verify(xmlMessageMapper).mapBatchMessage(xmlMessages, acknowledgmentCallback, false);

		validateSpringBatchPayload(springMessage.getPayload(), expectedPayloads);
		validateSpringBatchHeaders(springMessage.getHeaders(), xmlMessages);
		assertNull(StaticMessageHeaderAccessor.getSourceData(springMessage));
	}

	@ParameterizedTest
	@MethodSource("xmlMessageTypeProviders")
	public <T, MT extends XMLMessage> void testMapXMLMessageToSpringMessage_WithRawMessageHeader(
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
		Message<?> springMessage = xmlMessageMapper.map(xmlMessage, acknowledgmentCallback, true);

		validateSpringPayload(springMessage.getPayload(), expectedPayload);
		validateSpringHeaders(springMessage.getHeaders(), xmlMessage);
		assertEquals(xmlMessage, StaticMessageHeaderAccessor.getSourceData(springMessage));
	}

	@ParameterizedTest
	@MethodSource("xmlMessageTypeProviders")
	public <T, MT extends XMLMessage> void testMapXMLMessageToSpringMessageBatch_WithRawMessageHeader(
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
		Message<List<?>> springMessage = xmlMessageMapper.mapBatchMessage(xmlMessages, acknowledgmentCallback, true);

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
	public <T, MT extends XMLMessage> void testMapXMLMessageToSpringMessage_WithContentTypeHeaderAndHTTPContentType(
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
		Message<?> springMessage;
		MessageHeaders springMessageHeaders;
		if (batchMode) {
			List<MT> xmlMessages = Collections.singletonList(xmlMessage);
			springMessage = xmlMessageMapper.mapBatchMessage(xmlMessages, acknowledgmentCallback);
			Mockito.verify(xmlMessageMapper).mapBatchMessage(xmlMessages, acknowledgmentCallback, false);
			@SuppressWarnings("unchecked")
			Map<String, Object> messageHeaders = (Map<String, Object>) Objects.requireNonNull(springMessage.getHeaders()
					.get(SolaceBinderHeaders.BATCHED_HEADERS, List.class)).get(0);
			springMessageHeaders = new MessageHeaders(messageHeaders);
		} else {
			springMessage = xmlMessageMapper.map(xmlMessage, acknowledgmentCallback);
			Mockito.verify(xmlMessageMapper).map(xmlMessage, acknowledgmentCallback, false);
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
	public void testMapXMLMessageToSpringMessage_ReadSolaceProperties(boolean batchMode) throws Exception {
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
					Mockito.when(xmlMessage.getExpiration()).thenReturn(RandomUtils.nextLong());
					break;
				case SolaceHeaders.IS_REPLY:
					Mockito.when(xmlMessage.isReplyMessage()).thenReturn(!defaultXmlMessage.isReplyMessage());
					break;
				case SolaceHeaders.HTTP_CONTENT_ENCODING:
					Mockito.when(xmlMessage.getHTTPContentEncoding()).thenReturn(header.getKey());
					break;
				case SolaceHeaders.PRIORITY:
					Mockito.when(xmlMessage.getPriority()).thenReturn(RandomUtils.nextInt());
					break;
				case SolaceHeaders.RECEIVE_TIMESTAMP:
					Mockito.when(xmlMessage.getReceiveTimestamp()).thenReturn(RandomUtils.nextLong());
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
					Mockito.when(xmlMessage.getSenderTimestamp()).thenReturn(RandomUtils.nextLong());
					break;
				case SolaceHeaders.SEQUENCE_NUMBER:
					Mockito.when(xmlMessage.getSequenceNumber()).thenReturn(RandomUtils.nextLong());
					break;
				case SolaceHeaders.TIME_TO_LIVE:
					Mockito.when(xmlMessage.getTimeToLive()).thenReturn(RandomUtils.nextLong());
					break;
				case SolaceHeaders.USER_DATA:
					Mockito.when(xmlMessage.getUserData()).thenReturn(header.getKey().getBytes());
					break;
				case SolaceBinderHeaders.MESSAGE_VERSION:
					metadata.putInteger(header.getKey(), RandomUtils.nextInt());
					break;
				default:
					fail(String.format("no test for header %s", header.getKey()));
			}
		}

		Mockito.when(xmlMessage.getProperties()).thenReturn(metadata);
		Mockito.when(xmlMessage.getText()).thenReturn("testPayload");
		metadata.putString(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE);

		AcknowledgmentCallback acknowledgmentCallback = Mockito.mock(AcknowledgmentCallback.class);
		Message<?> springMessage;
		MessageHeaders springMessageHeaders;
		if (batchMode) {
			List<TextMessage> xmlMessages = Collections.singletonList(xmlMessage);
			springMessage = xmlMessageMapper.mapBatchMessage(xmlMessages, acknowledgmentCallback);
			Mockito.verify(xmlMessageMapper).mapBatchMessage(xmlMessages, acknowledgmentCallback, false);

			@SuppressWarnings("unchecked")
			Map<String, Object> messageHeaders = (Map<String, Object>) Objects.requireNonNull(springMessage.getHeaders()
							.get(SolaceBinderHeaders.BATCHED_HEADERS, List.class)).get(0);
			springMessageHeaders = new MessageHeaders(messageHeaders);
		} else {
			springMessage = xmlMessageMapper.map(xmlMessage, acknowledgmentCallback);
			Mockito.verify(xmlMessageMapper).map(xmlMessage, acknowledgmentCallback, false);
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
	public void testMapXMLMessageToSpringMessage_NonReadableSolaceProperties(boolean batchMode) throws Exception {
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
				default:
					fail(String.format("no test for header %s", header.getKey()));
			}
		}

		Mockito.when(xmlMessage.getProperties()).thenReturn(metadata);
		Mockito.when(xmlMessage.getText()).thenReturn("testPayload");
		Mockito.when(xmlMessage.getDeliveryCount()).thenThrow(new UnsupportedOperationException("Feature is disabled"));
		metadata.putString(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE);

		AcknowledgmentCallback acknowledgmentCallback = Mockito.mock(AcknowledgmentCallback.class);

		Message<?> springMessage;
		MessageHeaders springMessageHeaders;
		if (batchMode) {
			List<TextMessage> xmlMessages = Collections.singletonList(xmlMessage);
			springMessage = xmlMessageMapper.mapBatchMessage(xmlMessages, acknowledgmentCallback);
			Mockito.verify(xmlMessageMapper).mapBatchMessage(xmlMessages, acknowledgmentCallback, false);

			@SuppressWarnings("unchecked")
			Map<String, Object> messageHeaders = (Map<String, Object>) Objects.requireNonNull(springMessage.getHeaders()
					.get(SolaceBinderHeaders.BATCHED_HEADERS, List.class)).get(0);
			springMessageHeaders = new MessageHeaders(messageHeaders);
		} else {
			springMessage = xmlMessageMapper.map(xmlMessage, acknowledgmentCallback);
			Mockito.verify(xmlMessageMapper).map(xmlMessage, acknowledgmentCallback, false);
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
	public void testMapXMLMessageToSpringMessage_ReadLocalSolaceProperties(boolean batchMode) throws Exception {
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

		Message<?> springMessage;
		MessageHeaders springMessageHeaders;
		if (batchMode) {
			List<TextMessage> xmlMessages = Collections.singletonList(xmlMessage);
			springMessage = xmlMessageMapper.mapBatchMessage(xmlMessages, acknowledgmentCallback);
			Mockito.verify(xmlMessageMapper).mapBatchMessage(xmlMessages, acknowledgmentCallback, false);

			@SuppressWarnings("unchecked")
			Map<String, Object> messageHeaders = (Map<String, Object>) Objects.requireNonNull(springMessage.getHeaders()
					.get(SolaceBinderHeaders.BATCHED_HEADERS, List.class)).get(0);
			springMessageHeaders = new MessageHeaders(messageHeaders);
		} else {
			springMessage = xmlMessageMapper.map(xmlMessage, acknowledgmentCallback);
			Mockito.verify(xmlMessageMapper).map(xmlMessage, acknowledgmentCallback, false);
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
	public void testMapXMLMessageToSpringMessage_deliveryCountFeatureEnabled(boolean batchMode) {
		int deliveryCount = 42;
		TextMessage xmlMessage = Mockito.mock(TextMessage.class);
		Mockito.when(xmlMessage.getText()).thenReturn("testPayload");
		Mockito.when(xmlMessage.getDeliveryCount()).thenReturn(deliveryCount);

		AcknowledgmentCallback acknowledgmentCallback = Mockito.mock(AcknowledgmentCallback.class);
		MapAssert<String, Object> headersAssert;
		if (batchMode) {
			headersAssert = Assertions.assertThat(Objects.requireNonNull(xmlMessageMapper
									.mapBatchMessage(Collections.singletonList(xmlMessage), acknowledgmentCallback)
									.getHeaders()
					.get(SolaceBinderHeaders.BATCHED_HEADERS, List.class))
					.get(0))
					.asInstanceOf(InstanceOfAssertFactories.map(String.class, Object.class));
		} else {
			headersAssert = Assertions.assertThat(xmlMessageMapper.map(xmlMessage, acknowledgmentCallback)
					.getHeaders());
		}
		headersAssert.extractingByKey(SolaceHeaders.DELIVERY_COUNT).isEqualTo(deliveryCount);
	}

	@ParameterizedTest(name = "[{index}] batchMode={0}")
	@ValueSource(booleans = {false, true})
	public void testMapXMLMessageToSpringMessage_ReadUndefinedSolaceHeader(boolean batchMode) throws Exception {
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

		Message<?> springMessage;
		MessageHeaders springMessageHeaders;
		if (batchMode) {
			List<TextMessage> xmlMessages = Collections.singletonList(xmlMessage);
			springMessage = xmlMessageMapper.mapBatchMessage(xmlMessages, acknowledgmentCallback);
			@SuppressWarnings("unchecked")
			Map<String, Object> messageHeaders = (Map<String, Object>) Objects.requireNonNull(springMessage.getHeaders()
					.get(SolaceBinderHeaders.BATCHED_HEADERS, List.class)).get(0);
			springMessageHeaders = new MessageHeaders(messageHeaders);
		} else {
			springMessage = xmlMessageMapper.map(xmlMessage, acknowledgmentCallback);
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
	public void testMapXMLMessageToSpringMessage_WithNullPayload(boolean batchMode) {
		BytesMessage xmlMessage = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
		AcknowledgmentCallback acknowledgmentCallback = Mockito.mock(AcknowledgmentCallback.class);

		Message<?> springMessage;
		MessageHeaders springMessageHeaders;
		if (batchMode) {
			List<BytesMessage> xmlMessages = Collections.singletonList(xmlMessage);
			springMessage = xmlMessageMapper.mapBatchMessage(xmlMessages, acknowledgmentCallback);
			@SuppressWarnings("unchecked")
			Map<String, Object> messageHeaders = (Map<String, Object>) Objects.requireNonNull(springMessage.getHeaders()
					.get(SolaceBinderHeaders.BATCHED_HEADERS, List.class)).get(0);
			springMessageHeaders = new MessageHeaders(messageHeaders);
		} else {
			springMessage = xmlMessageMapper.map(xmlMessage, acknowledgmentCallback);
			springMessageHeaders = springMessage.getHeaders();
		}

		assertEquals(Boolean.TRUE, springMessageHeaders.get(SolaceBinderHeaders.NULL_PAYLOAD, Boolean.class));
		if (batchMode) {
			Assertions.assertThat(springMessage.getHeaders()).doesNotContainKey(SolaceBinderHeaders.NULL_PAYLOAD);
		}
	}

	@ParameterizedTest(name = "[{index}] batchMode={0}")
	@ValueSource(booleans = {false, true})
	public void testMapXMLMessageToSpringMessage_WithListPayload(boolean batchMode) throws Exception {
		BytesMessage xmlMessage = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
		List<SerializableFoo> expectedPayload = Collections.singletonList(new SerializableFoo(
				RandomStringUtils.randomAlphanumeric(100), RandomStringUtils.randomAlphanumeric(100)));
		xmlMessage.setData(SerializationUtils.serialize(expectedPayload));
		SDTMap metadata = JCSMPFactory.onlyInstance().createMap();
		metadata.putString(MessageHeaders.CONTENT_TYPE, "application/x-java-serialized-object");
		metadata.putBoolean(SolaceBinderHeaders.SERIALIZED_PAYLOAD, true);
		xmlMessage.setProperties(metadata);
		AcknowledgmentCallback acknowledgmentCallback = Mockito.mock(AcknowledgmentCallback.class);

		Message<?> springMessage;
		if (batchMode) {
			springMessage = xmlMessageMapper.mapBatchMessage(Collections.singletonList(xmlMessage),
					acknowledgmentCallback);
		} else {
			springMessage = xmlMessageMapper.map(xmlMessage, acknowledgmentCallback);
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
	public void testMapMessageHeadersToSDTMap_Serializable() throws Exception {
		String key = "a";
		SerializableFoo value = new SerializableFoo("abc123", "HOOPLA!");
		Map<String,Object> headers = new HashMap<>();
		headers.put(key, value);
		headers.put(BinderHeaders.TARGET_DESTINATION, "redirected-target");

		SDTMap sdtMap = xmlMessageMapper.map(new MessageHeaders(headers), Collections.emptyList(), false);

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
	public void testMapMessageHeadersToSDTMap_NonSerializable() {
		SolaceMessageConversionException thrown = assertThrows(SolaceMessageConversionException.class,
				() -> xmlMessageMapper.map(new MessageHeaders(Collections.singletonMap("a", new Object())),
						Collections.emptyList(), false));
		assertThat(thrown.getCause(), instanceOf(IllegalArgumentException.class));
		assertThat(thrown.getMessage(), containsString("Invalid type as value - Object"));
	}

	@Test
	public void testMapMessageHeadersToSDTMap_NonSerializableToString() throws Exception {
		String key = "a";
		Object value = new Object();
		SDTMap sdtMap = xmlMessageMapper.map(new MessageHeaders(Collections.singletonMap(key, value)),
				Collections.emptyList(), true);
		assertEquals(value.toString(), sdtMap.get(key));
	}

	@Test
	public void testMapMessageHeadersToSDTMap_Null() throws Exception {
		String key = "a";
		Map<String,Object> headers = Collections.singletonMap(key, null);
		SDTMap sdtMap = xmlMessageMapper.map(new MessageHeaders(headers), Collections.emptyList(), false);
		assertThat(sdtMap.keySet(), hasItem(key));
		assertNull(sdtMap.get(key));
	}

	@Test
	public void testMapMessageHeadersToSDTMap_NonJmsCompatible() throws Exception {
		byte[] value = "test".getBytes(); // byte[] values are not supported by JMS
		Map<String,Object> headers = new HashMap<>();
		JMS_INVALID_HEADER_NAMES.forEach(h -> headers.put(h, value));

		SDTMap sdtMap = xmlMessageMapper.map(new MessageHeaders(headers), Collections.emptyList(), false);

		for (String header : JMS_INVALID_HEADER_NAMES) {
			assertThat(sdtMap.keySet(), hasItem(header));
			assertEquals(value, sdtMap.getBytes(header));
		}
	}

	@Test
	public void testMapSDTMapToMessageHeaders_Serializable() throws Exception {
		String key = "a";
		SerializableFoo value = new SerializableFoo("abc123", "HOOPLA!");
		SDTMap sdtMap = JCSMPFactory.onlyInstance().createMap();
		sdtMap.putObject(key, SerializationUtils.serialize(value));
		List<String> serializedHeaders = Arrays.asList(key, key);
		sdtMap.putString(SolaceBinderHeaders.SERIALIZED_HEADERS, objectWriter.writeValueAsString(serializedHeaders));

		MessageHeaders messageHeaders = xmlMessageMapper.map(sdtMap);

		assertThat(messageHeaders.keySet(), hasItem(key));
		assertThat(messageHeaders.keySet(), not(hasItem(SolaceBinderHeaders.SERIALIZED_HEADERS)));
		assertEquals(value, messageHeaders.get(key));
		assertNull(messageHeaders.get(SolaceBinderHeaders.SERIALIZED_HEADERS));
	}

	@Test
	public void testMapSDTMapToMessageHeaders_Null() throws Exception {
		String key = "a";
		SDTMap sdtMap = JCSMPFactory.onlyInstance().createMap();
		sdtMap.putObject(key, null);

		MessageHeaders messageHeaders = xmlMessageMapper.map(sdtMap);

		assertThat(messageHeaders.keySet(), hasItem(key));
		assertNull(messageHeaders.get(key));
	}

	@Test
	public void testMapSDTMapToMessageHeaders_EncodedSerializable() throws Exception {
		String key = "a";
		SerializableFoo value = new SerializableFoo("abc123", "HOOPLA!");
		SDTMap sdtMap = JCSMPFactory.onlyInstance().createMap();
		sdtMap.putString(key, Base64.getEncoder().encodeToString(SerializationUtils.serialize(value)));
		Set<String> serializedHeaders = Collections.singleton(key);
		sdtMap.putString(SolaceBinderHeaders.SERIALIZED_HEADERS, objectWriter.writeValueAsString(serializedHeaders));
		sdtMap.putString(SolaceBinderHeaders.SERIALIZED_HEADERS_ENCODING, "base64");

		MessageHeaders messageHeaders = xmlMessageMapper.map(sdtMap);

		assertThat(messageHeaders.keySet(), hasItem(key));
		assertThat(messageHeaders.keySet(), not(hasItem(SolaceBinderHeaders.SERIALIZED_HEADERS)));
		assertEquals(value, messageHeaders.get(key));
		assertNull(messageHeaders.get(SolaceBinderHeaders.SERIALIZED_HEADERS));
	}

	@Test
	public void testMapSDTMapToMessageHeaders_ExtraSerializableHeader() throws Exception {
		String key = "a";
		SDTMap sdtMap = JCSMPFactory.onlyInstance().createMap();
		Set<String> serializedHeaders = Collections.singleton(key);
		sdtMap.putString(SolaceBinderHeaders.SERIALIZED_HEADERS, objectWriter.writeValueAsString(serializedHeaders));

		MessageHeaders messageHeaders = xmlMessageMapper.map(sdtMap);

		assertThat(messageHeaders.keySet(), not(hasItem(key)));
		assertThat(messageHeaders.keySet(), not(hasItem(SolaceBinderHeaders.SERIALIZED_HEADERS)));
	}

	@Test
	public void testMapSDTMapToMessageHeaders_NullSerializableHeader() throws Exception {
		String key = "a";
		SDTMap sdtMap = JCSMPFactory.onlyInstance().createMap();
		sdtMap.putObject(key, null);
		Set<String> serializedHeaders = Collections.singleton(key);
		sdtMap.putString(SolaceBinderHeaders.SERIALIZED_HEADERS, objectWriter.writeValueAsString(serializedHeaders));
		sdtMap.putString(SolaceBinderHeaders.SERIALIZED_HEADERS_ENCODING, "base64");

		MessageHeaders messageHeaders = xmlMessageMapper.map(sdtMap);

		assertThat(messageHeaders.keySet(), hasItem(key));
		assertNull(messageHeaders.get(key));
		assertThat(messageHeaders.keySet(), not(hasItem(SolaceBinderHeaders.SERIALIZED_HEADERS)));
	}

	@Test
	public void testFailMapSDTMapToMessageHeaders_InvalidEncoding() throws Exception {
		String key = "a";
		SerializableFoo value = new SerializableFoo("abc123", "HOOPLA!");
		SDTMap sdtMap = JCSMPFactory.onlyInstance().createMap();
		sdtMap.putString(key, Base64.getEncoder().encodeToString(SerializationUtils.serialize(value)));
		Set<String> serializedHeaders = Collections.singleton(key);
		sdtMap.putString(SolaceBinderHeaders.SERIALIZED_HEADERS, objectWriter.writeValueAsString(serializedHeaders));
		sdtMap.putString(SolaceBinderHeaders.SERIALIZED_HEADERS_ENCODING, "abc");

		SolaceMessageConversionException exception = assertThrows(SolaceMessageConversionException.class,
				() -> xmlMessageMapper.map(sdtMap));
		assertThat(exception.getMessage(), containsString("encoding is not supported"));
	}

	@Test
	public void testMapSDTMapToMessageHeaders_NonJmsCompatible() throws Exception {
		byte[] value = "test".getBytes(); // byte[] values are not supported by JMS
		SDTMap sdtMap = JCSMPFactory.onlyInstance().createMap();
		for (String header : JMS_INVALID_HEADER_NAMES) {
			sdtMap.putBytes(header, value);
		}

		MessageHeaders messageHeaders = xmlMessageMapper.map(sdtMap);

		for (String header : JMS_INVALID_HEADER_NAMES) {
			assertThat(messageHeaders.keySet(), hasItem(header));
			assertEquals(value, messageHeaders.get(header, byte[].class));
		}
	}

	@Test
	public void testMapLoop() throws Exception {
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

		Message<?> springMessage = expectedSpringMessage;
		XMLMessage xmlMessage;
		int i = 0;
		do {
			logger.info(String.format("Iteration %s - Message<?> to XMLMessage:\n%s", i, springMessage));
			xmlMessage = xmlMessageMapper.map(springMessage, null, false);
			validateXMLProperties(xmlMessage, expectedSpringMessage, springHeaders);

			logger.info(String.format("Iteration %s - XMLMessage to Message<?>:\n%s", i, xmlMessage));
			AcknowledgmentCallback acknowledgmentCallback = Mockito.mock(AcknowledgmentCallback.class);
			springMessage = xmlMessageMapper.map(xmlMessage, acknowledgmentCallback);
			validateSpringHeaders(springMessage.getHeaders(), expectedXmlMessage);

			// Update the expected default spring headers
			springHeaders.put(MessageHeaders.ID, springMessage.getHeaders().getId());
			springHeaders.put(MessageHeaders.TIMESTAMP, springMessage.getHeaders().getTimestamp());

			i++;
		} while (i < 3);
	}

	private void validateXMLProperties(XMLMessage xmlMessage, Message<?> springMessage)
			throws Exception {
		validateXMLProperties(xmlMessage, springMessage, springMessage.getHeaders());
	}

	private void validateXMLProperties(XMLMessage xmlMessage, Message<?> springMessage, Map<String, Object> expectedHeaders)
			throws Exception {

		assertEquals(DeliveryMode.PERSISTENT, xmlMessage.getDeliveryMode());
		assertEquals(Objects.requireNonNull(StaticMessageHeaderAccessor.getContentType(springMessage)).toString(),
				xmlMessage.getHTTPContentType());

		SDTMap metadata = xmlMessage.getProperties();

		assertEquals((Integer) XMLMessageMapper.MESSAGE_VERSION, metadata.getInteger(SolaceBinderHeaders.MESSAGE_VERSION));

		Set<String> serializedHeaders = new HashSet<>();

		if (metadata.containsKey(SolaceBinderHeaders.SERIALIZED_HEADERS)) {
			XMLMessageMapper.Encoder encoder = XMLMessageMapper.Encoder
					.getByName(metadata.getString(SolaceBinderHeaders.SERIALIZED_HEADERS_ENCODING));
			Set<String> serializedHeadersSet = objectReader.forType(new TypeReference<Set<String>>(){})
					.readValue(metadata.getString(SolaceBinderHeaders.SERIALIZED_HEADERS));
			assertThat(serializedHeadersSet, not(empty()));
			for (String serializedHeader : serializedHeadersSet) {
				serializedHeaders.add(serializedHeader);
				assertThat(metadata.keySet(), hasItem(serializedHeader));
				Object headerValue = SerializationUtils.deserialize(encoder != null ?
						encoder.decode(metadata.getString(serializedHeader)) : metadata.getBytes(serializedHeader));
				if (expectedHeaders.containsKey(serializedHeader)) {
					assertEquals(expectedHeaders.get(serializedHeader), headerValue);
				} else {
					assertNotNull(headerValue);
				}
			}
		}

		Map<String, SolaceHeaderMeta<?>> readWriteableSolaceHeaders = SolaceHeaderMeta.META
				.entrySet()
				.stream()
				.filter(h -> h.getValue().isWritable())
				.filter(h -> h.getValue().isReadable())
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

		for (Map.Entry<String,Object> header : expectedHeaders.entrySet()) {
			if (readWriteableSolaceHeaders.containsKey(header.getKey())) {
				Object value = readWriteableSolaceHeaders.get(header.getKey()).getReadAction().apply(xmlMessage);
				assertEquals(header.getValue(), value);
			} else if (!serializedHeaders.contains(header.getKey())) {
				assertThat(metadata.keySet(), hasItem(header.getKey()));
				assertEquals(header.getValue(), metadata.get(header.getKey()));
			}
		}
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
				.hasSize(xmlMessages.size())
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
				.collect(Collectors.toList());

		for (String customHeaderName : nonReadableBinderHeaderMeta) {
			assertThat(messageHeaders.keySet(), not(hasItem(customHeaderName)));
		}

		Set<String> serializedHeaders = new HashSet<>();
		if (expectedHeaders.containsKey(SolaceBinderHeaders.SERIALIZED_HEADERS)) {
			String serializedHeaderJson = expectedHeaders.getString(SolaceBinderHeaders.SERIALIZED_HEADERS);
			assertThat(serializedHeaderJson, not(emptyString()));
			Set<String> serializedHeaderSet = objectReader.forType(new TypeReference<Set<String>>() {})
					.readValue(serializedHeaderJson);
			assertThat(serializedHeaderSet, not(empty()));
			for (String serializedHeader : serializedHeaderSet) {
				serializedHeaders.add(serializedHeader);
				assertThat(expectedHeaders.keySet(), hasItem(serializedHeader));
				assertThat(messageHeaders.keySet(), hasItem(serializedHeader));
				assertEquals(SerializationUtils.deserialize(expectedHeaders.getBytes(serializedHeader)),
						messageHeaders.get(serializedHeader));
			}
		}

		for (String headerName : expectedHeaders.keySet()) {
			if (nonReadableBinderHeaderMeta.contains(headerName) || serializedHeaders.contains(headerName)) continue;
			assertEquals(expectedHeaders.get(headerName), messageHeaders.get(headerName));
		}

		if (batchMode) {
			Assertions.assertThat(messageHeaders)
					.doesNotContainKey(IntegrationMessageHeaderAccessor.ACKNOWLEDGMENT_CALLBACK);
		} else {
			Assertions.assertThat(messageHeaders.get(IntegrationMessageHeaderAccessor.ACKNOWLEDGMENT_CALLBACK,
							AcknowledgmentCallback.class))
					.isNotNull()
					.isInstanceOf(AcknowledgmentCallback.class);
		}

		if (batchMode) {
			Assertions.assertThat(messageHeaders)
					.doesNotContainKey(IntegrationMessageHeaderAccessor.DELIVERY_ATTEMPT);
		} else {
			Assertions.assertThat(messageHeaders.get(IntegrationMessageHeaderAccessor.DELIVERY_ATTEMPT,
							AtomicInteger.class))
					.isNotNull()
					.asInstanceOf(InstanceOfAssertFactories.ATOMIC_INTEGER)
					.hasValue(0);
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
		Assertions.assertThat(payloads).hasSize(expectedPayloads.size());
		for (int i = 0; i < expectedPayloads.size(); i++) {
			validateSpringPayload(payloads.get(i), expectedPayloads.get(i));
		}
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

	private static class XmlMessageTypeCartesianProvider implements CartesianArgumentsProvider<
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
}
