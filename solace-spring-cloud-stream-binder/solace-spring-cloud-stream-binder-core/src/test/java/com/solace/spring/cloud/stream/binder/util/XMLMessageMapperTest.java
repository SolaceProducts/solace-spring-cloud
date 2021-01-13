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
import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.MapMessage;
import com.solacesystems.jcsmp.SDTException;
import com.solacesystems.jcsmp.SDTMap;
import com.solacesystems.jcsmp.SDTStream;
import com.solacesystems.jcsmp.StreamMessage;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.XMLContentMessage;
import com.solacesystems.jcsmp.XMLMessage;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.integration.StaticMessageHeaderAccessor;
import org.springframework.integration.acks.AcknowledgmentCallback;
import org.springframework.integration.support.DefaultMessageBuilderFactory;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;
import org.springframework.util.SerializationUtils;

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
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class XMLMessageMapperTest {
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
	private final ObjectWriter objectWriter = OBJECT_MAPPER.writer();
	private final ObjectReader objectReader = OBJECT_MAPPER.reader();

	@Spy
	private final XMLMessageMapper xmlMessageMapper = new XMLMessageMapper();

	@Before
	public void setupMockito() {
		MockitoAnnotations.initMocks(this);
	}

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

		XMLMessage xmlMessage = xmlMessageMapper.map(testSpringMessage);

		assertThat(xmlMessage, CoreMatchers.instanceOf(BytesMessage.class));
		assertEquals(testSpringMessage.getPayload(), ((BytesMessage) xmlMessage).getData());
		validateXMLMessage(xmlMessage, testSpringMessage);
	}

	@Test
	public void testMapSpringMessageToXMLMessage_String() throws Exception {
		Message<?> testSpringMessage = new DefaultMessageBuilderFactory()
				.withPayload("testPayload")
				.setHeader("test-header-1", "test-header-val-1")
				.setHeader("test-header-2", "test-header-val-2")
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		XMLMessage xmlMessage = xmlMessageMapper.map(testSpringMessage);

		assertThat(xmlMessage, CoreMatchers.instanceOf(TextMessage.class));
		assertEquals(testSpringMessage.getPayload(), ((TextMessage) xmlMessage).getText());
		validateXMLMessage(xmlMessage, testSpringMessage);
	}

	@Test
	public void testMapSpringMessageToXMLMessage_Serializable() throws Exception {
		Message<?> testSpringMessage = new DefaultMessageBuilderFactory()
				.withPayload(new SerializableFoo("abc123", "HOOPLA!"))
				.setHeader("test-header-1", "test-header-val-1")
				.setHeader("test-header-2", "test-header-val-2")
				.setHeader(MessageHeaders.CONTENT_TYPE, "application/x-java-serialized-object")
				.build();

		XMLMessage xmlMessage = xmlMessageMapper.map(testSpringMessage);

		assertThat(xmlMessage, CoreMatchers.instanceOf(BytesMessage.class));
		assertEquals(testSpringMessage.getPayload(),
				SerializationUtils.deserialize(((BytesMessage) xmlMessage).getData()));
		assertThat(xmlMessage.getProperties().keySet(),
				hasItem(SolaceBinderHeaders.SERIALIZED_PAYLOAD));
		assertEquals(true, xmlMessage.getProperties().getBoolean(SolaceBinderHeaders.SERIALIZED_PAYLOAD));
		validateXMLMessage(xmlMessage, testSpringMessage);
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

		XMLMessage xmlMessage = xmlMessageMapper.map(testSpringMessage);

		assertThat(xmlMessage, CoreMatchers.instanceOf(StreamMessage.class));
		assertEquals(testSpringMessage.getPayload(), ((StreamMessage) xmlMessage).getStream());
		validateXMLMessage(xmlMessage, testSpringMessage);
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

		XMLMessage xmlMessage = xmlMessageMapper.map(testSpringMessage);

		assertThat(xmlMessage, CoreMatchers.instanceOf(MapMessage.class));
		assertEquals(testSpringMessage.getPayload(), ((MapMessage) xmlMessage).getMap());
		validateXMLMessage(xmlMessage, testSpringMessage);
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
		assertNotEquals("Test header set was empty", 0, writeableHeaders.size());

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
		XMLMessage xmlMessage = xmlMessageMapper.map(testSpringMessage);

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

		validateXMLMessage(xmlMessage, testSpringMessage);
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
		assertNotEquals("Test header set was empty", 0, nonWriteableHeaders.size());

		for (Map.Entry<String, ? extends HeaderMeta<?>> header : nonWriteableHeaders) {
			// Doesn't matter what we said the values to
			messageBuilder.setHeader(header.getKey(), new Object());
		}

		Message<?> testSpringMessage = messageBuilder.build();
		XMLMessage xmlMessage = xmlMessageMapper.map(testSpringMessage);

		for (Map.Entry<String, ? extends HeaderMeta<?>> header : nonWriteableHeaders) {
			switch (header.getKey()) {
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
				case SolaceBinderHeaders.RAW_MESSAGE:
				case SolaceBinderHeaders.SERIALIZED_PAYLOAD:
					assertNull(xmlMessage.getProperties().get(header.getKey()));
					break;
				default:
					fail(String.format("no test for header %s", header.getKey()));
			}
		}

		validateXMLMessage(xmlMessage, testSpringMessage,
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

		XMLMessage xmlMessage = xmlMessageMapper.map(testSpringMessage);

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

		validateXMLMessage(xmlMessage, testSpringMessage);
	}

	@Test
	public void testMapSpringMessageToXMLMessage_OverrideDefaultSolaceProperties() throws Exception {
		Set<Map.Entry<String, ? extends SolaceHeaderMeta<?>>> overriddenWriteableHeaders = SolaceHeaderMeta.META
				.entrySet()
				.stream()
				.filter(h -> h.getValue().isWritable())
				.filter(h -> h.getValue().hasOverriddenDefaultValue())
				.collect(Collectors.toSet());
		assertNotEquals("Test header set was empty", 0, overriddenWriteableHeaders.size());

		Message<?> testSpringMessage = new DefaultMessageBuilderFactory()
				.withPayload("")
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();
		XMLMessage xmlMessage = xmlMessageMapper.map(testSpringMessage);

		for (Map.Entry<String, ? extends HeaderMeta<?>> header : overriddenWriteableHeaders) {
			switch (header.getKey()) {
				case SolaceHeaders.DMQ_ELIGIBLE:
					assertTrue(xmlMessage.isDMQEligible());
					break;
				default:
					fail(String.format("no test for header %s", header.getKey()));
			}
		}

		validateXMLMessage(xmlMessage, testSpringMessage);
	}

	@Test(expected = SolaceMessageConversionException.class)
	public void testFailMapSpringMessageToXMLMessage_InvalidPayload() {
		Message<?> testSpringMessage = new DefaultMessageBuilderFactory().withPayload(new Object()).build();
		xmlMessageMapper.map(testSpringMessage);
	}

	@Test
	public void testFailMapSpringMessageToXMLMessage_InvalidHeaderType() {
		Set<Map.Entry<String, ? extends HeaderMeta<?>>> writeableHeaders = Stream.of(
					SolaceHeaderMeta.META.entrySet().stream(),
					SolaceBinderHeaderMeta.META.entrySet().stream())
				.flatMap(h -> h)
				.filter(h -> h.getValue().isWritable())
				.collect(Collectors.toSet());
		assertNotEquals("Test header set was empty", 0, writeableHeaders.size());

		for (Map.Entry<String, ? extends HeaderMeta<?>> header : writeableHeaders) {
			Message<?> testSpringMessage = new DefaultMessageBuilderFactory().withPayload("")
					.setHeader(header.getKey(), new Object())
					.build();
			try {
				xmlMessageMapper.map(testSpringMessage);
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
	public void testMapConsumerErrorSpringMessageToXMLMessage() {
		String testPayload = "testPayload";
		Message<?> testSpringMessage = new DefaultMessageBuilderFactory().withPayload(testPayload).build();

		XMLMessage xmlMessage = xmlMessageMapper.mapError(testSpringMessage, new SolaceConsumerProperties());
		Mockito.verify(xmlMessageMapper).map(testSpringMessage);

		assertEquals(0, xmlMessage.getTimeToLive());
	}

	@Test
	public void testMapConsumerErrorSpringMessageToXMLMessage_WithProperties() {
		String testPayload = "testPayload";
		XMLMessage defaultXmlMessage = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
		Message<?> testSpringMessage = new DefaultMessageBuilderFactory().withPayload(testPayload).build();
		SolaceConsumerProperties consumerProperties = new SolaceConsumerProperties();
		consumerProperties.setErrorMsgDmqEligible(!defaultXmlMessage.isDMQEligible());
		consumerProperties.setErrorMsgTtl(100L);

		XMLMessage xmlMessage = xmlMessageMapper.mapError(testSpringMessage, consumerProperties);
		Mockito.verify(xmlMessageMapper).map(testSpringMessage);

		assertEquals(consumerProperties.getErrorMsgDmqEligible(), xmlMessage.isDMQEligible());
		assertEquals(consumerProperties.getErrorMsgTtl().longValue(), xmlMessage.getTimeToLive());
	}

	@Test
	public void testMapProducerSpringMessageToXMLMessage_WithExcludedHeader() throws SDTException {
		String testPayload = "testPayload";
		List<String> excludedHeaders = new ArrayList<>(Arrays
				.asList("io.opentracing.contrib.spring.integration.messaging.OpenTracingChannelInterceptor.SCOPE"));
		Message<?> testSpringMessage = new DefaultMessageBuilderFactory().withPayload(testPayload)
				.setHeader(
						"io.opentracing.contrib.spring.integration.messaging.OpenTracingChannelInterceptor.SCOPE",
						"any")
				.build();

		XMLMessage xmlMessage = xmlMessageMapper.map(testSpringMessage, excludedHeaders);
		Mockito.verify(xmlMessageMapper).map(testSpringMessage, excludedHeaders);

		assertEquals(null, xmlMessage.getProperties()
				.getMap("io.opentracing.contrib.spring.integration.messaging.OpenTracingChannelInterceptor.SCOPE"));
	}

	@Test
	public void testMapProducerSpringMessageToXMLMessage_WithExcludedHeader_ShouldNotMatchPartially() throws SDTException {
		String testPayload = "testPayload";
		List<String> excludedHeaders = new ArrayList<>(Arrays
				.asList("io.opentracing.contrib.spring.integration.messaging.OpenTracingChannelInterceptor.SCOPE"));
		Message<?> testSpringMessage = new DefaultMessageBuilderFactory().withPayload(testPayload)
				.setHeader(
						"io.opentracing.contrib.spring.integration.messaging.OpenTracingChannelInterceptor",
						"any")
				.build();

		XMLMessage xmlMessage = xmlMessageMapper.map(testSpringMessage, excludedHeaders);
		Mockito.verify(xmlMessageMapper).map(testSpringMessage, excludedHeaders);
		assertEquals("any", xmlMessage.getProperties()
				.get("io.opentracing.contrib.spring.integration.messaging.OpenTracingChannelInterceptor"));
	}

	@Test
	public void testMapProducerSpringMessageToXMLMessage_WithExcludedHeader_ShouldNotFilterSolaceHeader() throws SDTException {
		String testPayload = "testPayload";
		List<String> excludedHeaders = new ArrayList<>(Arrays
				.asList(SolaceHeaders.CORRELATION_ID));
		Message<?> testSpringMessage = new DefaultMessageBuilderFactory().withPayload(testPayload)
				.setHeader(SolaceHeaders.CORRELATION_ID, "some_unique_id")
				.build();

		XMLMessage xmlMessage = xmlMessageMapper.map(testSpringMessage, excludedHeaders);
		Mockito.verify(xmlMessageMapper).map(testSpringMessage, excludedHeaders);
		assertEquals("some_unique_id", xmlMessage.getCorrelationId());
	}

	@Test
	public void testMapXMLMessageToSpringMessage_ByteArray() throws Exception {
		BytesMessage xmlMessage = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
		xmlMessage.setData("testPayload".getBytes(StandardCharsets.UTF_8));
		SDTMap metadata = JCSMPFactory.onlyInstance().createMap();
		metadata.putString("test-header-1", "test-header-val-1");
		metadata.putString("test-header-2", "test-header-val-2");
		metadata.putString(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_OCTET_STREAM_VALUE);
		xmlMessage.setProperties(metadata);

		AcknowledgmentCallback acknowledgmentCallback = Mockito.mock(JCSMPAcknowledgementCallbackFactory.JCSMPAcknowledgementCallback.class);
		Message<?> springMessage = xmlMessageMapper.map(xmlMessage, acknowledgmentCallback);
		Mockito.verify(xmlMessageMapper).map(xmlMessage, acknowledgmentCallback, false);

		assertThat(springMessage.getPayload(), CoreMatchers.instanceOf(byte[].class));
		assertArrayEquals(xmlMessage.getData(), (byte[]) springMessage.getPayload());
		validateSpringMessage(springMessage, xmlMessage);
	}

	@Test
	public void testMapXMLMessageToSpringMessage_String() throws Exception {
		TextMessage xmlMessage = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
		xmlMessage.setText("testPayload");
		SDTMap metadata = JCSMPFactory.onlyInstance().createMap();
		metadata.putString("test-header-1", "test-header-val-1");
		metadata.putString("test-header-2", "test-header-val-2");
		metadata.putString(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE);
		xmlMessage.setProperties(metadata);

		AcknowledgmentCallback acknowledgmentCallback = Mockito.mock(JCSMPAcknowledgementCallbackFactory.JCSMPAcknowledgementCallback.class);
		Message<?> springMessage = xmlMessageMapper.map(xmlMessage, acknowledgmentCallback);
		Mockito.verify(xmlMessageMapper).map(xmlMessage, acknowledgmentCallback, false);

		assertThat(springMessage.getPayload(), CoreMatchers.instanceOf(String.class));
		assertEquals(xmlMessage.getText(), springMessage.getPayload());
		validateSpringMessage(springMessage, xmlMessage);
	}

	@Test
	public void testMapXMLMessageToSpringMessage_Serializable() throws Exception {
		BytesMessage xmlMessage = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
		SerializableFoo expectedPayload = new SerializableFoo("abc123", "HOOPLA!!");
		xmlMessage.setData(SerializationUtils.serialize(expectedPayload));
		SDTMap metadata = JCSMPFactory.onlyInstance().createMap();
		metadata.putString("test-header-1", "test-header-val-1");
		metadata.putString("test-header-2", "test-header-val-2");
		metadata.putString(MessageHeaders.CONTENT_TYPE, "application/x-java-serialized-object");
		metadata.putBoolean(SolaceBinderHeaders.SERIALIZED_PAYLOAD, true);
		xmlMessage.setProperties(metadata);

		AcknowledgmentCallback acknowledgmentCallback = Mockito.mock(JCSMPAcknowledgementCallbackFactory.JCSMPAcknowledgementCallback.class);
		Message<?> springMessage = xmlMessageMapper.map(xmlMessage, acknowledgmentCallback);
		Mockito.verify(xmlMessageMapper).map(xmlMessage, acknowledgmentCallback, false);

		assertThat(springMessage.getPayload(), CoreMatchers.instanceOf(SerializableFoo.class));
		assertEquals(expectedPayload, springMessage.getPayload());
		validateSpringMessage(springMessage, xmlMessage);
	}

	@Test
	public void testMapXMLMessageToSpringMessage_STDStream() throws Exception {
		SDTStream expectedPayload = JCSMPFactory.onlyInstance().createStream();
		expectedPayload.writeBoolean(true);
		expectedPayload.writeCharacter('s');
		expectedPayload.writeMap(JCSMPFactory.onlyInstance().createMap());
		expectedPayload.writeStream(JCSMPFactory.onlyInstance().createStream());
		StreamMessage xmlMessage = JCSMPFactory.onlyInstance().createMessage(StreamMessage.class);
		xmlMessage.setStream(expectedPayload);
		SDTMap metadata = JCSMPFactory.onlyInstance().createMap();
		metadata.putString("test-header-1", "test-header-val-1");
		metadata.putString("test-header-2", "test-header-val-2");
		metadata.putString(MessageHeaders.CONTENT_TYPE, "application/x-java-serialized-object");
		xmlMessage.setProperties(metadata);

		AcknowledgmentCallback acknowledgmentCallback = Mockito.mock(JCSMPAcknowledgementCallbackFactory.JCSMPAcknowledgementCallback.class);
		Message<?> springMessage = xmlMessageMapper.map(xmlMessage, acknowledgmentCallback);
		Mockito.verify(xmlMessageMapper).map(xmlMessage, acknowledgmentCallback, false);

		assertThat(springMessage.getPayload(), CoreMatchers.instanceOf(SDTStream.class));
		assertEquals(expectedPayload, springMessage.getPayload());
		validateSpringMessage(springMessage, xmlMessage);
	}

	@Test
	public void testMapXMLMessageToSpringMessage_STDMap() throws Exception {
		SDTMap expectedPayload = JCSMPFactory.onlyInstance().createMap();
		expectedPayload.putBoolean("a", true);
		expectedPayload.putCharacter("b", 's');
		expectedPayload.putMap("c", JCSMPFactory.onlyInstance().createMap());
		expectedPayload.putStream("d", JCSMPFactory.onlyInstance().createStream());
		MapMessage xmlMessage = JCSMPFactory.onlyInstance().createMessage(MapMessage.class);
		xmlMessage.setMap(expectedPayload);
		SDTMap metadata = JCSMPFactory.onlyInstance().createMap();
		metadata.putString("test-header-1", "test-header-val-1");
		metadata.putString("test-header-2", "test-header-val-2");
		metadata.putString(MessageHeaders.CONTENT_TYPE, "application/x-java-serialized-object");
		xmlMessage.setProperties(metadata);

		AcknowledgmentCallback acknowledgmentCallback = Mockito.mock(JCSMPAcknowledgementCallbackFactory.JCSMPAcknowledgementCallback.class);
		Message<?> springMessage = xmlMessageMapper.map(xmlMessage, acknowledgmentCallback);
		Mockito.verify(xmlMessageMapper).map(xmlMessage, acknowledgmentCallback, false);

		assertThat(springMessage.getPayload(), CoreMatchers.instanceOf(SDTMap.class));
		assertEquals(expectedPayload, springMessage.getPayload());
		validateSpringMessage(springMessage, xmlMessage);
	}

	@Test
	public void testMapXMLMessageToSpringMessage_XMLContent() throws Exception {
		XMLContentMessage xmlMessage = JCSMPFactory.onlyInstance().createMessage(XMLContentMessage.class);
		xmlMessage.setXMLContent("<a><b>testPayload</b><c>testPayload2</c></a>");
		SDTMap metadata = JCSMPFactory.onlyInstance().createMap();
		metadata.putString("test-header-1", "test-header-val-1");
		metadata.putString("test-header-2", "test-header-val-2");
		metadata.putString(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_XML_VALUE);
		xmlMessage.setProperties(metadata);

		AcknowledgmentCallback acknowledgmentCallback = Mockito.mock(JCSMPAcknowledgementCallbackFactory.JCSMPAcknowledgementCallback.class);
		Message<?> springMessage = xmlMessageMapper.map(xmlMessage, acknowledgmentCallback);
		Mockito.verify(xmlMessageMapper).map(xmlMessage, acknowledgmentCallback, false);

		assertThat(springMessage.getPayload(), CoreMatchers.instanceOf(String.class));
		assertEquals(xmlMessage.getXMLContent(), springMessage.getPayload());
		validateSpringMessage(springMessage, xmlMessage);
	}

	@Test
	public void testMapXMLMessageToSpringMessage_WithRawMessageHeader() throws Exception {
		BytesMessage xmlMessage = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
		SerializableFoo expectedPayload = new SerializableFoo("abc123", "HOOPLA!!");
		xmlMessage.setData(SerializationUtils.serialize(expectedPayload));
		SDTMap metadata = JCSMPFactory.onlyInstance().createMap();
		metadata.putString("test-header-1", "test-header-val-1");
		metadata.putString("test-header-2", "test-header-val-2");
		metadata.putString(MessageHeaders.CONTENT_TYPE, "application/x-java-serialized-object");
		metadata.putBoolean(SolaceBinderHeaders.SERIALIZED_PAYLOAD, true);
		xmlMessage.setProperties(metadata);

		AcknowledgmentCallback acknowledgmentCallback = Mockito.mock(JCSMPAcknowledgementCallbackFactory.JCSMPAcknowledgementCallback.class);
		Message<?> springMessage = xmlMessageMapper.map(xmlMessage, acknowledgmentCallback, true);

		assertThat(springMessage.getPayload(), CoreMatchers.instanceOf(SerializableFoo.class));
		assertEquals(expectedPayload, springMessage.getPayload());
		validateSpringMessage(springMessage, xmlMessage);

		assertEquals(xmlMessage, springMessage.getHeaders().get(SolaceBinderHeaders.RAW_MESSAGE));
	}

	@Test
	public void testMapXMLMessageToSpringMessage_WithContentTypeHeaderAndHTTPContentType() throws Exception {
		TextMessage xmlMessage = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
		xmlMessage.setText("testPayload");
		SDTMap metadata = JCSMPFactory.onlyInstance().createMap();
		metadata.putString(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE);
		xmlMessage.setProperties(metadata);
		xmlMessage.setHTTPContentType(MimeTypeUtils.TEXT_HTML_VALUE);

		AcknowledgmentCallback acknowledgmentCallback = Mockito.mock(JCSMPAcknowledgementCallbackFactory.JCSMPAcknowledgementCallback.class);
		Message<?> springMessage = xmlMessageMapper.map(xmlMessage, acknowledgmentCallback);
		Mockito.verify(xmlMessageMapper).map(xmlMessage, acknowledgmentCallback, false);

		assertEquals(metadata.getString(MessageHeaders.CONTENT_TYPE),
				Objects.requireNonNull(StaticMessageHeaderAccessor.getContentType(springMessage)).toString());
		validateSpringMessage(springMessage, xmlMessage);
	}

	@Test
	public void testMapXMLMessageToSpringMessage_ReadSolaceProperties() throws Exception {
		Set<Map.Entry<String, ? extends HeaderMeta<?>>> readableHeaders = Stream.of(
					SolaceHeaderMeta.META.entrySet().stream(),
					SolaceBinderHeaderMeta.META.entrySet().stream())
				.flatMap(h -> h)
				.filter(h -> h.getValue().isReadable())
				.collect(Collectors.toSet());
		assertNotEquals("Test header set was empty", 0, readableHeaders.size());

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

		AcknowledgmentCallback acknowledgmentCallback = Mockito.mock(JCSMPAcknowledgementCallbackFactory.JCSMPAcknowledgementCallback.class);
		Message<?> springMessage = xmlMessageMapper.map(xmlMessage, acknowledgmentCallback);
		Mockito.verify(xmlMessageMapper).map(xmlMessage, acknowledgmentCallback, false);

		for (Map.Entry<String, ? extends HeaderMeta<?>> header : readableHeaders) {
			Object actualValue = springMessage.getHeaders().get(header.getKey());
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
				case SolaceHeaders.PRIORITY:
					assertEquals(xmlMessage.getPriority(), actualValue);
					break;
				case SolaceHeaders.RECEIVE_TIMESTAMP:
					assertEquals(xmlMessage.getReceiveTimestamp(), actualValue);
					break;
				case SolaceHeaders.REDELIVERED:
					assertEquals(xmlMessage.getRedelivered(), actualValue);
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
				case SolaceBinderHeaders.RAW_MESSAGE:
					assertNull(actualValue);
					break;
				default:
					fail(String.format("no test for header %s", header.getKey()));
			}
		}

		validateSpringMessage(springMessage, xmlMessage);
	}

	@Test
	public void testMapXMLMessageToSpringMessage_NonReadableSolaceProperties() throws Exception {
		Set<Map.Entry<String, ? extends HeaderMeta<?>>> nonReadableHeaders = Stream.of(
				SolaceHeaderMeta.META.entrySet().stream(),
				SolaceBinderHeaderMeta.META.entrySet().stream())
				.flatMap(h -> h)
				.filter(h -> !h.getValue().isReadable())
				.collect(Collectors.toSet());
		assertNotEquals("Test header set was empty", 0, nonReadableHeaders.size());

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
				default:
					fail(String.format("no test for header %s", header.getKey()));
			}
		}

		Mockito.when(xmlMessage.getProperties()).thenReturn(metadata);
		Mockito.when(xmlMessage.getText()).thenReturn("testPayload");
		metadata.putString(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE);

		AcknowledgmentCallback acknowledgmentCallback = Mockito.mock(JCSMPAcknowledgementCallbackFactory.JCSMPAcknowledgementCallback.class);
		Message<?> springMessage = xmlMessageMapper.map(xmlMessage, acknowledgmentCallback);
		Mockito.verify(xmlMessageMapper).map(xmlMessage, acknowledgmentCallback, false);

		for (Map.Entry<String, ? extends HeaderMeta<?>> header : nonReadableHeaders) {
			assertThat(springMessage.getHeaders(), not(hasKey(header)));
		}

		SDTMap filteredMetadata = JCSMPFactory.onlyInstance().createMap();
		for (String metadataKey : metadata.keySet()) {
			if (nonReadableHeaders.stream().map(Map.Entry::getKey).noneMatch(metadataKey::equals)) {
				filteredMetadata.putObject(metadataKey, metadata.get(metadataKey));
			}
		}

		validateSpringMessage(springMessage, xmlMessage, filteredMetadata);
	}

	@Test
	public void testMapXMLMessageToSpringMessage_ReadLocalSolaceProperties() throws Exception {
		Set<Map.Entry<String, ? extends HeaderMeta<?>>> readableLocalHeaders = Stream.of(
				SolaceHeaderMeta.META.entrySet().stream(),
				SolaceBinderHeaderMeta.META.entrySet().stream())
				.flatMap(h -> h)
				.filter(h -> h.getValue().isReadable())
				.filter(h -> HeaderMeta.Scope.LOCAL.equals(h.getValue().getScope()))
				.collect(Collectors.toSet());
		assertNotEquals("Test header set was empty", 0, readableLocalHeaders.size());

		TextMessage xmlMessage = Mockito.mock(TextMessage.class);
		SDTMap metadata = JCSMPFactory.onlyInstance().createMap();

		for (Map.Entry<String, ? extends HeaderMeta<?>> header : readableLocalHeaders) {
			// Since these properties are local-scoped, their wire-values should be ignored
			metadata.putString(header.getKey(), "test");
		}

		Mockito.when(xmlMessage.getProperties()).thenReturn(metadata);
		Mockito.when(xmlMessage.getText()).thenReturn("testPayload");
		metadata.putString(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE);

		AcknowledgmentCallback acknowledgmentCallback = Mockito.mock(JCSMPAcknowledgementCallbackFactory.JCSMPAcknowledgementCallback.class);
		Message<?> springMessage = xmlMessageMapper.map(xmlMessage, acknowledgmentCallback);
		Mockito.verify(xmlMessageMapper).map(xmlMessage, acknowledgmentCallback, false);

		for (Map.Entry<String, ? extends HeaderMeta<?>> header : readableLocalHeaders) {
			switch (header.getKey()) {
				case SolaceBinderHeaders.RAW_MESSAGE:
					assertThat(springMessage.getHeaders(), not(hasKey(header)));
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

		validateSpringMessage(springMessage, xmlMessage, filteredMetadata);
	}

	@Test
	public void testMapXMLMessageToSpringMessage_ReadUndefinedSolaceHeader() throws Exception {
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

		AcknowledgmentCallback acknowledgmentCallback = Mockito.mock(JCSMPAcknowledgementCallbackFactory.JCSMPAcknowledgementCallback.class);
		Message<?> springMessage = xmlMessageMapper.map(xmlMessage, acknowledgmentCallback);

		assertEquals(undefinedSolaceHeader1, springMessage.getHeaders().get("solace_foo1", String.class));
		assertEquals(undefinedSolaceHeader2, springMessage.getHeaders().get("solace_foo2", SerializableFoo.class));
		validateSpringMessage(springMessage, xmlMessage);
	}

	@Test(expected = SolaceMessageConversionException.class)
	public void testFailMapXMLMessageToSpringMessage_WithNullPayload() {
		BytesMessage xmlMessage = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
		AcknowledgmentCallback acknowledgmentCallback = Mockito.mock(JCSMPAcknowledgementCallbackFactory.JCSMPAcknowledgementCallback.class);
		xmlMessageMapper.map(xmlMessage, acknowledgmentCallback);
	}

	@Test
	public void testMapMessageHeadersToSDTMap_Serializable() throws Exception {
		String key = "a";
		SerializableFoo value = new SerializableFoo("abc123", "HOOPLA!");
		Map<String,Object> headers = new HashMap<>();
		headers.put(key, value);
		headers.put(BinderHeaders.TARGET_DESTINATION, "redirected-target");

		SDTMap sdtMap = xmlMessageMapper.map(new MessageHeaders(headers), Collections.emptyList());

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
	public void testMapMessageHeadersToSDTMap_Null() throws Exception {
		String key = "a";
		Map<String,Object> headers = Collections.singletonMap(key, null);
		SDTMap sdtMap = xmlMessageMapper.map(new MessageHeaders(headers), Collections.emptyList());
		assertThat(sdtMap.keySet(), hasItem(key));
		assertNull(sdtMap.get(key));
	}

	@Test
	public void testMapMessageHeadersToSDTMap_NonJmsCompatible() throws Exception {
		byte[] value = "test".getBytes(); // byte[] values are not supported by JMS
		Map<String,Object> headers = new HashMap<>();
		JMS_INVALID_HEADER_NAMES.forEach(h -> headers.put(h, value));

		SDTMap sdtMap = xmlMessageMapper.map(new MessageHeaders(headers), Collections.emptyList());

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
			xmlMessage = xmlMessageMapper.map(springMessage);
			validateXMLMessage(xmlMessage, expectedSpringMessage, springHeaders);

			logger.info(String.format("Iteration %s - XMLMessage to Message<?>:\n%s", i, xmlMessage));
			AcknowledgmentCallback acknowledgmentCallback = Mockito.mock(JCSMPAcknowledgementCallbackFactory.JCSMPAcknowledgementCallback.class);
			springMessage = xmlMessageMapper.map(xmlMessage, acknowledgmentCallback);
			validateSpringMessage(springMessage, expectedXmlMessage);

			// Update the expected default spring headers
			springHeaders.put(MessageHeaders.ID, springMessage.getHeaders().getId());
			springHeaders.put(MessageHeaders.TIMESTAMP, springMessage.getHeaders().getTimestamp());

			i++;
		} while (i < 3);
	}

	private void validateXMLMessage(XMLMessage xmlMessage, Message<?> springMessage)
			throws Exception {
		validateXMLMessage(xmlMessage, springMessage, springMessage.getHeaders());
	}

	private void validateXMLMessage(XMLMessage xmlMessage, Message<?> springMessage, Map<String, Object> expectedHeaders)
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

	private void validateSpringMessage(Message<?> message, XMLMessage xmlMessage)
			throws SDTException, JsonProcessingException {
		validateSpringMessage(message, xmlMessage, xmlMessage.getProperties());
	}

	private void validateSpringMessage(Message<?> message, XMLMessage xmlMessage, SDTMap expectedHeaders)
			throws SDTException, JsonProcessingException {
		MessageHeaders messageHeaders = message.getHeaders();

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

		AcknowledgmentCallback ackCallback = StaticMessageHeaderAccessor.getAcknowledgmentCallback(message);
		assertNotNull(ackCallback);
		assertThat(ackCallback,
				CoreMatchers.instanceOf(JCSMPAcknowledgementCallbackFactory.JCSMPAcknowledgementCallback.class));

		AtomicInteger deliveryAttempt = StaticMessageHeaderAccessor.getDeliveryAttempt(message);
		assertNotNull(deliveryAttempt);
		assertThat(deliveryAttempt, CoreMatchers.instanceOf(AtomicInteger.class));
		assertEquals(0, deliveryAttempt.get());

		MimeType contentType = StaticMessageHeaderAccessor.getContentType(message);
		assertNotNull(contentType);
		if (!expectedHeaders.containsKey(MessageHeaders.CONTENT_TYPE)) {
			assertEquals(xmlMessage.getHTTPContentType(), contentType.toString());
		}
	}
}
