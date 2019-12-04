package com.solace.spring.cloud.stream.binder.util;

import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceProducerProperties;
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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.springframework.integration.StaticMessageHeaderAccessor;
import org.springframework.integration.acks.AcknowledgmentCallback;
import org.springframework.integration.support.DefaultMessageBuilderFactory;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.util.MimeTypeUtils;
import org.springframework.util.SerializationUtils;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class XMLMessageMapperTest {
	@Spy
	private XMLMessageMapper xmlMessageMapper = new XMLMessageMapper();

	@Before
	public void setupMockito() {
		MockitoAnnotations.initMocks(this);
	}

	private static Log logger = LogFactory.getLog(XMLMessageMapperTest.class);

	@Test
	public void testMapSpringMessageToXMLMessage_ByteArray() throws Exception {
		Message<?> testSpringMessage = new DefaultMessageBuilderFactory()
				.withPayload("testPayload".getBytes(StandardCharsets.UTF_8))
				.setHeader("test-header-1", "test-header-val-1")
				.setHeader("test-header-2", "test-header-val-2")
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_OCTET_STREAM_VALUE)
				.build();

		XMLMessage xmlMessage = xmlMessageMapper.map(testSpringMessage);

		Assert.assertThat(xmlMessage, CoreMatchers.instanceOf(BytesMessage.class));
		Assert.assertEquals(testSpringMessage.getPayload(), ((BytesMessage) xmlMessage).getData());
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

		Assert.assertThat(xmlMessage, CoreMatchers.instanceOf(TextMessage.class));
		Assert.assertEquals(testSpringMessage.getPayload(), ((TextMessage) xmlMessage).getText());
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

		Assert.assertThat(xmlMessage, CoreMatchers.instanceOf(BytesMessage.class));
		Assert.assertEquals(testSpringMessage.getPayload(),
				SerializationUtils.deserialize(((BytesMessage) xmlMessage).getData()));
		Assert.assertThat(xmlMessage.getProperties().keySet(),
				CoreMatchers.hasItem(XMLMessageMapper.JAVA_SERIALIZED_OBJECT_HEADER));
		Assert.assertEquals(true, xmlMessage.getProperties().getBoolean(XMLMessageMapper.JAVA_SERIALIZED_OBJECT_HEADER));
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

		Assert.assertThat(xmlMessage, CoreMatchers.instanceOf(StreamMessage.class));
		Assert.assertEquals(testSpringMessage.getPayload(), ((StreamMessage) xmlMessage).getStream());
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

		Assert.assertThat(xmlMessage, CoreMatchers.instanceOf(MapMessage.class));
		Assert.assertEquals(testSpringMessage.getPayload(), ((MapMessage) xmlMessage).getMap());
		validateXMLMessage(xmlMessage, testSpringMessage);
	}

	@Test(expected = SolaceMessageConversionException.class)
	public void testFailMapSpringMessageToXMLMessage_InvalidPayload() {
		Message<?> testSpringMessage = new DefaultMessageBuilderFactory().withPayload(new Object()).build();
		xmlMessageMapper.map(testSpringMessage);
	}

	@Test
	public void testMapProducerSpringMessageToXMLMessage() {
		String testPayload = "testPayload";
		Message<?> testSpringMessage = new DefaultMessageBuilderFactory().withPayload(testPayload).build();

		XMLMessage xmlMessage = xmlMessageMapper.map(testSpringMessage, new SolaceProducerProperties());
		Mockito.verify(xmlMessageMapper).map(testSpringMessage);

		Assert.assertFalse(xmlMessage.isDMQEligible());
		Assert.assertEquals(0, xmlMessage.getTimeToLive());
	}

	@Test
	public void testMapProducerSpringMessageToXMLMessage_WithProperties() {
		String testPayload = "testPayload";
		Message<?> testSpringMessage = new DefaultMessageBuilderFactory().withPayload(testPayload).build();
		SolaceProducerProperties producerProperties = new SolaceProducerProperties();
		producerProperties.setMsgInternalDmqEligible(true);
		producerProperties.setMsgTtl(100L);

		XMLMessage xmlMessage1 = xmlMessageMapper.map(testSpringMessage, producerProperties);
		Mockito.verify(xmlMessageMapper).map(testSpringMessage);

		Assert.assertTrue(xmlMessage1.isDMQEligible());
		Assert.assertEquals(producerProperties.getMsgTtl().longValue(), xmlMessage1.getTimeToLive());
	}

	@Test
	public void testMapConsumerSpringMessageToXMLMessage() {
		String testPayload = "testPayload";
		Message<?> testSpringMessage = new DefaultMessageBuilderFactory().withPayload(testPayload).build();

		XMLMessage xmlMessage = xmlMessageMapper.map(testSpringMessage, new SolaceConsumerProperties());
		Mockito.verify(xmlMessageMapper).map(testSpringMessage);

		Assert.assertEquals(0, xmlMessage.getTimeToLive());
	}

	@Test
	public void testMapConsumerSpringMessageToXMLMessage_WithProperties() {
		String testPayload = "testPayload";
		Message<?> testSpringMessage = new DefaultMessageBuilderFactory().withPayload(testPayload).build();
		SolaceConsumerProperties consumerProperties = new SolaceConsumerProperties();
		consumerProperties.setRepublishedMsgTtl(100L);

		XMLMessage xmlMessage = xmlMessageMapper.map(testSpringMessage, consumerProperties);
		Mockito.verify(xmlMessageMapper).map(testSpringMessage);

		Assert.assertEquals(consumerProperties.getRepublishedMsgTtl().longValue(), xmlMessage.getTimeToLive());
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

		Message<?> springMessage = xmlMessageMapper.map(xmlMessage);
		Mockito.verify(xmlMessageMapper).map(xmlMessage, false);

		Assert.assertThat(springMessage.getPayload(), CoreMatchers.instanceOf(byte[].class));
		Assert.assertArrayEquals(xmlMessage.getData(), (byte[]) springMessage.getPayload());
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

		Message<?> springMessage = xmlMessageMapper.map(xmlMessage);
		Mockito.verify(xmlMessageMapper).map(xmlMessage, false);

		Assert.assertThat(springMessage.getPayload(), CoreMatchers.instanceOf(String.class));
		Assert.assertEquals(xmlMessage.getText(), springMessage.getPayload());
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
		metadata.putBoolean(XMLMessageMapper.JAVA_SERIALIZED_OBJECT_HEADER, true);
		xmlMessage.setProperties(metadata);

		Message<?> springMessage = xmlMessageMapper.map(xmlMessage);
		Mockito.verify(xmlMessageMapper).map(xmlMessage, false);

		Assert.assertThat(springMessage.getPayload(), CoreMatchers.instanceOf(SerializableFoo.class));
		Assert.assertEquals(expectedPayload, springMessage.getPayload());
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

		Message<?> springMessage = xmlMessageMapper.map(xmlMessage);
		Mockito.verify(xmlMessageMapper).map(xmlMessage, false);

		Assert.assertThat(springMessage.getPayload(), CoreMatchers.instanceOf(SDTStream.class));
		Assert.assertEquals(expectedPayload, springMessage.getPayload());
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

		Message<?> springMessage = xmlMessageMapper.map(xmlMessage);
		Mockito.verify(xmlMessageMapper).map(xmlMessage, false);

		Assert.assertThat(springMessage.getPayload(), CoreMatchers.instanceOf(SDTMap.class));
		Assert.assertEquals(expectedPayload, springMessage.getPayload());
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

		Message<?> springMessage = xmlMessageMapper.map(xmlMessage);
		Mockito.verify(xmlMessageMapper).map(xmlMessage, false);

		Assert.assertThat(springMessage.getPayload(), CoreMatchers.instanceOf(String.class));
		Assert.assertEquals(xmlMessage.getXMLContent(), springMessage.getPayload());
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
		metadata.putBoolean(XMLMessageMapper.JAVA_SERIALIZED_OBJECT_HEADER, true);
		xmlMessage.setProperties(metadata);

		Message<?> springMessage = xmlMessageMapper.map(xmlMessage, true);

		Assert.assertThat(springMessage.getPayload(), CoreMatchers.instanceOf(SerializableFoo.class));
		Assert.assertEquals(expectedPayload, springMessage.getPayload());
		validateSpringMessage(springMessage, xmlMessage);

		Assert.assertEquals(xmlMessage,
				springMessage.getHeaders().get(SolaceMessageHeaderErrorMessageStrategy.SOLACE_RAW_MESSAGE));
	}

	@Test
	public void testMapXMLMessageToSpringMessage_WithContentTypeHeaderAndHTTPContentType() throws Exception {
		TextMessage xmlMessage = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
		xmlMessage.setText("testPayload");
		SDTMap metadata = JCSMPFactory.onlyInstance().createMap();
		metadata.putString(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE);
		xmlMessage.setProperties(metadata);
		xmlMessage.setHTTPContentType(MimeTypeUtils.TEXT_HTML_VALUE);

		Message<?> springMessage = xmlMessageMapper.map(xmlMessage);
		Mockito.verify(xmlMessageMapper).map(xmlMessage, false);

		Assert.assertEquals(metadata.getString(MessageHeaders.CONTENT_TYPE),
				Objects.requireNonNull(StaticMessageHeaderAccessor.getContentType(springMessage)).toString());
		validateSpringMessage(springMessage, xmlMessage);
	}

	@Test(expected = SolaceMessageConversionException.class)
	public void testFailMapXMLMessageToSpringMessage_WithNullPayload() {
		BytesMessage xmlMessage = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
		xmlMessageMapper.map(xmlMessage);
	}

	@Test
	public void testMapMessageHeadersToSDTMap_Serializable() throws Exception {
		String key = "a";
		SerializableFoo value = new SerializableFoo("abc123", "HOOPLA!");
		Map<String,Object> headers = new HashMap<>();
		headers.put(key, value);

		SDTMap sdtMap = xmlMessageMapper.map(new MessageHeaders(headers));

		Assert.assertThat(sdtMap.keySet(), CoreMatchers.hasItem(key));
		Assert.assertThat(sdtMap.keySet(), CoreMatchers.hasItem(xmlMessageMapper.getIsHeaderSerializedMetadataKey(key)));
		Assert.assertEquals(value, SerializationUtils.deserialize(sdtMap.getBytes(key)));
		Assert.assertEquals(true, sdtMap.getBoolean(xmlMessageMapper.getIsHeaderSerializedMetadataKey(key)));
	}

	@Test
	public void testMapSDTMapToMessageHeaders_Serializable() throws Exception {
		String key = "a";
		SerializableFoo value = new SerializableFoo("abc123", "HOOPLA!");
		SDTMap sdtMap = JCSMPFactory.onlyInstance().createMap();
		sdtMap.putObject(key, SerializationUtils.serialize(value));
		sdtMap.putBoolean(xmlMessageMapper.getIsHeaderSerializedMetadataKey(key), true);

		MessageHeaders messageHeaders = xmlMessageMapper.map(sdtMap);

		Assert.assertThat(messageHeaders.keySet(), CoreMatchers.hasItem(key));
		Assert.assertThat(messageHeaders.keySet(),
				CoreMatchers.not(CoreMatchers.hasItem(xmlMessageMapper.getIsHeaderSerializedMetadataKey(key))));
		Assert.assertEquals(value, messageHeaders.get(key));
		Assert.assertNull(messageHeaders.get(xmlMessageMapper.getIsHeaderSerializedMetadataKey(key)));
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
			springMessage = xmlMessageMapper.map(xmlMessage);
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

		Assert.assertEquals(DeliveryMode.PERSISTENT, xmlMessage.getDeliveryMode());
		Assert.assertEquals(Objects.requireNonNull(StaticMessageHeaderAccessor.getContentType(springMessage)).toString(),
				xmlMessage.getHTTPContentType());

		SDTMap metadata = xmlMessage.getProperties();

		Assert.assertEquals(XMLMessageMapper.BINDER_VERSION, metadata.getString(XMLMessageMapper.BINDER_VERSION_HEADER));

		for (Map.Entry<String,Object> header : expectedHeaders.entrySet()) {
			Assert.assertThat(metadata.keySet(), CoreMatchers.hasItem(header.getKey()));

			Object actualValue = metadata.get(header.getKey());
			if (metadata.containsKey(xmlMessageMapper.getIsHeaderSerializedMetadataKey(header.getKey()))) {
				actualValue = SerializationUtils.deserialize(metadata.getBytes(header.getKey()));
			}
			Assert.assertEquals(header.getValue(), actualValue);
		}
	}

	private void validateSpringMessage(Message<?> message, XMLMessage xmlMessage) throws SDTException {
		validateSpringMessage(message, xmlMessage, xmlMessage.getProperties());
	}

	private void validateSpringMessage(Message<?> message, XMLMessage xmlMessage, SDTMap expectedHeaders) throws SDTException {
		MessageHeaders messageHeaders = message.getHeaders();

		for (String customHeaderName : XMLMessageMapper.BINDER_INTERNAL_HEADERS) {
			Assert.assertThat(messageHeaders.keySet(), CoreMatchers.not(CoreMatchers.hasItem(customHeaderName)));
		}

		for (String headerName : expectedHeaders.keySet()) {
			if (XMLMessageMapper.BINDER_INTERNAL_HEADERS.contains(headerName)) continue;
			Assert.assertEquals(expectedHeaders.get(headerName), messageHeaders.get(headerName));
		}

		AcknowledgmentCallback ackCallback = StaticMessageHeaderAccessor.getAcknowledgmentCallback(message);
		Assert.assertNotNull(ackCallback);
		Assert.assertThat(ackCallback,
				CoreMatchers.instanceOf(JCSMPAcknowledgementCallbackFactory.JCSMPAcknowledgementCallback.class));

		AtomicInteger deliveryAttempt = StaticMessageHeaderAccessor.getDeliveryAttempt(message);
		Assert.assertNotNull(deliveryAttempt);
		Assert.assertThat(deliveryAttempt, CoreMatchers.instanceOf(AtomicInteger.class));
		Assert.assertEquals(0, deliveryAttempt.get());

		Object contentType = StaticMessageHeaderAccessor.getContentType(message);
		Assert.assertNotNull(contentType);
		if (!expectedHeaders.containsKey(MessageHeaders.CONTENT_TYPE)) {
			Assert.assertEquals(xmlMessage.getHTTPContentType(), contentType);
		}
	}
}
