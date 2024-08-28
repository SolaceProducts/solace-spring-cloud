package com.solace.spring.cloud.stream.binder.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.solace.spring.boot.autoconfigure.SolaceJavaAutoConfiguration;
import com.solace.spring.cloud.stream.binder.messaging.HeaderMeta;
import com.solace.spring.cloud.stream.binder.messaging.SolaceBinderHeaderMeta;
import com.solace.spring.cloud.stream.binder.messaging.SolaceBinderHeaders;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.test.util.SerializableFoo;
import com.solace.test.integration.junit.jupiter.extension.PubSubPlusExtension;
import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import com.solacesystems.jcsmp.MapMessage;
import com.solacesystems.jcsmp.SDTMap;
import com.solacesystems.jcsmp.SDTStream;
import com.solacesystems.jcsmp.StreamMessage;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.XMLMessage;
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.XMLMessageListener;
import com.solacesystems.jcsmp.XMLMessageProducer;
import com.solacesystems.jms.SolConnectionFactory;
import com.solacesystems.jms.SolJmsUtility;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.integration.support.DefaultMessageBuilderFactory;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.util.SerializationUtils;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringJUnitConfig(classes = SolaceJavaAutoConfiguration.class,
		initializers = ConfigDataApplicationContextInitializer.class)
@ExtendWith(PubSubPlusExtension.class)
public class JmsCompatibilityIT {
	private String topicName;
	private Connection jmsConnection;
	private Session jmsSession;
	private MessageProducer jmsProducer;
	private MessageConsumer jmsConsumer;
	private com.solacesystems.jcsmp.Topic jcsmpTopic;
	private XMLMessageProducer jcsmpProducer;
	private final XMLMessageMapper xmlMessageMapper = new XMLMessageMapper();

	private static final Logger LOGGER = LoggerFactory.getLogger(JmsCompatibilityIT.class);
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	@BeforeEach
	public void setup(JCSMPSession jcsmpSession) throws Exception {
		topicName = RandomStringUtils.randomAlphabetic(10);

		SolConnectionFactory solConnectionFactory = SolJmsUtility.createConnectionFactory();
		solConnectionFactory.setHost((String) jcsmpSession.getProperty(JCSMPProperties.HOST));
		solConnectionFactory.setVPN((String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME));
		solConnectionFactory.setUsername((String) jcsmpSession.getProperty(JCSMPProperties.USERNAME));
		solConnectionFactory.setPassword((String) jcsmpSession.getProperty(JCSMPProperties.PASSWORD));
		jmsConnection = solConnectionFactory.createConnection();
		jmsSession = jmsConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		jmsProducer = jmsSession.createProducer(jmsSession.createTopic(topicName));
		jmsConsumer = jmsSession.createConsumer(jmsSession.createTopic(topicName));

		jcsmpTopic = JCSMPFactory.onlyInstance().createTopic(topicName);
		jcsmpProducer = jcsmpSession.getMessageProducer(new JCSMPStreamingPublishCorrelatingEventHandler() {
			@Override
			public void responseReceivedEx(Object key) {
				LOGGER.debug("Got message with key: {}", key);
			}

			@Override
			public void handleErrorEx(Object o, JCSMPException e, long l) {
				LOGGER.error("Got error at {} while publishing message with key {}", l, o, e);
			}
		});
	}

	@AfterEach
	public void cleanup() throws Exception {
		if (jmsConnection != null) {
			jmsConnection.stop();
		}

		if (jcsmpProducer != null) {
			jcsmpProducer.close();
		}

		if (jmsProducer != null) {
			jmsProducer.close();
		}

		if (jmsConsumer != null) {
			jmsConsumer.close();
		}

		if (jmsSession != null) {
			jmsSession.close();
		}

		if (jmsConnection != null) {
			jmsConnection.close();
		}
	}

	@Test
	public void testBinderHeaders(SoftAssertions softly) throws Exception {
		MessageBuilder<SerializableFoo> springMessageBuilder = new DefaultMessageBuilderFactory()
				.withPayload(new SerializableFoo("abc", "def"));

		for (Map.Entry<String, SolaceBinderHeaderMeta<?>> headerMeta : SolaceBinderHeaderMeta.META.entrySet()) {
			if (!HeaderMeta.Scope.WIRE.equals(headerMeta.getValue().getScope())) continue;
			Class<?> type = headerMeta.getValue().getType();
			Object value;
			try {
				if (Number.class.isAssignableFrom(type)) {
					value = type.getConstructor(String.class).newInstance("" + RandomUtils.nextInt(0, 100));
				} else if (Boolean.class.isAssignableFrom(type)) {
					value = true;
				} else if (String.class.isAssignableFrom(type)) {
					value = RandomStringUtils.randomAlphanumeric(10);
				} else {
					value = type.newInstance();
				}
			} catch (Exception e) {
				throw new Exception(String.format("Failed to generate test value for %s", headerMeta.getKey()), e);
			}

			springMessageBuilder.setHeader(headerMeta.getKey(), value);
		}

		XMLMessage jcsmpMessage = xmlMessageMapper.map(springMessageBuilder.build(), null, false);

		AtomicReference<Exception> exceptionAtomicReference = new AtomicReference<>();
		CountDownLatch latch = new CountDownLatch(1);
		jmsConsumer.setMessageListener(msg -> {
			LOGGER.info("Got message {}", msg);
			try {
				for (Map.Entry<String, SolaceBinderHeaderMeta<?>> headerMeta : SolaceBinderHeaderMeta.META.entrySet()) {
					if (!HeaderMeta.Scope.WIRE.equals(headerMeta.getValue().getScope())) continue;
					String headerName = headerMeta.getKey();
					if (headerName.equals(SolaceBinderHeaders.PARTITION_KEY)) {
						headerName = XMLMessage.MessageUserPropertyConstants.QUEUE_PARTITION_KEY;
					}
					// Everything should be receivable as a String in JMS
					softly.assertThat(msg.getStringProperty(headerName))
							.withFailMessage("Expecting JMS property %s to not be null", headerName)
							.isNotNull();
				}

				@SuppressWarnings("unchecked")
				Enumeration<String> propertyNames = msg.getPropertyNames();
				while (propertyNames.hasMoreElements()) {
					String headerName = propertyNames.nextElement();
					// Everything should be receivable as a String in JMS
					softly.assertThat(msg.getStringProperty(headerName))
							.withFailMessage("Expecting JMS property %s to not be null", headerName)
							.isNotNull();
				}
			} catch (Exception e) {
				exceptionAtomicReference.set(e);
				throw new RuntimeException(e);
			} finally {
				latch.countDown();
			}
		});

		jmsConnection.start();
		jcsmpProducer.send(jcsmpMessage, jcsmpTopic);

		assertTrue(latch.await(1, TimeUnit.MINUTES));
		assertNull(exceptionAtomicReference.get());
	}

	@Test
	public void testSerializedHeaders(SoftAssertions softly) throws Exception {
		final String headerName = "abc";
		final SerializableFoo headerValue = new SerializableFoo("Abc", "def");

		XMLMessage jcsmpMessage = xmlMessageMapper.map(new DefaultMessageBuilderFactory()
				.withPayload("test")
				.setHeader(headerName, headerValue)
				.build(), null, false);

		AtomicReference<Exception> exceptionAtomicReference = new AtomicReference<>();
		CountDownLatch latch = new CountDownLatch(1);
		jmsConsumer.setMessageListener(msg -> {
			LOGGER.info("Got message {}", msg);
			try {
				softly.assertThat(msg.getStringProperty(SolaceBinderHeaders.SERIALIZED_HEADERS_ENCODING))
						.isEqualTo("base64");

				String serializedHeadersJson = msg.getStringProperty(SolaceBinderHeaders.SERIALIZED_HEADERS);
				softly.assertThat(serializedHeadersJson).isNotEmpty();

				List<String> serializedHeaders = OBJECT_MAPPER.readerFor(new TypeReference<List<String>>() {})
						.readValue(serializedHeadersJson);
				softly.assertThat(serializedHeaders).containsOnlyOnce(headerName);

				String encodedTestProperty = msg.getStringProperty(headerName);
				softly.assertThat(encodedTestProperty).isNotNull();
				softly.assertThat(encodedTestProperty).isBase64();

				byte[] decodedTestProperty = Base64.getDecoder().decode(encodedTestProperty);
				softly.assertThat(SerializationUtils.deserialize(decodedTestProperty)).isEqualTo(headerValue);
			} catch (Exception e) {
				exceptionAtomicReference.set(e);
				throw new RuntimeException(e);
			} finally {
				latch.countDown();
			}
		});

		jmsConnection.start();
		jcsmpProducer.send(jcsmpMessage, jcsmpTopic);

		assertTrue(latch.await(1, TimeUnit.MINUTES));
		assertNull(exceptionAtomicReference.get());
	}

	@Test
	public void testPayloadFromSpringToJms(SoftAssertions softly) throws Exception {
		List<Message<?>> messages = new ArrayList<>();

		{
			messages.add(new DefaultMessageBuilderFactory()
					.withPayload("test".getBytes())
					.build());

			messages.add(new DefaultMessageBuilderFactory()
					.withPayload("test")
					.build());

			SDTStream sdtStream = JCSMPFactory.onlyInstance().createStream();
			sdtStream.writeString("test");
			messages.add(new DefaultMessageBuilderFactory()
					.withPayload(sdtStream)
					.build());

			SDTMap sdtMap = JCSMPFactory.onlyInstance().createMap();
			sdtMap.putString("test", "test");
			messages.add(new DefaultMessageBuilderFactory()
					.withPayload(sdtMap)
					.build());
		}

		Set<Class<? extends XMLMessage>> processedMessageTypes = new HashSet<>();
		AtomicReference<Exception> exceptionAtomicReference = new AtomicReference<>();
		CountDownLatch latch = new CountDownLatch(messages.size());
		jmsConsumer.setMessageListener(msg -> {
			LOGGER.info("Got message {}", msg);
			try {
				if (msg instanceof javax.jms.BytesMessage) {
					javax.jms.BytesMessage bytesMessage = (javax.jms.BytesMessage) msg;
					byte[] payload = new byte[(int) bytesMessage.getBodyLength()];
					softly.assertThat(bytesMessage.readBytes(payload)).isEqualTo(bytesMessage.getBodyLength());
					softly.assertThat(payload).isEqualTo("test".getBytes());
					processedMessageTypes.add(BytesMessage.class);
				} else if (msg instanceof javax.jms.TextMessage) {
					softly.assertThat(((javax.jms.TextMessage) msg).getText()).isEqualTo("test");
					processedMessageTypes.add(TextMessage.class);
				} else if (msg instanceof javax.jms.StreamMessage) {
					softly.assertThat(((javax.jms.StreamMessage) msg).readString()).isEqualTo("test");
					processedMessageTypes.add(StreamMessage.class);
				} else if (msg instanceof javax.jms.MapMessage) {
					softly.assertThat(((javax.jms.MapMessage) msg).getString("test")).isEqualTo("test");
					processedMessageTypes.add(MapMessage.class);
				} else {
					throw new IllegalStateException(String.format("Message type %s has no test", msg.getClass()));
				}
			} catch (Exception e) {
				exceptionAtomicReference.set(e);
				while (latch.getCount() > 0) { // fail-fast
					latch.countDown();
				}
				throw new RuntimeException(e);
			} finally {
				latch.countDown();
			}
		});
		jmsConnection.start();

		for (Message<?> message : messages) {
			jcsmpProducer.send(xmlMessageMapper.map(message, null, false), jcsmpTopic);
		}

		assertTrue(latch.await(1, TimeUnit.MINUTES));
		assertNull(exceptionAtomicReference.get());
		assertEquals(messages.size(), processedMessageTypes.size());
	}

	@Test
	public void testPayloadFromJmsToSpring(JCSMPSession jcsmpSession, SoftAssertions softly) throws Exception {
		List<javax.jms.Message> messages = new ArrayList<>();

		{
			javax.jms.BytesMessage bytesMessage = jmsSession.createBytesMessage();
			bytesMessage.writeBytes("test".getBytes());
			messages.add(bytesMessage);

			messages.add(jmsSession.createTextMessage("test"));

			javax.jms.StreamMessage streamMessage = jmsSession.createStreamMessage();
			streamMessage.writeString("test");
			messages.add(streamMessage);

			javax.jms.MapMessage mapMessage = jmsSession.createMapMessage();
			mapMessage.setString("test", "test");
			messages.add(mapMessage);
		}

		SolaceConsumerProperties consumerProperties = new SolaceConsumerProperties();
		XMLMessageConsumer messageConsumer = null;
		try {
			Set<Class<? extends XMLMessage>> processedMessageTypes = new HashSet<>();
			AtomicReference<Exception> exceptionAtomicReference = new AtomicReference<>();
			CountDownLatch latch = new CountDownLatch(messages.size());
			messageConsumer = jcsmpSession.getMessageConsumer(new XMLMessageListener() {
				@Override
				public void onReceive(BytesXMLMessage bytesXMLMessage) {
					LOGGER.info("Got message {}", bytesXMLMessage);
					try {
						Message<?> msg = xmlMessageMapper.map(bytesXMLMessage, null, consumerProperties);
						if (msg.getPayload() instanceof byte[]) {
							softly.assertThat(msg.getPayload()).isEqualTo("test".getBytes());
							processedMessageTypes.add(BytesMessage.class);
						} else if (msg.getPayload() instanceof String) {
							softly.assertThat(msg.getPayload()).isEqualTo("test");
							processedMessageTypes.add(TextMessage.class);
						} else if (msg.getPayload() instanceof SDTStream) {
							softly.assertThat(((SDTStream) msg.getPayload()).readString()).isEqualTo("test");
							processedMessageTypes.add(StreamMessage.class);
						} else if (msg.getPayload() instanceof SDTMap) {
							softly.assertThat(((SDTMap) msg.getPayload()).getString("test")).isEqualTo("test");
							processedMessageTypes.add(MapMessage.class);
						}
					} catch (Exception e) {
						exceptionAtomicReference.set(e);
						throw new RuntimeException(e);
					} finally {
						latch.countDown();
					}
				}

				@Override
				public void onException(JCSMPException e) {}
			});

			jcsmpSession.addSubscription(JCSMPFactory.onlyInstance().createTopic(topicName));
			messageConsumer.start();
			for (javax.jms.Message message : messages) {
				jmsProducer.send(message);
			}

			assertTrue(latch.await(1, TimeUnit.MINUTES));
			assertNull(exceptionAtomicReference.get());
			assertEquals(messages.size(), processedMessageTypes.size());
		} finally {
			if (messageConsumer != null) messageConsumer.close();
		}
	}

	@Test
	public void testSerializedPayloadFromSpringToJms(SoftAssertions softly) throws Exception {
		final SerializableFoo payload = new SerializableFoo("Abc", "def");

		AtomicReference<Exception> exceptionAtomicReference = new AtomicReference<>();
		CountDownLatch latch = new CountDownLatch(1);
		jmsConsumer.setMessageListener(msg -> {
			LOGGER.info("Got message {}", msg);
			try {
				softly.assertThat(msg.getBooleanProperty(SolaceBinderHeaders.SERIALIZED_PAYLOAD)).isTrue();

				javax.jms.BytesMessage bytesMessage = (javax.jms.BytesMessage) msg;
				byte[] receivedPayload = new byte[(int) bytesMessage.getBodyLength()];
				bytesMessage.readBytes(receivedPayload);
				softly.assertThat(SerializationUtils.deserialize(receivedPayload)).isEqualTo(payload);
			} catch (Exception e) {
				exceptionAtomicReference.set(e);
				throw new RuntimeException(e);
			} finally {
				latch.countDown();
			}
		});

		jmsConnection.start();
		jcsmpProducer.send(xmlMessageMapper.map(new DefaultMessageBuilderFactory().withPayload(payload).build(),
				null, false),
				jcsmpTopic);

		assertTrue(latch.await(1, TimeUnit.MINUTES));
		assertNull(exceptionAtomicReference.get());
	}

	@Test
	public void testSerializedPayloadFromJmsToSpring(JCSMPSession jcsmpSession, SoftAssertions softly)
			throws Exception {
		SerializableFoo payload = new SerializableFoo("test", "test");
		ObjectMessage message = jmsSession.createObjectMessage(payload);
		message.setBooleanProperty(SolaceBinderHeaders.SERIALIZED_PAYLOAD, true);

		SolaceConsumerProperties consumerProperties = new SolaceConsumerProperties();
		XMLMessageConsumer messageConsumer = null;
		try {
			AtomicReference<Exception> exceptionAtomicReference = new AtomicReference<>();
			CountDownLatch latch = new CountDownLatch(1);
			messageConsumer = jcsmpSession.getMessageConsumer(new XMLMessageListener() {
				@Override
				public void onReceive(BytesXMLMessage bytesXMLMessage) {
					LOGGER.info("Got message {}", bytesXMLMessage);
					try {
						softly.assertThat(xmlMessageMapper.map(bytesXMLMessage, null, consumerProperties).getPayload())
								.isEqualTo(payload);
					} catch (Exception e) {
						exceptionAtomicReference.set(e);
						throw new RuntimeException(e);
					} finally {
						latch.countDown();
					}
				}

				@Override
				public void onException(JCSMPException e) {}
			});

			jcsmpSession.addSubscription(JCSMPFactory.onlyInstance().createTopic(topicName));
			messageConsumer.start();
			jmsProducer.send(message);

			assertTrue(latch.await(1, TimeUnit.MINUTES));
			assertNull(exceptionAtomicReference.get());
		} finally {
			if (messageConsumer != null) messageConsumer.close();
		}
	}
}
