package com.solace.spring.cloud.stream.binder.util;

import com.solace.spring.boot.autoconfigure.SolaceJavaAutoConfiguration;
import com.solace.spring.cloud.stream.binder.ITBase;
import com.solace.spring.cloud.stream.binder.messaging.SolaceBinderHeaderMeta;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import com.solacesystems.jcsmp.XMLMessage;
import com.solacesystems.jcsmp.XMLMessageProducer;
import com.solacesystems.jms.SolConnectionFactory;
import com.solacesystems.jms.SolJmsUtility;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.assertj.core.api.SoftAssertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.boot.test.context.ConfigFileApplicationContextInitializer;
import org.springframework.integration.support.DefaultMessageBuilderFactory;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.test.context.ContextConfiguration;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import java.util.Enumeration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@ContextConfiguration(classes = SolaceJavaAutoConfiguration.class,
		initializers = ConfigFileApplicationContextInitializer.class)
public class JmsCompatibilityIT extends ITBase {
	private Connection jmsConnection;
	private Session jmsSession;
	private MessageConsumer jmsConsumer;
	private com.solacesystems.jcsmp.Topic jcsmpTopic;
	private XMLMessageProducer jcsmpProducer;
	private final XMLMessageMapper xmlMessageMapper = new XMLMessageMapper();

	private static final Log logger = LogFactory.getLog(JmsCompatibilityIT.class);

	@Before
	public void setup() throws Exception {
		String topicName = RandomStringUtils.randomAlphabetic(10);

		SolConnectionFactory solConnectionFactory = SolJmsUtility.createConnectionFactory();
		solConnectionFactory.setHost((String) jcsmpSession.getProperty(JCSMPProperties.HOST));
		solConnectionFactory.setVPN((String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME));
		solConnectionFactory.setUsername((String) jcsmpSession.getProperty(JCSMPProperties.USERNAME));
		solConnectionFactory.setPassword((String) jcsmpSession.getProperty(JCSMPProperties.PASSWORD));
		jmsConnection = solConnectionFactory.createConnection();
		jmsSession = jmsConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		jmsConsumer = jmsSession.createConsumer(jmsSession.createTopic(topicName));

		jcsmpTopic = JCSMPFactory.onlyInstance().createTopic(topicName);
		jcsmpProducer = jcsmpSession.getMessageProducer(new JCSMPStreamingPublishCorrelatingEventHandler() {

			@Override
			public void handleError(String s, JCSMPException e, long l) {
				//never called
			}

			@Override
			public void responseReceived(String s) {
				//never called
			}

			@Override
			public void responseReceivedEx(Object key) {
				logger.debug("Got message with key: " + key);
			}

			@Override
			public void handleErrorEx(Object o, JCSMPException e, long l) {
				logger.error(e);
			}
		});
	}

	@After
	public void cleanup() throws Exception {
		if (jmsConnection != null) {
			jmsConnection.stop();
		}

		if (jcsmpProducer != null) {
			jcsmpProducer.close();
		}

		if (jmsSession != null) {
			jmsSession.close();
		}

		if (jmsConnection != null) {
			jmsConnection.close();
		}
	}

	@Test
	public void testBinderHeaders() throws Exception {
		MessageBuilder<SerializableFoo> springMessageBuilder = new DefaultMessageBuilderFactory()
				.withPayload(new SerializableFoo("abc", "def"));

		for (Map.Entry<String, SolaceBinderHeaderMeta<?>> headerMeta : SolaceBinderHeaderMeta.META.entrySet()) {
			Class<?> type = headerMeta.getValue().getType();
			Object value;
			try {
				if (Number.class.isAssignableFrom(type)) {
					value = type.getConstructor(String.class).newInstance("" + RandomUtils.nextInt(100));
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

		XMLMessage jcsmpMessage = xmlMessageMapper.map(springMessageBuilder.build());

		SoftAssertions softly = new SoftAssertions();
		AtomicReference<Exception> exceptionAtomicReference = new AtomicReference<>();
		CountDownLatch latch = new CountDownLatch(1);
		jmsConsumer.setMessageListener(msg -> {
			try {
				for (String headerName : SolaceBinderHeaderMeta.META.keySet()) {
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
			} catch (JMSException e) {
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
		softly.assertAll();
	}
}
