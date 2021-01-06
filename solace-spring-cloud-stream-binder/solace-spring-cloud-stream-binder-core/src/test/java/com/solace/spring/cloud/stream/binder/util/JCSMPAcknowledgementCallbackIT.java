package com.solace.spring.cloud.stream.binder.util;

import com.solace.spring.boot.autoconfigure.SolaceJavaAutoConfiguration;
import com.solace.spring.cloud.stream.binder.ITBase;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.test.integration.semp.v2.monitor.ApiException;
import com.solacesystems.jcsmp.Consumer;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.XMLMessageProducer;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.springframework.boot.test.context.ConfigFileApplicationContextInitializer;
import org.springframework.integration.acks.AcknowledgmentCallback;
import org.springframework.test.context.ContextConfiguration;

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertNotNull;

@RunWith(Parameterized.class)
@ContextConfiguration(classes = SolaceJavaAutoConfiguration.class,
		initializers = ConfigFileApplicationContextInitializer.class)
public class JCSMPAcknowledgementCallbackIT extends ITBase {
	@Rule
	public Timeout globalTimeout = new Timeout(1, TimeUnit.MINUTES);

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Parameterized.Parameter
	public String parameterSetName; // Only used for parameter set naming

	@Parameterized.Parameter(1)
	public boolean isDurable;

	private FlowReceiverContainer flowReceiverContainer;
	private JCSMPAcknowledgementCallbackFactory acknowledgementCallbackFactory;
	private XMLMessageProducer producer;
	private Queue queue;
	private Runnable closeErrorQueueInfrastructureCallback;

	private static final Log logger = LogFactory.getLog(JCSMPAcknowledgementCallbackIT.class);

	@Parameterized.Parameters(name = "{0}")
	public static Collection<?> headerSets() {
		return Arrays.asList(new Object[][]{
				{"Durable", true},
				{"Temporary", false}
		});
	}

	@Before
	public void setup() throws Exception {
		if (isDurable) {
			queue = JCSMPFactory.onlyInstance().createQueue(RandomStringUtils.randomAlphanumeric(20));
			EndpointProperties endpointProperties = new EndpointProperties();
			jcsmpSession.provision(queue, endpointProperties, JCSMPSession.WAIT_FOR_CONFIRM);
		} else {
			queue = jcsmpSession.createTemporaryQueue(RandomStringUtils.randomAlphanumeric(20));
		}

		flowReceiverContainer = new FlowReceiverContainer(jcsmpSession, queue.getName(), new EndpointProperties());
		flowReceiverContainer.bind();
		acknowledgementCallbackFactory = new JCSMPAcknowledgementCallbackFactory(flowReceiverContainer, !isDurable);
		producer = jcsmpSession.getMessageProducer(new JCSMPStreamingPublishCorrelatingEventHandler() {

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
		if (producer != null) {
			producer.close();
		}

		if (flowReceiverContainer != null) {
			Optional.ofNullable(flowReceiverContainer.get()).ifPresent(Consumer::close);
		}

		if (closeErrorQueueInfrastructureCallback != null) {
			closeErrorQueueInfrastructureCallback.run();
		}

		if (isDurable && jcsmpSession != null && !jcsmpSession.isClosed()) {
			jcsmpSession.deprovision(queue, JCSMPSession.WAIT_FOR_CONFIRM);
		}
	}

	@Test
	public void testAccept() throws Exception {
		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);
		MessageContainer messageContainer = flowReceiverContainer.receive((int) TimeUnit.SECONDS.toMillis(10));
		assertNotNull(messageContainer);
		AcknowledgmentCallback acknowledgmentCallback = acknowledgementCallbackFactory.createCallback(messageContainer);

		acknowledgmentCallback.acknowledge(AcknowledgmentCallback.Status.ACCEPT);
		Thread.sleep(TimeUnit.SECONDS.toMillis(3));

		validateNumEnqueuedMessages(queue.getName(), 0);
		validateNumRedeliveredMessages(queue.getName(), 0);
		validateQueueBindAttempts(queue.getName(), 1);
	}

	@Test
	public void testReject() throws Exception {
		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);
		MessageContainer messageContainer = flowReceiverContainer.receive((int) TimeUnit.SECONDS.toMillis(10));
		assertNotNull(messageContainer);
		AcknowledgmentCallback acknowledgmentCallback = acknowledgementCallbackFactory.createCallback(messageContainer);

		acknowledgmentCallback.acknowledge(AcknowledgmentCallback.Status.REJECT);
		Thread.sleep(TimeUnit.SECONDS.toMillis(3));

		if (isDurable) {
			// Message was redelivered
			validateNumEnqueuedMessages(queue.getName(), 1);
			validateNumRedeliveredMessages(queue.getName(), 1);
			validateQueueBindAttempts(queue.getName(), 2);
		} else {
			// Message was discarded
			validateNumEnqueuedMessages(queue.getName(), 0);
			validateNumRedeliveredMessages(queue.getName(), 0);
			validateQueueBindAttempts(queue.getName(), 1);
		}
	}

	@Test
	public void testRejectWithErrorQueue() throws Exception {
		TextMessage messageToSend = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
		messageToSend.setText("test");
		producer.send(messageToSend, queue);

		MessageContainer messageContainer = flowReceiverContainer.receive((int) TimeUnit.SECONDS.toMillis(10));
		assertNotNull(messageContainer);
		ErrorQueueInfrastructure errorQueueInfrastructure = initializeErrorQueueInfrastructure();
		AcknowledgmentCallback acknowledgmentCallback = acknowledgementCallbackFactory.createCallback(messageContainer);

		acknowledgmentCallback.acknowledge(AcknowledgmentCallback.Status.REJECT);
		Thread.sleep(TimeUnit.SECONDS.toMillis(3));

		validateNumEnqueuedMessages(queue.getName(), 0);
		validateNumRedeliveredMessages(queue.getName(), 0);
		validateQueueBindAttempts(queue.getName(), 1);
		validateNumEnqueuedMessages(errorQueueInfrastructure.getErrorQueueName(), 1);
		validateNumRedeliveredMessages(errorQueueInfrastructure.getErrorQueueName(), 0);
	}

	@Test
	public void testRequeue() throws Exception {
		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);

		MessageContainer messageContainer = flowReceiverContainer.receive((int) TimeUnit.SECONDS.toMillis(10));
		assertNotNull(messageContainer);
		AcknowledgmentCallback acknowledgmentCallback = acknowledgementCallbackFactory.createCallback(messageContainer);

		if (isDurable) {
			acknowledgmentCallback.acknowledge(AcknowledgmentCallback.Status.REQUEUE);
			Thread.sleep(TimeUnit.SECONDS.toMillis(3));

			validateNumEnqueuedMessages(queue.getName(), 1);
			validateNumRedeliveredMessages(queue.getName(), 1);
			validateQueueBindAttempts(queue.getName(), 2);
		} else {
			thrown.expect(SolaceAcknowledgmentException.class);
			thrown.expectCause(allOf(instanceOf(UnsupportedOperationException.class),
					hasProperty("message",
							containsString("not supported with temporary queues"))));
			acknowledgmentCallback.acknowledge(AcknowledgmentCallback.Status.REQUEUE);
		}
	}

	@Test
	public void testReAckAfterAccept() throws Exception {
		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);
		MessageContainer messageContainer = flowReceiverContainer.receive((int) TimeUnit.SECONDS.toMillis(10));
		assertNotNull(messageContainer);
		AcknowledgmentCallback acknowledgmentCallback = acknowledgementCallbackFactory.createCallback(messageContainer);

		Runnable verifyExpectedState = () -> {
			validateNumEnqueuedMessages(queue.getName(), 0);
			validateNumRedeliveredMessages(queue.getName(), 0);
			validateQueueBindAttempts(queue.getName(), 1);
		};

		acknowledgmentCallback.acknowledge(AcknowledgmentCallback.Status.ACCEPT);
		Thread.sleep(TimeUnit.SECONDS.toMillis(3));
		verifyExpectedState.run();

		for (AcknowledgmentCallback.Status status : AcknowledgmentCallback.Status.values()) {
			acknowledgmentCallback.acknowledge(status);
			verifyExpectedState.run();
		}
	}

	@Test
	public void testReAckAfterReject() throws Exception {
		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);
		MessageContainer messageContainer = flowReceiverContainer.receive((int) TimeUnit.SECONDS.toMillis(10));
		assertNotNull(messageContainer);
		AcknowledgmentCallback acknowledgmentCallback = acknowledgementCallbackFactory.createCallback(messageContainer);

		Runnable verifyExpectedState = () -> {
			if (isDurable) {
				// Message was redelivered
				validateNumEnqueuedMessages(queue.getName(), 1);
				validateNumRedeliveredMessages(queue.getName(), 1);
				validateQueueBindAttempts(queue.getName(), 2);
			} else {
				// Message was discarded
				validateNumEnqueuedMessages(queue.getName(), 0);
				validateNumRedeliveredMessages(queue.getName(), 0);
				validateQueueBindAttempts(queue.getName(), 1);
			}
		};

		acknowledgmentCallback.acknowledge(AcknowledgmentCallback.Status.REJECT);
		Thread.sleep(TimeUnit.SECONDS.toMillis(3));
		verifyExpectedState.run();

		for (AcknowledgmentCallback.Status status : AcknowledgmentCallback.Status.values()) {
			acknowledgmentCallback.acknowledge(status);
			verifyExpectedState.run();
		}
	}

	@Test
	public void testReAckAfterRejectWithErrorQueue() throws Exception {
		TextMessage messageToSend = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
		messageToSend.setText("test");
		producer.send(messageToSend, queue);

		MessageContainer messageContainer = flowReceiverContainer.receive((int) TimeUnit.SECONDS.toMillis(10));
		assertNotNull(messageContainer);
		ErrorQueueInfrastructure errorQueueInfrastructure = initializeErrorQueueInfrastructure();
		AcknowledgmentCallback acknowledgmentCallback = acknowledgementCallbackFactory.createCallback(messageContainer);

		Runnable verifyExpectedState = () -> {
			validateNumEnqueuedMessages(queue.getName(), 0);
			validateNumRedeliveredMessages(queue.getName(), 0);
			validateQueueBindAttempts(queue.getName(), 1);
			validateNumEnqueuedMessages(errorQueueInfrastructure.getErrorQueueName(), 1);
			validateNumRedeliveredMessages(errorQueueInfrastructure.getErrorQueueName(), 0);
		};

		acknowledgmentCallback.acknowledge(AcknowledgmentCallback.Status.REJECT);
		Thread.sleep(TimeUnit.SECONDS.toMillis(3));
		verifyExpectedState.run();

		for (AcknowledgmentCallback.Status status : AcknowledgmentCallback.Status.values()) {
			acknowledgmentCallback.acknowledge(status);
			verifyExpectedState.run();
		}
	}

	@Test
	public void testReAckAfterRequeue() throws Exception {
		if (!isDurable) {
			logger.info("This test does not apply for non-durable endpoints");
			return;
		}

		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);

		MessageContainer messageContainer = flowReceiverContainer.receive((int) TimeUnit.SECONDS.toMillis(10));
		assertNotNull(messageContainer);
		AcknowledgmentCallback acknowledgmentCallback = acknowledgementCallbackFactory.createCallback(messageContainer);

		Runnable verifyExpectedState = () -> {
			validateNumEnqueuedMessages(queue.getName(), 1);
			validateNumRedeliveredMessages(queue.getName(), 1);
			validateQueueBindAttempts(queue.getName(), 2);
		};

		acknowledgmentCallback.acknowledge(AcknowledgmentCallback.Status.REQUEUE);
		Thread.sleep(TimeUnit.SECONDS.toMillis(3));
		verifyExpectedState.run();

		for (AcknowledgmentCallback.Status status : AcknowledgmentCallback.Status.values()) {
			acknowledgmentCallback.acknowledge(status);
			verifyExpectedState.run();
		}
	}

	private ErrorQueueInfrastructure initializeErrorQueueInfrastructure() {
		if (closeErrorQueueInfrastructureCallback != null) {
			throw new IllegalStateException("Should only have one error queue infrastructure");
		}

		String producerManagerKey = UUID.randomUUID().toString();
		JCSMPSessionProducerManager jcsmpSessionProducerManager = new JCSMPSessionProducerManager(jcsmpSession);
		ErrorQueueInfrastructure errorQueueInfrastructure = new ErrorQueueInfrastructure(jcsmpSessionProducerManager,
				producerManagerKey, RandomStringUtils.randomAlphanumeric(20), new SolaceConsumerProperties());
		Queue errorQueue = JCSMPFactory.onlyInstance().createQueue(errorQueueInfrastructure.getErrorQueueName());
		acknowledgementCallbackFactory.setErrorQueueInfrastructure(errorQueueInfrastructure);
		closeErrorQueueInfrastructureCallback = () -> {
			jcsmpSessionProducerManager.release(producerManagerKey);

			try {
				jcsmpSession.deprovision(errorQueue, JCSMPSession.WAIT_FOR_CONFIRM);
			} catch (JCSMPException e) {
				throw new RuntimeException(e);
			}
		};

		try {
			jcsmpSession.provision(errorQueue, new EndpointProperties(), JCSMPSession.WAIT_FOR_CONFIRM);
		} catch (JCSMPException e) {
			throw new RuntimeException(e);
		}

		return errorQueueInfrastructure;
	}

	private void validateNumEnqueuedMessages(String queueName, int expectedCount) {
		String msgVpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);
		try {
			assertThat(sempV2Api.monitor()
					.getMsgVpnQueueMsgs(msgVpnName, queueName, null, null, null, null)
					.getData())
					.hasSize(expectedCount);
		} catch (ApiException e) {
			throw new RuntimeException(e);
		}
	}

	private void validateNumRedeliveredMessages(String queueName, int expectedCount) {
		String msgVpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);
		try {
			assertThat(sempV2Api.monitor()
					.getMsgVpnQueue(msgVpnName, queueName, null)
					.getData()
					.getRedeliveredMsgCount())
					.isEqualTo(expectedCount);
		} catch (ApiException e) {
			throw new RuntimeException(e);
		}
	}

	private void validateQueueBindAttempts(String queueName, int expectedCount) {
		String msgVpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);
		try {
			assertThat(sempV2Api.monitor()
					.getMsgVpnQueue(msgVpnName, queueName, null)
					.getData()
					.getBindRequestCount())
					.isEqualTo(expectedCount);
		} catch (ApiException e) {
			throw new RuntimeException(e);
		}
	}
}
