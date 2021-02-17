package com.solace.spring.cloud.stream.binder.util;

import com.solace.spring.boot.autoconfigure.SolaceJavaAutoConfiguration;
import com.solace.spring.cloud.stream.binder.ITBase;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.test.integration.semp.v2.config.model.ConfigMsgVpnQueue;
import com.solacesystems.jcsmp.Consumer;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
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
import org.junit.function.ThrowingRunnable;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;
import org.springframework.boot.test.context.ConfigFileApplicationContextInitializer;
import org.springframework.integration.acks.AcknowledgmentCallback;
import org.springframework.test.context.ContextConfiguration;

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@ContextConfiguration(classes = SolaceJavaAutoConfiguration.class,
		initializers = ConfigFileApplicationContextInitializer.class)
public class JCSMPAcknowledgementCallbackIT extends ITBase {
	@Rule
	public Timeout globalTimeout = new Timeout(1, TimeUnit.MINUTES);

	@Parameterized.Parameter
	public String parameterSetName; // Only used for parameter set naming

	@Parameterized.Parameter(1)
	public boolean isDurable;

	private FlowReceiverContainer flowReceiverContainer;
	private JCSMPAcknowledgementCallbackFactory acknowledgementCallbackFactory;
	private XMLMessageProducer producer;
	private String vpnName;
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
		acknowledgementCallbackFactory = new JCSMPAcknowledgementCallbackFactory(flowReceiverContainer, !isDurable,
				new RetryableTaskService());
		producer = jcsmpSession.getMessageProducer(new JCSMPSessionProducerManager.CloudStreamEventHandler());
		vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);
	}

	@After
	public void cleanup() throws Exception {
		if (producer != null) {
			producer.close();
		}

		if (flowReceiverContainer != null) {
			Optional.ofNullable(flowReceiverContainer.getFlowReceiverReference())
					.map(FlowReceiverContainer.FlowReceiverReference::get)
					.ifPresent(Consumer::close);
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
		assertThat(acknowledgmentCallback.isAcknowledged()).isTrue();
		validateNumEnqueuedMessages(queue.getName(), 0);
		validateNumRedeliveredMessages(queue.getName(), 0);
		validateQueueBindSuccesses(queue.getName(), 1);
	}

	@Test
	public void testReject() throws Exception {
		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);
		MessageContainer messageContainer = flowReceiverContainer.receive((int) TimeUnit.SECONDS.toMillis(10));
		assertNotNull(messageContainer);
		AcknowledgmentCallback acknowledgmentCallback = acknowledgementCallbackFactory.createCallback(messageContainer);

		acknowledgmentCallback.acknowledge(AcknowledgmentCallback.Status.REJECT);
		assertThat(acknowledgmentCallback.isAcknowledged()).isTrue();

		if (isDurable) {
			// Message was redelivered
			validateNumRedeliveredMessages(queue.getName(), 1);
			validateQueueBindSuccesses(queue.getName(), 2);
			validateNumEnqueuedMessages(queue.getName(), 1);
		} else {
			// Message was discarded
			validateNumEnqueuedMessages(queue.getName(), 0);
			validateNumRedeliveredMessages(queue.getName(), 0);
			validateQueueBindSuccesses(queue.getName(), 1);
		}
	}

	@Test
	public void testRejectFail() throws Exception {
		if (!isDurable) {
			logger.info("This test does not apply for non-durable endpoints");
			return;
		}

		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);
		MessageContainer messageContainer = flowReceiverContainer.receive((int) TimeUnit.SECONDS.toMillis(10));
		assertNotNull(messageContainer);
		AcknowledgmentCallback acknowledgmentCallback = acknowledgementCallbackFactory.createCallback(messageContainer);

		logger.info(String.format("Disabling egress for queue %s", queue.getName()));
		sempV2Api.config().updateMsgVpnQueue((String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME),
				queue.getName(), new ConfigMsgVpnQueue().egressEnabled(false), null);
		retryAssert(() -> assertFalse(sempV2Api.monitor()
				.getMsgVpnQueue(vpnName, queue.getName(), null)
				.getData()
				.isEgressEnabled()));

		logger.info(String.format("Acknowledging message container %s", messageContainer.getId()));
		acknowledgmentCallback.acknowledge(AcknowledgmentCallback.Status.REJECT);
		assertThat(acknowledgmentCallback.isAcknowledged()).isTrue();
		Thread.sleep(TimeUnit.SECONDS.toMillis(3));

		logger.info(String.format("Verifying message container %s hasn't been ack'd", messageContainer.getId()));
		assertThat(messageContainer.isAcknowledged()).isFalse();
		assertThat(messageContainer.isStale()).isTrue();
		validateNumEnqueuedMessages(queue.getName(), 1);
		validateNumRedeliveredMessages(queue.getName(), 0);
		validateQueueBindSuccesses(queue.getName(), 1);
		assertThat(sempV2Api.monitor()
				.getMsgVpnQueue(vpnName, queue.getName(), null)
				.getData()
				.getBindRequestCount())
				.isGreaterThan(1);

		logger.info(String.format("Enabling egress for queue %s", queue.getName()));
		sempV2Api.config().updateMsgVpnQueue((String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME),
				queue.getName(), new ConfigMsgVpnQueue().egressEnabled(true), null);
		retryAssert(() -> assertTrue(sempV2Api.monitor()
				.getMsgVpnQueue(vpnName, queue.getName(), null)
				.getData()
				.isEgressEnabled()));
		Thread.sleep(TimeUnit.SECONDS.toMillis(5)); // rebind task retry interval

		// Message was redelivered
		logger.info("Verifying message was redelivered");
		validateNumRedeliveredMessages(queue.getName(), 1);
//		validateQueueBindSuccesses(queue.getName(), 2); //TODO Re-enable once SOL-45982 is fixed
		validateNumEnqueuedMessages(queue.getName(), 1);
	}

	@Test
	public void testRejectWithErrorQueue() throws Exception {
		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);

		MessageContainer messageContainer = flowReceiverContainer.receive((int) TimeUnit.SECONDS.toMillis(10));
		assertNotNull(messageContainer);
		ErrorQueueInfrastructure errorQueueInfrastructure = initializeErrorQueueInfrastructure();
		AcknowledgmentCallback acknowledgmentCallback = acknowledgementCallbackFactory.createCallback(messageContainer);

		acknowledgmentCallback.acknowledge(AcknowledgmentCallback.Status.REJECT);

		assertThat(acknowledgmentCallback.isAcknowledged()).isTrue();
		validateNumEnqueuedMessages(queue.getName(), 0);
		validateNumRedeliveredMessages(queue.getName(), 0);
		validateQueueBindSuccesses(queue.getName(), 1);
		validateNumEnqueuedMessages(errorQueueInfrastructure.getErrorQueueName(), 1);
		validateNumRedeliveredMessages(errorQueueInfrastructure.getErrorQueueName(), 0);
	}

	@Test
	public void testRejectWithErrorQueueFail() throws Exception {
		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);

		MessageContainer messageContainer = flowReceiverContainer.receive((int) TimeUnit.SECONDS.toMillis(10));
		assertNotNull(messageContainer);
		ErrorQueueInfrastructure errorQueueInfrastructure = initializeErrorQueueInfrastructure();
		AcknowledgmentCallback acknowledgmentCallback = acknowledgementCallbackFactory.createCallback(messageContainer);

		logger.info(String.format("Disabling ingress for error queue %s",
				errorQueueInfrastructure.getErrorQueueName()));
		sempV2Api.config().updateMsgVpnQueue((String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME),
				errorQueueInfrastructure.getErrorQueueName(), new ConfigMsgVpnQueue().ingressEnabled(false), null);
		retryAssert(() -> assertFalse(sempV2Api.monitor()
				.getMsgVpnQueue(vpnName, errorQueueInfrastructure.getErrorQueueName(), null)
				.getData()
				.isIngressEnabled()));

		logger.info(String.format("Acknowledging message container %s", messageContainer.getId()));
		acknowledgmentCallback.acknowledge(AcknowledgmentCallback.Status.REJECT);
		assertThat(acknowledgmentCallback.isAcknowledged()).isTrue();

		if (isDurable) {
			// Message was redelivered
			logger.info("Verifying message was redelivered");
			validateNumRedeliveredMessages(queue.getName(), 1);
			validateQueueBindSuccesses(queue.getName(), 2);
			validateNumEnqueuedMessages(queue.getName(), 1);
		} else {
			// Message was discarded
			logger.info("Verifying message was discarded");
			validateNumEnqueuedMessages(queue.getName(), 0);
			validateNumRedeliveredMessages(queue.getName(), 0);
			validateQueueBindSuccesses(queue.getName(), 1);
		}

		validateNumEnqueuedMessages(errorQueueInfrastructure.getErrorQueueName(), 0);
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
			assertThat(acknowledgmentCallback.isAcknowledged()).isTrue();
			validateNumRedeliveredMessages(queue.getName(), 1);
			validateQueueBindSuccesses(queue.getName(), 2);
			validateNumEnqueuedMessages(queue.getName(), 1);
		} else {
			SolaceAcknowledgmentException thrown = assertThrows(SolaceAcknowledgmentException.class,
					() -> acknowledgmentCallback.acknowledge(AcknowledgmentCallback.Status.REQUEUE));
			assertThat(acknowledgmentCallback.isAcknowledged()).isFalse();
			assertThat(thrown).hasCauseInstanceOf(UnsupportedOperationException.class);
			assertThat(thrown.getCause()).hasMessageContaining("not supported with temporary queues");
		}
	}

	@Test
	public void testRequeueFail() throws Exception {
		if (!isDurable) {
			logger.info("This test does not apply for non-durable endpoints");
			return;
		}

		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);

		MessageContainer messageContainer = flowReceiverContainer.receive((int) TimeUnit.SECONDS.toMillis(10));
		assertNotNull(messageContainer);
		AcknowledgmentCallback acknowledgmentCallback = acknowledgementCallbackFactory.createCallback(messageContainer);

		logger.info(String.format("Disabling egress for queue %s", queue.getName()));
		sempV2Api.config().updateMsgVpnQueue((String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME),
				queue.getName(), new ConfigMsgVpnQueue().egressEnabled(false), null);
		retryAssert(() -> assertFalse(sempV2Api.monitor()
				.getMsgVpnQueue(vpnName, queue.getName(), null)
				.getData()
				.isEgressEnabled()));

		logger.info(String.format("Acknowledging message container %s", messageContainer.getId()));
		acknowledgmentCallback.acknowledge(AcknowledgmentCallback.Status.REQUEUE);
		assertThat(acknowledgmentCallback.isAcknowledged()).isTrue();
		Thread.sleep(TimeUnit.SECONDS.toMillis(3));

		logger.info(String.format("Verifying message container %s hasn't been ack'd", messageContainer.getId()));
		assertThat(messageContainer.isAcknowledged()).isFalse();
		assertThat(messageContainer.isStale()).isTrue();
		validateNumEnqueuedMessages(queue.getName(), 1);
		validateNumRedeliveredMessages(queue.getName(), 0);
		validateQueueBindSuccesses(queue.getName(), 1);
		assertThat(sempV2Api.monitor()
				.getMsgVpnQueue(vpnName, queue.getName(), null)
				.getData()
				.getBindRequestCount())
				.isGreaterThan(1);

		logger.info(String.format("Enabling egress for queue %s", queue.getName()));
		sempV2Api.config().updateMsgVpnQueue((String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME),
				queue.getName(), new ConfigMsgVpnQueue().egressEnabled(true), null);
		retryAssert(() -> assertTrue(sempV2Api.monitor()
				.getMsgVpnQueue(vpnName, queue.getName(), null)
				.getData()
				.isEgressEnabled()));
		Thread.sleep(TimeUnit.SECONDS.toMillis(5)); // rebind task retry interval

		// Message was redelivered
		logger.info("Verifying message was redelivered");
		validateNumRedeliveredMessages(queue.getName(), 1);
//		validateQueueBindSuccesses(queue.getName(), 2); //TODO Re-enable once SOL-45982 is fixed
		validateNumEnqueuedMessages(queue.getName(), 1);
	}

	@Test
	public void testReAckAfterAccept() throws Throwable {
		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);
		MessageContainer messageContainer = flowReceiverContainer.receive((int) TimeUnit.SECONDS.toMillis(10));
		assertNotNull(messageContainer);
		AcknowledgmentCallback acknowledgmentCallback = acknowledgementCallbackFactory.createCallback(messageContainer);

		ThrowingRunnable verifyExpectedState = () -> {
			validateNumEnqueuedMessages(queue.getName(), 0);
			validateNumRedeliveredMessages(queue.getName(), 0);
			validateQueueBindSuccesses(queue.getName(), 1);
		};

		acknowledgmentCallback.acknowledge(AcknowledgmentCallback.Status.ACCEPT);
		assertThat(acknowledgmentCallback.isAcknowledged()).isTrue();
		verifyExpectedState.run();

		for (AcknowledgmentCallback.Status status : AcknowledgmentCallback.Status.values()) {
			acknowledgmentCallback.acknowledge(status);
			assertThat(acknowledgmentCallback.isAcknowledged()).isTrue();
			verifyExpectedState.run();
		}
	}

	@Test
	public void testReAckAfterReject() throws Throwable {
		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);
		MessageContainer messageContainer = flowReceiverContainer.receive((int) TimeUnit.SECONDS.toMillis(10));
		assertNotNull(messageContainer);
		AcknowledgmentCallback acknowledgmentCallback = acknowledgementCallbackFactory.createCallback(messageContainer);

		ThrowingRunnable verifyExpectedState = () -> {
			if (isDurable) {
				// Message was redelivered
				validateNumRedeliveredMessages(queue.getName(), 1);
				validateQueueBindSuccesses(queue.getName(), 2);
				validateNumEnqueuedMessages(queue.getName(), 1);
			} else {
				// Message was discarded
				validateNumEnqueuedMessages(queue.getName(), 0);
				validateNumRedeliveredMessages(queue.getName(), 0);
				validateQueueBindSuccesses(queue.getName(), 1);
			}
		};

		acknowledgmentCallback.acknowledge(AcknowledgmentCallback.Status.REJECT);
		assertThat(acknowledgmentCallback.isAcknowledged()).isTrue();
		verifyExpectedState.run();

		for (AcknowledgmentCallback.Status status : AcknowledgmentCallback.Status.values()) {
			acknowledgmentCallback.acknowledge(status);
			assertThat(acknowledgmentCallback.isAcknowledged()).isTrue();
			verifyExpectedState.run();
		}
	}

	@Test
	public void testReAckAfterRejectWithErrorQueue() throws Throwable {
		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);

		MessageContainer messageContainer = flowReceiverContainer.receive((int) TimeUnit.SECONDS.toMillis(10));
		assertNotNull(messageContainer);
		ErrorQueueInfrastructure errorQueueInfrastructure = initializeErrorQueueInfrastructure();
		AcknowledgmentCallback acknowledgmentCallback = acknowledgementCallbackFactory.createCallback(messageContainer);

		ThrowingRunnable verifyExpectedState = () -> {
			validateNumEnqueuedMessages(queue.getName(), 0);
			validateNumRedeliveredMessages(queue.getName(), 0);
			validateQueueBindSuccesses(queue.getName(), 1);
			validateNumEnqueuedMessages(errorQueueInfrastructure.getErrorQueueName(), 1);
			validateNumRedeliveredMessages(errorQueueInfrastructure.getErrorQueueName(), 0);
		};

		acknowledgmentCallback.acknowledge(AcknowledgmentCallback.Status.REJECT);
		assertThat(acknowledgmentCallback.isAcknowledged()).isTrue();
		verifyExpectedState.run();

		for (AcknowledgmentCallback.Status status : AcknowledgmentCallback.Status.values()) {
			acknowledgmentCallback.acknowledge(status);
			assertThat(acknowledgmentCallback.isAcknowledged()).isTrue();
			verifyExpectedState.run();
		}
	}

	@Test
	public void testReAckAfterRequeue() throws Throwable {
		if (!isDurable) {
			logger.info("This test does not apply for non-durable endpoints");
			return;
		}

		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);

		MessageContainer messageContainer = flowReceiverContainer.receive((int) TimeUnit.SECONDS.toMillis(10));
		assertNotNull(messageContainer);
		AcknowledgmentCallback acknowledgmentCallback = acknowledgementCallbackFactory.createCallback(messageContainer);

		ThrowingRunnable verifyExpectedState = () -> {
			validateNumRedeliveredMessages(queue.getName(), 1);
			validateQueueBindSuccesses(queue.getName(), 2);
			validateNumEnqueuedMessages(queue.getName(), 1);
		};

		acknowledgmentCallback.acknowledge(AcknowledgmentCallback.Status.REQUEUE);
		assertThat(acknowledgmentCallback.isAcknowledged()).isTrue();
		verifyExpectedState.run();

		for (AcknowledgmentCallback.Status status : AcknowledgmentCallback.Status.values()) {
			acknowledgmentCallback.acknowledge(status);
			assertThat(acknowledgmentCallback.isAcknowledged()).isTrue();
			verifyExpectedState.run();
		}
	}

	@Test
	public void testAckStaleMessage() throws Exception {
		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);
		MessageContainer messageContainer = Mockito.spy(flowReceiverContainer.receive(
				(int) TimeUnit.SECONDS.toMillis(10)));
		assertNotNull(messageContainer);
		Mockito.when(messageContainer.isStale()).thenReturn(true);
		AcknowledgmentCallback acknowledgmentCallback = acknowledgementCallbackFactory.createCallback(messageContainer);

		for (AcknowledgmentCallback.Status status : AcknowledgmentCallback.Status.values()) {
			SolaceAcknowledgmentException exception = assertThrows(SolaceAcknowledgmentException.class,
					() -> acknowledgmentCallback.acknowledge(status));
			assertThat(acknowledgmentCallback.isAcknowledged()).isFalse();
			if (!isDurable && status.equals(AcknowledgmentCallback.Status.REQUEUE)) {
				assertThat(exception).hasCauseInstanceOf(UnsupportedOperationException.class);
			} else {
				assertThat(exception).hasCauseInstanceOf(SolaceStaleMessageException.class);
			}
			validateNumEnqueuedMessages(queue.getName(), 1);
			validateNumRedeliveredMessages(queue.getName(), 0);
			validateQueueBindSuccesses(queue.getName(), 1);
		}
	}

	@Test
	public void testAckStaleMessageWithErrorQueue() throws Exception {
		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);
		MessageContainer messageContainer = Mockito.spy(flowReceiverContainer.receive(
				(int) TimeUnit.SECONDS.toMillis(10)));
		assertNotNull(messageContainer);
		Mockito.when(messageContainer.isStale()).thenReturn(true);
		ErrorQueueInfrastructure errorQueueInfrastructure = initializeErrorQueueInfrastructure();
		AcknowledgmentCallback acknowledgmentCallback = acknowledgementCallbackFactory.createCallback(messageContainer);

		for (AcknowledgmentCallback.Status status : AcknowledgmentCallback.Status.values()) {
			SolaceAcknowledgmentException exception = assertThrows(SolaceAcknowledgmentException.class,
					() -> acknowledgmentCallback.acknowledge(status));
			assertThat(acknowledgmentCallback.isAcknowledged()).isFalse();
			if (!isDurable && status.equals(AcknowledgmentCallback.Status.REQUEUE)) {
				assertThat(exception).hasCauseInstanceOf(UnsupportedOperationException.class);
			} else {
				assertThat(exception).hasCauseInstanceOf(SolaceStaleMessageException.class);
			}
			validateNumEnqueuedMessages(queue.getName(), 1);
			validateNumRedeliveredMessages(queue.getName(), 0);
			validateQueueBindSuccesses(queue.getName(), 1);
			validateNumEnqueuedMessages(errorQueueInfrastructure.getErrorQueueName(), 0);
			validateNumRedeliveredMessages(errorQueueInfrastructure.getErrorQueueName(), 0);
		}
	}

	private ErrorQueueInfrastructure initializeErrorQueueInfrastructure() {
		if (closeErrorQueueInfrastructureCallback != null) {
			throw new IllegalStateException("Should only have one error queue infrastructure");
		}

		String producerManagerKey = UUID.randomUUID().toString();
		JCSMPSessionProducerManager jcsmpSessionProducerManager = new JCSMPSessionProducerManager(jcsmpSession);
		ErrorQueueInfrastructure errorQueueInfrastructure = new ErrorQueueInfrastructure(jcsmpSessionProducerManager,
				producerManagerKey, RandomStringUtils.randomAlphanumeric(20), new SolaceConsumerProperties(),
				new RetryableTaskService());
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

	private void validateNumEnqueuedMessages(String queueName, int expectedCount) throws InterruptedException {
		retryAssert(() -> assertThat(sempV2Api.monitor()
				.getMsgVpnQueueMsgs(vpnName, queueName, null, null, null, null)
				.getData())
				.hasSize(expectedCount));
	}

	private void validateNumRedeliveredMessages(String queueName, int expectedCount) throws InterruptedException {
		retryAssert(() -> assertThat(sempV2Api.monitor()
				.getMsgVpnQueue(vpnName, queueName, null)
				.getData()
				.getRedeliveredMsgCount())
				.isEqualTo(expectedCount));
	}

	private void validateQueueBindSuccesses(String queueName, int expectedCount) throws InterruptedException {
		retryAssert(() -> assertThat(sempV2Api.monitor()
				.getMsgVpnQueue(vpnName, queueName, null)
				.getData()
				.getBindSuccessCount())
				.isEqualTo(expectedCount));
	}
}
