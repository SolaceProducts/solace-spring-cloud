package com.solace.spring.cloud.stream.binder.util;

import com.solace.spring.boot.autoconfigure.SolaceJavaAutoConfiguration;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.test.integration.junit.jupiter.extension.ExecutorServiceExtension;
import com.solace.test.integration.junit.jupiter.extension.ExecutorServiceExtension.ExecSvc;
import com.solace.test.integration.junit.jupiter.extension.PubSubPlusExtension;
import com.solace.test.integration.semp.v2.SempV2Api;
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
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.function.ThrowingRunnable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.integration.acks.AcknowledgmentCallback;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.solace.spring.cloud.stream.binder.test.util.RetryableAssertions.retryAssert;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringJUnitConfig(classes = SolaceJavaAutoConfiguration.class,
		initializers = ConfigDataApplicationContextInitializer.class)
@ExtendWith(ExecutorServiceExtension.class)
@ExtendWith(PubSubPlusExtension.class)
@Timeout(value = 1, unit = TimeUnit.MINUTES)
public class JCSMPAcknowledgementCallbackIT {
	private RetryableTaskService retryableTaskService;
	private final AtomicReference<FlowReceiverContainer> flowReceiverContainerReference = new AtomicReference<>();
	private XMLMessageProducer producer;
	private String vpnName;
	private Runnable closeErrorQueueInfrastructureCallback;

	private static final Log logger = LogFactory.getLog(JCSMPAcknowledgementCallbackIT.class);

	@BeforeEach
	public void setup(JCSMPSession jcsmpSession) throws Exception {
		retryableTaskService = Mockito.spy(new RetryableTaskService());
		producer = jcsmpSession.getMessageProducer(new JCSMPSessionProducerManager.CloudStreamEventHandler());
		vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);
	}

	@AfterEach
	public void cleanup() {
		if (producer != null) {
			producer.close();
		}

		Optional.ofNullable(flowReceiverContainerReference.getAndSet(null))
				.map(FlowReceiverContainer::getFlowReceiverReference)
				.map(FlowReceiverContainer.FlowReceiverReference::get)
				.ifPresent(Consumer::close);

		if (closeErrorQueueInfrastructureCallback != null) {
			closeErrorQueueInfrastructureCallback.run();
		}
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testAccept(boolean isDurable, JCSMPSession jcsmpSession, Queue durableQueue, SempV2Api sempV2Api)
			throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = initializeFlowReceiverContainer(jcsmpSession, queue);
		JCSMPAcknowledgementCallbackFactory acknowledgementCallbackFactory = new JCSMPAcknowledgementCallbackFactory(
				flowReceiverContainer, !isDurable, retryableTaskService);

		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);
		MessageContainer messageContainer = flowReceiverContainer.receive((int) TimeUnit.SECONDS.toMillis(10));
		assertNotNull(messageContainer);
		AcknowledgmentCallback acknowledgmentCallback = acknowledgementCallbackFactory.createCallback(messageContainer);

		acknowledgmentCallback.acknowledge(AcknowledgmentCallback.Status.ACCEPT);
		assertThat(acknowledgmentCallback.isAcknowledged()).isTrue();
		validateNumEnqueuedMessages(sempV2Api, queue.getName(), 0);
		validateNumRedeliveredMessages(sempV2Api, queue.getName(), 0);
		validateQueueBindSuccesses(sempV2Api, queue.getName(), 1);
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testReject(boolean isDurable, JCSMPSession jcsmpSession, Queue durableQueue, SempV2Api sempV2Api)
			throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = initializeFlowReceiverContainer(jcsmpSession, queue);
		JCSMPAcknowledgementCallbackFactory acknowledgementCallbackFactory = new JCSMPAcknowledgementCallbackFactory(
				flowReceiverContainer, !isDurable, retryableTaskService);

		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);
		MessageContainer messageContainer = flowReceiverContainer.receive((int) TimeUnit.SECONDS.toMillis(10));
		assertNotNull(messageContainer);
		AcknowledgmentCallback acknowledgmentCallback = acknowledgementCallbackFactory.createCallback(messageContainer);

		acknowledgmentCallback.acknowledge(AcknowledgmentCallback.Status.REJECT);
		assertThat(acknowledgmentCallback.isAcknowledged()).isTrue();

		if (isDurable) {
			// Message was redelivered
			validateNumRedeliveredMessages(sempV2Api, queue.getName(), 1);
			validateQueueBindSuccesses(sempV2Api, queue.getName(), 2);
			validateNumEnqueuedMessages(sempV2Api, queue.getName(), 1);
		} else {
			// Message was discarded
			validateNumEnqueuedMessages(sempV2Api, queue.getName(), 0);
			validateNumRedeliveredMessages(sempV2Api, queue.getName(), 0);
			validateQueueBindSuccesses(sempV2Api, queue.getName(), 1);
		}
	}

	@Test
	public void testRejectFail(JCSMPSession jcsmpSession, Queue queue, SempV2Api sempV2Api) throws Exception {
		FlowReceiverContainer flowReceiverContainer = initializeFlowReceiverContainer(jcsmpSession, queue);
		JCSMPAcknowledgementCallbackFactory acknowledgementCallbackFactory = new JCSMPAcknowledgementCallbackFactory(
				flowReceiverContainer, false, retryableTaskService);

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
		validateNumEnqueuedMessages(sempV2Api, queue.getName(), 1);
		validateNumRedeliveredMessages(sempV2Api, queue.getName(), 0);
		validateQueueBindSuccesses(sempV2Api, queue.getName(), 1);
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
		validateNumRedeliveredMessages(sempV2Api, queue.getName(), 1);
		validateQueueBindSuccesses(sempV2Api, queue.getName(), 2);
		validateNumEnqueuedMessages(sempV2Api, queue.getName(), 1);
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testRejectWithErrorQueue(boolean isDurable, JCSMPSession jcsmpSession, Queue durableQueue,
										 SempV2Api sempV2Api) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = initializeFlowReceiverContainer(jcsmpSession, queue);
		JCSMPAcknowledgementCallbackFactory acknowledgementCallbackFactory = new JCSMPAcknowledgementCallbackFactory(
				flowReceiverContainer, !isDurable, retryableTaskService);

		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);

		MessageContainer messageContainer = flowReceiverContainer.receive((int) TimeUnit.SECONDS.toMillis(10));
		assertNotNull(messageContainer);
		ErrorQueueInfrastructure errorQueueInfrastructure = initializeErrorQueueInfrastructure(jcsmpSession,
				acknowledgementCallbackFactory);
		AcknowledgmentCallback acknowledgmentCallback = acknowledgementCallbackFactory.createCallback(messageContainer);

		acknowledgmentCallback.acknowledge(AcknowledgmentCallback.Status.REJECT);

		assertThat(acknowledgmentCallback.isAcknowledged()).isTrue();
		validateNumEnqueuedMessages(sempV2Api, queue.getName(), 0);
		validateNumRedeliveredMessages(sempV2Api, queue.getName(), 0);
		validateQueueBindSuccesses(sempV2Api, queue.getName(), 1);
		validateNumEnqueuedMessages(sempV2Api, errorQueueInfrastructure.getErrorQueueName(), 1);
		validateNumRedeliveredMessages(sempV2Api, errorQueueInfrastructure.getErrorQueueName(), 0);
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testRejectWithErrorQueueFail(boolean isDurable, JCSMPSession jcsmpSession, Queue durableQueue,
											 SempV2Api sempV2Api) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = initializeFlowReceiverContainer(jcsmpSession, queue);
		JCSMPAcknowledgementCallbackFactory acknowledgementCallbackFactory = new JCSMPAcknowledgementCallbackFactory(
				flowReceiverContainer, !isDurable, retryableTaskService);

		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);

		MessageContainer messageContainer = flowReceiverContainer.receive((int) TimeUnit.SECONDS.toMillis(10));
		assertNotNull(messageContainer);
		ErrorQueueInfrastructure errorQueueInfrastructure = initializeErrorQueueInfrastructure(jcsmpSession,
				acknowledgementCallbackFactory);
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
			validateNumRedeliveredMessages(sempV2Api, queue.getName(), 1);
			validateQueueBindSuccesses(sempV2Api, queue.getName(), 2);
			validateNumEnqueuedMessages(sempV2Api, queue.getName(), 1);
		} else {
			// Message was discarded
			logger.info("Verifying message was discarded");
			validateNumEnqueuedMessages(sempV2Api, queue.getName(), 0);
			validateNumRedeliveredMessages(sempV2Api, queue.getName(), 0);
			validateQueueBindSuccesses(sempV2Api, queue.getName(), 1);
		}

		validateNumEnqueuedMessages(sempV2Api, errorQueueInfrastructure.getErrorQueueName(), 0);
		validateNumRedeliveredMessages(sempV2Api, errorQueueInfrastructure.getErrorQueueName(), 0);
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testRequeue(boolean isDurable, JCSMPSession jcsmpSession, Queue durableQueue, SempV2Api sempV2Api)
			throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = initializeFlowReceiverContainer(jcsmpSession, queue);
		JCSMPAcknowledgementCallbackFactory acknowledgementCallbackFactory = new JCSMPAcknowledgementCallbackFactory(
				flowReceiverContainer, !isDurable, retryableTaskService);

		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);

		MessageContainer messageContainer = flowReceiverContainer.receive((int) TimeUnit.SECONDS.toMillis(10));
		assertNotNull(messageContainer);
		AcknowledgmentCallback acknowledgmentCallback = acknowledgementCallbackFactory.createCallback(messageContainer);

		if (isDurable) {
			acknowledgmentCallback.acknowledge(AcknowledgmentCallback.Status.REQUEUE);
			assertThat(acknowledgmentCallback.isAcknowledged()).isTrue();
			validateNumRedeliveredMessages(sempV2Api, queue.getName(), 1);
			validateQueueBindSuccesses(sempV2Api, queue.getName(), 2);
			validateNumEnqueuedMessages(sempV2Api, queue.getName(), 1);
		} else {
			SolaceAcknowledgmentException thrown = assertThrows(SolaceAcknowledgmentException.class,
					() -> acknowledgmentCallback.acknowledge(AcknowledgmentCallback.Status.REQUEUE));
			assertThat(acknowledgmentCallback.isAcknowledged()).isFalse();
			assertThat(thrown).hasCauseInstanceOf(UnsupportedOperationException.class);
			assertThat(thrown.getCause()).hasMessageContaining("not supported with temporary queues");
		}
	}

	@Test
	public void testRequeueFail(JCSMPSession jcsmpSession, Queue queue, SempV2Api sempV2Api)
			throws Exception {
		FlowReceiverContainer flowReceiverContainer = initializeFlowReceiverContainer(jcsmpSession, queue);
		JCSMPAcknowledgementCallbackFactory acknowledgementCallbackFactory = new JCSMPAcknowledgementCallbackFactory(
				flowReceiverContainer, false, retryableTaskService);

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
		validateNumEnqueuedMessages(sempV2Api, queue.getName(), 1);
		validateNumRedeliveredMessages(sempV2Api, queue.getName(), 0);
		validateQueueBindSuccesses(sempV2Api, queue.getName(), 1);
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
		validateNumRedeliveredMessages(sempV2Api, queue.getName(), 1);
		validateQueueBindSuccesses(sempV2Api, queue.getName(), 2);
		validateNumEnqueuedMessages(sempV2Api, queue.getName(), 1);
	}

	@Test
	public void testRequeueDelegateWhileRebinding(JCSMPSession jcsmpSession, Queue queue, SempV2Api sempV2Api,
												  @ExecSvc(poolSize = 1) ExecutorService executorService)
			throws Exception {
		FlowReceiverContainer flowReceiverContainer = initializeFlowReceiverContainer(jcsmpSession, queue);
		JCSMPAcknowledgementCallbackFactory acknowledgementCallbackFactory = new JCSMPAcknowledgementCallbackFactory(
				flowReceiverContainer, false, retryableTaskService);

		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);
		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);

		MessageContainer messageContainer = flowReceiverContainer.receive((int) TimeUnit.SECONDS.toMillis(10));
		MessageContainer blockingContainer = flowReceiverContainer.receive((int) TimeUnit.SECONDS.toMillis(10));
		assertNotNull(messageContainer);
		AcknowledgmentCallback acknowledgmentCallback = acknowledgementCallbackFactory.createCallback(messageContainer);

		Future<UUID> rebindFuture = executorService.submit(() ->
				flowReceiverContainer.rebind(messageContainer.getFlowReceiverReferenceId()));
		Thread.sleep(1000);
		assertFalse(rebindFuture.isDone());

		Mockito.doReturn(null).doCallRealMethod().when(flowReceiverContainer)
				.acknowledgeRebind(messageContainer, true);

		logger.info(String.format("Acknowledging message container %s", messageContainer.getId()));
		acknowledgmentCallback.acknowledge(AcknowledgmentCallback.Status.REQUEUE);
		assertThat(acknowledgmentCallback.isAcknowledged()).isTrue();
		Mockito.verify(retryableTaskService)
				.submit(new RetryableAckRebindTask(flowReceiverContainer, messageContainer, retryableTaskService));
		Thread.sleep(TimeUnit.SECONDS.toMillis(3));

		logger.info(String.format("Verifying message container %s hasn't been ack'd", messageContainer.getId()));
		assertFalse(rebindFuture.isDone());
		assertThat(messageContainer.isAcknowledged()).isTrue();
		assertThat(messageContainer.isStale()).isFalse();
		validateNumEnqueuedMessages(sempV2Api, queue.getName(), 2);
		validateNumRedeliveredMessages(sempV2Api, queue.getName(), 0);
		validateQueueBindSuccesses(sempV2Api, queue.getName(), 1);

		flowReceiverContainer.acknowledge(blockingContainer);
		Thread.sleep(TimeUnit.SECONDS.toMillis(5)); // rebind task retry interval
		assertNotNull(rebindFuture.get(1, TimeUnit.MINUTES));

		// Message was redelivered
		logger.info("Verifying message was redelivered");
		validateNumRedeliveredMessages(sempV2Api, queue.getName(), 1);
		validateQueueBindSuccesses(sempV2Api, queue.getName(), 2);
		validateNumEnqueuedMessages(sempV2Api, queue.getName(), 1);
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testReAckAfterAccept(boolean isDurable, JCSMPSession jcsmpSession, Queue durableQueue,
									 SempV2Api sempV2Api) throws Throwable {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = initializeFlowReceiverContainer(jcsmpSession, queue);
		JCSMPAcknowledgementCallbackFactory acknowledgementCallbackFactory = new JCSMPAcknowledgementCallbackFactory(
				flowReceiverContainer, !isDurable, retryableTaskService);

		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);
		MessageContainer messageContainer = flowReceiverContainer.receive((int) TimeUnit.SECONDS.toMillis(10));
		assertNotNull(messageContainer);
		AcknowledgmentCallback acknowledgmentCallback = acknowledgementCallbackFactory.createCallback(messageContainer);

		ThrowingRunnable verifyExpectedState = () -> {
			validateNumEnqueuedMessages(sempV2Api, queue.getName(), 0);
			validateNumRedeliveredMessages(sempV2Api, queue.getName(), 0);
			validateQueueBindSuccesses(sempV2Api, queue.getName(), 1);
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

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testReAckAfterReject(boolean isDurable, JCSMPSession jcsmpSession, Queue durableQueue,
									 SempV2Api sempV2Api) throws Throwable {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = initializeFlowReceiverContainer(jcsmpSession, queue);
		JCSMPAcknowledgementCallbackFactory acknowledgementCallbackFactory = new JCSMPAcknowledgementCallbackFactory(
				flowReceiverContainer, !isDurable, retryableTaskService);

		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);
		MessageContainer messageContainer = flowReceiverContainer.receive((int) TimeUnit.SECONDS.toMillis(10));
		assertNotNull(messageContainer);
		AcknowledgmentCallback acknowledgmentCallback = acknowledgementCallbackFactory.createCallback(messageContainer);

		ThrowingRunnable verifyExpectedState = () -> {
			if (isDurable) {
				// Message was redelivered
				validateNumRedeliveredMessages(sempV2Api, queue.getName(), 1);
				validateQueueBindSuccesses(sempV2Api, queue.getName(), 2);
				validateNumEnqueuedMessages(sempV2Api, queue.getName(), 1);
			} else {
				// Message was discarded
				validateNumEnqueuedMessages(sempV2Api, queue.getName(), 0);
				validateNumRedeliveredMessages(sempV2Api, queue.getName(), 0);
				validateQueueBindSuccesses(sempV2Api, queue.getName(), 1);
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

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testReAckAfterRejectWithErrorQueue(boolean isDurable, JCSMPSession jcsmpSession, Queue durableQueue,
												   SempV2Api sempV2Api) throws Throwable {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = initializeFlowReceiverContainer(jcsmpSession, queue);
		JCSMPAcknowledgementCallbackFactory acknowledgementCallbackFactory = new JCSMPAcknowledgementCallbackFactory(
				flowReceiverContainer, !isDurable, retryableTaskService);

		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);

		MessageContainer messageContainer = flowReceiverContainer.receive((int) TimeUnit.SECONDS.toMillis(10));
		assertNotNull(messageContainer);
		ErrorQueueInfrastructure errorQueueInfrastructure = initializeErrorQueueInfrastructure(jcsmpSession,
				acknowledgementCallbackFactory);
		AcknowledgmentCallback acknowledgmentCallback = acknowledgementCallbackFactory.createCallback(messageContainer);

		ThrowingRunnable verifyExpectedState = () -> {
			validateNumEnqueuedMessages(sempV2Api, queue.getName(), 0);
			validateNumRedeliveredMessages(sempV2Api, queue.getName(), 0);
			validateQueueBindSuccesses(sempV2Api, queue.getName(), 1);
			validateNumEnqueuedMessages(sempV2Api, errorQueueInfrastructure.getErrorQueueName(), 1);
			validateNumRedeliveredMessages(sempV2Api, errorQueueInfrastructure.getErrorQueueName(), 0);
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
	public void testReAckAfterRequeue(JCSMPSession jcsmpSession, Queue queue, SempV2Api sempV2Api) throws Throwable {
		FlowReceiverContainer flowReceiverContainer = initializeFlowReceiverContainer(jcsmpSession, queue);
		JCSMPAcknowledgementCallbackFactory acknowledgementCallbackFactory = new JCSMPAcknowledgementCallbackFactory(
				flowReceiverContainer, false, retryableTaskService);

		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);

		MessageContainer messageContainer = flowReceiverContainer.receive((int) TimeUnit.SECONDS.toMillis(10));
		assertNotNull(messageContainer);
		AcknowledgmentCallback acknowledgmentCallback = acknowledgementCallbackFactory.createCallback(messageContainer);

		ThrowingRunnable verifyExpectedState = () -> {
			validateNumRedeliveredMessages(sempV2Api, queue.getName(), 1);
			validateQueueBindSuccesses(sempV2Api, queue.getName(), 2);
			validateNumEnqueuedMessages(sempV2Api, queue.getName(), 1);
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

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testAckStaleMessage(boolean isDurable, JCSMPSession jcsmpSession, Queue durableQueue,
									SempV2Api sempV2Api) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = initializeFlowReceiverContainer(jcsmpSession, queue);
		JCSMPAcknowledgementCallbackFactory acknowledgementCallbackFactory = new JCSMPAcknowledgementCallbackFactory(
				flowReceiverContainer, !isDurable, retryableTaskService);

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
			validateNumEnqueuedMessages(sempV2Api, queue.getName(), 1);
			validateNumRedeliveredMessages(sempV2Api, queue.getName(), 0);
			validateQueueBindSuccesses(sempV2Api, queue.getName(), 1);
		}
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testAckStaleMessageWithErrorQueue(boolean isDurable, JCSMPSession jcsmpSession, Queue durableQueue,
												  SempV2Api sempV2Api) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = initializeFlowReceiverContainer(jcsmpSession, queue);
		JCSMPAcknowledgementCallbackFactory acknowledgementCallbackFactory = new JCSMPAcknowledgementCallbackFactory(
				flowReceiverContainer, !isDurable, retryableTaskService);

		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);
		MessageContainer messageContainer = Mockito.spy(flowReceiverContainer.receive(
				(int) TimeUnit.SECONDS.toMillis(10)));
		assertNotNull(messageContainer);
		Mockito.when(messageContainer.isStale()).thenReturn(true);
		ErrorQueueInfrastructure errorQueueInfrastructure = initializeErrorQueueInfrastructure(jcsmpSession,
				acknowledgementCallbackFactory);
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
			validateNumEnqueuedMessages(sempV2Api, queue.getName(), 1);
			validateNumRedeliveredMessages(sempV2Api, queue.getName(), 0);
			validateQueueBindSuccesses(sempV2Api, queue.getName(), 1);
			validateNumEnqueuedMessages(sempV2Api, errorQueueInfrastructure.getErrorQueueName(), 0);
			validateNumRedeliveredMessages(sempV2Api, errorQueueInfrastructure.getErrorQueueName(), 0);
		}
	}

	private FlowReceiverContainer initializeFlowReceiverContainer(JCSMPSession jcsmpSession, Queue queue)
			throws JCSMPException {
		if (flowReceiverContainerReference.compareAndSet(null, Mockito.spy(new FlowReceiverContainer(
				jcsmpSession, queue.getName(), new EndpointProperties())))) {
			flowReceiverContainerReference.get().bind();
		}
		return flowReceiverContainerReference.get();
	}

	private ErrorQueueInfrastructure initializeErrorQueueInfrastructure(JCSMPSession jcsmpSession,
															JCSMPAcknowledgementCallbackFactory ackCallbackFactory) {
		if (closeErrorQueueInfrastructureCallback != null) {
			throw new IllegalStateException("Should only have one error queue infrastructure");
		}

		String producerManagerKey = UUID.randomUUID().toString();
		JCSMPSessionProducerManager jcsmpSessionProducerManager = new JCSMPSessionProducerManager(jcsmpSession);
		ErrorQueueInfrastructure errorQueueInfrastructure = new ErrorQueueInfrastructure(jcsmpSessionProducerManager,
				producerManagerKey, RandomStringUtils.randomAlphanumeric(20), new SolaceConsumerProperties(),
				new RetryableTaskService());
		Queue errorQueue = JCSMPFactory.onlyInstance().createQueue(errorQueueInfrastructure.getErrorQueueName());
		ackCallbackFactory.setErrorQueueInfrastructure(errorQueueInfrastructure);
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

	private void validateNumEnqueuedMessages(SempV2Api sempV2Api, String queueName, int expectedCount)
			throws InterruptedException {
		retryAssert(() -> assertThat(sempV2Api.monitor()
				.getMsgVpnQueueMsgs(vpnName, queueName, null, null, null, null)
				.getData())
				.hasSize(expectedCount));
	}

	private void validateNumRedeliveredMessages(SempV2Api sempV2Api, String queueName, int expectedCount)
			throws InterruptedException {
		retryAssert(() -> assertThat(sempV2Api.monitor()
				.getMsgVpnQueue(vpnName, queueName, null)
				.getData()
				.getRedeliveredMsgCount())
				.isEqualTo(expectedCount));
	}

	private void validateQueueBindSuccesses(SempV2Api sempV2Api, String queueName, int expectedCount)
			throws InterruptedException {
		retryAssert(() -> assertThat(sempV2Api.monitor()
				.getMsgVpnQueue(vpnName, queueName, null)
				.getData()
				.getBindSuccessCount())
				.isEqualTo(expectedCount));
	}
}
