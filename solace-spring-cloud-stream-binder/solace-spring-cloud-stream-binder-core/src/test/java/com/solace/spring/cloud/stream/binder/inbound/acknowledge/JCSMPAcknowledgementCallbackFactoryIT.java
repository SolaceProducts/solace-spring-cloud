package com.solace.spring.cloud.stream.binder.inbound.acknowledge;

import com.solace.spring.boot.autoconfigure.SolaceJavaAutoConfiguration;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.util.ErrorQueueInfrastructure;
import com.solace.spring.cloud.stream.binder.util.FlowReceiverContainer;
import com.solace.spring.cloud.stream.binder.util.JCSMPSessionProducerManager;
import com.solace.spring.cloud.stream.binder.util.MessageContainer;
import com.solace.spring.cloud.stream.binder.util.SolaceAcknowledgmentException;
import com.solace.spring.cloud.stream.binder.util.SolaceBatchAcknowledgementException;
import com.solace.spring.cloud.stream.binder.util.UnboundFlowReceiverContainerException;
import com.solace.test.integration.junit.jupiter.extension.ExecutorServiceExtension;
import com.solace.test.integration.junit.jupiter.extension.PubSubPlusExtension;
import com.solace.test.integration.semp.v2.SempV2Api;
import com.solace.test.integration.semp.v2.config.model.ConfigMsgVpnQueue;
import com.solace.test.integration.semp.v2.monitor.model.MonitorMsgVpnQueueMsg;
import com.solace.test.integration.semp.v2.monitor.model.MonitorMsgVpnQueueMsgsResponse;
import com.solace.test.integration.semp.v2.monitor.model.MonitorSempMeta;
import com.solace.test.integration.semp.v2.monitor.model.MonitorSempPaging;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.ClosedFacilityException;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.XMLMessage.Outcome;
import com.solacesystems.jcsmp.XMLMessageProducer;
import com.solacesystems.jcsmp.transaction.RollbackException;
import com.solacesystems.jcsmp.transaction.TransactedSession;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.assertj.core.api.SoftAssertions;
import org.junit.function.ThrowingRunnable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Values;
import org.mockito.Mockito;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.integration.acks.AcknowledgmentCallback;
import org.springframework.lang.Nullable;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.solace.spring.cloud.stream.binder.test.util.RetryableAssertions.retryAssert;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

@SpringJUnitConfig(classes = SolaceJavaAutoConfiguration.class,
		initializers = ConfigDataApplicationContextInitializer.class)
@ExtendWith(ExecutorServiceExtension.class)
@ExtendWith(PubSubPlusExtension.class)
@Timeout(value = 2, unit = TimeUnit.MINUTES)
public class JCSMPAcknowledgementCallbackFactoryIT {
	private final AtomicReference<FlowReceiverContainer> flowReceiverContainerReference = new AtomicReference<>();
	private final AtomicReference<FlowReceiverContainer> transactedFlowReceiverContainerReference = new AtomicReference<>();
	private XMLMessageProducer producer;
	private String vpnName;
	private Runnable closeErrorQueueInfrastructureCallback;

	private static final Log logger = LogFactory.getLog(JCSMPAcknowledgementCallbackFactoryIT.class);

	@BeforeEach
	public void setup(JCSMPSession jcsmpSession) throws Exception {
		producer = jcsmpSession.getMessageProducer(new JCSMPSessionProducerManager.CloudStreamEventHandler());
		vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);
	}

	@AfterEach
	public void cleanup() {
		if (producer != null) {
			producer.close();
		}

		if (closeErrorQueueInfrastructureCallback != null) {
			closeErrorQueueInfrastructureCallback.run();
		}
	}

	@CartesianTest(name = "[{index}] numMessages={0}, transacted={1} isDurable={2}")
	public void testAccept(@Values(ints = {1, 255}) int numMessages,
						   @Values(booleans = {false, true}) boolean transacted,
						   @Values(booleans = {false, true}) boolean isDurable,
						   JCSMPSession jcsmpSession,
						   Queue durableQueue,
						   SempV2Api sempV2Api) throws Exception {
		if (numMessages == 1 && transacted) {
			logger.info("No support yet for non-batched, transacted consumers");
			return;
		}

		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = initializeFlowReceiverContainer(jcsmpSession, queue, transacted);
		JCSMPAcknowledgementCallbackFactory acknowledgementCallbackFactory = new JCSMPAcknowledgementCallbackFactory(
				flowReceiverContainer);
		List<MessageContainer> messageContainers = sendAndReceiveMessages(queue, flowReceiverContainer, numMessages);
		AcknowledgmentCallback acknowledgmentCallback = createAcknowledgmentCallback(acknowledgementCallbackFactory,
				messageContainers, flowReceiverContainer.getTransactedSession());

		acknowledgmentCallback.acknowledge(AcknowledgmentCallback.Status.ACCEPT);
		assertThat(acknowledgmentCallback.isAcknowledged()).isTrue();
		validateNumEnqueuedMessages(sempV2Api, queue.getName(), 0);
		validateNumRedeliveredMessages(sempV2Api, queue.getName(), 0);
		validateQueueBindSuccesses(sempV2Api, queue.getName(), 1);

		if (transacted) {
			validateTransaction(sempV2Api, jcsmpSession, numMessages, true);
		}
	}

	@CartesianTest(name = "[{index}] numMessages={0}, transacted={1}, isDurable={2}")
	public void testReject(@Values(ints = {1, 255}) int numMessages,
						   @Values(booleans = {false, true}) boolean transacted,
						   @Values(booleans = {false, true}) boolean isDurable,
						   JCSMPSession jcsmpSession,
						   Queue durableQueue,
						   SempV2Api sempV2Api)
			throws Exception {
		if (numMessages == 1 && transacted) {
			logger.info("No support yet for non-batched, transacted consumers");
			return;
		}

		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = initializeFlowReceiverContainer(jcsmpSession, queue, transacted);
		JCSMPAcknowledgementCallbackFactory acknowledgementCallbackFactory = new JCSMPAcknowledgementCallbackFactory(
				flowReceiverContainer);
		List<MessageContainer> messageContainers = sendAndReceiveMessages(queue, flowReceiverContainer, numMessages);
		AcknowledgmentCallback acknowledgmentCallback = createAcknowledgmentCallback(acknowledgementCallbackFactory,
				messageContainers, flowReceiverContainer.getTransactedSession());

		acknowledgmentCallback.acknowledge(AcknowledgmentCallback.Status.REJECT);
		assertThat(acknowledgmentCallback.isAcknowledged()).isTrue();

		validateNumEnqueuedMessages(sempV2Api, queue.getName(), transacted ? numMessages : 0);
		validateNumRedeliveredMessages(sempV2Api, queue.getName(), transacted ? numMessages : 0);
		validateQueueBindSuccesses(sempV2Api, queue.getName(), 1);

		if (transacted) {
			validateTransaction(sempV2Api, jcsmpSession, numMessages, false);
		}
	}

	@CartesianTest(name = "[{index}] numMessages={0}, transacted={1}")
	public void testRejectFail(@Values(ints = {1, 255}) int numMessages,
							   @Values(booleans = {false, true}) boolean transacted,
							   JCSMPSession jcsmpSession,
							   Queue queue,
							   SempV2Api sempV2Api) throws Exception {
		if (numMessages == 1 && transacted) {
			logger.info("No support yet for non-batched, transacted consumers");
			return;
		}

		FlowReceiverContainer flowReceiverContainer = initializeFlowReceiverContainer(jcsmpSession, queue, transacted);
		JCSMPAcknowledgementCallbackFactory acknowledgementCallbackFactory = new JCSMPAcknowledgementCallbackFactory(
				flowReceiverContainer);
		List<MessageContainer> messageContainers = sendAndReceiveMessages(queue, flowReceiverContainer, numMessages);
		AcknowledgmentCallback acknowledgmentCallback = createAcknowledgmentCallback(acknowledgementCallbackFactory,
				messageContainers, flowReceiverContainer.getTransactedSession());

		logger.info(String.format("Disabling egress for queue %s", queue.getName()));
		sempV2Api.config().updateMsgVpnQueue((String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME),
				queue.getName(), new ConfigMsgVpnQueue().egressEnabled(false), null);
		retryAssert(() -> assertFalse(sempV2Api.monitor()
				.getMsgVpnQueue(vpnName, queue.getName(), null)
				.getData()
				.isEgressEnabled()));

		logger.info("Acknowledging messages");
		acknowledgmentCallback.acknowledge(AcknowledgmentCallback.Status.REJECT);
		assertThat(acknowledgmentCallback.isAcknowledged()).isTrue();

		validateNumEnqueuedMessages(sempV2Api, queue.getName(), numMessages);
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

		// Message was redelivered
		logger.info("Verifying message was redelivered");
		validateNumRedeliveredMessages(sempV2Api, queue.getName(), numMessages);
		validateQueueBindSuccesses(sempV2Api, queue.getName(), 2);
		validateNumEnqueuedMessages(sempV2Api, queue.getName(), numMessages);
		if (transacted) {
			validateTransaction(sempV2Api, jcsmpSession, numMessages, false);
		}
	}

	@CartesianTest(name = "[{index}] numMessages={0}, isDurable={1}")
	public void testRejectWithErrorQueue(@Values(ints = {1, 255}) int numMessages,
										 @Values(booleans = {false, true}) boolean isDurable,
										 JCSMPSession jcsmpSession,
										 Queue durableQueue,
										 SempV2Api sempV2Api) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = initializeFlowReceiverContainer(jcsmpSession, queue, false); //TODO Add support for transacted error queue
		JCSMPAcknowledgementCallbackFactory acknowledgementCallbackFactory = new JCSMPAcknowledgementCallbackFactory(
				flowReceiverContainer);

		List<MessageContainer> messageContainers = sendAndReceiveMessages(queue, flowReceiverContainer, numMessages);
		ErrorQueueInfrastructure errorQueueInfrastructure = initializeErrorQueueInfrastructure(jcsmpSession,
				acknowledgementCallbackFactory);
		AcknowledgmentCallback acknowledgmentCallback = createAcknowledgmentCallback(acknowledgementCallbackFactory,
				messageContainers, flowReceiverContainer.getTransactedSession());

		acknowledgmentCallback.acknowledge(AcknowledgmentCallback.Status.REJECT);

		assertThat(acknowledgmentCallback.isAcknowledged()).isTrue();
		validateNumEnqueuedMessages(sempV2Api, queue.getName(), 0);
		validateNumRedeliveredMessages(sempV2Api, queue.getName(), 0);
		validateQueueBindSuccesses(sempV2Api, queue.getName(), 1);
		validateNumEnqueuedMessages(sempV2Api, errorQueueInfrastructure.getErrorQueueName(), numMessages);
		validateNumRedeliveredMessages(sempV2Api, errorQueueInfrastructure.getErrorQueueName(), 0);
	}

	@CartesianTest(name = "[{index}] numMessages={0}, isDurable={1}")
	public void testRejectWithErrorQueueFail(@Values(ints = {1, 255}) int numMessages,
											 @Values(booleans = {false, true}) boolean isDurable,
											 JCSMPSession jcsmpSession,
											 Queue durableQueue,
											 SempV2Api sempV2Api) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = initializeFlowReceiverContainer(jcsmpSession, queue, false); //TODO add support for transacted error queue
		JCSMPAcknowledgementCallbackFactory acknowledgementCallbackFactory = new JCSMPAcknowledgementCallbackFactory(
				flowReceiverContainer);

		List<MessageContainer> messageContainers = sendAndReceiveMessages(queue, flowReceiverContainer, numMessages);
		ErrorQueueInfrastructure errorQueueInfrastructure = initializeErrorQueueInfrastructure(jcsmpSession,
				acknowledgementCallbackFactory);
		AcknowledgmentCallback acknowledgmentCallback = createAcknowledgmentCallback(acknowledgementCallbackFactory,
				messageContainers, flowReceiverContainer.getTransactedSession());

		logger.info(String.format("Disabling ingress for error queue %s",
				errorQueueInfrastructure.getErrorQueueName()));
		sempV2Api.config().updateMsgVpnQueue((String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME),
				errorQueueInfrastructure.getErrorQueueName(), new ConfigMsgVpnQueue().ingressEnabled(false), null);
		retryAssert(() -> assertFalse(sempV2Api.monitor()
				.getMsgVpnQueue(vpnName, errorQueueInfrastructure.getErrorQueueName(), null)
				.getData()
				.isIngressEnabled()));

		logger.info("Acknowledging messages");
		acknowledgmentCallback.acknowledge(AcknowledgmentCallback.Status.REJECT);
		assertThat(acknowledgmentCallback.isAcknowledged()).isTrue();

		validateNumRedeliveredMessages(sempV2Api, queue.getName(), numMessages);
		validateQueueBindSuccesses(sempV2Api, queue.getName(), 1);
		validateNumEnqueuedMessages(sempV2Api, queue.getName(), numMessages);

		//Validate Error Queue Stats
		validateNumEnqueuedMessages(sempV2Api, errorQueueInfrastructure.getErrorQueueName(), 0);
		validateNumRedeliveredMessages(sempV2Api, errorQueueInfrastructure.getErrorQueueName(), 0);
	}

	@CartesianTest(name = "[{index}] numMessages={0}, transacted={1} isDurable={2}")
	public void testRequeue(@Values(ints = {1, 255}) int numMessages,
							@Values(booleans = {false, true}) boolean transacted,
							@Values(booleans = {false, true}) boolean isDurable,
							JCSMPSession jcsmpSession,
							Queue durableQueue,
							SempV2Api sempV2Api) throws Exception {
		if (numMessages == 1 && transacted) {
			logger.info("No support yet for non-batched, transacted consumers");
			return;
		}

		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = initializeFlowReceiverContainer(jcsmpSession, queue, transacted);
		JCSMPAcknowledgementCallbackFactory acknowledgementCallbackFactory = new JCSMPAcknowledgementCallbackFactory(
				flowReceiverContainer);

		List<MessageContainer> messageContainers = sendAndReceiveMessages(queue, flowReceiverContainer, numMessages);
		AcknowledgmentCallback acknowledgmentCallback = createAcknowledgmentCallback(acknowledgementCallbackFactory,
				messageContainers, flowReceiverContainer.getTransactedSession());

		acknowledgmentCallback.acknowledge(AcknowledgmentCallback.Status.REQUEUE);
		assertThat(acknowledgmentCallback.isAcknowledged()).isTrue();
		validateNumRedeliveredMessages(sempV2Api, queue.getName(), numMessages);
		validateQueueBindSuccesses(sempV2Api, queue.getName(), 1);
		validateNumEnqueuedMessages(sempV2Api, queue.getName(), numMessages);

		if (transacted) {
			validateTransaction(sempV2Api, jcsmpSession, numMessages, false);
		}
	}

	@CartesianTest(name = "[{index}] numMessages={0}, transacted={1}")
	public void testRequeueFail(@Values(ints = {1, 255}) int numMessages,
								@Values(booleans = {false, true}) boolean transacted,
								JCSMPSession jcsmpSession,
								Queue queue,
								SempV2Api sempV2Api) throws Exception {
		if (numMessages == 1 && transacted) {
			logger.info("No support yet for non-batched, transacted consumers");
			return;
		}

		FlowReceiverContainer flowReceiverContainer = initializeFlowReceiverContainer(jcsmpSession, queue, transacted);
		JCSMPAcknowledgementCallbackFactory acknowledgementCallbackFactory = new JCSMPAcknowledgementCallbackFactory(
				flowReceiverContainer);

		List<MessageContainer> messageContainers = sendAndReceiveMessages(queue, flowReceiverContainer, numMessages);
		AcknowledgmentCallback acknowledgmentCallback = createAcknowledgmentCallback(acknowledgementCallbackFactory,
				messageContainers, flowReceiverContainer.getTransactedSession());

		logger.info(String.format("Disabling egress for queue %s", queue.getName()));
		sempV2Api.config().updateMsgVpnQueue((String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME),
				queue.getName(), new ConfigMsgVpnQueue().egressEnabled(false), null);
		retryAssert(() -> assertFalse(sempV2Api.monitor()
				.getMsgVpnQueue(vpnName, queue.getName(), null)
				.getData()
				.isEgressEnabled()));

		logger.info("Acknowledging messages");
		acknowledgmentCallback.acknowledge(AcknowledgmentCallback.Status.REQUEUE);
		assertThat(acknowledgmentCallback.isAcknowledged()).isTrue();

		validateNumEnqueuedMessages(sempV2Api, queue.getName(), numMessages);
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

		// Message was redelivered
		logger.info("Verifying message was redelivered");
		validateNumRedeliveredMessages(sempV2Api, queue.getName(), numMessages);
		validateQueueBindSuccesses(sempV2Api, queue.getName(), 2);
		validateNumEnqueuedMessages(sempV2Api, queue.getName(), numMessages);

		if (transacted) {
			validateTransaction(sempV2Api, jcsmpSession, numMessages, false);
		}
	}

	@CartesianTest(name = "[{index}] numMessages={0}, transacted={1} isDurable={2}")
	public void testReAckAfterAccept(@Values(ints = {1, 255}) int numMessages,
									 @Values(booleans = {false, true}) boolean transacted,
									 @Values(booleans = {false, true}) boolean isDurable,
									 JCSMPSession jcsmpSession,
									 Queue durableQueue,
									 SempV2Api sempV2Api) throws Throwable {
		if (numMessages == 1 && transacted) {
			logger.info("No support yet for non-batched, transacted consumers");
			return;
		}

		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = initializeFlowReceiverContainer(jcsmpSession, queue, transacted);
		JCSMPAcknowledgementCallbackFactory acknowledgementCallbackFactory = new JCSMPAcknowledgementCallbackFactory(
				flowReceiverContainer);

		List<MessageContainer> messageContainers = sendAndReceiveMessages(queue, flowReceiverContainer, numMessages);
		AcknowledgmentCallback acknowledgmentCallback = createAcknowledgmentCallback(acknowledgementCallbackFactory,
				messageContainers, flowReceiverContainer.getTransactedSession());

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

		if (transacted) {
			validateTransaction(sempV2Api, jcsmpSession, numMessages, true);
		}
	}

	@CartesianTest(name = "[{index}] numMessages={0}, transacted={1}, isDurable={2}")
	public void testReAckAfterReject(@Values(ints = {1, 255}) int numMessages,
									 @Values(booleans = {false, true}) boolean transacted,
									 @Values(booleans = {false, true}) boolean isDurable,
									 JCSMPSession jcsmpSession,
									 Queue durableQueue,
									 SempV2Api sempV2Api) throws Throwable {
		if (numMessages == 1 && transacted) {
			logger.info("No support yet for non-batched, transacted consumers");
			return;
		}

		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = initializeFlowReceiverContainer(jcsmpSession, queue, transacted);
		JCSMPAcknowledgementCallbackFactory acknowledgementCallbackFactory = new JCSMPAcknowledgementCallbackFactory(
				flowReceiverContainer);

		List<MessageContainer> messageContainers = sendAndReceiveMessages(queue, flowReceiverContainer, numMessages);
		AcknowledgmentCallback acknowledgmentCallback = createAcknowledgmentCallback(acknowledgementCallbackFactory,
				messageContainers, flowReceiverContainer.getTransactedSession());

		ThrowingRunnable verifyExpectedState = () -> {
			validateNumEnqueuedMessages(sempV2Api, queue.getName(), transacted ? numMessages : 0);
			validateNumRedeliveredMessages(sempV2Api, queue.getName(), transacted ? numMessages : 0);
			validateQueueBindSuccesses(sempV2Api, queue.getName(), 1);
		};

		acknowledgmentCallback.acknowledge(AcknowledgmentCallback.Status.REJECT);
		assertThat(acknowledgmentCallback.isAcknowledged()).isTrue();
		verifyExpectedState.run();

		for (AcknowledgmentCallback.Status status : AcknowledgmentCallback.Status.values()) {
			acknowledgmentCallback.acknowledge(status);
			assertThat(acknowledgmentCallback.isAcknowledged()).isTrue();
			verifyExpectedState.run();
		}

		if (transacted) {
			validateTransaction(sempV2Api, jcsmpSession, numMessages, false);
		}
	}

	@CartesianTest(name = "[{index}] numMessages={0}, isDurable={1}")
	public void testReAckAfterRejectWithErrorQueue(@Values(ints = {1, 255}) int numMessages,
												   @Values(booleans = {false, true}) boolean isDurable,
												   JCSMPSession jcsmpSession,
												   Queue durableQueue,
												   SempV2Api sempV2Api) throws Throwable {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = initializeFlowReceiverContainer(jcsmpSession, queue, false); //TODO Add transacted error queue support
		JCSMPAcknowledgementCallbackFactory acknowledgementCallbackFactory = new JCSMPAcknowledgementCallbackFactory(
				flowReceiverContainer);

		List<MessageContainer> messageContainers = sendAndReceiveMessages(queue, flowReceiverContainer, numMessages);
		ErrorQueueInfrastructure errorQueueInfrastructure = initializeErrorQueueInfrastructure(jcsmpSession,
				acknowledgementCallbackFactory);
		AcknowledgmentCallback acknowledgmentCallback = createAcknowledgmentCallback(acknowledgementCallbackFactory,
				messageContainers, flowReceiverContainer.getTransactedSession());

		ThrowingRunnable verifyExpectedState = () -> {
			validateNumEnqueuedMessages(sempV2Api, queue.getName(), 0);
			validateNumRedeliveredMessages(sempV2Api, queue.getName(), 0);
			validateQueueBindSuccesses(sempV2Api, queue.getName(), 1);
			validateNumEnqueuedMessages(sempV2Api, errorQueueInfrastructure.getErrorQueueName(), numMessages);
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

	@CartesianTest(name = "[{index}] numMessages={0}, transacted={1}, isDurable={2}")
	public void testReAckAfterRequeue(@Values(ints = {1, 255}) int numMessages,
									  @Values(booleans = {false, true}) boolean transacted,
									  @Values(booleans = {false, true}) boolean isDurable,
									  JCSMPSession jcsmpSession,
									  Queue durableQueue,
									  SempV2Api sempV2Api) throws Throwable {
		if (numMessages == 1 && transacted) {
			logger.info("No support yet for non-batched, transacted consumers");
			return;
		}

		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = initializeFlowReceiverContainer(jcsmpSession, queue, transacted);
		JCSMPAcknowledgementCallbackFactory acknowledgementCallbackFactory = new JCSMPAcknowledgementCallbackFactory(
				flowReceiverContainer);

		List<MessageContainer> messageContainers = sendAndReceiveMessages(queue, flowReceiverContainer, numMessages);
		AcknowledgmentCallback acknowledgmentCallback = createAcknowledgmentCallback(acknowledgementCallbackFactory,
				messageContainers, flowReceiverContainer.getTransactedSession());

		ThrowingRunnable verifyExpectedState = () -> {
			validateNumRedeliveredMessages(sempV2Api, queue.getName(), numMessages);
			validateQueueBindSuccesses(sempV2Api, queue.getName(), 1);
			validateNumEnqueuedMessages(sempV2Api, queue.getName(), numMessages);
		};

		acknowledgmentCallback.acknowledge(AcknowledgmentCallback.Status.REQUEUE);
		assertThat(acknowledgmentCallback.isAcknowledged()).isTrue();
		verifyExpectedState.run();

		for (AcknowledgmentCallback.Status status : AcknowledgmentCallback.Status.values()) {
			acknowledgmentCallback.acknowledge(status);
			assertThat(acknowledgmentCallback.isAcknowledged()).isTrue();
			verifyExpectedState.run();
		}

		if (transacted) {
			validateTransaction(sempV2Api, jcsmpSession, numMessages, false);
		}
	}

	@CartesianTest(name = "[{index}] status={0}, numMessages={1}, transacted={2}, isDurable={3}, createErrorQueue={4}")
	public void testAckWhenFlowUnbound(
			@CartesianTest.Enum(AcknowledgmentCallback.Status.class) AcknowledgmentCallback.Status status,
			@Values(ints = {1, 255}) int numMessages,
			@Values(booleans = {false, true}) boolean transacted,
			@Values(booleans = {false, true}) boolean isDurable,
			@Values(booleans = {false, true}) boolean createErrorQueue,
			JCSMPSession jcsmpSession,
			Queue durableQueue,
			SempV2Api sempV2Api) throws Exception {
		if (numMessages == 1 && transacted) {
			logger.info("No support yet for non-batched, transacted consumers");
			return;
		}

		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = initializeFlowReceiverContainer(jcsmpSession, queue, transacted);
		JCSMPAcknowledgementCallbackFactory acknowledgementCallbackFactory = new JCSMPAcknowledgementCallbackFactory(
				flowReceiverContainer);

		List<MessageContainer> messageContainers = sendAndReceiveMessages(queue, flowReceiverContainer, numMessages)
				.stream()
				.map(Mockito::spy)
				.peek(m -> when(m.isStale()).thenReturn(true))
				.collect(Collectors.toList());
		Optional<String> errorQueueName = Optional.of(createErrorQueue)
				.filter(e -> e)
				.map(c -> initializeErrorQueueInfrastructure(jcsmpSession, acknowledgementCallbackFactory))
				.map(ErrorQueueInfrastructure::getErrorQueueName);
		AcknowledgmentCallback acknowledgmentCallback = createAcknowledgmentCallback(acknowledgementCallbackFactory,
				messageContainers, flowReceiverContainer.getTransactedSession());

		flowReceiverContainer.unbind();

		SolaceAcknowledgmentException exception = assertThrows(SolaceAcknowledgmentException.class,
				() -> acknowledgmentCallback.acknowledge(status));
		Class<? extends Throwable> expectedRootCause = transacted ? ClosedFacilityException.class :
				IllegalStateException.class;

		assertThat(acknowledgmentCallback.isAcknowledged())
				.describedAs("Unexpected ack state for %s re-ack", status)
				.isFalse();
		assertThat(exception)
				.describedAs("Unexpected root cause for %s re-ack", status)
				.hasRootCauseInstanceOf(expectedRootCause);
		if (exception instanceof SolaceBatchAcknowledgementException) {
			assertThat((SolaceBatchAcknowledgementException) exception)
					.describedAs("Unexpected stale batch state for %s re-ack", status)
					.hasRootCauseInstanceOf(expectedRootCause);
		}

		if(isDurable) {
			validateNumEnqueuedMessages(sempV2Api, queue.getName(), numMessages);
			validateNumRedeliveredMessages(sempV2Api, queue.getName(), 0);
			validateQueueBindSuccesses(sempV2Api, queue.getName(), 1);
		}
		if (errorQueueName.isPresent()) {
			validateNumEnqueuedMessages(sempV2Api, errorQueueName.get(), 0);
			validateNumRedeliveredMessages(sempV2Api, errorQueueName.get(), 0);
		}
	}

	@CartesianTest(name = "[{index}] status={0}, numMessages={1}, transacted={2}, isDurable={3}")
	public void testAckThrowsException(
			@CartesianTest.Enum(AcknowledgmentCallback.Status.class) AcknowledgmentCallback.Status status,
			@Values(ints = {1, 255}) int numMessages,
			@Values(booleans = {false, true}) boolean transacted,
			@Values(booleans = {false, true}) boolean isDurable,
			JCSMPSession jcsmpSession, Queue durableQueue, SempV2Api sempV2Api) throws Exception {
		if (numMessages == 1 && transacted) {
			logger.info("No support yet for non-batched, transacted consumers");
			return;
		}

		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = initializeFlowReceiverContainer(jcsmpSession, queue, transacted);
		JCSMPAcknowledgementCallbackFactory acknowledgementCallbackFactory = new JCSMPAcknowledgementCallbackFactory(
				flowReceiverContainer);

		JCSMPException expectedException = new JCSMPException("expected exception");
		List<MessageContainer> messageContainers = sendAndReceiveMessages(queue, flowReceiverContainer, numMessages)
				.stream()
				.map(Mockito::spy)
				.peek(m -> {
					BytesXMLMessage bytesXMLMessage = spy(m.getMessage());
					when(m.getMessage()).thenReturn(bytesXMLMessage);
					try {
						doThrow(expectedException).when(bytesXMLMessage)
								.settle(any(Outcome.class));
					} catch (Exception e) {
						fail("Unwanted exception", e);
					}
				}).collect(Collectors.toList());

		AcknowledgmentCallback acknowledgmentCallback = createAcknowledgmentCallback(acknowledgementCallbackFactory,
				messageContainers,
				Optional.ofNullable(flowReceiverContainer.getTransactedSession())
						.map(txnSession -> {
							TransactedSession spy = spy(txnSession);
							try {
								doThrow(expectedException).when(spy).commit();
								doThrow(expectedException).when(spy).rollback();
							} catch (Exception e) {
								fail("Unwanted exception", e);
							}
							return spy;
						})
						.orElse(null));

		assertThat(acknowledgmentCallback.isAcknowledged()).isFalse();
		assertThat(acknowledgmentCallback.isAutoAck()).isTrue();
		assertThat(SolaceAckUtil.isErrorQueueEnabled(acknowledgmentCallback)).isFalse();

		SolaceAcknowledgmentException exception = assertThrows(SolaceAcknowledgmentException.class,
				() -> acknowledgmentCallback.acknowledge(status));

		assertThat(acknowledgmentCallback.isAcknowledged())
				.describedAs("Unexpected ack state for %s re-ack", status)
				.isFalse();
		assertThat(exception)
				.describedAs("Unexpected root cause for %s re-ack", status)
				.hasRootCause(expectedException);

		validateNumEnqueuedMessages(sempV2Api, queue.getName(), numMessages);
		validateNumRedeliveredMessages(sempV2Api, queue.getName(), 0);
		validateQueueBindSuccesses(sempV2Api, queue.getName(), 1);
	}

	@CartesianTest(name = "[{index}] exceptionType={0}")
	public void testTransactedAcceptThrowsExceptionAndRollback(
			@Values(classes = {JCSMPException.class, RollbackException.class}) Class<? extends JCSMPException> exceptionType,
			JCSMPSession jcsmpSession,
			Queue durableQueue) throws Exception {
		FlowReceiverContainer flowReceiverContainer = initializeFlowReceiverContainer(jcsmpSession, durableQueue, true);
		JCSMPAcknowledgementCallbackFactory acknowledgementCallbackFactory = new JCSMPAcknowledgementCallbackFactory(
				flowReceiverContainer);

		TransactedSession transactedSession = spy(Objects.requireNonNull(flowReceiverContainer.getTransactedSession()));
		JCSMPException expectedException = exceptionType.getConstructor(String.class).newInstance("expected exception");
		JCSMPException expectedRollbackException = exceptionType.getConstructor(String.class).newInstance("expected exception during rollback");
		doThrow(expectedException).when(transactedSession).commit();
		doThrow(expectedRollbackException).when(transactedSession).rollback();

		AcknowledgmentCallback acknowledgmentCallback = acknowledgementCallbackFactory.createTransactedBatchCallback(
				sendAndReceiveMessages(durableQueue, flowReceiverContainer, 1),
				transactedSession);

		assertThatThrownBy(() -> acknowledgmentCallback.acknowledge(AcknowledgmentCallback.Status.ACCEPT))
				.isInstanceOf(SolaceAcknowledgmentException.class)
				.rootCause()
				.isInstanceOf(expectedException.getClass())
				.hasMessage(expectedException.getMessage())
				.satisfies(e -> {
					if (RollbackException.class.isAssignableFrom(exceptionType)) {
						Mockito.verify(transactedSession, never()).rollback();
						assertThat(e.getSuppressed()).isEmpty();
					} else {
						Mockito.verify(transactedSession, times(1)).rollback();
						assertThat(e.getSuppressed()).containsExactly(expectedRollbackException);
					}
				});
	}

	@Test
	public void testTransactedNoAutoAckThrowsException(JCSMPSession jcsmpSession, Queue durableQueue)
			throws Exception {
		FlowReceiverContainer flowReceiverContainer = initializeFlowReceiverContainer(jcsmpSession, durableQueue, true);
		JCSMPAcknowledgementCallbackFactory acknowledgementCallbackFactory = new JCSMPAcknowledgementCallbackFactory(
				flowReceiverContainer);
		AcknowledgmentCallback acknowledgmentCallback = acknowledgementCallbackFactory.createTransactedBatchCallback(
				sendAndReceiveMessages(durableQueue, flowReceiverContainer, 1),
				flowReceiverContainer.getTransactedSession());

		assertThatThrownBy(acknowledgmentCallback::noAutoAck)
				.isInstanceOf(UnsupportedOperationException.class);
	}

	@ParameterizedTest
	@EnumSource(AcknowledgmentCallback.Status.class)
	public void testTransactedAsyncThrowsException(AcknowledgmentCallback.Status status,
												   JCSMPSession jcsmpSession,
												   Queue durableQueue,
												   SoftAssertions softly) throws Exception {
		FlowReceiverContainer flowReceiverContainer = initializeFlowReceiverContainer(jcsmpSession, durableQueue, true);
		JCSMPAcknowledgementCallbackFactory acknowledgementCallbackFactory = new JCSMPAcknowledgementCallbackFactory(
				flowReceiverContainer);
		AcknowledgmentCallback acknowledgmentCallback = acknowledgementCallbackFactory.createTransactedBatchCallback(
				sendAndReceiveMessages(durableQueue, flowReceiverContainer, 1),
				flowReceiverContainer.getTransactedSession());

		ExecutorService executorService = Executors.newSingleThreadExecutor();
		try {
			executorService.submit(() -> {
				softly.assertThatThrownBy(() -> acknowledgmentCallback.acknowledge(status))
						.isInstanceOf(UnsupportedOperationException.class)
						.hasMessage("Transactions must be resolved on the message handler's thread");
			}).get(1, TimeUnit.MINUTES);
		} finally {
			executorService.shutdownNow();
		}
	}

	private List<MessageContainer> sendAndReceiveMessages(Queue queue,
														  FlowReceiverContainer flowReceiverContainer,
														  int numMessages)
			throws JCSMPException, UnboundFlowReceiverContainerException {
		assertThat(numMessages).isGreaterThan(0);

		for (int i = 0; i < numMessages; i++) {
			producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);
		}

		if (numMessages > 1) {
			List<MessageContainer> messageContainers = new ArrayList<>();
			for (int i = 0; i < numMessages; i++) {
				MessageContainer messageContainer = flowReceiverContainer.receive((int)
						TimeUnit.SECONDS.toMillis(10));
				assertNotNull(messageContainer);
				messageContainers.add(messageContainer);
			}
			return messageContainers;
		} else {
			MessageContainer messageContainer = flowReceiverContainer.receive((int)
					TimeUnit.SECONDS.toMillis(10));
			assertNotNull(messageContainer);
			return Collections.singletonList(messageContainer);
		}
	}

	private AcknowledgmentCallback createAcknowledgmentCallback(
			JCSMPAcknowledgementCallbackFactory acknowledgementCallbackFactory,
			List<MessageContainer> messageContainers,
			@Nullable TransactedSession transactedSession) {
		assertThat(messageContainers).hasSizeGreaterThan(0);
		if (messageContainers.size() > 1) {
			return transactedSession != null ?
					acknowledgementCallbackFactory.createTransactedBatchCallback(messageContainers, transactedSession) :
					acknowledgementCallbackFactory.createBatchCallback(messageContainers);
		} else {
			return acknowledgementCallbackFactory.createCallback(messageContainers.get(0));
		}
	}

	private FlowReceiverContainer initializeFlowReceiverContainer(
			JCSMPSession jcsmpSession, Queue queue, boolean transacted)
			throws JCSMPException {
		AtomicReference<FlowReceiverContainer> ref = transacted ?
				transactedFlowReceiverContainerReference :
				flowReceiverContainerReference;

		if (ref.compareAndSet(null, spy(new FlowReceiverContainer(
				jcsmpSession,
				JCSMPFactory.onlyInstance().createQueue(queue.getName()),
				transacted,
				new EndpointProperties(),
				new ConsumerFlowProperties())))) {
			ref.get().bind();
		}
		FlowReceiverContainer flowReceiverContainer = ref.get();
		if (transacted) {
			assertThat(flowReceiverContainer.getTransactedSession()).isNotNull();
		} else {
			assertThat(flowReceiverContainer.getTransactedSession()).isNull();
		}

		return flowReceiverContainer;
	}

	private ErrorQueueInfrastructure initializeErrorQueueInfrastructure(JCSMPSession jcsmpSession,
															JCSMPAcknowledgementCallbackFactory ackCallbackFactory) {
		if (closeErrorQueueInfrastructureCallback != null) {
			throw new IllegalStateException("Should only have one error queue infrastructure");
		}

		String producerManagerKey = UUID.randomUUID().toString();
		JCSMPSessionProducerManager jcsmpSessionProducerManager = new JCSMPSessionProducerManager(jcsmpSession);
		ErrorQueueInfrastructure errorQueueInfrastructure = new ErrorQueueInfrastructure(jcsmpSessionProducerManager,
				producerManagerKey, RandomStringUtils.randomAlphanumeric(20), new SolaceConsumerProperties());
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
		retryAssert(() -> {
			List<MonitorMsgVpnQueueMsg> messages = new ArrayList<>();
			Optional<String> cursor = Optional.empty();
			do {
				MonitorMsgVpnQueueMsgsResponse response = sempV2Api.monitor()
						.getMsgVpnQueueMsgs(vpnName, queueName, Integer.MAX_VALUE, cursor.orElse(null),
								null, null);
				cursor = Optional.ofNullable(response.getMeta())
						.map(MonitorSempMeta::getPaging)
						.map(MonitorSempPaging::getCursorQuery);
				messages.addAll(response.getData());
			} while (cursor.isPresent());
			assertThat(messages).hasSize(expectedCount);
		});
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

	private void validateTransaction(SempV2Api sempV2Api, JCSMPSession jcsmpSession, int numMessages, boolean committed)
			throws InterruptedException {
		retryAssert(() -> assertThat(sempV2Api.monitor()
				.getMsgVpnClientTransactedSessions(vpnName,
						(String) jcsmpSession.getProperty(JCSMPProperties.CLIENT_NAME), null, null, null, null)
				.getData())
				.singleElement()
				.satisfies(
						d -> assertThat(d.getSuccessCount()).isEqualTo(1),
						d -> assertThat(d.getCommitCount()).isEqualTo(committed ? 1 : 0),
						d -> assertThat(d.getRollbackCount()).isEqualTo(!committed ? 1 : 0),
						d -> assertThat(d.getFailureCount()).isEqualTo(0),
						d -> assertThat(d.getConsumedMsgCount()).isEqualTo(committed ? numMessages : 0),
						d -> assertThat(d.getPendingConsumedMsgCount()).isEqualTo(committed ? 0 : numMessages)
				));
	}
}
