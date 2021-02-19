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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;
import org.springframework.boot.test.context.ConfigFileApplicationContextInitializer;
import org.springframework.test.context.ContextConfiguration;

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@ContextConfiguration(classes = SolaceJavaAutoConfiguration.class,
		initializers = ConfigFileApplicationContextInitializer.class)
public class ErrorQueueRepublishCorrelationKeyIT extends ITBase {
	@Parameterized.Parameter
	public String parameterSetName; // Only used for parameter set naming

	@Parameterized.Parameter(1)
	public boolean isDurable;

	private String vpnName;
	private Queue queue;
	private Queue errorQueue;
	private XMLMessageProducer producer;
	private FlowReceiverContainer flowReceiverContainer;
	private ErrorQueueInfrastructure errorQueueInfrastructure;
	private RetryableTaskService retryableTaskService;

	private static final Log logger = LogFactory.getLog(ErrorQueueRepublishCorrelationKeyIT.class);

	@Parameterized.Parameters(name = "{0}")
	public static Collection<?> headerSets() {
		return Arrays.asList(new Object[][]{
				{"Durable", true},
				{"Temporary", false}
		});
	}

	@Before
	public void setUp() throws Exception {
		vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);
		retryableTaskService = Mockito.spy(new RetryableTaskService());

		if (isDurable) {
			queue = JCSMPFactory.onlyInstance().createQueue(RandomStringUtils.randomAlphanumeric(20));
			jcsmpSession.provision(queue, new EndpointProperties(), JCSMPSession.WAIT_FOR_CONFIRM);
		} else {
			queue = jcsmpSession.createTemporaryQueue(RandomStringUtils.randomAlphanumeric(20));
		}

		flowReceiverContainer = Mockito.spy(new FlowReceiverContainer(jcsmpSession, queue.getName(),
				new EndpointProperties()));
		flowReceiverContainer.bind();

		String producerManagerKey = UUID.randomUUID().toString();
		JCSMPSessionProducerManager producerManager = new JCSMPSessionProducerManager(jcsmpSession);
		producer = producerManager.get(producerManagerKey);

		errorQueueInfrastructure = Mockito.spy(new ErrorQueueInfrastructure(producerManager, producerManagerKey,
				RandomStringUtils.randomAlphanumeric(20), new SolaceConsumerProperties(),
				retryableTaskService));
		errorQueue = JCSMPFactory.onlyInstance().createQueue(errorQueueInfrastructure.getErrorQueueName());
		jcsmpSession.provision(errorQueue, new EndpointProperties(), JCSMPSession.WAIT_FOR_CONFIRM);
	}

	@After
	public void tearDown() throws Exception {
		if (producer != null) {
			producer.close();
		}

		if (flowReceiverContainer != null) {
			Optional.ofNullable(flowReceiverContainer.getFlowReceiverReference())
					.map(FlowReceiverContainer.FlowReceiverReference::get)
					.ifPresent(Consumer::close);
		}

		if (jcsmpSession != null && !jcsmpSession.isClosed()) {
			if (isDurable) {
				jcsmpSession.deprovision(queue, JCSMPSession.WAIT_FOR_CONFIRM);
			}
			jcsmpSession.deprovision(errorQueue, JCSMPSession.WAIT_FOR_CONFIRM);
		}
	}

	@Test
	public void testHandleSuccess() throws Exception {
		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);
		MessageContainer messageContainer = flowReceiverContainer.receive(5000);
		ErrorQueueRepublishCorrelationKey key = createKey(messageContainer);

		key.handleSuccess();
		assertTrue(messageContainer.isAcknowledged());
	}

	@Test
	public void testHandleSuccessStale() throws Exception {
		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);
		MessageContainer messageContainer = Mockito.spy(flowReceiverContainer.receive(5000));
		Mockito.when(messageContainer.isStale()).thenReturn(true);
		ErrorQueueRepublishCorrelationKey key = createKey(messageContainer);

		assertThrows(SolaceStaleMessageException.class, key::handleSuccess);
		assertEquals(0, key.getErrorQueueDeliveryAttempt());
	}

	@Test
	public void testHandleError() throws Exception {
		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);
		MessageContainer messageContainer = flowReceiverContainer.receive(5000);
		ErrorQueueRepublishCorrelationKey key = createKey(messageContainer);

		key.handleError(false);

		Mockito.verify(errorQueueInfrastructure, Mockito.times(1)).send(messageContainer, key);
		assertEquals(1, key.getErrorQueueDeliveryAttempt());
		validateNumEnqueuedMessages(queue, 0);
		validateNumEnqueuedMessages(errorQueue, 1);
	}

	@Test
	public void testHandleErrorRetry() throws Exception {
		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);
		MessageContainer messageContainer = flowReceiverContainer.receive(5000);
		ErrorQueueRepublishCorrelationKey key = createKey(messageContainer);

		Mockito.doAnswer(invocation -> {
			if (key.getErrorQueueDeliveryAttempt() < errorQueueInfrastructure.getMaxDeliveryAttempts()) {
				throw new JCSMPException("Test");
			} else {
				return invocation.callRealMethod();
			}
		}).when(errorQueueInfrastructure).send(messageContainer, key);

		key.handleError(false);

		int maxDeliveryAttempts = Math.toIntExact(errorQueueInfrastructure.getMaxDeliveryAttempts());
		Mockito.verify(errorQueueInfrastructure, Mockito.times(maxDeliveryAttempts)).send(messageContainer, key);
		assertEquals(maxDeliveryAttempts, key.getErrorQueueDeliveryAttempt());
		validateNumEnqueuedMessages(queue, 0);
		validateNumEnqueuedMessages(errorQueue, 1);
	}

	@Test
	public void testHandleErrorAsyncRetry() throws Exception {
		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);
		MessageContainer messageContainer = flowReceiverContainer.receive(5000);
		ErrorQueueRepublishCorrelationKey key = createKey(messageContainer);

		logger.info(String.format("Shutting down ingress for queue %s", errorQueue.getName()));
		sempV2Api.config().updateMsgVpnQueue(vpnName, errorQueue.getName(),
				new ConfigMsgVpnQueue().ingressEnabled(false), null);
		retryAssert(() -> assertFalse(sempV2Api.monitor()
				.getMsgVpnQueue(vpnName, errorQueue.getName(), null)
				.getData()
				.isIngressEnabled()));

		CountDownLatch latch = new CountDownLatch(1);
		Mockito.doAnswer(invocation -> {
			if (key.getErrorQueueDeliveryAttempt() == errorQueueInfrastructure.getMaxDeliveryAttempts()) {
				logger.info(String.format("Starting ingress for queue %s", errorQueue.getName()));
				sempV2Api.config().updateMsgVpnQueue(vpnName, errorQueue.getName(),
						new ConfigMsgVpnQueue().ingressEnabled(true), null);
				retryAssert(() -> assertTrue(sempV2Api.monitor()
						.getMsgVpnQueue(vpnName, errorQueue.getName(), null)
						.getData()
						.isIngressEnabled()));
				latch.countDown();
			}

			return invocation.callRealMethod();
		}).when(errorQueueInfrastructure).send(messageContainer, key);

		key.handleError(false);
		assertTrue(latch.await(1, TimeUnit.MINUTES));

		int maxDeliveryAttempts = Math.toIntExact(errorQueueInfrastructure.getMaxDeliveryAttempts());
		Mockito.verify(errorQueueInfrastructure, Mockito.times(maxDeliveryAttempts)).send(messageContainer, key);
		assertEquals(maxDeliveryAttempts, key.getErrorQueueDeliveryAttempt());
		validateNumEnqueuedMessages(queue, 0);
		validateNumEnqueuedMessages(errorQueue, 1);
	}

	@Test
	public void testHandleErrorRequeueFallback() throws Exception {
		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);
		MessageContainer messageContainer = flowReceiverContainer.receive(5000);
		ErrorQueueRepublishCorrelationKey key = createKey(messageContainer);

		Mockito.doThrow(new JCSMPException("test")).when(errorQueueInfrastructure).send(messageContainer, key);

		key.handleError(false);

		int maxDeliveryAttempts = Math.toIntExact(errorQueueInfrastructure.getMaxDeliveryAttempts());
		Mockito.verify(errorQueueInfrastructure, Mockito.times(maxDeliveryAttempts)).send(messageContainer, key);
		assertEquals(maxDeliveryAttempts, key.getErrorQueueDeliveryAttempt());
		validateNumEnqueuedMessages(errorQueue, 0);

		if (isDurable) {
			validateNumEnqueuedMessages(queue, 1);
			Mockito.verify(flowReceiverContainer).acknowledgeRebind(messageContainer, true);
			assertEquals((Long) 2L, sempV2Api.monitor()
					.getMsgVpnQueue(vpnName, queue.getName(), null)
					.getData()
					.getBindSuccessCount());
		} else {
			validateNumEnqueuedMessages(queue, 0);
		}
	}

	@Test
	public void testHandleErrorRequeueFallbackSkipSync() throws Exception {
		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);
		MessageContainer messageContainer = flowReceiverContainer.receive(5000);
		ErrorQueueRepublishCorrelationKey key = createKey(messageContainer);

		Mockito.doThrow(new JCSMPException("test")).when(errorQueueInfrastructure).send(messageContainer, key);

		key.handleError(true);

		int maxDeliveryAttempts = Math.toIntExact(errorQueueInfrastructure.getMaxDeliveryAttempts());
		Mockito.verify(errorQueueInfrastructure, Mockito.times(maxDeliveryAttempts)).send(messageContainer, key);
		assertEquals(maxDeliveryAttempts, key.getErrorQueueDeliveryAttempt());
		validateNumEnqueuedMessages(errorQueue, 0);

		if (isDurable) {
			validateNumEnqueuedMessages(queue, 1);
			Mockito.verify(retryableTaskService)
					.submit(new RetryableRebindTask(flowReceiverContainer, messageContainer, retryableTaskService));
			Mockito.verify(flowReceiverContainer).acknowledgeRebind(messageContainer, true);
			retryAssert(() -> assertEquals((Long) 2L, sempV2Api.monitor()
					.getMsgVpnQueue(vpnName, queue.getName(), null)
					.getData()
					.getBindSuccessCount()));
		} else {
			validateNumEnqueuedMessages(queue, 0);
		}
	}

	@Test
	public void testHandleErrorRequeueFallbackFail() throws Exception {
		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);
		MessageContainer messageContainer = flowReceiverContainer.receive(5000);
		ErrorQueueRepublishCorrelationKey key = createKey(messageContainer);

		Mockito.doThrow(new JCSMPException("test")).when(errorQueueInfrastructure).send(messageContainer, key);
		Mockito.doThrow(new JCSMPException("test")).doCallRealMethod().when(flowReceiverContainer)
				.acknowledgeRebind(messageContainer, true);

		key.handleError(false);

		int maxDeliveryAttempts = Math.toIntExact(errorQueueInfrastructure.getMaxDeliveryAttempts());
		Mockito.verify(errorQueueInfrastructure, Mockito.times(maxDeliveryAttempts)).send(messageContainer, key);
		assertEquals(maxDeliveryAttempts, key.getErrorQueueDeliveryAttempt());
		validateNumEnqueuedMessages(errorQueue, 0);

		if (isDurable) {
			validateNumEnqueuedMessages(queue, 1);
			Mockito.verify(retryableTaskService)
					.submit(new RetryableRebindTask(flowReceiverContainer, messageContainer, retryableTaskService));
			Mockito.verify(flowReceiverContainer, Mockito.times(2))
					.acknowledgeRebind(messageContainer, true);
			retryAssert(() -> assertEquals((Long) 2L, sempV2Api.monitor()
					.getMsgVpnQueue(vpnName, queue.getName(), null)
					.getData()
					.getBindSuccessCount()));
		} else {
			validateNumEnqueuedMessages(queue, 0);
		}
	}

	@Test
	public void testHandleErrorStale() throws Exception {
		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);
		MessageContainer messageContainer = Mockito.spy(flowReceiverContainer.receive(5000));
		Mockito.when(messageContainer.isStale()).thenReturn(true);
		ErrorQueueRepublishCorrelationKey key = createKey(messageContainer);

		assertThrows(SolaceStaleMessageException.class, () -> key.handleError(false));
		assertEquals(0, key.getErrorQueueDeliveryAttempt());
	}

	private ErrorQueueRepublishCorrelationKey createKey(MessageContainer messageContainer) {
		return new ErrorQueueRepublishCorrelationKey(errorQueueInfrastructure,
				messageContainer,
				flowReceiverContainer,
				!isDurable,
				retryableTaskService);
	}

	private void validateNumEnqueuedMessages(Queue queue, int expectedCount) throws InterruptedException {
		retryAssert(() -> assertThat(sempV2Api.monitor()
				.getMsgVpnQueueMsgs(vpnName, queue.getName(), null, null, null, null)
				.getData())
				.hasSize(expectedCount));
	}
}
