package com.solace.spring.cloud.stream.binder.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.solace.spring.boot.autoconfigure.SolaceJavaAutoConfiguration;
import com.solace.spring.cloud.stream.binder.ITBase;
import com.solace.spring.cloud.stream.binder.util.FlowReceiverContainer.FlowReceiverReference;
import com.solace.test.integration.semp.v2.action.model.ActionMsgVpnClientDisconnect;
import com.solace.test.integration.semp.v2.config.model.ConfigMsgVpnQueue;
import com.solace.test.integration.semp.v2.monitor.ApiException;
import com.solace.test.integration.semp.v2.monitor.model.MonitorMsgVpnQueue;
import com.solace.test.integration.semp.v2.monitor.model.MonitorMsgVpnQueueTxFlow;
import com.solace.test.integration.semp.v2.monitor.model.MonitorSempMetaOnlyResponse;
import com.solacesystems.jcsmp.ClosedFacilityException;
import com.solacesystems.jcsmp.Consumer;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import com.solacesystems.jcsmp.JCSMPTransportException;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.XMLMessageProducer;
import com.solacesystems.jcsmp.impl.flow.FlowHandle;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.math.RandomUtils;
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
import org.mockito.Mockito;
import org.springframework.boot.test.context.ConfigFileApplicationContextInitializer;
import org.springframework.test.annotation.Repeat;
import org.springframework.test.context.ContextConfiguration;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@ContextConfiguration(classes = SolaceJavaAutoConfiguration.class,
		initializers = ConfigFileApplicationContextInitializer.class)
public class FlowReceiverContainerIT extends ITBase {
	@Rule
	public Timeout globalTimeout = new Timeout(5, TimeUnit.MINUTES);

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Parameterized.Parameter
	public String parameterSetName; // Only used for parameter set naming

	@Parameterized.Parameter(1)
	public boolean isDurable;

	private FlowReceiverContainer flowReceiverContainer;
	private XMLMessageProducer producer;
	private Queue queue;

	private static final Log logger = LogFactory.getLog(FlowReceiverContainerIT.class);

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

		flowReceiverContainer = Mockito.spy(new FlowReceiverContainer(jcsmpSession, queue.getName(),
				new EndpointProperties()));
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
			Optional.ofNullable(flowReceiverContainer.getFlowReceiverReference())
					.map(FlowReceiverReference::get)
					.ifPresent(Consumer::close);
		}

		if (isDurable && jcsmpSession != null && !jcsmpSession.isClosed()) {
			jcsmpSession.deprovision(queue, JCSMPSession.WAIT_FOR_CONFIRM);
		}
	}

	@Test
	public void testBind() throws Exception {
		assertNull(flowReceiverContainer.getFlowReceiverReference());
		UUID flowReferenceId = flowReceiverContainer.bind();
		assertNotNull(flowReferenceId);

		FlowReceiverReference flowReference1 = flowReceiverContainer.getFlowReceiverReference();
		assertNotNull(flowReference1);
		assertEquals(flowReferenceId, flowReference1.getId());
		assertEquals(queue, flowReference1.get().getEndpoint());
		assertEquals(0, flowReceiverContainer.getNumUnacknowledgedMessages());
	}

	@Test
	public void testBindABoundFlow() throws Exception {
		UUID flowReferenceId = flowReceiverContainer.bind();
		assertEquals(flowReferenceId, flowReceiverContainer.bind());

		FlowReceiverReference flowReference = flowReceiverContainer.getFlowReceiverReference();
		assertNotNull(flowReference);

		MonitorMsgVpnQueue queueInfo = getQueueInfo();
		assertNotNull(queueInfo);
		assertEquals((Long) 1L, queueInfo.getBindRequestCount());
		assertEquals((Long) 1L, queueInfo.getBindSuccessCount());

		List<MonitorMsgVpnQueueTxFlow> txFlows = getTxFlows(2, null);
		assertThat(txFlows, hasSize(1));
		assertEquals((Long) ((FlowHandle) flowReference.get()).getFlowId(), txFlows.get(0).getFlowId());
	}

	@Test
	public void testBindAnUnboundFlow() throws Exception {
		UUID flowReferenceId = flowReceiverContainer.bind();
		flowReceiverContainer.unbind();

		UUID reboundFlowReferenceId = flowReceiverContainer.bind();
		assertNotEquals(flowReferenceId, reboundFlowReferenceId);

		FlowReceiverReference flowReference = flowReceiverContainer.getFlowReceiverReference();
		assertNotNull(flowReference);

		List<MonitorMsgVpnQueueTxFlow> txFlows = getTxFlows(2, null);
		assertThat(txFlows, hasSize(1));
		assertEquals((Long) ((FlowHandle) flowReference.get()).getFlowId(), txFlows.get(0).getFlowId());
	}

	@Test
	public void testBindWhileRebinding() throws Exception {
		UUID flowReferenceId = flowReceiverContainer.bind();

		CountDownLatch midRebindLatch = new CountDownLatch(1);
		CountDownLatch finishRebindLatch = new CountDownLatch(1);
		Mockito.doAnswer(invocation -> {
			invocation.callRealMethod();
			midRebindLatch.countDown();
			finishRebindLatch.await();
			return null;
		}).when(flowReceiverContainer).unbind();

		ExecutorService executorService = Executors.newFixedThreadPool(2);
		try {
			Future<UUID> rebindFuture = executorService.submit(() -> flowReceiverContainer.rebind(flowReferenceId));
			assertTrue(midRebindLatch.await(1, TimeUnit.MINUTES));
			Future<UUID> bindFuture = executorService.submit(() -> flowReceiverContainer.bind());
			executorService.shutdown();

			Thread.sleep(TimeUnit.SECONDS.toMillis(5));
			assertFalse(bindFuture.isDone());

			finishRebindLatch.countDown();
			UUID rebindFlowReferenceId = rebindFuture.get(1, TimeUnit.MINUTES);
			assertThat(rebindFlowReferenceId, allOf(notNullValue(), not(equalTo(flowReferenceId))));

			UUID bindFlowReferenceId = bindFuture.get(1, TimeUnit.MINUTES);
			assertThat(bindFlowReferenceId, allOf(notNullValue(),
					not(equalTo(flowReferenceId)), equalTo(rebindFlowReferenceId)));

			FlowReceiverReference flowReference = flowReceiverContainer.getFlowReceiverReference();
			assertNotNull(flowReference);
			long currentFlowId = ((FlowHandle) flowReference.get()).getFlowId();

			MonitorMsgVpnQueue queueInfo = getQueueInfo();
			assertNotNull(queueInfo);
			assertEquals((Long) (isDurable ? 2L : 1L), queueInfo.getBindRequestCount());
			assertEquals((Long) (isDurable ? 2L : 1L), queueInfo.getBindSuccessCount());

			List<MonitorMsgVpnQueueTxFlow> txFlows = getTxFlows(2, null);
			assertThat(txFlows, hasSize(1));
			assertThat(txFlows.get(0).getFlowId(), allOf(notNullValue(), equalTo(currentFlowId)));
		} finally {
			executorService.shutdownNow();
		}
	}

	@Test
	public void testBindWhileReceiving() throws Exception {
		UUID flowReferenceId = flowReceiverContainer.bind();
		ExecutorService executorService = Executors.newSingleThreadExecutor();
		try {
			Future<MessageContainer> receiveFuture = executorService.submit(() -> flowReceiverContainer.receive());

			// To make sure the flow receive is actually blocked
			Thread.sleep(TimeUnit.SECONDS.toMillis(5));
			assertFalse(receiveFuture.isDone());

			UUID newFlowReferenceId = flowReceiverContainer.bind();
			assertNotNull(flowReceiverContainer.getFlowReceiverReference());
			assertEquals(flowReferenceId, newFlowReferenceId);
			assertThat(getTxFlows(2, null), hasSize(1));

			producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);
			assertNotNull(receiveFuture.get(1, TimeUnit.MINUTES));
		} finally {
			executorService.shutdownNow();
		}
	}

	@Test
	public void testConcurrentBind() throws Exception {
		CyclicBarrier barrier = new CyclicBarrier(30);
		ExecutorService executorService = Executors.newFixedThreadPool(barrier.getParties());
		try {
			Set<Future<UUID>> futures = IntStream.range(0, barrier.getParties())
					.mapToObj(i -> (Callable<UUID>) () -> {
						barrier.await();
						return flowReceiverContainer.bind();
					})
					.map(executorService::submit)
					.collect(Collectors.toSet());
			executorService.shutdown();

			List<UUID> newFlowReferenceIds = futures.stream()
					.map((ThrowingFunction<Future<UUID>, UUID>) f -> f.get(1, TimeUnit.MINUTES))
					.collect(Collectors.toList());
			assertThat(newFlowReferenceIds.stream().distinct().collect(Collectors.toList()), hasSize(1));

			UUID newFlowReferenceId = newFlowReferenceIds.stream()
					.filter(Objects::nonNull)
					.findAny()
					.orElse(null);
			assertNotNull(newFlowReferenceId);

			FlowReceiverReference flowReference = flowReceiverContainer.getFlowReceiverReference();
			assertNotNull(flowReference);

			MonitorMsgVpnQueue queueInfo = getQueueInfo();
			assertNotNull(queueInfo);
			assertEquals((Long) 1L, queueInfo.getBindRequestCount());
			assertEquals((Long) 1L, queueInfo.getBindSuccessCount());

			List<MonitorMsgVpnQueueTxFlow> txFlows = getTxFlows(2, null);
			assertThat(txFlows, hasSize(1));
			assertEquals((Long) ((FlowHandle) flowReference.get()).getFlowId(), txFlows.get(0).getFlowId());
		} finally {
			executorService.shutdownNow();
		}
	}

	@Test
	public void testUnbind() throws Exception {
		flowReceiverContainer.bind();
		flowReceiverContainer.unbind();
		assertNull(flowReceiverContainer.getFlowReceiverReference());
		assertThat(getTxFlows(1, null), hasSize(0));
	}

	@Test
	public void testUnbindANonBoundFlow() throws Exception {
		flowReceiverContainer.unbind();
		if (isDurable) {
			MonitorMsgVpnQueue queueInfo = getQueueInfo();
			assertNotNull(queueInfo);
			assertEquals((Long) 0L, queueInfo.getBindRequestCount());
		} else {
			assertNull(getQueueInfo());
		}
	}

	@Test
	public void testUnbindAnUnboundFlow() throws Exception {
		flowReceiverContainer.bind();
		flowReceiverContainer.unbind();
		flowReceiverContainer.unbind();
		if (isDurable) {
			MonitorMsgVpnQueue queueInfo = getQueueInfo();
			assertNotNull(queueInfo);
			assertEquals((Long) 1L, queueInfo.getBindRequestCount());
			assertEquals((Long) 1L, queueInfo.getBindSuccessCount());
		} else {
			assertNull(getQueueInfo());
		}
	}

	@Test
	public void testUnbindWhileRebinding() throws Exception {
		UUID flowReferenceId = flowReceiverContainer.bind();

		CountDownLatch midRebindLatch = new CountDownLatch(1);
		CountDownLatch finishRebindLatch = new CountDownLatch(1);
		Mockito.doAnswer(invocation -> {
			midRebindLatch.countDown();
			finishRebindLatch.await();
			return invocation.callRealMethod();
		}).when(flowReceiverContainer).bind();

		ExecutorService executorService = Executors.newFixedThreadPool(2);
		try {
			Future<UUID> rebindFuture = executorService.submit(() -> flowReceiverContainer.rebind(flowReferenceId));
			assertTrue(midRebindLatch.await(1, TimeUnit.MINUTES));
			Future<?> unbindFuture = executorService.submit(() -> flowReceiverContainer.unbind());
			executorService.shutdown();

			Thread.sleep(TimeUnit.SECONDS.toMillis(5));
			assertFalse(rebindFuture.isDone());
			assertFalse(unbindFuture.isDone());

			finishRebindLatch.countDown();
			assertThat(rebindFuture.get(1, TimeUnit.MINUTES),
					allOf(notNullValue(), not(equalTo(flowReferenceId))));
			unbindFuture.get(1, TimeUnit.MINUTES);

			assertNull(flowReceiverContainer.getFlowReceiverReference());

			MonitorMsgVpnQueue queueInfo = getQueueInfo();
			if (isDurable) {
				assertNotNull(queueInfo);
				assertEquals((Long) 2L, queueInfo.getBindRequestCount()); // 1 for initial bind, 1 for rebind
				assertEquals((Long) 2L, queueInfo.getBindSuccessCount()); // 1 for initial bind, 1 for rebind
			} else {
				assertNull(queueInfo);
			}

			List<MonitorMsgVpnQueueTxFlow> txFlows = getTxFlows(1, null);
			assertNotNull(txFlows);
			assertThat(txFlows, hasSize(0));
		} finally {
			executorService.shutdownNow();
		}
	}

	@Test
	public void testUnbindWhileReceiving() throws Exception {
		flowReceiverContainer.bind();
		ExecutorService executorService = Executors.newSingleThreadExecutor();
		try {
			Future<MessageContainer> receiveFuture = executorService.submit(() -> flowReceiverContainer.receive());

			// To make sure the flow receive is actually blocked
			Thread.sleep(TimeUnit.SECONDS.toMillis(5));
			assertFalse(receiveFuture.isDone());

			flowReceiverContainer.unbind();
			assertNull(flowReceiverContainer.getFlowReceiverReference());
			assertThat(getTxFlows(1, null), hasSize(0));

			thrown.expect(ExecutionException.class);
			thrown.expectCause(instanceOf(JCSMPTransportException.class));
			thrown.expectMessage("Consumer was closed while in receive");
			receiveFuture.get(1, TimeUnit.MINUTES);
		} finally {
			executorService.shutdownNow();
		}
	}

	@Test
	public void testUnbindWithUnacknowledgedMessage() throws Exception {
		flowReceiverContainer.bind();

		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);
		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);
		assertNotNull(flowReceiverContainer.receive());
		assertNotNull(flowReceiverContainer.receive());
		assertEquals(2, flowReceiverContainer.getNumUnacknowledgedMessages());

		flowReceiverContainer.unbind();
		assertEquals(0, flowReceiverContainer.getNumUnacknowledgedMessages());
	}

	@Test
	public void testConcurrentUnbind() throws Exception {
		flowReceiverContainer.bind();

		CyclicBarrier barrier = new CyclicBarrier(30);
		ExecutorService executorService = Executors.newFixedThreadPool(barrier.getParties());
		try {
			Set<Future<?>> futures = IntStream.range(0, barrier.getParties())
					.mapToObj(i -> (Callable<?>) () -> {
						barrier.await();
						flowReceiverContainer.unbind();
						return null;
					})
					.map(executorService::submit)
					.collect(Collectors.toSet());
			executorService.shutdown();

			for (Future<?> future : futures) {
				future.get(1, TimeUnit.MINUTES);
			}

			MonitorMsgVpnQueue queueInfo = getQueueInfo();
			if (isDurable) {
				assertNotNull(queueInfo);
				assertEquals((Long) 1L, queueInfo.getBindRequestCount());
				assertEquals((Long) 1L, queueInfo.getBindSuccessCount());

				List<MonitorMsgVpnQueueTxFlow> txFlows = getTxFlows(1, null);
				assertThat(txFlows, hasSize(0));
			} else {
				assertNull(queueInfo);
			}
		} finally {
			executorService.shutdownNow();
		}
	}

	@Test
	public void testRebind() throws Exception {
		UUID flowReferenceId1 = flowReceiverContainer.bind();

		FlowReceiverReference flowReference1 = flowReceiverContainer.getFlowReceiverReference();
		assertNotNull(flowReference1);

		List<MonitorMsgVpnQueueTxFlow> txFlows1 = getTxFlows(2, null);
		assertThat(txFlows1, hasSize(1));
		assertEquals((Long) ((FlowHandle) flowReference1.get()).getFlowId(), txFlows1.get(0).getFlowId());

		UUID flowReferenceId2 = flowReceiverContainer.rebind(flowReferenceId1);
		assertNotNull(flowReferenceId2);
		assertNotEquals(flowReferenceId1, flowReferenceId2);

		FlowReceiverReference flowReference2 = flowReceiverContainer.getFlowReceiverReference();
		assertNotNull(flowReference2);
		assertEquals(queue, flowReference2.get().getEndpoint());
		assertEquals(flowReference1.get().getEndpoint(), flowReference2.get().getEndpoint());

		assertNotEquals(flowReference1, flowReference2);
		assertNotEquals(flowReference1.getId(), flowReference2.getId());
		assertNotEquals(flowReference1.get(), flowReference2.get());
		assertNotEquals(((FlowHandle) flowReference1.get()).getFlowId(),
				((FlowHandle) flowReference2.get()).getFlowId());
		assertEquals(flowReference1.get().getDestination(), flowReference2.get().getDestination());
		assertEquals(flowReference1.get().getEndpoint(), flowReference2.get().getEndpoint());

		List<MonitorMsgVpnQueueTxFlow> txFlows2 = getTxFlows(2, null);
		assertThat(txFlows2, hasSize(1));
		assertEquals((Long) ((FlowHandle) flowReference2.get()).getFlowId(), txFlows2.get(0).getFlowId());
	}

	@Test
	public void testRebindANonBoundFlow() throws Exception {
		thrown.expect(IllegalStateException.class);
		thrown.expectMessage("is not bound");
		try {
			flowReceiverContainer.rebind(UUID.randomUUID());
		} catch (IllegalStateException e) {
			if (isDurable) {
				MonitorMsgVpnQueue queueInfo = getQueueInfo();
				assertNotNull(queueInfo);
				assertEquals((Long) 0L, queueInfo.getBindRequestCount());
			} else {
				assertNull(getQueueInfo());
			}
			throw e;
		}
	}

	@Test
	public void testRebindAReboundFlow() throws Exception {
		TextMessage message = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);

		flowReceiverContainer.bind();
		producer.send(message, queue);
		MessageContainer receivedMessage = flowReceiverContainer.receive();
		assertNotNull(receivedMessage);
		assertEquals(1L, flowReceiverContainer.getNumUnacknowledgedMessages());

		UUID reboundFlowReferenceId = flowReceiverContainer.acknowledgeRebind(receivedMessage);
		assertNotEquals(receivedMessage.getFlowReceiverReferenceId(), reboundFlowReferenceId);
		assertEquals(0L, flowReceiverContainer.getNumUnacknowledgedMessages());

		assertEquals(reboundFlowReferenceId,
				flowReceiverContainer.rebind(receivedMessage.getFlowReceiverReferenceId()));
	}

	@Test
	public void testRebindWhileReceiving() throws Exception {
		UUID flowReferenceId = flowReceiverContainer.bind();
		ExecutorService executorService = Executors.newSingleThreadExecutor();
		try {
			Future<MessageContainer> receiveFuture = executorService.submit(() -> flowReceiverContainer.receive());

			// To make sure the flow receive is actually blocked
			Thread.sleep(TimeUnit.SECONDS.toMillis(5));
			assertFalse(receiveFuture.isDone());

			UUID newFlowReferenceId = flowReceiverContainer.rebind(flowReferenceId);
			assertNotNull(flowReceiverContainer.getFlowReceiverReference());
			assertNotEquals(flowReferenceId, newFlowReferenceId);
			assertThat(getTxFlows(2, null), hasSize(1));

			assertNull(receiveFuture.get(1, TimeUnit.MINUTES));
		} finally {
			executorService.shutdownNow();
		}
	}

	@Test
	public void testRebindWithUnacknowledgedMessage() throws Exception {
		UUID flowReferenceId = flowReceiverContainer.bind();
		ExecutorService executorService = Executors.newSingleThreadExecutor();
		try {
			producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);
			producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);
			MessageContainer receivedMessage1 = flowReceiverContainer.receive();
			MessageContainer receivedMessage2 = flowReceiverContainer.receive();
			assertNotNull(receivedMessage1);
			assertNotNull(receivedMessage2);
			assertEquals(2L, flowReceiverContainer.getNumUnacknowledgedMessages());

			Future<UUID> rebindFuture = executorService.submit(() -> flowReceiverContainer.rebind(flowReferenceId));

			// To make sure the flow rebind is actually blocked
			Thread.sleep(TimeUnit.SECONDS.toMillis(5));
			assertFalse(rebindFuture.isDone());

			// To make sure the flow rebind is still blocked after acking 1 message
			flowReceiverContainer.acknowledge(receivedMessage1);
			Thread.sleep(TimeUnit.SECONDS.toMillis(5));
			assertFalse(rebindFuture.isDone());

			flowReceiverContainer.acknowledge(receivedMessage2);
			assertThat(rebindFuture.get(1, TimeUnit.MINUTES),
					allOf(notNullValue(), not(equalTo(flowReferenceId))));
			assertEquals(0L, flowReceiverContainer.getNumUnacknowledgedMessages());
		} finally {
			executorService.shutdownNow();
		}
	}

	@Test
	public void testRebindInterrupt() throws Exception {
		UUID flowReferenceId = flowReceiverContainer.bind();

		FlowReceiverReference flowReference = flowReceiverContainer.getFlowReceiverReference();
		assertNotNull(flowReference);

		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);
		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);

		MessageContainer receivedMessage = flowReceiverContainer.receive();
		assertNotNull(receivedMessage);

		MessageContainer receivedMessage1 = flowReceiverContainer.receive();
		assertNotNull(receivedMessage1);

		ExecutorService executorService = Executors.newSingleThreadExecutor();
		try {
			Future<UUID> rebindFuture = executorService.submit(() ->
					flowReceiverContainer.acknowledgeRebind(receivedMessage));
			executorService.shutdown();

			// To make sure the flow rebind is actually blocked
			Thread.sleep(TimeUnit.SECONDS.toMillis(5));
			assertFalse(rebindFuture.isDone());

			executorService.shutdownNow();
			assertTrue(executorService.awaitTermination(1, TimeUnit.MINUTES));
		} finally {
			executorService.shutdownNow();
		}

		FlowReceiverReference reboundFlowReference = flowReceiverContainer.getFlowReceiverReference();
		assertNotNull(reboundFlowReference);
		assertEquals(flowReferenceId, reboundFlowReference.getId());
		assertEquals(((FlowHandle) flowReference.get()).getFlowId(),
				((FlowHandle) reboundFlowReference.get()).getFlowId());

		assertEquals(2, flowReceiverContainer.getNumUnacknowledgedMessages());

		flowReceiverContainer.acknowledge(receivedMessage);
		flowReceiverContainer.acknowledge(receivedMessage1);
		assertEquals(0, flowReceiverContainer.getNumUnacknowledgedMessages());

		// Give some time for the messages to be acknowledged off the broker
		Thread.sleep(TimeUnit.SECONDS.toMillis(3));

		List<MonitorMsgVpnQueueTxFlow> txFlows = getTxFlows(2, null);
		assertThat(txFlows, hasSize(1));
		assertEquals((Long) 2L, txFlows.get(0).getAckedMsgCount());
		assertEquals((Long) 0L, txFlows.get(0).getUnackedMsgCount());
	}

	@Test
	public void testRebindAfterFlowReconnect() throws Exception {
		if (!isDurable) {
			logger.info("Test does not apply for non-durable queues");
			return;
		}

		TextMessage message = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);

		UUID flowReferenceId = flowReceiverContainer.bind();

		producer.send(message, queue);
		MessageContainer receivedMessage = flowReceiverContainer.receive();
		assertNotNull(receivedMessage);

		String vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);

		logger.info(String.format("Disabling egress to queue %s", queue.getName()));
		sempV2Api.config().updateMsgVpnQueue(vpnName, queue.getName(), new ConfigMsgVpnQueue().egressEnabled(false),
				null);
		Thread.sleep(TimeUnit.SECONDS.toMillis(5));

		logger.info(String.format("Enabling egress to queue %s", queue.getName()));
		sempV2Api.config().updateMsgVpnQueue(vpnName, queue.getName(), new ConfigMsgVpnQueue().egressEnabled(true),
				null);
		Thread.sleep(TimeUnit.SECONDS.toMillis(5));

		assertEquals(1, flowReceiverContainer.getNumUnacknowledgedMessages());
		logger.info(String.format("Initiating rebind with message container %s", receivedMessage.getId()));
		UUID flowReferenceId2 = flowReceiverContainer.acknowledgeRebind(receivedMessage);
		assertThat(flowReferenceId2, allOf(notNullValue(), not(equalTo(flowReferenceId))));
		assertEquals(0, flowReceiverContainer.getNumUnacknowledgedMessages());

		Mockito.verify(flowReceiverContainer, Mockito.times(1)).unbind();
		Mockito.verify(flowReceiverContainer, Mockito.times(2)).bind(); // +1 for init bind

		Thread.sleep(TimeUnit.SECONDS.toMillis(5));

		List<MonitorMsgVpnQueueTxFlow> txFlows = getTxFlows(2, null);
		assertThat(txFlows, hasSize(1));
		assertEquals((Long) 0L, txFlows.get(0).getAckedMsgCount());
		assertEquals((Long) 1L, txFlows.get(0).getUnackedMsgCount());
		assertEquals((Long) 1L, txFlows.get(0).getRedeliveredMsgCount());
	}

	@Test
	public void testRebindAfterSessionReconnect() throws Exception {
		TextMessage message = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);

		UUID flowReferenceId = flowReceiverContainer.bind();

		producer.send(message, queue);
		MessageContainer receivedMessage = flowReceiverContainer.receive();
		assertNotNull(receivedMessage);

		String vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);
		String clientName = (String) jcsmpSession.getProperty(JCSMPProperties.CLIENT_NAME);

		logger.info(String.format("Remotely disconnecting session %s", jcsmpSession.getSessionName()));
		sempV2Api.action().doMsgVpnClientDisconnect(vpnName, clientName, new ActionMsgVpnClientDisconnect());
		Thread.sleep(TimeUnit.SECONDS.toMillis(5));

		assertEquals(1, flowReceiverContainer.getNumUnacknowledgedMessages());
		logger.info(String.format("Initiating rebind with message container %s", receivedMessage.getId()));
		UUID flowReferenceId2 = flowReceiverContainer.acknowledgeRebind(receivedMessage);
		assertThat(flowReferenceId2, allOf(notNullValue(), not(equalTo(flowReferenceId))));
		assertEquals(0, flowReceiverContainer.getNumUnacknowledgedMessages());

		Mockito.verify(flowReceiverContainer, Mockito.times(1)).unbind();
		Mockito.verify(flowReceiverContainer, Mockito.times(2)).bind(); // +1 for init bind

		if (!isDurable) {
			// Re-sending message since rebind deletes the temporary queue
			producer.send(message, queue);
		}

		Thread.sleep(TimeUnit.SECONDS.toMillis(5));

		List<MonitorMsgVpnQueueTxFlow> txFlows = getTxFlows(2, null);
		assertThat(txFlows, hasSize(1));
		assertEquals((Long) 0L, txFlows.get(0).getAckedMsgCount());
		assertEquals((Long) 1L, txFlows.get(0).getUnackedMsgCount());
		assertEquals(isDurable ? (Long) 1L : (Long) 0L, txFlows.get(0).getRedeliveredMsgCount());
	}

	@Test
	public void testConcurrentRebind() throws Exception {
		CyclicBarrier barrier = new CyclicBarrier(30);

		UUID flowReferenceId = flowReceiverContainer.bind();

		ScheduledExecutorService executorService = Executors.newScheduledThreadPool(barrier.getParties());
		try {
			Set<Future<UUID>> futures = IntStream.range(0, barrier.getParties())
					.mapToObj(i -> (Callable<UUID>) () -> {
						barrier.await();
						return flowReceiverContainer.rebind(flowReferenceId);
					})
					.map(c -> executorService.schedule(c, RandomUtils.nextInt(100), TimeUnit.MILLISECONDS))
					.collect(Collectors.toSet());
			executorService.shutdown();

			Set<UUID> newFlowReferenceIds = futures.stream()
					.map((ThrowingFunction<Future<UUID>, UUID>) f -> f.get(1, TimeUnit.MINUTES))
					.collect(Collectors.toSet());
			Mockito.verify(flowReceiverContainer, Mockito.times(1)).unbind();
			Mockito.verify(flowReceiverContainer, Mockito.times(2)).bind(); // +1 for init bind
			assertThat(newFlowReferenceIds, hasSize(1));
			assertNotEquals(flowReferenceId, newFlowReferenceIds.iterator().next());
		} finally {
			executorService.shutdownNow();
		}
	}

	@Test
	public void testReceive() throws Exception {
		TextMessage message = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);

		UUID flowReferenceId1 = flowReceiverContainer.bind();
		producer.send(message, queue);
		MessageContainer messageReceived = flowReceiverContainer.receive();

		assertNotNull(messageReceived);
		assertThat(messageReceived.getMessage(), instanceOf(TextMessage.class));
		assertEquals(flowReferenceId1, messageReceived.getFlowReceiverReferenceId());
		assertEquals(1, flowReceiverContainer.getNumUnacknowledgedMessages());
	}

	@Test
	public void testReceiveOnANonBoundFlow() throws Exception {
		thrown.expect(IllegalStateException.class);
		thrown.expectMessage("is not bound");
		flowReceiverContainer.receive();
	}

	@Test
	public void testReceiveWhileRebinding() throws Exception {
		TextMessage message = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
		UUID flowReferenceId = flowReceiverContainer.bind();
		producer.send(message, queue);

		CountDownLatch midRebindLatch = new CountDownLatch(1);
		CountDownLatch finishRebindLatch = new CountDownLatch(1);
		Mockito.doAnswer(invocation -> {
			midRebindLatch.countDown();
			finishRebindLatch.await();
			return invocation.callRealMethod();
		}).when(flowReceiverContainer).bind();

		ExecutorService executorService = Executors.newFixedThreadPool(2);
		try {
			Future<UUID> rebindFuture = executorService.submit(() -> flowReceiverContainer.rebind(flowReferenceId));
			assertTrue(midRebindLatch.await(1, TimeUnit.MINUTES));
			Future<MessageContainer> receiveFuture = executorService.submit(() -> flowReceiverContainer.receive());
			executorService.shutdown();

			Thread.sleep(TimeUnit.SECONDS.toMillis(5));
			assertFalse(receiveFuture.isDone());

			finishRebindLatch.countDown();
			assertThat(rebindFuture.get(1, TimeUnit.MINUTES),
					allOf(notNullValue(), not(equalTo(flowReferenceId))));

			if (!isDurable) {
				// Re-sending message since rebind deletes the temporary queue
				producer.send(message, queue);
			}
			assertNotNull(receiveFuture.get(1, TimeUnit.MINUTES));
		} finally {
			executorService.shutdownNow();
		}
	}

	@Test
	public void testReceiveWithTimeout() throws Exception {
		TextMessage message = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);

		UUID flowReferenceId = flowReceiverContainer.bind();
		producer.send(message, queue);
		MessageContainer messageReceived =
				flowReceiverContainer.receive((int) TimeUnit.MINUTES.toMillis(5));

		assertNotNull(messageReceived);
		assertThat(messageReceived.getMessage(), instanceOf(TextMessage.class));
		assertEquals(flowReferenceId, messageReceived.getFlowReceiverReferenceId());
	}

	@Test
	public void testReceiveThrowTimeout() throws Exception {
		flowReceiverContainer.bind();
		assertNull(flowReceiverContainer.receive(1));
	}

	@Test
	public void testReceiveInterrupt() throws Exception {
		flowReceiverContainer.bind();
		ExecutorService executorService = Executors.newSingleThreadExecutor();
		try {
			Future<MessageContainer> receiveFuture = executorService.submit(() -> flowReceiverContainer.receive());
			executorService.shutdown();

			Thread.sleep(TimeUnit.SECONDS.toMillis(5));
			assertFalse(receiveFuture.isDone());

			executorService.shutdownNow(); // interrupt
			assertNull(receiveFuture.get(1, TimeUnit.MINUTES));
		} finally {
			executorService.shutdownNow();
		}
	}

	@Test
	public void testReceiveInterruptedByFlowReconnect() throws Exception {
		if (!isDurable) {
			logger.info("Test does not apply for non-durable queues");
			return;
		}

		flowReceiverContainer.bind();

		String vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);

		ExecutorService executorService = Executors.newSingleThreadExecutor();
		try {
			Future<MessageContainer> receiveFuture = executorService.submit(() -> flowReceiverContainer.receive());
			executorService.shutdown();

			Thread.sleep(TimeUnit.SECONDS.toMillis(5));
			assertFalse(receiveFuture.isDone());

			logger.info(String.format("Disabling egress to queue %s", queue.getName()));
			sempV2Api.config().updateMsgVpnQueue(vpnName, queue.getName(), new ConfigMsgVpnQueue().egressEnabled(false),
					null);
			Thread.sleep(TimeUnit.SECONDS.toMillis(5));

			logger.info(String.format("Sending message to queue %s", queue.getName()));
			producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);

			logger.info(String.format("Enabling egress to queue %s", queue.getName()));
			sempV2Api.config().updateMsgVpnQueue(vpnName, queue.getName(), new ConfigMsgVpnQueue().egressEnabled(true),
					null);
			Thread.sleep(TimeUnit.SECONDS.toMillis(5));

			assertNotNull(receiveFuture.get(1, TimeUnit.MINUTES));
		} finally {
			executorService.shutdownNow();
		}
	}

	@Test
	public void testReceiveInterruptedBySessionReconnect() throws Exception {
		flowReceiverContainer.bind();

		String vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);
		String clientName = (String) jcsmpSession.getProperty(JCSMPProperties.CLIENT_NAME);

		ExecutorService executorService = Executors.newSingleThreadExecutor();
		try {
			Future<MessageContainer> receiveFuture = executorService.submit(() -> flowReceiverContainer.receive());
			executorService.shutdown();

			Thread.sleep(TimeUnit.SECONDS.toMillis(5));
			assertFalse(receiveFuture.isDone());

			logger.info(String.format("Remotely disconnecting session %s", jcsmpSession.getSessionName()));
			sempV2Api.action().doMsgVpnClientDisconnect(vpnName, clientName, new ActionMsgVpnClientDisconnect());
			Thread.sleep(TimeUnit.SECONDS.toMillis(5));

			logger.info(String.format("Sending message to queue %s", queue.getName()));
			producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);

			assertNotNull(receiveFuture.get(1, TimeUnit.MINUTES));
		} finally {
			executorService.shutdownNow();
		}
	}

	@Test
	public void testReceiveAfterRebind() throws Exception {
		TextMessage message = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);

		UUID flowReferenceId1 = flowReceiverContainer.bind();

		producer.send(message, queue);

		MessageContainer messageReceived = flowReceiverContainer.receive();
		assertNotNull(messageReceived);
		assertThat(messageReceived.getMessage(), instanceOf(TextMessage.class));
		assertEquals(flowReferenceId1, messageReceived.getFlowReceiverReferenceId());
		assertEquals(1, flowReceiverContainer.getNumUnacknowledgedMessages());

		UUID flowReferenceId2 = flowReceiverContainer.acknowledgeRebind(messageReceived);
		assertNotNull(flowReferenceId2);
		assertEquals(0, flowReceiverContainer.getNumUnacknowledgedMessages());

		producer.send(message, queue);

		messageReceived = flowReceiverContainer.receive();
		assertNotNull(messageReceived);
		assertThat(messageReceived.getMessage(), instanceOf(TextMessage.class));
		assertEquals(flowReferenceId2, messageReceived.getFlowReceiverReferenceId());
		assertEquals(1, flowReceiverContainer.getNumUnacknowledgedMessages());
	}

	@Test
	public void testAcknowledgeNull() {
		flowReceiverContainer.acknowledge(null);
	}

	@Test
	public void testAcknowledgeAfterUnbind() throws Exception {
		flowReceiverContainer.bind();

		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);
		MessageContainer messageReceived = flowReceiverContainer.receive();
		assertNotNull(messageReceived);

		flowReceiverContainer.unbind();

		thrown.expect(IllegalStateException.class);
		thrown.expectMessage("Attempted an operation on a closed message consumer");
		flowReceiverContainer.acknowledge(messageReceived);
	}

	@Test
	public void testAcknowledgeAfterFlowReconnect() throws Exception {
		if (!isDurable) {
			logger.info("Test does not apply for non-durable queues");
			return;
		}

		TextMessage message = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);

		flowReceiverContainer.bind();

		producer.send(message, queue);
		MessageContainer receivedMessage = flowReceiverContainer.receive();
		assertNotNull(receivedMessage);

		String vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);

		logger.info(String.format("Disabling egress to queue %s", queue.getName()));
		sempV2Api.config().updateMsgVpnQueue(vpnName, queue.getName(), new ConfigMsgVpnQueue().egressEnabled(false),
				null);
		Thread.sleep(TimeUnit.SECONDS.toMillis(5));

		logger.info(String.format("Enabling egress to queue %s", queue.getName()));
		sempV2Api.config().updateMsgVpnQueue(vpnName, queue.getName(), new ConfigMsgVpnQueue().egressEnabled(true),
				null);
		Thread.sleep(TimeUnit.SECONDS.toMillis(5));

		assertEquals(1, flowReceiverContainer.getNumUnacknowledgedMessages());
		logger.info(String.format("Acknowledging message %s", receivedMessage.getMessage().getMessageId()));
		flowReceiverContainer.acknowledge(receivedMessage);
		assertEquals(0, flowReceiverContainer.getNumUnacknowledgedMessages());

		Thread.sleep(TimeUnit.SECONDS.toMillis(5));

		List<MonitorMsgVpnQueueTxFlow> txFlows = getTxFlows(2, null);
		assertThat(txFlows, hasSize(1));
		assertEquals((Long) 1L, txFlows.get(0).getAckedMsgCount());
		assertEquals((Long) 0L, txFlows.get(0).getUnackedMsgCount());
		assertEquals((Long) 1L, txFlows.get(0).getRedeliveredMsgCount());
	}

	@Test
	public void testAcknowledgeAfterSessionReconnect() throws Exception {
		TextMessage message = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);

		flowReceiverContainer.bind();

		producer.send(message, queue);
		MessageContainer receivedMessage = flowReceiverContainer.receive();
		assertNotNull(receivedMessage);

		String vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);
		String clientName = (String) jcsmpSession.getProperty(JCSMPProperties.CLIENT_NAME);

		logger.info(String.format("Remotely disconnecting session %s", jcsmpSession.getSessionName()));
		sempV2Api.action().doMsgVpnClientDisconnect(vpnName, clientName, new ActionMsgVpnClientDisconnect());
		Thread.sleep(TimeUnit.SECONDS.toMillis(5));

		assertEquals(1, flowReceiverContainer.getNumUnacknowledgedMessages());
		logger.info(String.format("Acknowledging message %s", receivedMessage.getMessage().getMessageId()));
		flowReceiverContainer.acknowledge(receivedMessage);
		assertEquals(0, flowReceiverContainer.getNumUnacknowledgedMessages());

		Thread.sleep(TimeUnit.SECONDS.toMillis(5));

		List<MonitorMsgVpnQueueTxFlow> txFlows = getTxFlows(2, null);
		assertThat(txFlows, hasSize(1));
		assertEquals((Long) 1L, txFlows.get(0).getAckedMsgCount());
		assertEquals((Long) 0L, txFlows.get(0).getUnackedMsgCount());
		assertEquals((Long) 1L, txFlows.get(0).getRedeliveredMsgCount());
	}

	@Test
//	@Repeat(10) // should run a few times to make sure its stable
	public void testConcurrentAll() throws Exception {
		UUID flowReferenceId = flowReceiverContainer.bind();

		Callable<?>[] actions = new Callable[]{
				(Callable<?>) () -> flowReceiverContainer.bind(),
				(Callable<?>) () -> {flowReceiverContainer.unbind(); return null;},
				(Callable<?>) () -> {
					try {
						return flowReceiverContainer.rebind(flowReferenceId);
					} catch (IllegalStateException e) {
						if (e.getMessage().contains("is not bound")) {
							logger.info("Received expected exception due to no bound flow", e);
							return null;
						} else {
							throw e;
						}
					}
				},
				(Callable<?>) () -> {
					MessageContainer messageContainer;
					try {
						logger.info(String.format("Receiving message from %s %s",
								FlowReceiverContainer.class.getSimpleName(), flowReceiverContainer.getId()));
						messageContainer = flowReceiverContainer.receive();
					} catch (JCSMPTransportException e) {
						if (e.getMessage().contains("Consumer was closed while in receive")) {
							logger.info("Received expected exception due to interrupt from flow shutdown", e);
							return null;
						} else {
							throw e;
						}
					} catch (ClosedFacilityException e) {
						if (e.getMessage().contains("Tried to call receive on a stopped message consumer")) {
							logger.info("Received expected exception due to interrupt from flow shutdown", e);
							return null;
						} else {
							throw e;
						}
					} catch (IllegalStateException e) {
						if (e.getMessage().contains("is not bound")) {
							logger.info("Received expected exception due to no bound flow", e);
							return null;
						} else {
							throw e;
						}
					}

					if (messageContainer == null) {
						return null;
					}

					try {
						logger.info(String.format("Acknowledging message %s %s",
								((TextMessage) messageContainer.getMessage()).getText(), messageContainer));
						flowReceiverContainer.acknowledge(messageContainer);
					} catch (IllegalStateException e) {
						assertThat(e.getMessage(),
								containsString("Attempted an operation on a closed message consumer"));
					}

					return null;
				}
		};

		CyclicBarrier barrier = new CyclicBarrier(actions.length * 50);
		ScheduledExecutorService executorService = Executors.newScheduledThreadPool(barrier.getParties());
		AtomicInteger counter = new AtomicInteger();
		try {
			executorService.scheduleAtFixedRate(() -> {
				try {
					TextMessage message = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
					int cnt = counter.getAndIncrement();
					message.setText("Message " + cnt);
					producer.send(message, queue);
					logger.info("Sent message " + cnt);
				} catch (JCSMPException e) {
					throw new RuntimeException(e);
				}
			}, 0, 1, TimeUnit.SECONDS);

			Set<ScheduledFuture<?>> futures = Arrays.stream(actions)
					.flatMap(action -> IntStream.range(0, barrier.getParties() / actions.length)
							.mapToObj(i -> (Callable<?>) () -> {
								barrier.await();
								return action.call();
							}))
					.map(c -> executorService.schedule(c, RandomUtils.nextInt(100), TimeUnit.MILLISECONDS))
					.collect(Collectors.toSet());

			for (ScheduledFuture<?> future : futures) {
				future.get(1, TimeUnit.MINUTES);
			}
		} finally {
			executorService.shutdownNow();
		}
	}

	private MonitorMsgVpnQueue getQueueInfo() throws ApiException, JsonProcessingException {
		try {
			return sempV2Api.monitor().getMsgVpnQueue((String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME),
					queue.getName(), null).getData();
		} catch (ApiException e) {
			return processApiException(e);
		}
	}

	private List<MonitorMsgVpnQueueTxFlow> getTxFlows(Integer count, String cursor)
			throws ApiException, JsonProcessingException {
		try {
			return sempV2Api.monitor()
					.getMsgVpnQueueTxFlows((String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME),
					queue.getName(), count, cursor, null, null).getData();
		} catch (ApiException e) {
			return processApiException(e);
		}
	}

	private <T> T processApiException(ApiException e) throws JsonProcessingException, ApiException {
		MonitorSempMetaOnlyResponse response = sempV2Api.monitor()
				.getApiClient()
				.getJSON()
				.getContext(null)
				.readValue(e.getResponseBody(), MonitorSempMetaOnlyResponse.class);
		if (response.getMeta().getError().getStatus().equals("NOT_FOUND")) {
			return null;
		} else {
			throw e;
		}
	}

	@FunctionalInterface
	private interface ThrowingFunction<T,R> extends Function<T,R> {

		@Override
		default R apply(T t) {
			try {
				return applyThrows(t);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

		R applyThrows(T t) throws Exception;
	}
}
