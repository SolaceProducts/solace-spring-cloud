package com.solace.spring.cloud.stream.binder.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.solace.spring.boot.autoconfigure.SolaceJavaAutoConfiguration;
import com.solace.spring.cloud.stream.binder.util.FlowReceiverContainer.FlowReceiverReference;
import com.solace.test.integration.junit.jupiter.extension.ExecutorServiceExtension;
import com.solace.test.integration.junit.jupiter.extension.ExecutorServiceExtension.ExecSvc;
import com.solace.test.integration.junit.jupiter.extension.PubSubPlusExtension;
import com.solace.test.integration.semp.v2.SempV2Api;
import com.solace.test.integration.semp.v2.action.model.ActionMsgVpnClientDisconnect;
import com.solace.test.integration.semp.v2.config.model.ConfigMsgVpnQueue;
import com.solace.test.integration.semp.v2.monitor.ApiException;
import com.solace.test.integration.semp.v2.monitor.model.MonitorMsgVpnQueue;
import com.solace.test.integration.semp.v2.monitor.model.MonitorMsgVpnQueueTxFlow;
import com.solace.test.integration.semp.v2.monitor.model.MonitorMsgVpnQueueTxFlowsResponse;
import com.solace.test.integration.semp.v2.monitor.model.MonitorSempMeta;
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
import org.apache.commons.lang3.RandomUtils;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Values;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.BackOffExecution;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;

import java.util.ArrayList;
import java.util.Arrays;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.solace.spring.cloud.stream.binder.test.util.RetryableAssertions.retryAssert;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringJUnitConfig(classes = SolaceJavaAutoConfiguration.class,
		initializers = ConfigDataApplicationContextInitializer.class)
@ExtendWith(ExecutorServiceExtension.class)
@ExtendWith(PubSubPlusExtension.class)
@Timeout(value = 5, unit = TimeUnit.MINUTES)
public class FlowReceiverContainerIT {
	private String vpnName;
	private final AtomicReference<FlowReceiverContainer> flowReceiverContainerReference = new AtomicReference<>();
	private XMLMessageProducer producer;

	private static final Logger logger = LoggerFactory.getLogger(FlowReceiverContainerIT.class);

	@BeforeEach
	public void setup(JCSMPSession jcsmpSession) throws Exception {
		vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);
		producer = jcsmpSession.getMessageProducer(new JCSMPStreamingPublishCorrelatingEventHandler() {
			@Override
			public void responseReceivedEx(Object key) {
				logger.debug("Got message with key: " + key);
			}

			@Override
			public void handleErrorEx(Object o, JCSMPException e, long l) {
				logger.error("Failed to send message", e);
			}
		});
	}

	@AfterEach
	public void cleanup() {
		if (producer != null) {
			producer.close();
		}

		Optional.ofNullable(flowReceiverContainerReference.getAndSet(null))
				.map(FlowReceiverContainer::getFlowReceiverReference)
				.map(FlowReceiverReference::get)
				.ifPresent(Consumer::close);
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testBind(boolean isDurable, JCSMPSession jcsmpSession, Queue durableQueue) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);

		assertNull(flowReceiverContainer.getFlowReceiverReference());
		UUID flowReferenceId = flowReceiverContainer.bind();
		assertNotNull(flowReferenceId);

		FlowReceiverReference flowReference1 = flowReceiverContainer.getFlowReceiverReference();
		assertNotNull(flowReference1);
		assertEquals(flowReferenceId, flowReference1.getId());
		assertEquals(queue, flowReference1.get().getEndpoint());
		assertEquals(0, flowReceiverContainer.getNumUnacknowledgedMessages());
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testBindABoundFlow(boolean isDurable, JCSMPSession jcsmpSession, Queue durableQueue, SempV2Api sempV2Api) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);

		UUID flowReferenceId = flowReceiverContainer.bind();
		assertEquals(flowReferenceId, flowReceiverContainer.bind());

		FlowReceiverReference flowReference = flowReceiverContainer.getFlowReceiverReference();
		assertNotNull(flowReference);

		MonitorMsgVpnQueue queueInfo = getQueueInfo(sempV2Api, queue);
		assertNotNull(queueInfo);
		assertEquals((Long) 1L, queueInfo.getBindRequestCount());
		assertEquals((Long) 1L, queueInfo.getBindSuccessCount());

		List<MonitorMsgVpnQueueTxFlow> txFlows = getTxFlows(sempV2Api, queue, 2);
		assertThat(txFlows, hasSize(1));
		assertEquals((Long) ((FlowHandle) flowReference.get()).getFlowId(), txFlows.get(0).getFlowId());
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testBindAnUnboundFlow(boolean isDurable, JCSMPSession jcsmpSession, Queue durableQueue, SempV2Api sempV2Api) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);

		UUID flowReferenceId = flowReceiverContainer.bind();
		flowReceiverContainer.unbind();

		UUID reboundFlowReferenceId = flowReceiverContainer.bind();
		assertNotEquals(flowReferenceId, reboundFlowReferenceId);

		FlowReceiverReference flowReference = flowReceiverContainer.getFlowReceiverReference();
		assertNotNull(flowReference);

		List<MonitorMsgVpnQueueTxFlow> txFlows = getTxFlows(sempV2Api, queue, 2);
		assertThat(txFlows, hasSize(1));
		assertEquals((Long) ((FlowHandle) flowReference.get()).getFlowId(), txFlows.get(0).getFlowId());
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testBindWhileRebinding(boolean isDurable, JCSMPSession jcsmpSession, Queue durableQueue,
									   SempV2Api sempV2Api, @ExecSvc(poolSize = 2) ExecutorService executorService)
			throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);

		UUID flowReferenceId = flowReceiverContainer.bind();

		CountDownLatch midRebindLatch = new CountDownLatch(1);
		CountDownLatch finishRebindLatch = new CountDownLatch(1);
		Mockito.doAnswer(invocation -> {
			invocation.callRealMethod();
			midRebindLatch.countDown();
			finishRebindLatch.await();
			return null;
		}).when(flowReceiverContainer).unbind();

		Future<UUID> rebindFuture = executorService.submit(() -> flowReceiverContainer.rebind(flowReferenceId));
		assertTrue(midRebindLatch.await(1, TimeUnit.MINUTES));
		Future<UUID> bindFuture = executorService.submit(flowReceiverContainer::bind);
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

		MonitorMsgVpnQueue queueInfo = getQueueInfo(sempV2Api, queue);
		assertNotNull(queueInfo);
		assertEquals((Long) (isDurable ? 2L : 1L), queueInfo.getBindRequestCount());
		assertEquals((Long) (isDurable ? 2L : 1L), queueInfo.getBindSuccessCount());

		List<MonitorMsgVpnQueueTxFlow> txFlows = getTxFlows(sempV2Api, queue, 2);
		assertThat(txFlows, hasSize(1));
		assertThat(txFlows.get(0).getFlowId(), allOf(notNullValue(), equalTo(currentFlowId)));
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testBindWhileReceiving(boolean isDurable, JCSMPSession jcsmpSession, Queue durableQueue,
									   SempV2Api sempV2Api, @ExecSvc(poolSize = 1) ExecutorService executorService)
			throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);

		UUID flowReferenceId = flowReceiverContainer.bind();
		Future<MessageContainer> receiveFuture = executorService.submit((Callable<MessageContainer>) flowReceiverContainer::receive);

		// To make sure the flow receive is actually blocked
		Thread.sleep(TimeUnit.SECONDS.toMillis(5));
		assertFalse(receiveFuture.isDone());

		UUID newFlowReferenceId = flowReceiverContainer.bind();
		assertNotNull(flowReceiverContainer.getFlowReceiverReference());
		assertEquals(flowReferenceId, newFlowReferenceId);
		assertThat(getTxFlows(sempV2Api, queue, 2), hasSize(1));

		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);
		assertNotNull(receiveFuture.get(1, TimeUnit.MINUTES));
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testConcurrentBind(boolean isDurable, JCSMPSession jcsmpSession, Queue durableQueue, SempV2Api sempV2Api) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);

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

			MonitorMsgVpnQueue queueInfo = getQueueInfo(sempV2Api, queue);
			assertNotNull(queueInfo);
			assertEquals((Long) 1L, queueInfo.getBindRequestCount());
			assertEquals((Long) 1L, queueInfo.getBindSuccessCount());

			List<MonitorMsgVpnQueueTxFlow> txFlows = getTxFlows(sempV2Api, queue, 2);
			assertThat(txFlows, hasSize(1));
			assertEquals((Long) ((FlowHandle) flowReference.get()).getFlowId(), txFlows.get(0).getFlowId());
		} finally {
			executorService.shutdownNow();
		}
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testUnbind(boolean isDurable, JCSMPSession jcsmpSession, Queue durableQueue, SempV2Api sempV2Api) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);

		flowReceiverContainer.bind();
		flowReceiverContainer.unbind();
		assertNull(flowReceiverContainer.getFlowReceiverReference());
		assertThat(getTxFlows(sempV2Api, queue, 1), hasSize(0));
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testUnbindANonBoundFlow(boolean isDurable, JCSMPSession jcsmpSession, Queue durableQueue, SempV2Api sempV2Api) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);

		flowReceiverContainer.unbind();
		if (isDurable) {
			MonitorMsgVpnQueue queueInfo = getQueueInfo(sempV2Api, queue);
			assertNotNull(queueInfo);
			assertEquals((Long) 0L, queueInfo.getBindRequestCount());
		} else {
			assertNull(getQueueInfo(sempV2Api, queue));
		}
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testUnbindAnUnboundFlow(boolean isDurable, JCSMPSession jcsmpSession, Queue durableQueue, SempV2Api sempV2Api) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);

		flowReceiverContainer.bind();
		flowReceiverContainer.unbind();
		flowReceiverContainer.unbind();
		if (isDurable) {
			MonitorMsgVpnQueue queueInfo = getQueueInfo(sempV2Api, queue);
			assertNotNull(queueInfo);
			assertEquals((Long) 1L, queueInfo.getBindRequestCount());
			assertEquals((Long) 1L, queueInfo.getBindSuccessCount());
		} else {
			assertNull(getQueueInfo(sempV2Api, queue));
		}
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testUnbindWhileRebinding(boolean isDurable, JCSMPSession jcsmpSession, Queue durableQueue, SempV2Api sempV2Api) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);

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
			Future<?> unbindFuture = executorService.submit(flowReceiverContainer::unbind);
			executorService.shutdown();

			Thread.sleep(TimeUnit.SECONDS.toMillis(5));
			assertFalse(rebindFuture.isDone());
			assertFalse(unbindFuture.isDone());

			finishRebindLatch.countDown();
			assertThat(rebindFuture.get(1, TimeUnit.MINUTES),
					allOf(notNullValue(), not(equalTo(flowReferenceId))));
			unbindFuture.get(1, TimeUnit.MINUTES);

			assertNull(flowReceiverContainer.getFlowReceiverReference());

			MonitorMsgVpnQueue queueInfo = getQueueInfo(sempV2Api, queue);
			if (isDurable) {
				assertNotNull(queueInfo);
				assertEquals((Long) 2L, queueInfo.getBindRequestCount()); // 1 for initial bind, 1 for rebind
				assertEquals((Long) 2L, queueInfo.getBindSuccessCount()); // 1 for initial bind, 1 for rebind
			} else {
				assertNull(queueInfo);
			}

			List<MonitorMsgVpnQueueTxFlow> txFlows = getTxFlows(sempV2Api, queue, 1);
			assertNotNull(txFlows);
			assertThat(txFlows, hasSize(0));
		} finally {
			executorService.shutdownNow();
		}
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testUnbindWhileReceiving(boolean isDurable, JCSMPSession jcsmpSession, Queue durableQueue, SempV2Api sempV2Api) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);

		flowReceiverContainer.bind();
		ExecutorService executorService = Executors.newSingleThreadExecutor();
		try {
			Future<MessageContainer> receiveFuture = executorService.submit((Callable<MessageContainer>) flowReceiverContainer::receive);

			// To make sure the flow receive is actually blocked
			Thread.sleep(TimeUnit.SECONDS.toMillis(5));
			assertFalse(receiveFuture.isDone());

			flowReceiverContainer.unbind();
			assertNull(flowReceiverContainer.getFlowReceiverReference());
			assertThat(getTxFlows(sempV2Api, queue, 1), hasSize(0));

			ExecutionException exception = assertThrows(ExecutionException.class,
					() -> receiveFuture.get(1, TimeUnit.MINUTES));
			assertThat(exception.getCause(), instanceOf(JCSMPTransportException.class));
			assertThat(exception.getMessage(), containsString("Consumer was closed while in receive"));
		} finally {
			executorService.shutdownNow();
		}
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testUnbindWithUnacknowledgedMessage(boolean isDurable, JCSMPSession jcsmpSession, Queue durableQueue) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);

		flowReceiverContainer.bind();

		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);
		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);
		List<MessageContainer> receivedMsgs = new ArrayList<>();
		receivedMsgs.add(flowReceiverContainer.receive());
		assertNotNull(receivedMsgs.get(0));
		receivedMsgs.add(flowReceiverContainer.receive());
		assertNotNull(receivedMsgs.get(receivedMsgs.size() - 1));
		assertEquals(2, flowReceiverContainer.getNumUnacknowledgedMessages());

		flowReceiverContainer.unbind();
		assertEquals(0, flowReceiverContainer.getNumUnacknowledgedMessages());
		assertTrue(receivedMsgs.stream().allMatch(MessageContainer::isStale));
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testConcurrentUnbind(boolean isDurable, JCSMPSession jcsmpSession, Queue durableQueue, SempV2Api sempV2Api) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);

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

			MonitorMsgVpnQueue queueInfo = getQueueInfo(sempV2Api, queue);
			if (isDurable) {
				assertNotNull(queueInfo);
				assertEquals((Long) 1L, queueInfo.getBindRequestCount());
				assertEquals((Long) 1L, queueInfo.getBindSuccessCount());

				List<MonitorMsgVpnQueueTxFlow> txFlows = getTxFlows(sempV2Api, queue, 1);
				assertThat(txFlows, hasSize(0));
			} else {
				assertNull(queueInfo);
			}
		} finally {
			executorService.shutdownNow();
		}
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testPauseResume(boolean isDurable, JCSMPSession jcsmpSession, Queue durableQueue, SempV2Api sempV2Api) throws Exception {
		int defaultWindowSize = (int) jcsmpSession.getProperty(JCSMPProperties.SUB_ACK_WINDOW_SIZE);
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);

		assertThat(getTxFlows(sempV2Api, queue, 2), hasSize(0));
		flowReceiverContainer.bind();
		assertEquals(defaultWindowSize, getTxFlows(sempV2Api, queue, 2).get(0).getWindowSize());

		flowReceiverContainer.pause();
		assertTrue(flowReceiverContainer.isPaused());
		assertEquals(0, getTxFlows(sempV2Api, queue, 1).get(0).getWindowSize());

		flowReceiverContainer.resume();
		assertFalse(flowReceiverContainer.isPaused());
		assertEquals(defaultWindowSize, getTxFlows(sempV2Api, queue, 1).get(0).getWindowSize());
	}

	@CartesianTest(name = "[{index}] testResuming={0} isDurable={1}")
	public void testPauseResumeANonBoundFlow(
			@Values(booleans = {false, true}) boolean testResuming,
			@Values(booleans = {false, true}) boolean isDurable,
			JCSMPSession jcsmpSession,
			Queue durableQueue,
			SempV2Api sempV2Api) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);

		if (testResuming) {
			flowReceiverContainer.pause();
		}

		assertNull(flowReceiverContainer.getFlowReceiverReference());
		assertEquals(testResuming, flowReceiverContainer.isPaused());

		if (testResuming) {
			flowReceiverContainer.resume();
		} else {
			flowReceiverContainer.pause();
		}
		assertNull(flowReceiverContainer.getFlowReceiverReference());
		assertEquals(!testResuming, flowReceiverContainer.isPaused());
		assertEquals(0, getTxFlows(sempV2Api, queue, 1).size());
	}

	@CartesianTest(name = "[{index}] testResuming={0} isDurable={1}")
	public void testPauseResumeAnUnboundFlow(
			@Values(booleans = {false, true}) boolean testResuming,
			@Values(booleans = {false, true}) boolean isDurable,
			JCSMPSession jcsmpSession,
			Queue durableQueue,
			SempV2Api sempV2Api) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);

		flowReceiverContainer.bind();
		flowReceiverContainer.unbind();

		if (testResuming) {
			flowReceiverContainer.pause();
		}

		assertNull(flowReceiverContainer.getFlowReceiverReference());
		assertEquals(testResuming, flowReceiverContainer.isPaused());
		if (testResuming) {
			flowReceiverContainer.resume();
		} else {
			flowReceiverContainer.pause();
		}
		assertNull(flowReceiverContainer.getFlowReceiverReference());
		assertEquals(!testResuming, flowReceiverContainer.isPaused());
		assertEquals(0, getTxFlows(sempV2Api, queue, 1).size());
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testRebindWhilePaused(boolean isDurable, JCSMPSession jcsmpSession, Queue durableQueue, SempV2Api sempV2Api) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		int defaultWindowSize = (int) jcsmpSession.getProperty(JCSMPProperties.SUB_ACK_WINDOW_SIZE);
		assertThat(getTxFlows(sempV2Api, queue, 2), hasSize(0));

		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);
		UUID flowReferenceId1 = flowReceiverContainer.bind();
		List<MonitorMsgVpnQueueTxFlow> flows = getTxFlows(sempV2Api, queue, 2);
		assertThat(flows, hasSize(1));
		assertEquals(defaultWindowSize, flows.get(0).getWindowSize());

		flowReceiverContainer.pause();
		assertTrue(flowReceiverContainer.isPaused());
		assertEquals(0, getTxFlows(sempV2Api, queue, 1).get(0).getWindowSize());

		UUID flowReferenceId2 = flowReceiverContainer.rebind(flowReferenceId1);
		assertNotEquals(flowReferenceId1, flowReferenceId2);

		//Check paused state was preserved post rebind
		assertTrue(flowReceiverContainer.isPaused());
		flows = getTxFlows(sempV2Api, queue, 2);
		assertThat(flows, hasSize(1));
		assertEquals(0, flows.get(0).getWindowSize());

		//Resume
		flowReceiverContainer.resume();
		assertFalse(flowReceiverContainer.isPaused());
		flows = getTxFlows(sempV2Api, queue, 2);
		assertThat(flows, hasSize(1));
		assertEquals(defaultWindowSize, flows.get(0).getWindowSize());
	}

	@CartesianTest(name = "[{index}] testResuming={0} isDurable={1}")
	public void testPauseResumeWhileRebinding(
			@Values(booleans = {false, true}) boolean testResuming,
			@Values(booleans = {false, true}) boolean isDurable,
			JCSMPSession jcsmpSession,
			Queue durableQueue,
			SempV2Api sempV2Api) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);

		UUID flowReferenceId = flowReceiverContainer.bind();
		if (testResuming) flowReceiverContainer.pause();

		CountDownLatch midRebindLatch = new CountDownLatch(1);
		CountDownLatch finishRebindLatch = new CountDownLatch(1);
		Mockito.doAnswer(invocation -> {
			midRebindLatch.countDown();
			finishRebindLatch.await();
			return invocation.callRealMethod();
		}).when(flowReceiverContainer).bind();

		ExecutorService executorService = Executors.newFixedThreadPool(2);
		try {
			assertEquals(testResuming, flowReceiverContainer.isPaused());

			Future<UUID> rebindFuture = executorService.submit(() -> flowReceiverContainer.rebind(flowReferenceId));
			assertTrue(midRebindLatch.await(1, TimeUnit.MINUTES));
			Future<?> pauseResumeFuture = executorService.submit(testResuming ? () -> {
				try {
					flowReceiverContainer.resume();
				} catch (JCSMPException e) {
					throw new RuntimeException(e);
				}
			} : flowReceiverContainer::pause);
			executorService.shutdown();

			Thread.sleep(TimeUnit.SECONDS.toMillis(5));
			assertFalse(rebindFuture.isDone());
			assertFalse(pauseResumeFuture.isDone());
			assertEquals(testResuming, flowReceiverContainer.isPaused());

			finishRebindLatch.countDown();
			pauseResumeFuture.get(1, TimeUnit.MINUTES);

			assertEquals(testResuming, !flowReceiverContainer.isPaused()); //Pause state has flipped
			List<MonitorMsgVpnQueueTxFlow> txFlows = getTxFlows(sempV2Api, queue, 2);
			assertThat(txFlows, hasSize(1));
			int defaultWindowSize = (int) jcsmpSession.getProperty(JCSMPProperties.SUB_ACK_WINDOW_SIZE);
			assertEquals(testResuming ? defaultWindowSize : 0, txFlows.get(0).getWindowSize());
		} finally {
			executorService.shutdownNow();
		}
	}

	@ParameterizedTest(name = "[{index}] isDurable={0}")
	@ValueSource(booleans = {false, true})
	public void testPauseWhileResuming(
			boolean isDurable,
			JCSMPSession jcsmpSession,
			Queue durableQueue,
			SempV2Api sempV2Api,
			@ExecSvc ExecutorService executorService) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);

		flowReceiverContainer.bind();
		flowReceiverContainer.pause();
		assertTrue(flowReceiverContainer.isPaused());

		CountDownLatch midResumeLatch = new CountDownLatch(1);
		// can't use latch, or else pause() will take write lock
		CountDownLatch finishResumeLatch = new CountDownLatch(1);
		Mockito.doAnswer(invocation -> {
			// Call real method first since the potential race condition can happen right after
			// this method returns in flowReceiverContainer.resume()
			Object toReturn = invocation.callRealMethod();
			midResumeLatch.countDown();
			finishResumeLatch.await();
			return toReturn;
		}).when(flowReceiverContainer).doFlowReceiverReferenceResume();

		Future<?> resumeFuture = executorService.submit(() -> {
			try {
				flowReceiverContainer.resume();
			} catch (JCSMPException e) {
				throw new RuntimeException(e);
			}
		});
		assertTrue(midResumeLatch.await(1, TimeUnit.MINUTES));
		Future<?> pauseFuture = executorService.submit(flowReceiverContainer::pause);
		executorService.shutdown();

		Thread.sleep(TimeUnit.SECONDS.toMillis(5));
		assertFalse(resumeFuture.isDone());
		assertFalse(pauseFuture.isDone());
		assertTrue(flowReceiverContainer.isPaused());

		finishResumeLatch.countDown();
		pauseFuture.get(1, TimeUnit.MINUTES);
		resumeFuture.get(1, TimeUnit.MINUTES);
		assertTrue(flowReceiverContainer.isPaused());

		List<MonitorMsgVpnQueueTxFlow> txFlows = getTxFlows(sempV2Api, queue, 2);
		assertThat(txFlows, hasSize(1));
		assertEquals(0, txFlows.get(0).getWindowSize());
	}

	@ParameterizedTest(name = "[{index}] isDurable={0}")
	@ValueSource(booleans = {false, true})
	public void testResumeWhilePausing(
			boolean isDurable,
			JCSMPSession jcsmpSession,
			Queue durableQueue,
			SempV2Api sempV2Api,
			@ExecSvc ExecutorService executorService) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);

		flowReceiverContainer.bind();
		assertFalse(flowReceiverContainer.isPaused());

		CountDownLatch midPauseLatch = new CountDownLatch(1);
		CountDownLatch finishPauseLatch = new CountDownLatch(1);
		Mockito.doAnswer(invocation -> {
			// Call real method first since the potential race condition can happen right after
			// this method returns in flowReceiverContainer.pause()
			Object toReturn = invocation.callRealMethod();
			midPauseLatch.countDown();
			finishPauseLatch.await();
			return toReturn;
		}).when(flowReceiverContainer).doFlowReceiverReferencePause();

		Future<?> pauseFuture = executorService.submit(flowReceiverContainer::pause);
		assertTrue(midPauseLatch.await(1, TimeUnit.MINUTES));
		Future<?> resumeFuture = executorService.submit(() -> {
			try {
				flowReceiverContainer.resume();
			} catch (JCSMPException e) {
				throw new RuntimeException(e);
			}
		});
		executorService.shutdown();

		Thread.sleep(TimeUnit.SECONDS.toMillis(5));
		assertFalse(pauseFuture.isDone());
		assertFalse(resumeFuture.isDone());
		assertFalse(flowReceiverContainer.isPaused());

		finishPauseLatch.countDown();
		resumeFuture.get(1, TimeUnit.MINUTES);
		pauseFuture.get(1, TimeUnit.MINUTES);
		assertFalse(flowReceiverContainer.isPaused());

		List<MonitorMsgVpnQueueTxFlow> txFlows = getTxFlows(sempV2Api, queue, 2);
		assertThat(txFlows, hasSize(1));
		int defaultWindowSize = (int) jcsmpSession.getProperty(JCSMPProperties.SUB_ACK_WINDOW_SIZE);
		assertEquals(defaultWindowSize, txFlows.get(0).getWindowSize());
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testRebind(boolean isDurable, JCSMPSession jcsmpSession, Queue durableQueue, SempV2Api sempV2Api) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);

		UUID flowReferenceId1 = flowReceiverContainer.bind();

		FlowReceiverReference flowReference1 = flowReceiverContainer.getFlowReceiverReference();
		assertNotNull(flowReference1);

		List<MonitorMsgVpnQueueTxFlow> txFlows1 = getTxFlows(sempV2Api, queue, 2);
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

		List<MonitorMsgVpnQueueTxFlow> txFlows2 = getTxFlows(sempV2Api, queue, 2);
		assertThat(txFlows2, hasSize(1));
		assertEquals((Long) ((FlowHandle) flowReference2.get()).getFlowId(), txFlows2.get(0).getFlowId());
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testRebindANonBoundFlow(boolean isDurable, JCSMPSession jcsmpSession, Queue durableQueue, SempV2Api sempV2Api) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);

		UnboundFlowReceiverContainerException exception = assertThrows(UnboundFlowReceiverContainerException.class,
				() -> flowReceiverContainer.rebind(UUID.randomUUID()));
		assertThat(exception.getMessage(), containsString("is not bound"));
		if (isDurable) {
			MonitorMsgVpnQueue queueInfo = getQueueInfo(sempV2Api, queue);
			assertNotNull(queueInfo);
			assertEquals((Long) 0L, queueInfo.getBindRequestCount());
		} else {
			assertNull(getQueueInfo(sempV2Api, queue));
		}
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testRebindAReboundFlow(boolean isDurable, JCSMPSession jcsmpSession, Queue durableQueue) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);

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

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testRebindWhileReceiving(boolean isDurable, JCSMPSession jcsmpSession, Queue durableQueue, SempV2Api sempV2Api) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);

		UUID flowReferenceId = flowReceiverContainer.bind();
		ExecutorService executorService = Executors.newSingleThreadExecutor();
		try {
			Future<MessageContainer> receiveFuture = executorService.submit((Callable<MessageContainer>) flowReceiverContainer::receive);

			// To make sure the flow receive is actually blocked
			Thread.sleep(TimeUnit.SECONDS.toMillis(5));
			assertFalse(receiveFuture.isDone());

			UUID newFlowReferenceId = flowReceiverContainer.rebind(flowReferenceId);
			assertNotNull(flowReceiverContainer.getFlowReceiverReference());
			assertNotEquals(flowReferenceId, newFlowReferenceId);
			assertThat(getTxFlows(sempV2Api, queue, 2), hasSize(1));

			assertNull(receiveFuture.get(1, TimeUnit.MINUTES));
		} finally {
			executorService.shutdownNow();
		}
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testRebindWithUnacknowledgedMessage(boolean isDurable, JCSMPSession jcsmpSession, Queue durableQueue) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);

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

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testRebindWithTimeout(boolean isDurable, JCSMPSession jcsmpSession, Queue durableQueue) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);

		flowReceiverContainer.setRebindWaitTimeout(1, TimeUnit.SECONDS);

		UUID flowReferenceId = flowReceiverContainer.bind();
		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);
		MessageContainer receivedMessage = flowReceiverContainer.receive();
		assertNotNull(receivedMessage);

		assertThat(flowReceiverContainer.rebind(flowReferenceId), allOf(notNullValue(), not(equalTo(flowReferenceId))));
		assertEquals(0L, flowReceiverContainer.getNumUnacknowledgedMessages());

		assertTrue(receivedMessage.isStale());
		assertThrows(SolaceStaleMessageException.class, () -> flowReceiverContainer.acknowledge(receivedMessage));
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testRebindReturnImmediately(boolean isDurable, JCSMPSession jcsmpSession, Queue durableQueue) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);

		flowReceiverContainer.setRebindWaitTimeout(-1, TimeUnit.SECONDS); // block forever

		UUID flowReferenceId = flowReceiverContainer.bind();
		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);
		MessageContainer blockingMsg = flowReceiverContainer.receive();
		assertNotNull(blockingMsg);

		ExecutorService executorService = Executors.newSingleThreadExecutor();
		try {
			Future<UUID> future = executorService.submit(() -> flowReceiverContainer.rebind(flowReferenceId));
			Thread.sleep(1000);
			assertNull(flowReceiverContainer.rebind(flowReferenceId, true));
			flowReceiverContainer.acknowledge(blockingMsg);
			assertThat(future.get(1, TimeUnit.MINUTES), allOf(notNullValue(), not(equalTo(flowReferenceId))));
		} finally {
			executorService.shutdownNow();
		}
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testRebindAckReturnImmediately(boolean isDurable, JCSMPSession jcsmpSession, Queue durableQueue) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);

		flowReceiverContainer.setRebindWaitTimeout(-1, TimeUnit.SECONDS); // block forever

		UUID flowReferenceId = flowReceiverContainer.bind();
		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);
		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);
		MessageContainer receivedMessage1 = flowReceiverContainer.receive();
		MessageContainer receivedMessage2 = flowReceiverContainer.receive();
		assertNotNull(receivedMessage1);
		assertNotNull(receivedMessage2);

		ExecutorService executorService = Executors.newSingleThreadExecutor();
		try {
			Future<UUID> future = executorService.submit(() -> flowReceiverContainer.rebind(flowReferenceId));
			Thread.sleep(1000);
			assertNull(flowReceiverContainer.acknowledgeRebind(receivedMessage1, true));
			assertFalse(future.isDone());
			assertNull(flowReceiverContainer.acknowledgeRebind(receivedMessage2, true));
			UUID newFlowReferenceId = future.get(1, TimeUnit.MINUTES);
			assertThat(newFlowReferenceId, allOf(notNullValue(), not(equalTo(flowReferenceId))));
			assertEquals(newFlowReferenceId, flowReceiverContainer.acknowledgeRebind(receivedMessage1, true));
			assertEquals(newFlowReferenceId, flowReceiverContainer.acknowledgeRebind(receivedMessage2, true));
		} finally {
			executorService.shutdownNow();
		}
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testRebindAckAlreadyAcknowledged(boolean isDurable, JCSMPSession jcsmpSession, Queue durableQueue) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);

		UUID flowReferenceId = flowReceiverContainer.bind();
		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);
		MessageContainer receivedMessage = flowReceiverContainer.receive();
		assertNotNull(receivedMessage);
		flowReceiverContainer.acknowledge(receivedMessage);
		assertEquals(flowReferenceId, flowReceiverContainer.acknowledgeRebind(receivedMessage));
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testRebindAckAlreadyAcknowledgedBlocked(boolean isDurable, JCSMPSession jcsmpSession, Queue durableQueue) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);

		UUID flowReferenceId = flowReceiverContainer.bind();
		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);
		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);
		MessageContainer receivedMsg = flowReceiverContainer.receive();
		MessageContainer blockingMsg = flowReceiverContainer.receive();
		assertNotNull(receivedMsg);
		assertNotNull(blockingMsg);
		flowReceiverContainer.acknowledge(receivedMsg);

		ExecutorService executorService = Executors.newSingleThreadExecutor();
		try {
			Future<UUID> rebindFuture = executorService.submit(() -> flowReceiverContainer.rebind(flowReferenceId));
			Future<UUID> ackFuture = executorService.submit(() -> flowReceiverContainer.acknowledgeRebind(receivedMsg));

			// To make sure the flow rebind and ack rebind are actually blocked
			Thread.sleep(TimeUnit.SECONDS.toMillis(5));
			assertFalse(rebindFuture.isDone());
			assertFalse(ackFuture.isDone());

			flowReceiverContainer.acknowledge(blockingMsg);
			UUID newFlowReferenceId = rebindFuture.get(1, TimeUnit.MINUTES);
			assertThat(newFlowReferenceId, allOf(notNullValue(), not(equalTo(flowReferenceId))));
			assertEquals(newFlowReferenceId, flowReceiverContainer.acknowledgeRebind(receivedMsg, true));
		} finally {
			executorService.shutdownNow();
		}
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testRebindAckAlreadyAcknowledgedAndReturnImmediately(boolean isDurable, JCSMPSession jcsmpSession, Queue durableQueue) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);

		UUID flowReferenceId = flowReceiverContainer.bind();
		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);
		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);
		MessageContainer receivedMsg = flowReceiverContainer.receive();
		MessageContainer blockingMsg = flowReceiverContainer.receive();
		assertNotNull(receivedMsg);
		assertNotNull(blockingMsg);
		flowReceiverContainer.acknowledge(receivedMsg);

		ExecutorService executorService = Executors.newSingleThreadExecutor();
		try {
			Future<UUID> future = executorService.submit(() -> flowReceiverContainer.rebind(flowReferenceId));
			Thread.sleep(1000);
			assertNull(flowReceiverContainer.acknowledgeRebind(receivedMsg, true));
			assertFalse(future.isDone());
			flowReceiverContainer.acknowledge(blockingMsg);
			UUID newFlowReferenceId = future.get(1, TimeUnit.MINUTES);
			assertThat(newFlowReferenceId, allOf(notNullValue(), not(equalTo(flowReferenceId))));
			assertEquals(newFlowReferenceId, flowReceiverContainer.acknowledgeRebind(receivedMsg, true));
		} finally {
			executorService.shutdownNow();
		}
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testRebindBackOff(boolean isDurable,
								  JCSMPSession jcsmpSession,
								  Queue durableQueue)
			throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		ExponentialBackOff backOff = Mockito.spy(new ExponentialBackOff(1, 1.5));
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue, backOff);

		AtomicReference<BackOffExecution> backOffExecutionRef = new AtomicReference<>();
		Mockito.doAnswer(invocation -> {
			backOffExecutionRef.set(Mockito.spy((BackOffExecution) invocation.callRealMethod()));
			return backOffExecutionRef.get();
		}).when(backOff).start();

		UUID flowReferenceId = flowReceiverContainer.bind();
		assertNull(backOffExecutionRef.get());

		flowReferenceId = flowReceiverContainer.rebind(flowReferenceId);
		BackOffExecution backOffExecution0 = backOffExecutionRef.get();
		assertNotNull(backOffExecution0);
		Mockito.verify(backOffExecution0, Mockito.times(0)).nextBackOff();

		flowReferenceId = flowReceiverContainer.rebind(flowReferenceId);
		assertEquals(backOffExecution0, backOffExecutionRef.get());
		Mockito.verify(backOffExecution0, Mockito.times(1)).nextBackOff();

		flowReferenceId = flowReceiverContainer.rebind(flowReferenceId);
		assertEquals(backOffExecution0, backOffExecutionRef.get());
		Mockito.verify(backOffExecution0, Mockito.times(2)).nextBackOff();

		TextMessage message = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
		producer.send(message, queue);
		flowReceiverContainer.acknowledge(Objects.requireNonNull(flowReceiverContainer.receive(1000)));

		flowReceiverContainer.rebind(flowReferenceId);
		BackOffExecution backOffExecution1 = backOffExecutionRef.get();
		Assertions.assertThat(backOffExecution1).isNotNull().isNotEqualTo(backOffExecution0);
		Mockito.verify(backOffExecution1, Mockito.times(0)).nextBackOff();
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testRebindBackOffInvalidStop(boolean isDurable,
											 JCSMPSession jcsmpSession,
											 Queue durableQueue)
			throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FixedBackOff backOff = Mockito.spy(new FixedBackOff(1, 1));
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue, backOff);

		AtomicReference<BackOffExecution> backOffExecutionRef = new AtomicReference<>();
		Mockito.doAnswer(invocation -> {
			backOffExecutionRef.set(Mockito.spy((BackOffExecution) invocation.callRealMethod()));
			return backOffExecutionRef.get();
		}).when(backOff).start();

		UUID flowReferenceId = flowReceiverContainer.bind();
		assertNull(backOffExecutionRef.get());

		flowReferenceId = flowReceiverContainer.rebind(flowReferenceId);
		BackOffExecution backOffExecution0 = backOffExecutionRef.get();
		Assertions.assertThat(backOffExecution0).isNotNull();
		Mockito.verify(backOffExecution0, Mockito.times(0)).nextBackOff();

		flowReferenceId = flowReceiverContainer.rebind(flowReferenceId);
		Assertions.assertThat(backOffExecutionRef.get()).isEqualTo(backOffExecution0);
		Mockito.verify(backOffExecution0, Mockito.times(1)).nextBackOff();

		flowReferenceId = flowReceiverContainer.rebind(flowReferenceId);
		BackOffExecution backOffExecution1 = backOffExecutionRef.get();
		Assertions.assertThat(backOffExecution1)
				.isNotNull()
				.isNotEqualTo(backOffExecution0);
		Mockito.verify(backOffExecution1, Mockito.times(1)).nextBackOff();

		TextMessage message = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
		producer.send(message, queue);
		flowReceiverContainer.acknowledge(Objects.requireNonNull(flowReceiverContainer.receive(1000)));
		flowReceiverContainer.rebind(flowReferenceId);
		BackOffExecution backOffExecution2 = backOffExecutionRef.get();
		Assertions.assertThat(backOffExecution2)
				.isNotNull()
				.isNotEqualTo(backOffExecution0)
				.isNotEqualTo(backOffExecution1);
		Mockito.verify(backOffExecution2, Mockito.times(0)).nextBackOff();
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testRebindInterrupt(boolean isDurable, JCSMPSession jcsmpSession, Queue durableQueue, SempV2Api sempV2Api) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);

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

		List<MonitorMsgVpnQueueTxFlow> txFlows = getTxFlows(sempV2Api, queue, 2);
		assertThat(txFlows, hasSize(1));
		assertEquals((Long) 2L, txFlows.get(0).getAckedMsgCount());
		assertEquals((Long) 0L, txFlows.get(0).getUnackedMsgCount());
	}

	@Test
	public void testRebindAfterFlowDisconnect(JCSMPSession jcsmpSession, Queue queue, SempV2Api sempV2Api) throws Exception {
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);

		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);
		flowReceiverContainer.bind();
		MessageContainer messageContainer = flowReceiverContainer.receive((int) TimeUnit.MINUTES.toMillis(1));

		logger.info(String.format("Disabling egress to queue %s", queue.getName()));
		sempV2Api.config().updateMsgVpnQueue(vpnName, queue.getName(), new ConfigMsgVpnQueue().egressEnabled(false),
				null);
		retryAssert(() -> assertFalse(sempV2Api.monitor()
				.getMsgVpnQueue(vpnName, queue.getName(), null)
				.getData()
				.isEgressEnabled()));

		assertThrows(JCSMPException.class, () -> flowReceiverContainer.acknowledgeRebind(messageContainer));
		assertEquals(0, flowReceiverContainer.getNumUnacknowledgedMessages());
		assertNull(flowReceiverContainer.getFlowReceiverReference());
	}

	@Test
	public void testRebindAfterFlowReconnect(JCSMPSession jcsmpSession, Queue queue, SempV2Api sempV2Api) throws Exception {
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);

		TextMessage message = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);

		UUID flowReferenceId = flowReceiverContainer.bind();

		producer.send(message, queue);
		MessageContainer receivedMessage = flowReceiverContainer.receive();
		assertNotNull(receivedMessage);

		logger.info(String.format("Disabling egress to queue %s", queue.getName()));
		sempV2Api.config().updateMsgVpnQueue(vpnName, queue.getName(), new ConfigMsgVpnQueue().egressEnabled(false),
				null);
		retryAssert(() -> assertFalse(sempV2Api.monitor()
				.getMsgVpnQueue(vpnName, queue.getName(), null)
				.getData()
				.isEgressEnabled()));

		Thread.sleep(TimeUnit.SECONDS.toMillis(1));

		logger.info(String.format("Enabling egress to queue %s", queue.getName()));
		sempV2Api.config().updateMsgVpnQueue(vpnName, queue.getName(), new ConfigMsgVpnQueue().egressEnabled(true),
				null);
		retryAssert(() -> assertTrue(sempV2Api.monitor()
				.getMsgVpnQueue(vpnName, queue.getName(), null)
				.getData()
				.isEgressEnabled()));

		assertEquals(1, flowReceiverContainer.getNumUnacknowledgedMessages());
		logger.info(String.format("Initiating rebind with message container %s", receivedMessage.getId()));
		UUID flowReferenceId2 = flowReceiverContainer.acknowledgeRebind(receivedMessage);
		assertThat(flowReferenceId2, allOf(notNullValue(), not(equalTo(flowReferenceId))));
		assertEquals(0, flowReceiverContainer.getNumUnacknowledgedMessages());

		Mockito.verify(flowReceiverContainer, Mockito.times(1)).unbind();
		Mockito.verify(flowReceiverContainer, Mockito.times(2)).bind(); // +1 for init bind

		retryAssert(() -> {
			List<MonitorMsgVpnQueueTxFlow> txFlows = getTxFlows(sempV2Api, queue, 2);
			assertThat(txFlows, hasSize(1));
			assertEquals((Long) 0L, txFlows.get(0).getAckedMsgCount());
			assertEquals((Long) 1L, txFlows.get(0).getUnackedMsgCount());
			assertEquals((Long) 1L, txFlows.get(0).getRedeliveredMsgCount());
		});
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testRebindAfterSessionReconnect(boolean isDurable, JCSMPSession jcsmpSession, Queue durableQueue,
												SempV2Api sempV2Api) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);

		TextMessage message = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);

		UUID flowReferenceId = flowReceiverContainer.bind();

		producer.send(message, queue);
		MessageContainer receivedMessage = flowReceiverContainer.receive();
		assertNotNull(receivedMessage);

		String clientName = (String) jcsmpSession.getProperty(JCSMPProperties.CLIENT_NAME);

		logger.info(String.format("Remotely disconnecting client %s", clientName));
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

		List<MonitorMsgVpnQueueTxFlow> txFlows = getTxFlows(sempV2Api, queue, 2);
		assertThat(txFlows, hasSize(1));
		assertEquals((Long) 0L, txFlows.get(0).getAckedMsgCount());
		assertEquals((Long) 1L, txFlows.get(0).getUnackedMsgCount());
		assertEquals(isDurable ? (Long) 1L : (Long) 0L, txFlows.get(0).getRedeliveredMsgCount());
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testConcurrentRebind(boolean isDurable, JCSMPSession jcsmpSession, Queue durableQueue) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);

		CyclicBarrier barrier = new CyclicBarrier(30);

		UUID flowReferenceId = flowReceiverContainer.bind();

		ScheduledExecutorService executorService = Executors.newScheduledThreadPool(barrier.getParties());
		try {
			Set<Future<UUID>> futures = IntStream.range(0, barrier.getParties())
					.mapToObj(i -> (Callable<UUID>) () -> {
						barrier.await();
						return flowReceiverContainer.rebind(flowReferenceId);
					})
					.map(c -> executorService.schedule(c, RandomUtils.nextInt(0, 100), TimeUnit.MILLISECONDS))
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

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testReceive(boolean isDurable, JCSMPSession jcsmpSession, Queue durableQueue) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);

		TextMessage message = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);

		UUID flowReferenceId1 = flowReceiverContainer.bind();
		producer.send(message, queue);
		MessageContainer messageReceived = flowReceiverContainer.receive();

		assertNotNull(messageReceived);
		assertThat(messageReceived.getMessage(), instanceOf(TextMessage.class));
		assertEquals(flowReferenceId1, messageReceived.getFlowReceiverReferenceId());
		assertEquals(1, flowReceiverContainer.getNumUnacknowledgedMessages());
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testReceiveOnANonBoundFlow(boolean isDurable, JCSMPSession jcsmpSession, Queue durableQueue) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);

		long startTime = System.currentTimeMillis();
		UnboundFlowReceiverContainerException exception = assertThrows(UnboundFlowReceiverContainerException.class,
				flowReceiverContainer::receive);
		assertThat(System.currentTimeMillis() - startTime, greaterThanOrEqualTo(TimeUnit.SECONDS.toMillis(5)));
		assertThat(exception.getMessage(), containsString("is not bound"));
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testReceiveWhileRebinding(boolean isDurable, JCSMPSession jcsmpSession, Queue durableQueue) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);

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
			Future<MessageContainer> receiveFuture = executorService.submit((Callable<MessageContainer>) flowReceiverContainer::receive);
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

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testReceiveWhilePreRebinding(boolean isDurable, JCSMPSession jcsmpSession, Queue durableQueue) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);

		TextMessage message = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
		UUID flowReferenceId = flowReceiverContainer.bind();
		producer.send(message, queue);
		producer.send(message, queue);

		MessageContainer messageContainer1 = flowReceiverContainer.receive();

		ExecutorService executorService = Executors.newFixedThreadPool(2);
		try {
			Future<UUID> rebindFuture = executorService.submit(() -> flowReceiverContainer.rebind(flowReferenceId));
			Thread.sleep(TimeUnit.SECONDS.toMillis(3));
			assertFalse(rebindFuture.isDone());

			Future<MessageContainer> receiveFuture = executorService.submit((Callable<MessageContainer>) flowReceiverContainer::receive);
			Thread.sleep(TimeUnit.SECONDS.toMillis(5));
			assertFalse(receiveFuture.isDone());
			assertFalse(rebindFuture.isDone());

			executorService.shutdown();

			logger.info(String.format("Acknowledging message container %s", messageContainer1.getId()));
			flowReceiverContainer.acknowledge(messageContainer1);

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

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testReceiveNoWait(boolean isDurable, JCSMPSession jcsmpSession, Queue durableQueue) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);

		flowReceiverContainer.bind();
		assertNull(flowReceiverContainer.receive(0));
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testReceiveWithTimeout(boolean isDurable, JCSMPSession jcsmpSession, Queue durableQueue) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);

		TextMessage message = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);

		UUID flowReferenceId = flowReceiverContainer.bind();
		producer.send(message, queue);
		MessageContainer messageReceived =
				flowReceiverContainer.receive((int) TimeUnit.MINUTES.toMillis(5));

		assertNotNull(messageReceived);
		assertThat(messageReceived.getMessage(), instanceOf(TextMessage.class));
		assertEquals(flowReferenceId, messageReceived.getFlowReceiverReferenceId());
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testReceiveElapsedTimeout(boolean isDurable, JCSMPSession jcsmpSession, Queue durableQueue) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);

		flowReceiverContainer.bind();
		assertNull(flowReceiverContainer.receive(1));
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testReceiveNegativeTimeout(boolean isDurable, JCSMPSession jcsmpSession, Queue durableQueue) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);

		flowReceiverContainer.bind();
		assertNull(flowReceiverContainer.receive(-1));
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testReceiveZeroTimeout(boolean isDurable, JCSMPSession jcsmpSession, Queue durableQueue) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);

		flowReceiverContainer.bind();
		assertNull(flowReceiverContainer.receive(0));
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testReceiveWithDelayedBind(boolean isDurable, JCSMPSession jcsmpSession, Queue durableQueue) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);

		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);

		ExecutorService executorService = Executors.newSingleThreadExecutor();
		try {
			long startTime = System.currentTimeMillis();
			Future<MessageContainer> future = executorService.submit((Callable<MessageContainer>) flowReceiverContainer::receive);

			Thread.sleep(TimeUnit.SECONDS.toMillis(2));
			assertFalse(future.isDone());
			flowReceiverContainer.bind();

			if (!isDurable) {
				producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);
			}

			assertNotNull(future.get(1, TimeUnit.MINUTES));
			assertThat(System.currentTimeMillis() - startTime, lessThan(5500L));
			assertEquals(1, flowReceiverContainer.getNumUnacknowledgedMessages());
		} finally {
			executorService.shutdownNow();
		}
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testReceiveWithTimeoutAndDelayedBind(boolean isDurable, JCSMPSession jcsmpSession, Queue durableQueue) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);

		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);

		ExecutorService executorService = Executors.newSingleThreadExecutor();
		try {
			long timeout = TimeUnit.SECONDS.toMillis(10);
			long bindDelay = TimeUnit.SECONDS.toMillis(5);

			long startTime = System.currentTimeMillis();
			Future<MessageContainer> future = executorService.submit(() -> flowReceiverContainer.receive((int) timeout));

			Thread.sleep(bindDelay);
			assertFalse(future.isDone());

			flowReceiverContainer.bind();

			if (isDurable) {
				assertNotNull(future.get(1, TimeUnit.MINUTES));
				assertThat(System.currentTimeMillis() - startTime, lessThan(timeout + 500));
				assertEquals(1, flowReceiverContainer.getNumUnacknowledgedMessages());
			} else {
				assertNull(future.get(1, TimeUnit.MINUTES));
				assertThat(System.currentTimeMillis() - startTime,
						allOf(greaterThanOrEqualTo(timeout), lessThan(timeout + 500)));
				assertEquals(0, flowReceiverContainer.getNumUnacknowledgedMessages());
			}
		} finally {
			executorService.shutdownNow();
		}
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testReceiveInterrupt(boolean isDurable, JCSMPSession jcsmpSession, Queue durableQueue) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);

		flowReceiverContainer.bind();
		ExecutorService executorService = Executors.newSingleThreadExecutor();
		try {
			Future<MessageContainer> receiveFuture = executorService.submit((Callable<MessageContainer>) flowReceiverContainer::receive);
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
	public void testReceiveInterruptedByFlowReconnect(JCSMPSession jcsmpSession, Queue queue, SempV2Api sempV2Api) throws Exception {
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);

		flowReceiverContainer.bind();

		ExecutorService executorService = Executors.newSingleThreadExecutor();
		try {
			Future<MessageContainer> receiveFuture = executorService.submit((Callable<MessageContainer>) flowReceiverContainer::receive);
			executorService.shutdown();

			Thread.sleep(TimeUnit.SECONDS.toMillis(5));
			assertFalse(receiveFuture.isDone());

			logger.info(String.format("Disabling egress to queue %s", queue.getName()));
			sempV2Api.config().updateMsgVpnQueue(vpnName, queue.getName(), new ConfigMsgVpnQueue().egressEnabled(false),
					null);
			retryAssert(() -> assertFalse(sempV2Api.monitor()
					.getMsgVpnQueue(vpnName, queue.getName(), null)
					.getData()
					.isEgressEnabled()));

			logger.info(String.format("Sending message to queue %s", queue.getName()));
			producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);

			logger.info(String.format("Enabling egress to queue %s", queue.getName()));
			sempV2Api.config().updateMsgVpnQueue(vpnName, queue.getName(), new ConfigMsgVpnQueue().egressEnabled(true),
					null);
			retryAssert(() -> assertTrue(sempV2Api.monitor()
					.getMsgVpnQueue(vpnName, queue.getName(), null)
					.getData()
					.isEgressEnabled()));

			assertNotNull(receiveFuture.get(1, TimeUnit.MINUTES));
		} finally {
			executorService.shutdownNow();
		}
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testReceiveInterruptedBySessionReconnect(boolean isDurable, JCSMPSession jcsmpSession,
														 Queue durableQueue, SempV2Api sempV2Api) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);

		flowReceiverContainer.bind();

		String clientName = (String) jcsmpSession.getProperty(JCSMPProperties.CLIENT_NAME);

		ExecutorService executorService = Executors.newSingleThreadExecutor();
		try {
			Future<MessageContainer> receiveFuture = executorService.submit((Callable<MessageContainer>) flowReceiverContainer::receive);
			executorService.shutdown();

			Thread.sleep(TimeUnit.SECONDS.toMillis(5));
			assertFalse(receiveFuture.isDone());

			logger.info(String.format("Remotely disconnecting client %s", clientName));
			sempV2Api.action().doMsgVpnClientDisconnect(vpnName, clientName, new ActionMsgVpnClientDisconnect());
			Thread.sleep(TimeUnit.SECONDS.toMillis(5));

			logger.info(String.format("Sending message to queue %s", queue.getName()));
			producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);

			assertNotNull(receiveFuture.get(1, TimeUnit.MINUTES));
		} finally {
			executorService.shutdownNow();
		}
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testReceiveAfterRebind(boolean isDurable, JCSMPSession jcsmpSession, Queue durableQueue) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);

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

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testWaitForBind(boolean isDurable, JCSMPSession jcsmpSession, Queue durableQueue) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);

		ExecutorService executorService = Executors.newSingleThreadExecutor();
		try {
			Future<Boolean> future = executorService.submit(() ->
					flowReceiverContainer.waitForBind(TimeUnit.HOURS.toMillis(1)));
			Thread.sleep(TimeUnit.SECONDS.toMillis(5));
			assertFalse(future.isDone());
			flowReceiverContainer.bind();
			assertTrue(future.get(1, TimeUnit.MINUTES));
		} finally {
			executorService.shutdownNow();
		}
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testWaitForBindNegative(boolean isDurable, JCSMPSession jcsmpSession, Queue durableQueue) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);

		long startTime = System.currentTimeMillis();
		assertFalse(flowReceiverContainer.waitForBind(-100));
		assertThat(System.currentTimeMillis() - startTime, lessThan(500L));
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testAcknowledgeNull(boolean isDurable, JCSMPSession jcsmpSession, Queue durableQueue) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);

		flowReceiverContainer.acknowledge(null);
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testAcknowledgeAfterUnbind(boolean isDurable, JCSMPSession jcsmpSession, Queue durableQueue) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);

		flowReceiverContainer.bind();

		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);
		MessageContainer messageReceived = flowReceiverContainer.receive();
		assertNotNull(messageReceived);

		flowReceiverContainer.unbind();
		assertEquals(0L, flowReceiverContainer.getNumUnacknowledgedMessages());
		assertTrue(messageReceived.isStale());

		assertThrows(SolaceStaleMessageException.class, () -> flowReceiverContainer.acknowledge(messageReceived));
	}

	@Test
	public void testAcknowledgeAfterFlowReconnect(JCSMPSession jcsmpSession, Queue queue, SempV2Api sempV2Api) throws Exception {
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);

		TextMessage message = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);

		flowReceiverContainer.bind();

		producer.send(message, queue);
		MessageContainer receivedMessage = flowReceiverContainer.receive();
		assertNotNull(receivedMessage);

		logger.info(String.format("Disabling egress to queue %s", queue.getName()));
		sempV2Api.config().updateMsgVpnQueue(vpnName, queue.getName(), new ConfigMsgVpnQueue().egressEnabled(false),
				null);
		retryAssert(() -> assertFalse(sempV2Api.monitor()
				.getMsgVpnQueue(vpnName, queue.getName(), null)
				.getData()
				.isEgressEnabled()));

		Thread.sleep(TimeUnit.SECONDS.toMillis(1));

		logger.info(String.format("Enabling egress to queue %s", queue.getName()));
		sempV2Api.config().updateMsgVpnQueue(vpnName, queue.getName(), new ConfigMsgVpnQueue().egressEnabled(true),
				null);
		retryAssert(() -> assertTrue(sempV2Api.monitor()
				.getMsgVpnQueue(vpnName, queue.getName(), null)
				.getData()
				.isEgressEnabled()));

		assertEquals(1, flowReceiverContainer.getNumUnacknowledgedMessages());
		logger.info(String.format("Acknowledging message %s", receivedMessage.getMessage().getMessageId()));
		flowReceiverContainer.acknowledge(receivedMessage);
		assertEquals(0, flowReceiverContainer.getNumUnacknowledgedMessages());

		Thread.sleep(TimeUnit.SECONDS.toMillis(5));

		List<MonitorMsgVpnQueueTxFlow> txFlows = getTxFlows(sempV2Api, queue, 2);
		assertThat(txFlows, hasSize(1));
		assertEquals((Long) 1L, txFlows.get(0).getAckedMsgCount());
		assertEquals((Long) 0L, txFlows.get(0).getUnackedMsgCount());
		assertEquals((Long) 1L, txFlows.get(0).getRedeliveredMsgCount());
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	public void testAcknowledgeAfterSessionReconnect(boolean isDurable, JCSMPSession jcsmpSession, Queue durableQueue,
													 SempV2Api sempV2Api) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);

		TextMessage message = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);

		flowReceiverContainer.bind();

		producer.send(message, queue);
		MessageContainer receivedMessage = flowReceiverContainer.receive();
		assertNotNull(receivedMessage);

		String clientName = (String) jcsmpSession.getProperty(JCSMPProperties.CLIENT_NAME);

		logger.info(String.format("Remotely disconnecting client %s", clientName));
		sempV2Api.action().doMsgVpnClientDisconnect(vpnName, clientName, new ActionMsgVpnClientDisconnect());
		Thread.sleep(TimeUnit.SECONDS.toMillis(5));

		assertEquals(1, flowReceiverContainer.getNumUnacknowledgedMessages());
		logger.info(String.format("Acknowledging message %s", receivedMessage.getMessage().getMessageId()));
		flowReceiverContainer.acknowledge(receivedMessage);
		assertEquals(0, flowReceiverContainer.getNumUnacknowledgedMessages());

		Thread.sleep(TimeUnit.SECONDS.toMillis(5));

		List<MonitorMsgVpnQueueTxFlow> txFlows = getTxFlows(sempV2Api, queue, 2);
		assertThat(txFlows, hasSize(1));
		assertEquals((Long) 1L, txFlows.get(0).getAckedMsgCount());
		assertEquals((Long) 0L, txFlows.get(0).getUnackedMsgCount());
		assertEquals((Long) 1L, txFlows.get(0).getRedeliveredMsgCount());
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	@Timeout(value = 10, unit = TimeUnit.MINUTES)
	@Execution(ExecutionMode.SAME_THREAD)
	public void testConcurrentAll(boolean isDurable,
								  JCSMPSession jcsmpSession,
								  Queue durableQueue,
								  SempV2Api sempV2Api) throws Exception {
		Queue queue = isDurable ? durableQueue : jcsmpSession.createTemporaryQueue();
		FlowReceiverContainer flowReceiverContainer = createFlowReceiverContainer(jcsmpSession, queue);

		UUID flowReferenceId = flowReceiverContainer.bind();

		Callable<?>[] actions = new Callable[]{
				(Callable<?>) () -> {
					AtomicReference<UUID> newFlowReferenceId = new AtomicReference<>();
					retryAssert(() -> newFlowReferenceId.set(assertDoesNotThrow(flowReceiverContainer::bind)),
							1, TimeUnit.MINUTES);
					return newFlowReferenceId.get();
				},
				(Callable<?>) () -> {flowReceiverContainer.unbind(); return null;},
				(Callable<?>) () -> {
					try {
						return flowReceiverContainer.rebind(flowReferenceId);
					} catch (UnboundFlowReceiverContainerException e) {
						logger.info("Received expected exception due to no bound flow", e);
						return null;
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
					} catch (UnboundFlowReceiverContainerException e) {
						logger.info("Received expected exception due to no bound flow", e);
						return null;
					}

					if (messageContainer == null) {
						return null;
					}

					try {
						logger.info(String.format("Acknowledging message %s %s",
								((TextMessage) messageContainer.getMessage()).getText(), messageContainer));
						flowReceiverContainer.acknowledge(messageContainer);
					} catch (SolaceStaleMessageException e) {
						assertThat(e.getMessage(),
								containsString("is stale"));
					}

					return null;
				}
		};

		CyclicBarrier barrier = new CyclicBarrier(actions.length * 10);
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
					.map(c -> executorService.schedule(c, RandomUtils.nextInt(0, 100), TimeUnit.MILLISECONDS))
					.collect(Collectors.toSet());

			for (ScheduledFuture<?> future : futures) {
				Assertions.assertThatCode(() -> future.get(5, TimeUnit.MINUTES))
						.as(() -> {
							String msgVpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);
							Optional<MonitorMsgVpnQueueTxFlowsResponse> msgVpnQueueTxFlowsResponse;
							try {
								msgVpnQueueTxFlowsResponse = Optional.of(sempV2Api.monitor().getMsgVpnQueueTxFlows(
										msgVpnName,
										queue.getName(),
										Integer.MAX_VALUE,
										null, null, null));
							} catch (ApiException e) {
								logger.error("Failed to get tx flows for queue {}", queue.getName(), e);
								msgVpnQueueTxFlowsResponse = Optional.empty();
							}
							return String.format("Check flow operation result for queue %s\n" +
											"Flow receiver is using flow ID %s\n" +
											"Queue has flows (%s) %s",
									queue.getName(),
									Optional.ofNullable(flowReceiverContainer.getFlowReceiverReference())
											.map(FlowReceiverReference::get)
											.map(r -> ((FlowHandle) r).getFlowId())
											.orElse(null),
									msgVpnQueueTxFlowsResponse.map(MonitorMsgVpnQueueTxFlowsResponse::getMeta)
											.map(MonitorSempMeta::getCount)
											.map(c -> Long.toString(c))
											.orElse("'Error getting flows'"),
									msgVpnQueueTxFlowsResponse.map(MonitorMsgVpnQueueTxFlowsResponse::getData)
											.map(Object::toString)
											.orElse("'Error getting flows'"));
						})
						.doesNotThrowAnyException();
			}
		} finally {
			executorService.shutdownNow();
		}
	}

	private FlowReceiverContainer createFlowReceiverContainer(JCSMPSession jcsmpSession, Queue queue) {
		return createFlowReceiverContainer(jcsmpSession, queue, new FixedBackOff(1, Long.MAX_VALUE));
	}

	private FlowReceiverContainer createFlowReceiverContainer(JCSMPSession jcsmpSession,
															  Queue queue,
															  BackOff backOff) {
		if (flowReceiverContainerReference.compareAndSet(null, Mockito.spy(new FlowReceiverContainer(
				jcsmpSession,
				queue.getName(),
				new EndpointProperties(),
				backOff)))) {
			logger.info("Created new FlowReceiverContainer " + flowReceiverContainerReference.get().getId());
		}
		return flowReceiverContainerReference.get();
	}

	private MonitorMsgVpnQueue getQueueInfo(SempV2Api sempV2Api, Queue queue) throws ApiException, JsonProcessingException {
		try {
			return sempV2Api.monitor().getMsgVpnQueue(vpnName, queue.getName(), null).getData();
		} catch (ApiException e) {
			return processApiException(sempV2Api, e);
		}
	}

	private List<MonitorMsgVpnQueueTxFlow> getTxFlows(SempV2Api sempV2Api, Queue queue, Integer count)
			throws ApiException, JsonProcessingException {
		try {
			return sempV2Api.monitor()
					.getMsgVpnQueueTxFlows(vpnName, queue.getName(), count, null, null, null)
					.getData();
		} catch (ApiException e) {
			return processApiException(sempV2Api, e);
		}
	}

	private <T> T processApiException(SempV2Api sempV2Api, ApiException e) throws JsonProcessingException, ApiException {
		MonitorSempMetaOnlyResponse response = sempV2Api.monitor()
				.getApiClient()
				.getJSON()
				.deserialize(e.getResponseBody(), MonitorSempMetaOnlyResponse.class);
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
