package com.solace.spring.cloud.stream.binder.util;

import com.solace.spring.boot.autoconfigure.SolaceJavaAutoConfiguration;
import com.solace.spring.cloud.stream.binder.ITBase;
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
import org.mockito.Mockito;
import org.springframework.boot.test.context.ConfigFileApplicationContextInitializer;
import org.springframework.test.context.ContextConfiguration;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@ContextConfiguration(classes = SolaceJavaAutoConfiguration.class,
		initializers = ConfigFileApplicationContextInitializer.class)
public class RetryableAckRebindTaskIT extends ITBase {
	private RetryableTaskService taskService;
	private String vpnName;
	private Queue queue;
	private FlowReceiverContainer flowReceiverContainer;
	private XMLMessageProducer producer;

	private static final Log logger = LogFactory.getLog(RetryableAckRebindTaskIT.class);

	@Before
	public void setUp() throws Exception {
		vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);
		taskService = Mockito.spy(new RetryableTaskService());
		queue = JCSMPFactory.onlyInstance().createQueue(RandomStringUtils.randomAlphanumeric(20));
		jcsmpSession.provision(queue, new EndpointProperties(), JCSMPSession.WAIT_FOR_CONFIRM);
		flowReceiverContainer = Mockito.spy(new FlowReceiverContainer(jcsmpSession, queue.getName(),
				new EndpointProperties()));
		flowReceiverContainer.bind();
		producer = jcsmpSession.getMessageProducer(new JCSMPSessionProducerManager.CloudStreamEventHandler());
	}

	@After
	public void tearDown() throws Exception {
		if (flowReceiverContainer != null) {
			Optional.ofNullable(flowReceiverContainer.getFlowReceiverReference())
					.map(FlowReceiverContainer.FlowReceiverReference::get)
					.ifPresent(Consumer::close);
		}

		if (jcsmpSession != null && !jcsmpSession.isClosed()) {
			jcsmpSession.deprovision(queue, JCSMPSession.WAIT_FOR_CONFIRM);
		}
	}

	@Test
	public void testRun() throws Exception {
		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);
		MessageContainer messageContainer = flowReceiverContainer.receive(5000);
		RetryableAckRebindTask task = new RetryableAckRebindTask(flowReceiverContainer, messageContainer, taskService);
		UUID flowId = Objects.requireNonNull(flowReceiverContainer.getFlowReceiverReference()).getId();

		assertTrue(task.run(1));
		Mockito.verify(flowReceiverContainer).acknowledgeRebind(messageContainer, true);
		Mockito.verify(taskService, Mockito.never()).submit(Mockito.any());
		assertTrue(messageContainer.isAcknowledged());
		assertTrue(messageContainer.isStale());
		assertTrue(flowReceiverContainer.isBound());
		assertNotEquals(flowId, Objects.requireNonNull(flowReceiverContainer.getFlowReceiverReference()).getId());
	}

	@Test
	public void testReturnNull() throws Exception {
		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);
		MessageContainer messageContainer = flowReceiverContainer.receive(5000);
		RetryableAckRebindTask task = new RetryableAckRebindTask(flowReceiverContainer, messageContainer, taskService);

		Mockito.doReturn(null).when(flowReceiverContainer)
				.acknowledgeRebind(messageContainer, true);
		assertFalse(task.run(1));
		Mockito.verify(flowReceiverContainer).acknowledgeRebind(messageContainer, true);
		Mockito.verify(taskService, Mockito.never()).submit(Mockito.any());
		assertFalse(messageContainer.isAcknowledged());
		assertFalse(messageContainer.isStale());
	}

	@Test
	public void testReturnNullAndAcknowledged() throws Exception {
		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);
		MessageContainer messageContainer = Mockito.spy(flowReceiverContainer.receive(5000));
		RetryableAckRebindTask task = new RetryableAckRebindTask(flowReceiverContainer, messageContainer, taskService);

		Mockito.when(messageContainer.isAcknowledged()).thenReturn(true);
		Mockito.doReturn(null).when(flowReceiverContainer)
				.acknowledgeRebind(messageContainer, true);

		assertTrue(task.run(1));
		Mockito.verify(flowReceiverContainer, Mockito.atLeastOnce()).acknowledgeRebind(messageContainer, true);
		Mockito.verify(taskService).submit(new RetryableRebindTask(flowReceiverContainer,
				messageContainer.getFlowReceiverReferenceId(), taskService));
		assertFalse(taskService.hasTask(task));
	}

	@Test
	public void testFail() throws Exception {
		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);
		MessageContainer messageContainer = flowReceiverContainer.receive(5000);
		RetryableAckRebindTask task = new RetryableAckRebindTask(flowReceiverContainer, messageContainer, taskService);

		Mockito.doThrow(new JCSMPException("test")).when(flowReceiverContainer)
				.acknowledgeRebind(messageContainer, true);
		assertFalse(task.run(1));
		Mockito.verify(flowReceiverContainer).acknowledgeRebind(messageContainer, true);
		Mockito.verify(taskService, Mockito.never()).submit(Mockito.any());
		assertFalse(messageContainer.isAcknowledged());
		assertFalse(messageContainer.isStale());
	}

	@Test
	public void testStale() throws Exception {
		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);
		MessageContainer messageContainer = Mockito.spy(flowReceiverContainer.receive(5000));
		RetryableAckRebindTask task = new RetryableAckRebindTask(flowReceiverContainer, messageContainer, taskService);
		UUID flowId = Objects.requireNonNull(flowReceiverContainer.getFlowReceiverReference()).getId();

		Mockito.when(messageContainer.isStale()).thenReturn(true);
		assertTrue(task.run(1));
		Mockito.verify(taskService, Mockito.never()).submit(Mockito.any());
		assertEquals(flowId, Objects.requireNonNull(flowReceiverContainer.getFlowReceiverReference()).getId());
		Mockito.verify(messageContainer).isStale();
	}

	@Test
	public void testFailAndStale() throws Exception {
		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);
		MessageContainer messageContainer = flowReceiverContainer.receive(5000);
		RetryableAckRebindTask task = new RetryableAckRebindTask(flowReceiverContainer, messageContainer, taskService);

		logger.info(String.format("Shutting down egress for queue %s", queue.getName()));
		sempV2Api.config().updateMsgVpnQueue(vpnName, queue.getName(), new ConfigMsgVpnQueue().egressEnabled(false),
				null);
		retryAssert(() -> assertFalse(sempV2Api.monitor()
				.getMsgVpnQueue(vpnName, queue.getName(), null)
				.getData()
				.isEgressEnabled()));

		assertTrue(task.run(1));
		assertFalse(messageContainer.isAcknowledged());
		assertTrue(messageContainer.isStale());
		assertFalse(taskService.hasTask(task));
		assertTrue(taskService.hasTask(new RetryableBindTask(flowReceiverContainer)));
		assertFalse(flowReceiverContainer.isBound());

		logger.info(String.format("Starting egress for queue %s", queue.getName()));
		sempV2Api.config().updateMsgVpnQueue(vpnName, queue.getName(), new ConfigMsgVpnQueue().egressEnabled(true),
				null);

		retryAssert(() -> {
			assertTrue(sempV2Api.monitor().getMsgVpnQueue(vpnName, queue.getName(), null).getData()
					.isEgressEnabled());
			assertFalse(taskService.hasTask(new RetryableBindTask(flowReceiverContainer)));
			assertTrue(flowReceiverContainer.isBound());
		}, 1, TimeUnit.MINUTES);

//		assertNotNull(flowReceiverContainer.receive(5000)); //TODO Re-enable once SOL-45982 is fixed
	}

	@Test
	public void testFailWhenUnbound() throws Exception {
		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);
		MessageContainer messageContainer = Mockito.spy(flowReceiverContainer.receive(5000));
		RetryableAckRebindTask task = new RetryableAckRebindTask(flowReceiverContainer, messageContainer, taskService);

		flowReceiverContainer.unbind();
		Mockito.when(messageContainer.isStale()).thenReturn(false).thenCallRealMethod();

		assertTrue(task.run(1));
		Mockito.verify(taskService).submit(new RetryableBindTask(flowReceiverContainer));
		assertFalse(messageContainer.isAcknowledged());
		assertTrue(messageContainer.isStale());
		assertFalse(taskService.hasTask(task));

		retryAssert(() -> {
			assertFalse(taskService.hasTask(new RetryableBindTask(flowReceiverContainer)));
			assertTrue(flowReceiverContainer.isBound());
		}, 1, TimeUnit.MINUTES);

		assertNotNull(flowReceiverContainer.receive(5000));
	}

	@Test
	public void testEquals() throws Exception {
		producer.send(JCSMPFactory.onlyInstance().createMessage(TextMessage.class), queue);
		MessageContainer messageContainer = flowReceiverContainer.receive(5000);
		RetryableAckRebindTask task1 = new RetryableAckRebindTask(flowReceiverContainer, messageContainer, taskService);
		RetryableAckRebindTask task2 = new RetryableAckRebindTask(flowReceiverContainer, messageContainer, taskService);
		assertEquals(task1, task2);
		assertEquals(task1.hashCode(), task1.hashCode());
		assertThat(new HashSet<>(Collections.singleton(task1)), contains(task2));
	}
}
